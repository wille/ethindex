# ethindex

[![CI](https://github.com/wille/ethindex/actions/workflows/commit.yml/badge.svg)](https://github.com/wille/ethindex/actions/workflows/commit.yml)
[![Go Version](https://img.shields.io/github/go-mod/go-version/wille/ethindex)](go.mod)
[![License: MIT](https://img.shields.io/github/license/wille/ethindex)](LICENSE)

A minimalist payment tracker for EVM chains.

ethindex watches a set of addresses across one or more EVM chains and
follows every matching transfer - native coin and ERC20 tokens, in both
directions - from the moment it appears in the mempool, through mining,
to a configurable confirmation depth or the chain's finality tag. It
handles the messy parts that decide whether a payment actually
happened: chain reorgs, reverted transactions, fee-bump/cancel
replacements, and mempool evictions. Every transition is logged, and
each transaction's latest state is persisted to a local SQLite database
that your service consumes.

It is not a block explorer and not a general-purpose indexing
framework: it does not index contract state, serve a query API, or
follow anything except value transfers touching your addresses. It is
an infrastructure tool for your own payment service - run it next to
your nodes, not exposed to the internet.

ethindex is to EVM chains roughly what
[NBXplorer](https://github.com/dgarage/NBXplorer) is to Bitcoin: a
small, self-hosted tracker your payment processing builds on.

## Typical usage

1. Put your deposit addresses and per-chain node endpoints in
   `config.yaml`.
2. Run the binary. Each chain gets an independent indexer that
   validates the node (chain ID, finality-tag support) at startup,
   loads any transactions already waiting in the mempool, and starts
   watching at the chain tip (pass `-resume` to also backfill every
   block missed since the last run).
3. Consume the results:
   - **Logs** (stderr) - one line per lifecycle transition, e.g.
     `transaction confirmed indexer=ethereum address=0x480c… tx=0x7eb1… direction=in value=26.95 asset=USDC block=25602234 after=12s`
   - **SQLite** - one row per matched transaction with its latest
     status and transfer legs, plus full terminal history (see
     [How to query](#how-to-query)).
4. Restart whenever: in-flight lifecycles are persisted and restored -
   a pending payment that mined while the process was down is resolved
   against its receipt on the first head. With `-resume`, missed blocks
   are also backfilled newest-first.

## General features

- Tracks native coin and ERC20 transfers, incoming, outgoing and
  self-transfers, for any number of addresses
- Any number of EVM chains concurrently, one node connection each
- Mempool detection: live pending-transaction subscriptions plus a
  one-shot `txpool_content` snapshot at startup, where the node
  supports them
- Confirmation by block depth or by the chain's `finalized`/`safe`
  consensus tags
- Reorg detection with automatic rescans; replacement (speed-up/cancel)
  detection via sender-nonce tracking; revert and eviction detection
- Persistent state and exact resume across restarts (SQLite, pure-Go
  driver - no CGO, single static binary)
- Fast catch-up after downtime (opt-in with `-resume`): newest blocks
  first, pipelined fetches, range log queries (~40-70 blocks/s against
  a decent node), with live events never delayed by a running backfill
- Live event stream over Server-Sent Events with cursor-based catch-up
  on reconnect (see [Events API](#events-api))

## Prerequisites

- Go 1.26.4+ to build (see `go.mod`)
- Per chain, a JSON-RPC node endpoint reachable over **WebSocket**
  (subscriptions) and **HTTP** (queries; derived from the ws URL by
  scheme swap unless `http_rpc_url` is set). The endpoint must accept
  **JSON-RPC batch requests** (all mainstream nodes and providers do) -
  ethindex batches aggressively and treats a failing batch as a node
  failure, retrying with backoff rather than degrading to per-call
  requests

Optional node capabilities, each degrading gracefully when absent:

| Node method | Used for | Without it |
|---|---|---|
| `newPendingTransactions` subscription (full or hash form) | mempool-time `pending` events | block-only mode; transfers detected when mined |
| `txpool_status` + `txpool_content` | detecting payments already waiting at startup | skipped |
| `eth_getBlockReceipts` | one receipt call per block | per-transaction receipts |
| `finalized`/`safe` block tags | tag-based confirmations | use depth-based `confirmations: N` (tag configs fail fast at startup) |

## How to build and run?

```sh
go build ./cmd/ethindex
cp config.example.yaml config.yaml   # edit addresses and indexers
./ethindex -config config.yaml
```

Flags:
- `-config path` - config file (default `./config.yaml`)
- `-log-level debug|info|warn|error` - log verbosity (default `info`)
- `-json` - JSON logs instead of colorized text; transaction logs then
  carry the full event object under `event` (same schema as `-print`
  and the events API) and durations serialize as float seconds
- `-print` - additionally print every lifecycle event as one JSON object
  per line on stdout (logs stay on stderr), for piping into `jq` or a
  downstream consumer
- `-resume` - catch up every block missed while the process was down.
  Without it each indexer starts at the current chain tip (instant);
  in-flight transactions from the previous run are restored and tracked
  to their outcome either way

### Docker

Images are published to `ghcr.io/wille/ethindex` for linux amd64/arm64:

```sh
docker run -v $PWD/config.yaml:/etc/ethindex/config.yaml \
  -v ethindex-data:/data \
  ghcr.io/wille/ethindex
```

Point `database:` somewhere on the mounted volume (e.g.
`/data/ethindex.db`) so state survives container restarts. Extra flags
go after the image name (the config path defaults to
`/etc/ethindex/config.yaml`).

## How to configure?

Addresses are global - every indexer watches the same set on its own
chain. Each entry in `indexers` runs concurrently against its own node.

`${VAR}` references anywhere in the file expand to environment
variables at load time, so provider API keys stay out of the config
(`rpc_url: wss://eth-mainnet.example.com/v2/${RPC_API_KEY}`). A
referenced variable that is unset fails startup with an error naming
it; a bare `$` without braces stays literal.

```yaml
# SQLite database persisting matched transactions and indexer progress
# (default: ethindex.db)
database: ethindex.db

# Prometheus /metrics + /healthz listen address; empty disables
metrics: ":9090"

# Consumer events API (SSE stream + latest); empty disables
api: ":8080"

addresses:
  - "0xYourDepositAddress"
  # or named - the name is printed on matched logs and events:
  - {name: hot-wallet, address: "0xYourHotWalletAddress"}

hd_wallets:                # optional: derive addresses from an xpub
  - name: deposits         # wallet label on logs/events (default: the xpub)
    xpub: "xpub6C..."      # account-level extended PUBLIC key
    path: "0/*"            # relative to the xpub, * = index (default 0/*)
    start: 0               # first index (default 0)
    count: 1000            # addresses to derive and track

indexers:
  - name: ethereum           # required, unique; tags rows and logs
    chain_id: 1              # required; cross-checked against the node
    rpc_url: wss://...       # required; must be ws:// or wss://
    # http_rpc_url: https:// # queries endpoint; derived from rpc_url when omitted
    confirmations: 12        # depth until "confirmed" (default 12), or
                             # "finalized"/"safe" to use the node's
                             # consensus tags instead of a depth guess
    pending_timeout: 30m     # pending tx re-checked / "dropped" after this
    concurrency: 8           # parallel RPC requests (catch-up, lookups)
    batch_blocks: 10         # blocks per JSON-RPC batch during catch-up
    max_catchup_age: 24h     # skip blocks older than this when catching
                             # up (default: no limit)
    native_symbol: ETH       # asset name for native transfers (BNB, POL, ...)
    tokens:                  # ERC20s on this chain; omit = native only
      - address: "0xdAC17F958D2ee523a2206206994597C13D831ec7"
        symbol: USDT
        decimals: 6
  - name: base
    chain_id: 8453
    rpc_url: wss://base-rpc.publicnode.com
    confirmations: safe
    tokens:
      - address: "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"
        symbol: USDC
        decimals: 6
```

There are no built-in token lists - every entry declares its tokens
explicitly (contract addresses and even decimals differ per chain:
e.g. USDT is 6 decimals on mainnet but 18 on BSC).

### HD wallets

`hd_wallets` derives watched addresses from a BIP32 extended public
key: export the xpub at the account level (e.g. `m/44'/60'/0'` - what
MetaMask-compatible wallets use for Ethereum) and the configured
`path`/`start`/`count` expand it into concrete addresses at load time,
which join the global watched set. Matched transactions log
`wallet: <name>` (the xpub when the wallet has no `name`); the label
is resolved from config at emission and read time, never stored, so
renaming a wallet relabels replayed history too. Things to know:

- **Never put an xprv in the config.** Private extended keys are
  rejected at startup. An xpub can only derive non-hardened children,
  so hardened path segments (`0'`) are rejected too - do the hardened
  part in your wallet before exporting.
- **The window is fixed.** ethindex tracks exactly the configured
  index range; there is no automatic gap-limit extension. Size `count`
  ahead of your address handout, and when you grow it, note that blocks
  processed before the restart are not rescanned for the new addresses -
  grow the window before handing out the new indexes, not after.
- **Large windows are fine.** Above 1000 watched addresses the indexer
  switches to client-side filtering: logs come from
  `eth_getBlockReceipts` and matching is a local map lookup, so query
  cost is flat in the address count (see "Detection and the two filter
  modes" under How does it work?). Deriving is parallelized across
  cores; a million addresses takes a few seconds at startup and
  ~100 MB of memory.

At startup each indexer verifies the node's `eth_chainId` against its
configured `chain_id`, and - when `confirmations` uses a tag - that the
node can actually resolve it (some backends serve `finalized` but not
`safe`). Either failing is treated as a configuration error and stops
the whole process with a non-zero exit, rather than silently never
confirming anything.

### Choosing `confirmations`

Depth is a heuristic; the consensus tags are guarantees. `finalized`
is protocol-irreversible and nearly instant on chains with fast
finality (seconds on BSC and Polygon) but trails ~13-19 minutes on
Ethereum mainnet; `safe` is the justified checkpoint (~6 min on
mainnet, under a minute on Base). On L2s, depth is nearly meaningless -
blocks in an unposted batch reorg together - so prefer tags there. A
sensible pattern for payment processing is two entries per chain
watching the same addresses: a small depth for low-value fast
acceptance, and a tag-based entry gating high-value orders.

## Transaction lifecycle

Each tracked transaction moves through these states, logged and
emitted as an event on every transition. All of them except `reorged`
also appear as values of the database's `status` column - a reorg is
an event only: the row returns to `pending` until the transaction is
re-included or resolved.

| event / status | meaning |
|---|---|
| `pending` | Seen in the mempool. For ERC20, decoded from `transfer`/`transferFrom` calldata (best effort). |
| `mined` | Included in a block and successful: native transfers are receipt-checked, ERC20 transfers come from `Transfer` logs. `confirmations` is the depth at discovery - 1 at the tip, more when found during catch-up. Skipped entirely when the discovery depth already satisfies the threshold: such transactions go straight to `confirmed`. |
| `confirmed` | Reached the configured confirmation depth - or, with `confirmations: finalized`/`safe`, the node reports the containing block at or below that consensus tag. Inclusion is re-verified against a fresh receipt just before emitting. Tracking ends. |
| `reorged` | The containing block left the canonical chain. The tx reverts to pending; a later `mined` follows if it is re-included. |
| `dropped` | Pending past `pending_timeout` and no longer known to the node (evicted). Tracking ends. |
| `failed` | Mined but reverted (receipt status 0) - no value moved. Tracking ends. |
| `replaced` | A different transaction from the same sender mined with the same nonce (speed-up or cancel), so this pending transaction can never mine. `replaced_by` carries the winning transaction's hash. Tracking ends. |

The table is normalized to ONE ROW PER TRANSFER LEG (`log_index` is
the leg identity; `-1` marks the native-coin leg), each row carrying
its transaction's full lifecycle: `status`, block hash/number,
`first_seen` (first observation), `first_seen_block` and `mined_at`
(the first block it was seen included in and when - 0/empty while only
known from the mempool, immutable once set even across reorgs),
`updated_at` (last transition - the terminal time on terminal rows)
and `replaced_by`. Addresses, asset, decimals and both raw and
decimal-adjusted values are plain indexed columns, so consumers query
by address directly. Per leg: `direction` is `"in"` (recipient is
watched), `"out"` (sender is watched; for `transferFrom` that is the
token owner, not the transaction signer) or `"self"` (both watched);
`value_raw` is in base units; `asset` is the chain's `native_symbol` or
the configured token symbol; several matching ERC20 transfers in one
transaction store one leg each, distinguished by `log_index`. Addresses
are EIP-55 checksummed. `first_seen` is when the indexer first observed
the transaction - mempool time if seen pending, block-scan time
otherwise - and never changes afterwards, so `updated_at - first_seen`
on a confirmed row measures detection-to-finality latency.

## How to query?

The database is the consumption interface - any SQLite client works,
concurrently with the running process (WAL mode):

```sh
# All payments to one deposit address, with lifecycle
sqlite3 ethindex.db "SELECT tx_hash, status, asset, value, first_seen, mined_at
                     FROM transactions
                     WHERE to_address = '0xYourDepositAddress'"

# Everything still in flight
sqlite3 ethindex.db "SELECT indexer, tx_hash, status FROM transactions
                     WHERE status IN ('pending','mined')"

# Tail changes since a timestamp (indexed on updated_at)
sqlite3 ethindex.db "SELECT tx_hash, log_index, status, updated_at
                     FROM transactions
                     WHERE indexer = 'ethereum' AND updated_at > '2026-07-25T00:00:00Z'
                     ORDER BY updated_at"

# Where is each chain?
sqlite3 ethindex.db "SELECT indexer, last_processed FROM indexer_state"
```

Terminal rows (`confirmed`, `dropped`, `failed`, `replaced`) are kept
as history. Consumers should treat rows idempotently per
`(indexer, tx_hash)`: after a crash mid-catch-up the scan re-runs and
may touch already-terminal rows again.

## Events API

With `api: ":8080"` set, lifecycle events are pushed live to connected
listeners over Server-Sent Events, with database-backed catch-up on
reconnect - inspired by NBXplorer's event stream, but without a
dedicated event log table: catch-up is synthesized from the
`transactions` table itself, so nothing grows unbounded.

| Endpoint | Description |
| --- | --- |
| `GET /v1/events` | SSE stream of lifecycle events as they happen |
| `GET /v1/events/latest?limit=10` | The most recent events as a JSON array (peek, no stream) |
| `GET /healthz` | Liveness |

Both event endpoints accept `?indexer=name` to scope to one chain.

```sh
curl -N 'localhost:8080/v1/events'
```

```
id: 2026-07-26T09:12:44.371205118Z
event: confirmed
data: {"type":"confirmed","indexer":"ethereum","chain_id":1,"direction":"in","tx_hash":"0x5f0c...","to":"0xYourDepositAddress","asset":"USDC","value":"250","block_number":23011460,"confirmations":12,...}
```

Every frame's `id` is the event's timestamp. To resume after a
disconnect, pass the last id you processed and every transfer updated
since is replayed before the stream goes live:

```sh
curl -N 'localhost:8080/v1/events?lastEventId=2026-07-26T09:12:44.371205118Z'
```

Browser/library `EventSource` clients do this automatically - the
`Last-Event-ID` header they send on reconnect is honored the same way.

Delivery semantics:

- **At-least-once.** The catch-up boundary can duplicate events, never
  drop them. Process events idempotently keyed on
  `(tx_hash, log_index, type)`.
- **Catch-up replays current state.** A transaction that went
  `pending` -> `mined` -> `confirmed` while you were disconnected
  replays as a single `confirmed` event per transfer leg, not three -
  the intermediate transitions are collapsed.
- **Slow consumers are disconnected.** A listener that stops reading
  is dropped once its buffer fills (indexing is never blocked by a
  stuck consumer). Reconnect with your cursor and catch up.

## How does it work?

Each indexer runs one dispatch goroutine per chain. The WebSocket
connection carries only subscriptions (`newHeads`, pending
transactions); all request/response calls - blocks, logs, receipts,
mempool - go over HTTP, where a connection pool lets concurrent fetches
actually run in parallel. Blocks are decoded from raw JSON, so unusual
transaction types (e.g. OP-stack deposit transactions on Base) don't
break scanning, and mined transactions use the node-reported sender -
no signature recovery.

Related calls travel as JSON-RPC **batch requests**, everywhere: a
catch-up worker fetches `batch_blocks` block bodies with their receipts
in one request, confirmation re-checks, stale-pending rechecks, mempool
lookups, reorg ancestor walks and log-filter pairs each go out as a
single batch. Batch support is assumed - a failing batch is treated as
a node failure and retried with backoff, never silently degraded to
per-call requests (which would multiply load on an already struggling
provider). The receipts of a number-addressed batch are verified
against the body's hash, so reorg safety is unchanged. Every request
carries a `User-Agent: ethindex/<version>` header.

- **Detection and the two filter modes**: per block, the body is
  scanned for native transfers (in-memory address map, any size) and
  ERC20 `Transfer`s are found via logs. How logs are sourced depends on
  the watched-set size, chosen once at startup and shown in the
  `connected` log line as `log_filter=server|client`:
  - **server** (up to 1000 addresses): a server-side-filtered
    `eth_getLogs` pair pins the watched addresses into the incoming and
    outgoing topic positions - tiny responses, works on any node.
    Revert statuses come from one `eth_getBlockReceipts` call, made
    only for blocks containing matches.
  - **client** (above 1000 - HD windows can reach millions): address
    topic filters would be rejected, so one `eth_getBlockReceipts`
    call per block supplies every log AND the revert statuses - the
    cheapest call a node serves, no log-index scan - and all matching
    happens locally. At the tip it is fetched in parallel with the
    block body: two flat RPC calls per block, regardless of address
    count. Address-topic filters are never used in this mode, in any
    fallback.
- **Degradation rules**: a *transient* `eth_getBlockReceipts` failure
  (lagging load-balanced node, timeout) degrades only that block to
  the log-query fallback; only a "method does not exist"-class error
  disables block receipts, per indexer, with one clear warning. A
  capability gap can therefore never lock the process into a slow
  path because of bad weather.
- **Reorg handling**: recent block headers are kept in a ring buffer;
  on a fork the common ancestor is located, affected transactions are
  demoted with `reorged`, and the replacement blocks are rescanned.
  Reorgs deeper than the buffer are caught by the receipt
  re-verification that precedes every `confirmed`.
- **Replacement detection**: every mined transaction consumes a
  (sender, nonce) slot; any tracked pending transaction holding the
  same slot under a different hash can never mine and is immediately
  `replaced`. Competing transactions in the mempool are all tracked
  until a block decides.
- **Catch-up** (with `-resume`, or after a mid-session gap): the
  freshest block is processed first and the backlog is walked
  backwards, so current activity surfaces in seconds. Fetching is
  pipelined across `concurrency` workers, each holding one JSON-RPC
  batch of `batch_blocks` blocks (bodies plus receipts in client-filter
  mode) in flight, while processing consumes strictly in scan order.
  In-flight data therefore scales with `concurrency x batch_blocks` -
  size the pair to the chain: an Ethereum block with receipts is ~1MB
  of JSON, so small batches (e.g. `batch_blocks: 4`) transfer and
  decode better there, while thin L2 blocks (Arbitrum, Base) tolerate
  large ones. In server-filter mode transfer logs are instead prefetched
  in 500-block range queries that shrink adaptively if the provider
  rejects a range, bottoming out in per-block queries; the chunked path
  also serves as the fallback for any block whose receipts fetch fails.
  New heads and pending notifications are processed between backfill
  blocks, so live events are never delayed. A transfer discovered
  already past its threshold goes straight to `confirmed`. Progress is
  persisted only when the range completes - a crash redoes the
  catch-up rather than ever skipping blocks.
- **Pending detection is best-effort**: mempool streams are lossy and
  provider-dependent; the block scan is the source of truth. Overflow
  drops notifications (counted in the logs) rather than blocking.
- **Reconnects**: exponential backoff (1s-30s), with missed blocks
  caught up through the same gap path. Chain-ID mismatches and
  unresolvable finality tags are never retried - they kill the process.

## Important notes

- A `failed` transaction stops being tracked immediately, even if its
  block is later reorged out. A `replaced` one likewise; in the rare
  case the replacing block reorgs out and the original mines after
  all, the block scan picks it up again as a fresh transaction.
- Zero-value transfers are never indexed. They move nothing and are,
  in practice, address-poisoning spam: attackers plant lookalike
  recipient addresses in a wallet's transfer history via zero-amount
  `transferFrom` calls, hoping a later payout copy-pastes the fake
  recipient. ethindex skips them with a debug log so the poison
  addresses cannot spread into events or the database.
- Fee-on-transfer or rebasing tokens report the `Transfer` log amount -
  what the token contract itself says the recipient was credited.
- The mempool snapshot and pending subscriptions never release value:
  never treat `pending` as payment received.

## What about alternatives?

1. **Hosted notification APIs** (Alchemy Webhooks, QuickNode Streams,
   Moralis Streams, Blocknative): no infrastructure to run, but a third
   party sits in your payment path, you pay per event, and mempool +
   finality semantics are whatever the vendor defines.
2. **Indexing frameworks** (The Graph, Ponder, Envio, rindexer,
   Shovel): built to index contract state into queryable form. Powerful
   for dApp backends, but contract-schema-first, usually no mempool, no
   payment lifecycle (reorg/replacement/finality states are your
   problem).
3. **TrueBlocks / Ethereum-ETL**: local chain indexes and data
   extraction - appearance-of-address, not lifecycle-of-payment.
4. **Block explorers** (Etherscan APIs): polling, rate limits, a third
   party, and still no lifecycle semantics.
5. **NBXplorer / BTCPay Server**: the closest philosophical relatives -
   self-hosted, minimalist, payment-first - but Bitcoin/UTXO only.
   ethindex fills the same role for EVM chains.

## Metrics

With `metrics: ":9090"` set, Prometheus metrics are served on
`/metrics` (plus a trivial `/healthz`). Everything is labeled by
`indexer`:

| Metric | Type | Meaning |
|---|---|---|
| `ethindex_transactions_total{status,direction}` | counter | lifecycle transitions - the business metric |
| `ethindex_confirmation_latency_seconds` | histogram | first observation to confirmed |
| `ethindex_last_processed_block` / `ethindex_chain_head_block` | gauge | alert when the difference grows: the indexer is falling behind |
| `ethindex_finalized_block` | gauge | node-reported finality position (tag mode) |
| `ethindex_tracked_transactions` | gauge | in-flight transactions |
| `ethindex_blocks_processed_total`, `ethindex_block_process_duration_seconds` | counter, histogram | throughput and per-block cost |
| `ethindex_catchup_remaining_blocks` | gauge | backlog of a running catch-up |
| `ethindex_reorgs_total`, `ethindex_reorg_depth` | counter, histogram | reorg frequency and severity |
| `ethindex_session_reconnects_total` | counter | node connection churn |
| `ethindex_pending_notifications_dropped_total` | counter | mempool stream overload |

The four to alert on: head-minus-processed lag, no
`blocks_processed_total` increase for minutes, `reorg_depth` nearing
your confirmation depth, and `session_reconnects_total` spiking.

## How to run the tests?

```sh
go test ./...        # unit + fake-chain end-to-end tests, no network
go test -race ./...
go vet ./...
```

The end-to-end tests drive a scripted fake chain through mining,
reorgs, replacements, catch-up, provider degradations and
restart-resume without touching a real node.

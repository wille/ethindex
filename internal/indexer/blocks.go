package indexer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"

	"github.com/wille/ethindex/internal/event"
	"github.com/wille/ethindex/internal/metrics"
	"github.com/wille/ethindex/internal/storage"
)

// A lagging load-balanced node should catch up within one mainnet slot
// (12s), so the retry window is sized to survive that.
const fetchRetries = 7

// fetchRetryDelay is a variable so tests can shrink it.
var fetchRetryDelay = 2 * time.Second

// withRetry runs fn a few times before giving up. Load-balanced RPC
// providers routinely route a request to a node lagging behind the one
// that announced the head ("block range extends beyond current head",
// not-found on fresh blocks), so transient per-block failures deserve a
// short retry before the session is torn down.
func withRetry[T any](ctx context.Context, fn func() (T, error)) (T, error) {
	var (
		out T
		err error
	)
	for attempt := range fetchRetries {
		out, err = fn()
		if err == nil {
			return out, nil
		}
		if attempt < fetchRetries-1 {
			slog.Debug("retrying node query", "attempt", attempt+1, "err", err)
			select {
			case <-ctx.Done():
				return out, ctx.Err()
			case <-time.After(fetchRetryDelay):
			}
		}
	}
	return out, err
}

// minedTx groups the transfer legs found for one transaction in a block.
type minedTx struct {
	hash      common.Hash
	transfers []Match
	failed    bool
}

// blockData bundles a block with whatever was prefetched alongside it:
// range-prefetched transfer logs (server-filter catch-up) or the
// block's receipts (client-filter mode).
type blockData struct {
	block        *Block
	logs         []types.Log
	haveLogs     bool
	receipts     []BlockReceipt
	haveReceipts bool
}

// receiptStatuses lazily serves one block's receipt data: revert
// statuses and, when available, every log of the block. The first
// lookup fetches the whole block's receipts in a single
// eth_getBlockReceipts call; blocks with no lookups cost nothing, and
// providers without the method degrade to per-transaction receipts
// (flagged once per process, not per block).
type receiptStatuses struct {
	ix        *Indexer
	blockHash common.Hash
	byTx      map[common.Hash]bool // tx hash -> reverted
	logs      []types.Log          // all logs of the block
	fetched   bool                 // block receipts succeeded
	tried     bool
}

// preload seeds the receipt data from receipts fetched elsewhere (the
// catch-up prefetch workers), skipping the lazy fetch entirely.
func (r *receiptStatuses) preload(receipts []BlockReceipt) {
	r.tried = true
	r.absorb(receipts)
}

func (r *receiptStatuses) absorb(receipts []BlockReceipt) {
	r.fetched = true
	r.byTx = make(map[common.Hash]bool, len(receipts))
	for _, rc := range receipts {
		r.byTx[rc.TxHash] = rc.Status == 0
		r.logs = append(r.logs, rc.Logs...)
	}
}

// methodUnsupported reports whether an RPC error means the node lacks
// the method entirely, as opposed to a transient failure (lagging
// load-balanced node, timeout). Only the former justifies permanently
// disabling a capability - flipping it on transients would lock the
// process into its slowest fallback path.
func methodUnsupported(err error) bool {
	if err == nil {
		return false
	}
	s := strings.ToLower(err.Error())
	for _, marker := range []string{
		"does not exist", "not supported", "unsupported",
		"not available", "method not found", "not implemented",
	} {
		if strings.Contains(s, marker) {
			return true
		}
	}
	return false
}

// noteBlockReceiptsError classifies a BlockReceipts failure: permanent
// lack of the method flips the process-wide flag; anything else stays
// a per-block degradation.
func (ix *Indexer) noteBlockReceiptsError(err error) {
	if methodUnsupported(err) {
		ix.blockReceiptsUnsupported.Store(true)
		ix.log.Warn("eth_getBlockReceipts unsupported by node, using per-transaction receipts and log queries", "err", err)
		return
	}
	ix.log.Debug("block receipts fetch failed, degrading for this block", "err", err)
}

// ensure fetches the block's receipts once, if the node supports it.
func (r *receiptStatuses) ensure(ctx context.Context) {
	if r.tried {
		return
	}
	r.tried = true
	if r.ix.blockReceiptsUnsupported.Load() {
		return
	}
	receipts, err := r.ix.client.BlockReceipts(ctx, r.blockHash)
	if err != nil {
		r.ix.noteBlockReceiptsError(err)
		return
	}
	r.absorb(receipts)
}

// preloadStatuses resolves the revert statuses of several transactions
// ahead of the per-tx lookups: when the block's receipts are not
// available (node without eth_getBlockReceipts, or a transient
// failure), one TransactionReceipts batch replaces N individual calls.
// Best effort - misses stay on the per-tx retry path.
func (r *receiptStatuses) preloadStatuses(ctx context.Context, hashes []common.Hash) {
	r.ensure(ctx)
	if r.fetched || len(hashes) < 2 {
		return
	}
	receipts, errs, err := r.ix.client.TransactionReceipts(ctx, hashes)
	if err != nil {
		return
	}
	if r.byTx == nil {
		r.byTx = make(map[common.Hash]bool, len(hashes))
	}
	for i, h := range hashes {
		if errs[i] == nil {
			r.byTx[h] = receipts[i].Status == types.ReceiptStatusFailed
		}
	}
}

func (r *receiptStatuses) failed(ctx context.Context, txHash common.Hash) (bool, error) {
	r.ensure(ctx)
	if reverted, ok := r.byTx[txHash]; ok {
		return reverted, nil
	}
	receipt, err := withRetry(ctx, func() (*types.Receipt, error) {
		return r.ix.client.TransactionReceipt(ctx, txHash)
	})
	if err != nil {
		return false, err
	}
	return receipt.Status == types.ReceiptStatusFailed, nil
}

// blockLogs returns every log of the block, when block receipts are
// available.
func (r *receiptStatuses) blockLogs(ctx context.Context) ([]types.Log, bool) {
	r.ensure(ctx)
	return r.logs, r.fetched
}

// serverFilterMaxAddresses is the largest watched set that is still
// pinned into eth_getLogs address topics. Above it (HD wallets can
// derive millions of addresses) the topic lists would be rejected by
// providers, so queries fetch ALL Transfer logs of the watched token
// contracts instead and MatchLog filters locally - the response size
// then scales with token traffic, not with the address count.
const serverFilterMaxAddresses = 1000

// transferTopicFilters are the eth_getLogs topic filters covering
// transfers touching watched addresses. With a small watched set: two
// server-side-filtered queries (eth_getLogs cannot OR across topic
// positions, so incoming and outgoing are separate). With a large one:
// a single unfiltered query per token contract set.
func (ix *Indexer) transferTopicFilters() [][][]common.Hash {
	if ix.matcher.WatchedCount() > serverFilterMaxAddresses {
		return [][][]common.Hash{
			{{transferTopic}},
		}
	}
	watched := ix.matcher.WatchedTopics()
	return [][][]common.Hash{
		{{transferTopic}, nil, watched},
		{{transferTopic}, watched},
	}
}

// fetchTopicFilterLogs runs every transfer topic filter over [lo, hi]
// in one JSON-RPC batch request and returns the per-filter results.
func (ix *Indexer) fetchTopicFilterLogs(ctx context.Context, lo, hi uint64, retry bool) ([][]types.Log, error) {
	filters := ix.transferTopicFilters()
	queries := make([]ethereum.FilterQuery, len(filters))
	for i, topics := range filters {
		queries[i] = ethereum.FilterQuery{
			FromBlock: new(big.Int).SetUint64(lo),
			ToBlock:   new(big.Int).SetUint64(hi),
			Addresses: ix.matcher.TokenAddresses(),
			Topics:    topics,
		}
	}

	// All filters ride one batch request (client mode has a single
	// unfiltered query, server mode the incoming/outgoing pair). Errors
	// propagate - range errors drive the caller's chunk halving.
	batchQuery := func() ([][]types.Log, error) {
		results, errs, err := ix.client.FilterLogsBatch(ctx, queries)
		if err != nil {
			return nil, err
		}
		for _, qerr := range errs {
			if qerr != nil {
				return nil, qerr
			}
		}
		return results, nil
	}
	if retry {
		return withRetry(ctx, batchQuery)
	}
	return batchQuery()
}

// fetchLogsRange fetches all watched-transfer logs in [lo, hi] grouped
// by block number - one query set covers the whole range, which is
// what makes server-filter catch-up fast. Single attempt, no retry:
// callers fall back to per-block queries, which have their own retry.
func (ix *Indexer) fetchLogsRange(ctx context.Context, lo, hi uint64) (map[uint64][]types.Log, error) {
	out := make(map[uint64][]types.Log)
	if len(ix.matcher.TokenAddresses()) == 0 {
		return out, nil
	}
	results, err := ix.fetchTopicFilterLogs(ctx, lo, hi, false)
	if err != nil {
		return nil, err
	}
	type logKey struct {
		block uint64
		index uint
	}
	seen := make(map[logKey]struct{})
	for _, logs := range results {
		for _, lg := range logs {
			k := logKey{lg.BlockNumber, lg.Index}
			if _, dup := seen[k]; dup {
				continue
			}
			seen[k] = struct{}{}
			out[lg.BlockNumber] = append(out[lg.BlockNumber], lg)
		}
	}
	return out, nil
}

// scanBlock finds all watched-address activity in one canonical block:
// native ETH transfers from the block body (receipt-checked) and ERC20
// transfers via Transfer logs (a log implies success). Candidate logs
// come from d (prefetched logs or receipts) or are queried per block.
func (ix *Indexer) scanBlock(ctx context.Context, d blockData, receipts *receiptStatuses) ([]minedTx, error) {
	block := d.block
	byTx := make(map[common.Hash]*minedTx)
	order := make([]common.Hash, 0, 4)
	get := func(h common.Hash) *minedTx {
		if m, ok := byTx[h]; ok {
			return m
		}
		m := &minedTx{hash: h}
		byTx[h] = m
		order = append(order, h)
		return m
	}

	blockHash := block.Hash

	// Native ETH transfers. Receipt statuses are consulted only for
	// matches (one block-receipts call at most; when the node lacks the
	// method, one receipt batch for all matched txs of the block).
	type matchedTx struct {
		tx      BlockTx
		matches []Match
	}
	var matched []matchedTx
	for _, tx := range block.Transactions {
		if matches := ix.matcher.MatchBlockTx(tx); len(matches) > 0 {
			matched = append(matched, matchedTx{tx: tx, matches: matches})
		}
	}
	if len(matched) > 0 {
		hashes := make([]common.Hash, len(matched))
		for i, m := range matched {
			hashes[i] = m.tx.Hash
		}
		receipts.preloadStatuses(ctx, hashes)
	}
	for _, mt := range matched {
		tx, matches := mt.tx, mt.matches
		reverted, err := receipts.failed(ctx, tx.Hash)
		if errors.Is(err, ethereum.NotFound) {
			// The block was likely reorged away while we were scanning
			// it; if the tx is re-included, scanning its new block will
			// pick it up.
			ix.log.Warn("receipt vanished during block scan, skipping tx", "tx", tx.Hash, "block", blockHash)
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("receipt %s: %w", tx.Hash, err)
		}
		m := get(tx.Hash)
		if reverted {
			m.failed = true
			m.transfers = matches // report the intended transfer as failed
			continue
		}
		// Keep only the native leg here; ERC20 legs come from logs below,
		// which are authoritative for actual token movement.
		for _, match := range matches {
			if match.Token == nil {
				m.transfers = append(m.transfers, match)
			}
		}
	}

	// ERC20 Transfer logs touching watched addresses, filtered
	// server-side by token contract and address topics. Queried by
	// number range (some providers mishandle single-block BlockHash
	// queries); each log's block hash is verified against the block
	// being scanned so a racing reorg cannot mix in logs from another
	// chain - mismatched logs are dropped and the reorg path rescans the
	// replacement block.
	tokenAddrs := ix.matcher.TokenAddresses()
	if len(tokenAddrs) > 0 {
		var (
			candidates []types.Log
			gotLogs    = d.haveLogs
		)
		if d.haveLogs {
			candidates = d.logs
		} else if ix.matcher.WatchedCount() > serverFilterMaxAddresses {
			// Huge watched sets can't be server-filtered, so a
			// per-block getLogs would return every token transfer
			// anyway - the block receipts (often already needed for
			// statuses) carry the same logs in one cheap call, with no
			// log-index scan on the node.
			candidates, gotLogs = receipts.blockLogs(ctx)
		}
		if !gotLogs {
			num := uint64(block.Number)
			results, err := ix.fetchTopicFilterLogs(ctx, num, num, true)
			if err != nil {
				return nil, fmt.Errorf("filter logs for block %s: %w", blockHash, err)
			}
			seenLogs := make(map[uint]struct{}) // self-transfers appear in both queries
			for _, logs := range results {
				for _, lg := range logs {
					if _, dup := seenLogs[lg.Index]; dup {
						continue
					}
					seenLogs[lg.Index] = struct{}{}
					candidates = append(candidates, lg)
				}
			}
		}
		for _, lg := range candidates {
			if lg.Removed || lg.BlockHash != blockHash {
				continue
			}
			match, ok := ix.matcher.MatchLog(lg)
			if !ok {
				continue
			}
			m := get(lg.TxHash)
			m.transfers = append(m.transfers, match)
		}
	}

	out := make([]minedTx, 0, len(order))
	for _, h := range order {
		out = append(out, *byTx[h])
	}
	return out, nil
}

// processBlock scans one block and feeds the results plus any tracked
// pending txs that appear in it into the tracker, emitting events.
func (ix *Indexer) processBlock(ctx context.Context, d blockData) error {
	start := time.Now()
	block := d.block
	receipts := &receiptStatuses{ix: ix, blockHash: block.Hash}
	if d.haveReceipts {
		receipts.preload(d.receipts)
	}
	mined, err := ix.scanBlock(ctx, d, receipts)
	if err != nil {
		return err
	}
	blockHash := block.Hash
	num := uint64(block.Number)
	matched := len(mined)

	conf := ix.confirmationsFor(num)
	fin := ix.tagFinalized()
	scanned := make(map[common.Hash]struct{}, len(mined))
	for _, m := range mined {
		scanned[m.hash] = struct{}{}
		ix.emitAll(ix.tracker.OnMined(m.hash, blockHash, num, m.transfers, m.failed, conf, fin))
	}

	// A tracked pending ERC20 tx can be mined without producing a
	// matching Transfer log (reverted, or transferred elsewhere).
	// Resolve any tracked-pending txs present in this block that the
	// scan didn't already cover.
	for _, tx := range block.Transactions {
		h := tx.Hash
		if _, done := scanned[h]; done {
			continue
		}
		if !ix.tracker.IsPending(h) {
			continue
		}
		reverted, err := receipts.failed(ctx, h)
		if err != nil {
			ix.log.Warn("receipt lookup for tracked tx failed", "tx", h, "err", err)
			continue
		}
		ix.emitAll(ix.tracker.OnMined(h, blockHash, num, nil, reverted, conf, fin))
		matched++
	}

	// Every mined tx consumes a (sender, nonce) slot: any tracked
	// pending tx holding the same slot under a different hash was
	// replaced and can never mine.
	for _, tx := range block.Transactions {
		ix.emitAll(ix.tracker.OnNonceUsed(tx.From, uint64(tx.Nonce), tx.Hash))
	}

	ix.chain.record(headRef{Number: num, Hash: blockHash, ParentHash: block.ParentHash})
	metrics.RecordBlockProcessed(ix.cfg.Name, time.Since(start))
	ix.log.Debug("block processed", "number", num, "hash", blockHash, "txs", len(block.Transactions), "matched", matched, "duration", time.Since(start).Round(time.Millisecond))
	return nil
}

// confirmationsFor is the confirmation depth of a block relative to the
// newest block seen so far (at least 1).
func (ix *Indexer) confirmationsFor(blockNum uint64) uint64 {
	if tip := ix.chain.tip; tip != nil && tip.Number > blockNum {
		return tip.Number - blockNum + 1
	}
	return 1
}

// saveState persists this indexer's resumable progress. Persistence
// failures are logged, never fatal to indexing. A run that never
// processed a block saves nothing - overwriting the previous run's
// position with 0 would erase what a later -resume could catch up
// from (e.g. a tip-start run stopped before its first block landed).
func (ix *Indexer) saveState() {
	if ix.store == nil || ix.lastProcessed == 0 {
		return
	}
	st := storage.IndexerState{LastProcessed: ix.lastProcessed}
	for _, h := range ix.chain.snapshot() {
		st.RecentHeaders = append(st.RecentHeaders, storage.HeaderRef{
			Number:     h.Number,
			Hash:       h.Hash.Hex(),
			ParentHash: h.ParentHash.Hex(),
		})
	}
	// Deliberately not the session context: progress writes should land
	// even while a shutdown or session teardown is in flight.
	if err := ix.store.SaveIndexerState(context.Background(), ix.cfg.Name, st); err != nil {
		ix.log.Error("persisting indexer state", "err", err)
	}
}

// logValue trims a decimal value string to at most 6 decimals for log
// readability; events and storage keep full precision.
func logValue(v string) string {
	i := strings.IndexByte(v, '.')
	if i < 0 {
		return v
	}
	if len(v) > i+7 {
		v = v[:i+7]
	}
	v = strings.TrimRight(v, "0")
	return strings.TrimSuffix(v, ".")
}

func (ix *Indexer) emitAll(evs []event.Event) {
	for _, ev := range evs {
		ev.Indexer = ix.cfg.Name
		ev.ChainID = ix.cfg.ChainID
		// The timestamp doubles as the event id on the API stream, so it
		// is stamped here rather than per sink.
		ev.Timestamp = time.Now().UTC().Format(event.TimeLayout)
		var attrs []any
		if ix.FullEventLogs {
			// Structured consumers get the whole event, same schema as
			// -print and the SSE stream.
			attrs = []any{"event", ev}
		} else {
			// The event carries the matcher-computed direction
			// (in/out/self); self transfers anchor on the recipient like
			// incoming ones.
			addr := ev.To
			if ev.Direction == event.DirectionOut {
				addr = ev.From
			}
			var block any = "mempool"
			if ev.BlockNumber != 0 {
				block = ev.BlockNumber
			}
			attrs = []any{"address", addr, "tx", ev.TxHash, "direction", string(ev.Direction),
				"value", logValue(ev.Value), "asset", ev.Asset, "block", block}
			if ev.ReplacedBy != "" {
				attrs = append(attrs, "replaced_by", ev.ReplacedBy)
			}
		}
		// Detection source: restored transactions keep their marker for
		// their whole lifecycle; otherwise the emitting path (catch-up
		// scan vs live) decides. Empty = live, unlabeled.
		if ix.restoredTx[ev.TxHash] {
			attrs = append(attrs, "source", "resume")
		} else if ix.logSource != "" {
			attrs = append(attrs, "source", ix.logSource)
		}
		// Computed durations exist in no event field, so both modes
		// carry them as attributes.
		if ev.Type == event.TypeMined {
			if wait, ok := ix.tracker.MempoolWait(common.HexToHash(ev.TxHash)); ok {
				attrs = append(attrs, "after", wait.Round(time.Millisecond))
			}
		}
		if ev.Type == event.TypeConfirmed && ev.MinedAt != "" {
			if minedAt, err := time.Parse(time.RFC3339, ev.MinedAt); err == nil {
				attrs = append(attrs, "after", time.Since(minedAt).Round(time.Second))
			}
		}
		ix.log.Info(fmt.Sprintf("transaction %s", ev.Type), attrs...)
		metrics.RecordTransaction(ix.cfg.Name, string(ev.Type), string(ev.Direction))
		if ev.Type == event.TypeConfirmed && ev.FirstSeen != "" {
			if seen, err := time.Parse(time.RFC3339, ev.FirstSeen); err == nil {
				metrics.RecordConfirmationLatency(ix.cfg.Name, time.Since(seen))
			}
		}
		// The sink fans out to the optional stdout NDJSON stream and the
		// API's live-subscriber hub; nil when neither is enabled.
		if ix.sink != nil {
			if err := ix.sink.Emit(ev); err != nil {
				ix.log.Error("emitting event", "err", err)
			}
		}
	}
	if len(evs) > 0 {
		metrics.SetTrackedTransactions(ix.cfg.Name, ix.tracker.Len())
	}
	ix.persistEvents(evs)
}

// persistEvents saves the latest state of every transaction touched by
// a batch of events. Transactions still tracked are snapshotted in
// full; ones the tracker just forgot reached a terminal status, which
// is recorded on the existing row - or, for transactions that went
// terminal at discovery (instantly confirmed during catch-up) and were
// never stored, reconstructed from the events themselves. Persistence
// failures are logged, never fatal to indexing.
func (ix *Indexer) persistEvents(evs []event.Event) {
	if ix.store == nil || len(evs) == 0 {
		return
	}
	ctx := context.Background()
	byHash := make(map[string][]event.Event, 1)
	var order []string
	for _, ev := range evs {
		if _, seen := byHash[ev.TxHash]; !seen {
			order = append(order, ev.TxHash)
		}
		byHash[ev.TxHash] = append(byHash[ev.TxHash], ev)
	}

	for _, hash := range order {
		group := byHash[hash]
		var err error
		if snap, ok := ix.tracker.ExportTx(common.HexToHash(hash)); ok {
			err = ix.store.SaveTransaction(ctx, ix.cfg.Name, snap)
		} else {
			last := group[len(group)-1]
			var updated bool
			updated, err = ix.store.UpdateTransactionStatus(ctx, ix.cfg.Name, hash, string(last.Type), last.ReplacedBy)
			if err == nil && !updated {
				err = ix.store.SaveTransaction(ctx, ix.cfg.Name, txFromEvents(group))
			}
		}
		if err != nil {
			ix.log.Error("persisting transaction", "tx", hash, "err", err)
		}
	}
}

// txFromEvents reconstructs a persistable snapshot from one
// transaction's event batch, for transactions that reached a terminal
// state without ever being stored.
func txFromEvents(group []event.Event) storage.TrackedTx {
	first := group[0]
	firstSeen, _ := time.Parse(time.RFC3339, first.FirstSeen)
	tx := storage.TrackedTx{
		Hash:           first.TxHash,
		Status:         string(first.Type),
		BlockHash:      first.BlockHash,
		BlockNumber:    first.BlockNumber,
		FirstSeen:      firstSeen,
		FirstSeenBlock: first.FirstSeenBlock,
		PendingSince:   firstSeen,
		Authoritative:  true,
		ReplacedBy:     first.ReplacedBy,
	}
	if first.MinedAt != "" {
		tx.MinedAt, _ = time.Parse(time.RFC3339, first.MinedAt)
	} else if first.BlockNumber != 0 {
		tx.MinedAt = time.Now()
	}
	for _, ev := range group {
		tx.Transfers = append(tx.Transfers, storage.Transfer{
			From:         ev.From,
			To:           ev.To,
			Direction:    string(ev.Direction),
			Asset:        ev.Asset,
			TokenAddress: ev.TokenAddress,
			Decimals:     ev.Decimals,
			ValueRaw:     ev.ValueRaw,
			Value:        ev.Value,
			LogIndex:     ev.LogIndex,
		})
	}
	return tx
}

// Package indexer watches Ethereum for transfers to a set of addresses,
// following each matched transaction from mempool to confirmation.
package indexer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"

	"github.com/wille/ethindex/internal/config"
	"github.com/wille/ethindex/internal/event"
	"github.com/wille/ethindex/internal/metrics"
	"github.com/wille/ethindex/internal/storage"
)

const (
	backoffMin     = time.Second
	backoffMax     = 30 * time.Second
	healthyUptime  = time.Minute
	headBufferSize = 16
)

// initialLogChunk is how many blocks one catch-up eth_getLogs range
// query covers to start with, sized under common provider range and
// result caps. Client-side filtering mode (huge watched sets) fetches
// every token transfer, so its chunks start smaller.
func (ix *Indexer) initialLogChunk() uint64 {
	if ix.matcher.WatchedCount() > serverFilterMaxAddresses {
		return 100
	}
	return 500
}

// errChainMismatch marks a node reporting a different chain ID than the
// indexer's configuration expects - a config error, never retried.
var errChainMismatch = errors.New("chain id mismatch")

// errFinalityTag marks a node that cannot resolve the configured
// confirmations tag (e.g. `safe` on a chain whose node doesn't serve
// it) - a config error, never retried.
var errFinalityTag = errors.New("finality tag unsupported")

// Indexer owns the connection lifecycle and drives the matcher, chain
// state and tracker from a single dispatch goroutine per session.
type Indexer struct {
	cfg  config.IndexerConfig
	sink event.Sink // nil disables event delivery (stdout stream, API hub)
	log  *slog.Logger

	// FullEventLogs logs the entire event object on transaction logs
	// instead of the curated attribute subset. Set in -json mode, where
	// structured consumers want the full schema (same as -print/SSE).
	FullEventLogs bool

	// Resume restores the persisted block position at startup and
	// catches up everything missed while the process was down. Off by
	// default: starting at the chain tip is instant, while catching up
	// hours of downtime can take a long time. In-flight transactions
	// are restored either way.
	Resume bool
	dial   func(ctx context.Context) (ChainClient, error)
	store  storage.Storage // nil disables persistence

	client        ChainClient
	matcher       *Matcher
	tracker       *Tracker
	chain         *chainState
	lastProcessed uint64

	// logSource names the detection path behind the transaction logs
	// currently being emitted ("catchup", "resume"; empty for live
	// traffic). Dispatch goroutine only, set via withSource.
	logSource string

	// restoredPending holds pending transactions loaded from storage,
	// rechecked against the node on the first head: they may have mined
	// during downtime in blocks this process will never scan (tip-start
	// skips them; even -resume reaches old blocks last). Dispatch
	// goroutine only; drained on first use.
	restoredPending []common.Hash
	// restoredTx marks every transaction imported from storage at
	// startup, so its lifecycle logs carry source=resume. Dispatch
	// goroutine only.
	restoredTx map[string]bool

	// Session-scoped channels, owned by the dispatch goroutine; the
	// catch-up path drains them between backfill blocks so live events
	// keep flowing during long scans.
	heads          chan *types.Header
	pendingMatches chan []Match

	// mempoolSnapshot flips when the one-shot txpool_content load has
	// been attempted, so reconnects don't repeat it.
	mempoolSnapshot atomic.Bool
	// blockReceiptsUnsupported flips on the first eth_getBlockReceipts
	// failure, falling back to per-transaction receipts from then on.
	blockReceiptsUnsupported atomic.Bool

	// Cached node-reported finalized/safe block number, used when
	// confirmations run in tag mode. Dispatch goroutine only.
	finalizedNum uint64
	finalizedAt  time.Time
	finalizedTTL time.Duration
	tagVerified  bool // startup tag-resolution check passed
}

// withSource tags every transaction log emitted inside fn with the
// named detection source. Nesting works: an inner source (a restored-tx
// recheck during a catch-up) overrides the outer one for its duration.
func (ix *Indexer) withSource(source string, fn func() error) error {
	prev := ix.logSource
	ix.logSource = source
	defer func() { ix.logSource = prev }()
	return fn()
}

// tagFinalized is the cached finality number for instant-confirm
// decisions at mining time; 0 outside tag mode (unused there).
func (ix *Indexer) tagFinalized() uint64 {
	if ix.cfg.Confirmations.Tag == "" {
		return 0
	}
	return ix.finalizedNum
}

// finalizedNumber returns the node's current finalized (or safe) block
// number, cached for a few seconds - finality only advances in epochs,
// so per-head refreshes on fast chains would be wasted calls.
func (ix *Indexer) finalizedNumber(ctx context.Context) (uint64, error) {
	if !ix.finalizedAt.IsZero() && time.Since(ix.finalizedAt) < ix.finalizedTTL {
		return ix.finalizedNum, nil
	}
	num, err := ix.client.BlockNumberByTag(ctx, ix.cfg.Confirmations.Tag)
	if err != nil {
		return ix.finalizedNum, err
	}
	ix.finalizedNum = num
	ix.finalizedAt = time.Now()
	metrics.SetFinalizedBlock(ix.cfg.Name, num)
	return num, nil
}

// New builds an Indexer watching addresses on the chain cfg describes.
// store may be nil to run without persistence.
func New(cfg config.IndexerConfig, addresses []common.Address, sink event.Sink, store storage.Storage) *Indexer {
	log := slog.Default().With("indexer", cfg.Name)
	matcher := NewMatcher(addresses, cfg.TokenList, new(big.Int).SetUint64(cfg.ChainID), cfg.NativeSymbol)
	matcher.log = log
	return &Indexer{
		cfg:  cfg,
		sink: sink,
		log:  log,
		dial: func(ctx context.Context) (ChainClient, error) {
			return Dial(ctx, cfg.RPCURL, cfg.HTTPRPCURL)
		},
		store:        store,
		matcher:      matcher,
		tracker:      NewTracker(cfg.Confirmations, cfg.PendingTimeout, nil),
		chain:        newChainState(cfg.Confirmations.RingDepth()),
		finalizedTTL: 10 * time.Second,
	}
}

// restore loads persisted progress and in-flight transactions so this
// process continues where the previous one stopped.
func (ix *Indexer) restore(ctx context.Context) error {
	if ix.store == nil {
		return nil
	}
	state, err := ix.store.LoadIndexerState(ctx, ix.cfg.Name)
	if err != nil {
		return fmt.Errorf("loading indexer state: %w", err)
	}
	if state != nil && ix.Resume {
		ix.lastProcessed = state.LastProcessed
		for _, h := range state.RecentHeaders {
			ix.chain.record(headRef{
				Number:     h.Number,
				Hash:       common.HexToHash(h.Hash),
				ParentHash: common.HexToHash(h.ParentHash),
			})
		}
	} else if state != nil {
		ix.log.Info("starting at the chain tip, ignoring persisted progress (pass -resume to catch up missed blocks)",
			"last_processed", state.LastProcessed)
	}
	txs, err := ix.store.LoadActiveTransactions(ctx, ix.cfg.Name)
	if err != nil {
		return fmt.Errorf("loading active transactions: %w", err)
	}
	if err := ix.tracker.Import(txs); err != nil {
		ix.log.Warn("skipped malformed persisted transactions", "err", err)
	}
	ix.restoredTx = make(map[string]bool, len(txs))
	for _, tx := range txs {
		hash := common.HexToHash(tx.Hash)
		ix.restoredTx[hash.Hex()] = true
		if ix.tracker.IsPending(hash) {
			ix.restoredPending = append(ix.restoredPending, hash)
		}
	}
	if (state != nil && ix.Resume) || len(txs) > 0 {
		ix.log.Info("resumed from storage", "last_processed", ix.lastProcessed, "active_txs", ix.tracker.Len())
	}
	return nil
}

// Run connects and processes events until ctx is cancelled, reconnecting
// with exponential backoff on failure. Tracked transaction state
// survives reconnects; missed blocks are caught up via the gap path.
// A chain-ID mismatch or a failure to restore persisted state is
// terminal and returned immediately.
func (ix *Indexer) Run(ctx context.Context) error {
	if err := ix.restore(ctx); err != nil {
		return fmt.Errorf("indexer %q: restoring state: %w", ix.cfg.Name, err)
	}
	backoff := backoffMin
	for {
		start := time.Now()
		err := ix.runSession(ctx)
		if ctx.Err() != nil {
			ix.shutdown()
			return nil
		}
		if errors.Is(err, errChainMismatch) || errors.Is(err, errFinalityTag) {
			return fmt.Errorf("indexer %q: %w", ix.cfg.Name, err)
		}
		if time.Since(start) >= healthyUptime {
			backoff = backoffMin
		}
		metrics.RecordSessionReconnect(ix.cfg.Name)
		ix.log.Error("session ended, reconnecting", "err", err, "backoff", backoff)
		select {
		case <-ctx.Done():
			ix.shutdown()
			return nil
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, backoffMax)
	}
}

// shutdown persists final progress and logs the indexer's exit stages.
func (ix *Indexer) shutdown() {
	ix.log.Info("saving indexer state")
	ix.saveState()
	ix.log.Info("indexer stopped", "last_processed", ix.lastProcessed, "tracked_txs", ix.tracker.Len())
}

// runSession dials, subscribes and dispatches until something breaks.
func (ix *Indexer) runSession(ctx context.Context) error {
	client, err := ix.dial(ctx)
	if err != nil {
		return fmt.Errorf("dialing %s: %w", ix.cfg.RPCURL, err)
	}
	ix.client = client
	defer func() {
		client.Close()
		ix.log.Debug("node connection closed")
	}()

	chainID, err := client.ChainID(ctx)
	if err != nil {
		return fmt.Errorf("fetching chain id: %w", err)
	}
	if !chainID.IsUint64() || chainID.Uint64() != ix.cfg.ChainID {
		return fmt.Errorf("%w: config expects %d, node at %s reports %s",
			errChainMismatch, ix.cfg.ChainID, ix.cfg.RPCURL, chainID)
	}

	// A confirmations tag the node cannot resolve would silently confirm
	// nothing forever - verify it up front, once, and treat failure as a
	// configuration error.
	if tag := ix.cfg.Confirmations.Tag; tag != "" && !ix.tagVerified {
		num, err := client.BlockNumberByTag(ctx, tag)
		if err != nil {
			return fmt.Errorf("%w: node at %s cannot resolve the %q tag: %v",
				errFinalityTag, ix.cfg.RPCURL, tag, err)
		}
		ix.tagVerified = true
		ix.finalizedNum = num
		ix.finalizedAt = time.Now()
		ix.log.Debug("finality tag verified", "tag", tag, "block", num)
	}

	logFilter := "server"
	if ix.matcher.WatchedCount() > serverFilterMaxAddresses {
		logFilter = "client"
	}
	ix.log.Info("connected", "tokens", len(ix.cfg.TokenList), "addresses", ix.matcher.WatchedCount(), "log_filter", logFilter)

	ix.heads = make(chan *types.Header, headBufferSize)
	headSub, err := client.SubscribeNewHead(ctx, ix.heads)
	if err != nil {
		return fmt.Errorf("subscribing to new heads: %w", err)
	}
	defer headSub.Unsubscribe()

	sessionCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// The pending watcher and its helpers run off the dispatch
	// goroutine; they get the session client explicitly so a session
	// teardown cannot race their reads.
	ix.pendingMatches = make(chan []Match, 64)
	pendingErr := make(chan error, 1)
	go func() {
		pendingErr <- ix.watchPending(sessionCtx, client, ix.pendingMatches)
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-headSub.Err():
			return fmt.Errorf("head subscription: %w", err)
		case err := <-pendingErr:
			if err != nil {
				return fmt.Errorf("pending subscription: %w", err)
			}
			// Block-only mode or clean shutdown of the pending watcher:
			// keep running on heads alone.
			pendingErr = nil
		case matches := <-ix.pendingMatches:
			ix.emitAll(ix.tracker.OnPending(matches))
		case head := <-ix.heads:
			if err := ix.onHead(ctx, head); err != nil {
				return err
			}
		}
	}
}

// onHead advances the canonical chain view and the tracked lifecycles.
func (ix *Indexer) onHead(ctx context.Context, head *types.Header) error {
	num := head.Number.Uint64()
	metrics.SetChainHeadBlock(ix.cfg.Name, num)
	kind := ix.chain.classify(head)
	// lastProcessed is the ground truth of what has been scanned; the
	// ring tip is merely the newest header seen. They disagree after a
	// fresh -resume restore (no tip yet) and after an interrupted
	// catch-up (the tip advanced past blocks that were never scanned) -
	// in both cases a head extending the tip still hides a gap, and
	// trusting the extend would silently abandon the unscanned range.
	if kind == headExtend && ix.lastProcessed > 0 && num > ix.lastProcessed+1 {
		kind = headGap
	}
	switch kind {
	case headStale:
		return nil

	case headExtend:
		d, err := ix.fetchBlockData(ctx, head.Hash())
		if err != nil {
			return fmt.Errorf("fetching block %d: %w", num, err)
		}
		if err := ix.processBlock(ctx, d); err != nil {
			return err
		}
		ix.lastProcessed = num
		metrics.SetLastProcessedBlock(ix.cfg.Name, num)
		ix.saveState()

	case headGap:
		highest, err := ix.catchUp(ctx, ix.lastProcessed+1, head)
		if err != nil {
			return err
		}
		num = highest

	case headReorg:
		if err := ix.handleReorg(ctx, num); err != nil {
			return err
		}
	}
	return ix.updateLifecycles(ctx, num, 0)
}

// clientFilterReceipts reports whether ERC20 logs should come from
// block receipts: huge watched set (server-side filtering impossible)
// and a node that serves eth_getBlockReceipts.
func (ix *Indexer) clientFilterReceipts() bool {
	return ix.matcher.WatchedCount() > serverFilterMaxAddresses && !ix.blockReceiptsUnsupported.Load()
}

// fetchBundle fetches a block and (in client-filter mode) its receipts
// in one round trip - a JSON-RPC batch, assumed supported; a failing
// batch is a failure, never a cue to try individual calls. byHash
// toggles between hash (tip - reorg-proof pairing by construction) and
// number (catch-up - the receipts' block hash is verified against the
// body afterwards).
func (ix *Indexer) fetchBundle(ctx context.Context, client ChainClient, hash common.Hash, number uint64, byHash bool) (blockData, error) {
	if ix.clientFilterReceipts() {
		bundle, err := withRetry(ctx, func() (BlockBundle, error) {
			if byHash {
				return client.BlockBundleByHash(ctx, hash)
			}
			return client.BlockBundleByNumber(ctx, number)
		})
		if err != nil {
			return blockData{}, err
		}
		return ix.bundleData(ctx, client, bundle), nil
	}

	// Server-filter mode needs no receipts: a single block fetch.
	block, err := withRetry(ctx, func() (*Block, error) {
		if byHash {
			return client.BlockByHash(ctx, hash)
		}
		return client.BlockByNumber(ctx, number)
	})
	if err != nil {
		return blockData{}, err
	}
	return blockData{block: block}, nil
}

// bundleData turns a fetched bundle into blockData, verifying the
// receipts belong to the fetched body. Number-addressed halves can
// race a reorg; mismatched receipts are refetched pinned to the body's
// hash. Receipt failures degrade to receipt-less data (the log-chunk
// fallback covers those blocks).
func (ix *Indexer) bundleData(ctx context.Context, client ChainClient, bundle BlockBundle) blockData {
	d := blockData{block: bundle.Block}
	switch {
	case bundle.ReceiptsErr != nil:
		ix.noteBlockReceiptsError(bundle.ReceiptsErr)
	case len(bundle.Receipts) > 0 && bundle.Receipts[0].BlockHash != bundle.Block.Hash:
		receipts, rerr := client.BlockReceipts(ctx, bundle.Block.Hash)
		if rerr != nil {
			ix.noteBlockReceiptsError(rerr)
		} else {
			d.receipts = receipts
			d.haveReceipts = true
		}
	default:
		d.receipts = bundle.Receipts
		d.haveReceipts = true
	}
	return d
}

// fetchBlockData fetches a block by hash plus, in client-filter mode,
// its receipts - one round trip for both.
func (ix *Indexer) fetchBlockData(ctx context.Context, hash common.Hash) (blockData, error) {
	return ix.fetchBundle(ctx, ix.client, hash, 0, true)
}

// catchUp backfills [from, head] scanning NEWEST-FIRST: the announcing
// head is processed immediately so fresh activity surfaces within
// seconds, then the scan walks backwards. Between backfill blocks any
// newly announced heads and pending matches are drained and processed,
// so live events flow with no delay during a long catch-up, and
// lifecycles are updated per block so deep transactions confirm as soon
// as they are discovered. Progress against the resume point is only
// persisted once the whole range is done - a crash mid-scan redoes the
// catch-up. Returns the highest block processed (new heads may arrive
// while scanning).
func (ix *Indexer) catchUp(ctx context.Context, from uint64, head *types.Header) (highest uint64, err error) {
	err = ix.withSource("catchup", func() error {
		var innerErr error
		highest, innerErr = ix.catchUpScan(ctx, from, head)
		return innerErr
	})
	return highest, err
}

func (ix *Indexer) catchUpScan(ctx context.Context, from uint64, head *types.Header) (uint64, error) {
	const progressInterval = 5 * time.Second
	to := head.Number.Uint64()
	highest := to
	ix.log.Info("catching up missed blocks", "from", from, "to", to, "order", "newest first")
	start := time.Now()
	lastReport := start

	// The announcing head first: current activity has no wait.
	headData, err := ix.fetchBlockData(ctx, head.Hash())
	if err != nil {
		return 0, fmt.Errorf("fetching block %d: %w", to, err)
	}
	if err := ix.processBlock(ctx, headData); err != nil {
		return 0, err
	}
	done := uint64(1)

	// Log sourcing: in client-filter mode the prefetch workers deliver
	// each block's receipts (logs included) pipelined with the bodies.
	// Blocks arriving without receipts (transient failures, or nodes
	// lacking the method) fall back to range-chunked log queries, which
	// shrink adaptively when the provider rejects a range and bottom
	// out in per-block queries.
	var (
		chunkLogs map[uint64][]types.Log
		chunkLo   uint64
		chunkSize = ix.initialLogChunk()
	)

	// Workers prefetch the backlog concurrently (RPC latency dominates);
	// processing consumes strictly in scan order.
	prefetchCtx, cancelPrefetch := context.WithCancel(ctx)
	defer cancelPrefetch()
	blocks := ix.prefetchBlocks(prefetchCtx, from, to-1, true, ix.clientFilterReceipts())

	// With an age cap, the newest-first order turns the cap into a
	// stream cutoff: the first block older than the cap ends the scan.
	var cutoff time.Time
	if ix.cfg.MaxCatchupAge > 0 {
		cutoff = time.Now().Add(-ix.cfg.MaxCatchupAge)
	}

	for slot := range blocks {
		var res fetchResult
		select {
		case res = <-slot:
		case <-ctx.Done():
			return 0, ctx.Err()
		}
		if res.err != nil {
			return 0, res.err
		}
		d := res.data
		n := uint64(d.block.Number)

		if !cutoff.IsZero() && time.Unix(int64(d.block.Time), 0).Before(cutoff) {
			ix.log.Warn("catch-up age cap reached, skipping older blocks",
				"cap", ix.cfg.MaxCatchupAge, "skipped_from", from, "skipped_to", n,
				"skipped", n-from+1)
			break
		}

		// Live traffic between backfill blocks.
		if err := ix.drainLive(ctx, &highest); err != nil {
			return 0, err
		}

		if !d.haveReceipts && chunkSize > 0 && (chunkLogs == nil || n < chunkLo) {
			lo := from
			if n >= chunkSize && lo < n-chunkSize+1 {
				lo = n - chunkSize + 1
			}
			logs, err := ix.fetchLogsRange(ctx, lo, n)
			if err != nil {
				chunkLogs = nil
				chunkSize /= 2
				if chunkSize < 8 {
					chunkSize = 0
					ix.log.Warn("range log queries keep failing, using per-block queries", "err", err)
				} else {
					ix.log.Debug("range log query failed, halving chunk", "size", chunkSize, "err", err)
				}
			} else {
				chunkLogs, chunkLo = logs, lo
			}
		}
		if !d.haveReceipts && chunkSize > 0 && chunkLogs != nil {
			d.logs = chunkLogs[n]
			d.haveLogs = true
		}
		if err := ix.processBlock(ctx, d); err != nil {
			return 0, err
		}
		done++

		// Confirm discovered transactions immediately: a deep-enough
		// block's transfers reach the threshold the moment we see them.
		// The scan floor keeps txs in not-yet-rescanned blocks waiting.
		if err := ix.updateLifecycles(ctx, highest, n); err != nil {
			return 0, err
		}

		metrics.SetCatchupRemainingBlocks(ix.cfg.Name, n-from)
		if time.Since(lastReport) >= progressInterval {
			lastReport = time.Now()
			// Progress is measured against the fixed backfill range;
			// live blocks drained mid-scan are processed but neither
			// counted as done nor added to the goal.
			total := to - from + 1
			rate := float64(done) / time.Since(start).Seconds()
			attrs := []any{
				"block", n,
				"progress", fmt.Sprintf("%.1f%%", float64(done)/float64(total)*100),
				"rate", fmt.Sprintf("%.1f/s", rate),
				"remaining", n - from,
			}
			if rate > 0 {
				eta := time.Duration(float64(n-from)/rate) * time.Second
				attrs = append(attrs, "eta", eta.Round(time.Second))
			}
			ix.log.Info("catch-up progress", attrs...)
		}
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	ix.lastProcessed = highest
	metrics.SetLastProcessedBlock(ix.cfg.Name, highest)
	metrics.SetCatchupRemainingBlocks(ix.cfg.Name, 0)
	ix.saveState()
	ix.log.Info("done catching up", "blocks", done, "duration", time.Since(start).Round(time.Millisecond))
	return highest, nil
}

// fetchResult is one prefetched block with its sidecar data (or the
// error fetching it).
type fetchResult struct {
	data blockData
	err  error
}

// prefetchBlocks fetches the blocks numbered [lo, hi] with a pool of
// concurrent workers and returns a channel of per-block result slots in
// scan order (descending when desc, else ascending), with a bounded
// lookahead. With withReceipts, each block's receipts ride along - the
// log source in client-filter mode, fully pipelined with the bodies.
// Workers fetch spans of up to batch_blocks consecutive blocks in one
// JSON-RPC batch request. The consumer must drain the returned channel
// or cancel ctx.
func (ix *Indexer) prefetchBlocks(ctx context.Context, lo, hi uint64, desc, withReceipts bool) <-chan chan fetchResult {
	type span struct {
		numbers []uint64
		slots   []chan fetchResult
	}
	batch := max(1, ix.cfg.BatchBlocks)
	// Concurrency budgets in-flight REQUESTS: each worker keeps one
	// span batch in flight, so blocks in flight (and catch-up memory)
	// scale with concurrency x batch_blocks. Size both to the provider.
	workers := max(1, ix.cfg.Concurrency)
	jobs := make(chan span, workers)
	// The slot buffer must exceed the span size or the feeder deadlocks
	// waiting for a span it can never fill (slots drain only after their
	// span is dispatched), and must cover workers*batch or the pool
	// starves. Slots are cheap; fetched-data memory is bounded by this
	// buffer times the block size.
	ordered := make(chan chan fetchResult, 2*workers*batch)
	// The workers get the session client explicitly so a session
	// teardown cannot race their reads.
	client := ix.client

	for range workers {
		go func() {
			for sp := range jobs {
				ix.fetchSpan(ctx, client, sp.numbers, sp.slots, withReceipts)
			}
		}()
	}

	go func() {
		defer close(jobs)
		defer close(ordered)
		var pending span
		flush := func() bool {
			if len(pending.numbers) == 0 {
				return true
			}
			sp := pending
			pending = span{}
			select {
			case jobs <- sp:
				return true
			case <-ctx.Done():
				return false
			}
		}
		emit := func(n uint64) bool {
			slot := make(chan fetchResult, 1)
			select {
			case ordered <- slot:
			case <-ctx.Done():
				return false
			}
			pending.numbers = append(pending.numbers, n)
			pending.slots = append(pending.slots, slot)
			if len(pending.numbers) >= batch {
				return flush()
			}
			return true
		}
		if desc {
			for n := hi; n >= lo; n-- {
				if !emit(n) {
					return
				}
				if n == lo {
					break
				}
			}
		} else {
			for n := lo; n <= hi; n++ {
				if !emit(n) {
					return
				}
			}
		}
		flush()
	}()

	return ordered
}

// bundlesResult pairs a multi-block batch's outputs for withRetry.
type bundlesResult struct {
	bundles []BlockBundle
	errs    []error
}

// fetchSpan resolves one span of blocks into its result slots with a
// single multi-block batch request (retried as a whole on transient
// failures). Per-block errors within a successful batch - a lagging
// node not serving a fresh block yet - are retried per block: a
// single-block bundle in client-filter mode, a plain block fetch in
// server-filter mode. A failed span fails all its slots.
func (ix *Indexer) fetchSpan(ctx context.Context, client ChainClient, numbers []uint64, slots []chan fetchResult, withReceipts bool) {
	fetched := make([]bool, len(numbers))
	wantReceipts := withReceipts && !ix.blockReceiptsUnsupported.Load()
	if len(numbers) > 1 {
		res, err := withRetry(ctx, func() (bundlesResult, error) {
			bundles, errs, err := client.BlockBundles(ctx, numbers, wantReceipts)
			return bundlesResult{bundles: bundles, errs: errs}, err
		})
		if err != nil {
			for i := range numbers {
				slots[i] <- fetchResult{err: fmt.Errorf("fetching block %d: %w", numbers[i], err)}
			}
			return
		}
		for i := range numbers {
			if res.errs[i] != nil {
				continue // individually retried below
			}
			var d blockData
			if wantReceipts {
				d = ix.bundleData(ctx, client, res.bundles[i])
			} else {
				d.block = res.bundles[i].Block
			}
			slots[i] <- fetchResult{data: d}
			fetched[i] = true
		}
	}
	for i, n := range numbers {
		if fetched[i] {
			continue
		}
		var res fetchResult
		if withReceipts {
			// One batch request per block: body + receipts in a single
			// round trip.
			res.data, res.err = ix.fetchBundle(ctx, client, common.Hash{}, n, false)
		} else {
			block, err := withRetry(ctx, func() (*Block, error) {
				return client.BlockByNumber(ctx, n)
			})
			res.data.block = block
			res.err = err
		}
		if res.err != nil {
			res.err = fmt.Errorf("fetching block %d: %w", n, res.err)
		}
		slots[i] <- res
	}
}

// drainLive processes, without blocking, everything the subscriptions
// delivered while a catch-up scan was busy: pending matches and newly
// announced heads (fetched forward by number from the highest block
// already processed). It runs inside catchUp but handles live traffic,
// so the catch-up log source is cleared for its duration.
func (ix *Indexer) drainLive(ctx context.Context, highest *uint64) error {
	return ix.withSource("", func() error { return ix.drainLiveInner(ctx, highest) })
}

func (ix *Indexer) drainLiveInner(ctx context.Context, highest *uint64) error {
	for {
		select {
		case matches := <-ix.pendingMatches:
			ix.emitAll(ix.tracker.OnPending(matches))
		case head := <-ix.heads:
			num := head.Number.Uint64()
			if num <= *highest {
				continue // duplicate or stale announcement
			}
			if err := ix.processGap(ctx, *highest+1, num); err != nil {
				return err
			}
			*highest = num
		default:
			return nil
		}
	}
}

// processGap fetches and processes the blocks [from, to] ascending,
// in multi-block batches of up to batch_blocks (receipts included in
// client-filter mode, sparing each block a lazy receipts call later).
// A failed batch fails the gap; per-block misses within a successful
// batch are refetched as single-block bundles.
func (ix *Indexer) processGap(ctx context.Context, from, to uint64) error {
	batch := max(1, ix.cfg.BatchBlocks)
	withReceipts := ix.clientFilterReceipts()
	for lo := from; lo <= to; lo += uint64(batch) {
		hi := min(lo+uint64(batch)-1, to)
		numbers := make([]uint64, 0, hi-lo+1)
		for n := lo; n <= hi; n++ {
			numbers = append(numbers, n)
		}

		var (
			bundles []BlockBundle
			errs    []error
		)
		if len(numbers) > 1 {
			res, err := withRetry(ctx, func() (bundlesResult, error) {
				bundles, errs, err := ix.client.BlockBundles(ctx, numbers, withReceipts)
				return bundlesResult{bundles: bundles, errs: errs}, err
			})
			if err != nil {
				return fmt.Errorf("fetching blocks %d-%d: %w", lo, hi, err)
			}
			bundles, errs = res.bundles, res.errs
		}
		for i, n := range numbers {
			var d blockData
			if bundles != nil && errs[i] == nil {
				if withReceipts {
					d = ix.bundleData(ctx, ix.client, bundles[i])
				} else {
					d.block = bundles[i].Block
				}
			} else {
				// Single-block gap, or a block the batch could not serve
				// (lagging node): fetched on its own, still bundled.
				var err error
				d, err = ix.fetchBundle(ctx, ix.client, common.Hash{}, n, false)
				if err != nil {
					return fmt.Errorf("fetching block %d: %w", n, err)
				}
			}
			if err := ix.processBlock(ctx, d); err != nil {
				return err
			}
		}
	}
	return nil
}

// processRange fetches and processes canonical blocks forward by
// number (used for reorg rescans, which must replay in order). Fetches
// are pipelined; processing stays sequential.
func (ix *Indexer) processRange(ctx context.Context, from, to uint64) error {
	prefetchCtx, cancelPrefetch := context.WithCancel(ctx)
	defer cancelPrefetch()

	for slot := range ix.prefetchBlocks(prefetchCtx, from, to, false, ix.clientFilterReceipts()) {
		var res fetchResult
		select {
		case res = <-slot:
		case <-ctx.Done():
			return ctx.Err()
		}
		if res.err != nil {
			return res.err
		}
		if err := ix.processBlock(ctx, res.data); err != nil {
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	ix.lastProcessed = to
	metrics.SetLastProcessedBlock(ix.cfg.Name, to)
	ix.saveState()
	return nil
}

// headerRefsByNumbers lets chainState.findAncestor query the canonical
// chain: one batch request per chunk.
func (ix *Indexer) headerRefsByNumbers(numbers []uint64) ([]headRef, []error, error) {
	blocks, berrs, err := ix.client.HeaderRefsByNumbers(context.Background(), numbers)
	if err != nil {
		return nil, nil, err
	}
	refs := make([]headRef, len(numbers))
	for i, b := range blocks {
		if berrs[i] != nil {
			continue
		}
		refs[i] = headRef{Number: uint64(b.Number), Hash: b.Hash, ParentHash: b.ParentHash}
	}
	return refs, berrs, nil
}

// handleReorg finds the common ancestor, demotes affected txs and
// rescans the new canonical chain up to the reported head.
func (ix *Indexer) handleReorg(ctx context.Context, headNumber uint64) error {
	walkFrom := headNumber
	if tip := ix.chain.tip; tip != nil && tip.Number < walkFrom {
		walkFrom = tip.Number
	}
	ancestor, deep, err := ix.chain.findAncestor(ix, walkFrom)
	if err != nil {
		return fmt.Errorf("walking back reorg: %w", err)
	}
	if deep {
		ix.log.Warn("reorg deeper than tracked history, rescanning from buffer floor", "floor", ancestor)
	}
	// Depth: how many of our previously canonical blocks were reverted.
	var depth uint64
	if tip := ix.chain.tip; tip != nil && tip.Number > ancestor {
		depth = tip.Number - ancestor
	}
	metrics.RecordReorg(ix.cfg.Name, depth)
	ix.log.Info("chain reorg detected", "depth", depth, "ancestor", ancestor, "head", headNumber)
	ix.emitAll(ix.tracker.OnReorg(ancestor))
	ix.lastProcessed = ancestor
	if err := ix.processRange(ctx, ancestor+1, headNumber); err != nil {
		return err
	}
	// The new canonical head may be LOWER than the orphaned old tip;
	// leaving the tip there would misclassify every following head as
	// another reorg.
	ix.chain.resetTip(headNumber)
	return nil
}

// confirmCheck is one pre-confirmation receipt verification.
type confirmCheck struct {
	hash    common.Hash
	tracked common.Hash
	receipt *types.Receipt
	err     error
}

// fetchConfirmReceipts fills the receipt/err of every check with one
// batch request; a failed batch is reported on every check.
func (ix *Indexer) fetchConfirmReceipts(ctx context.Context, checks []confirmCheck) {
	if len(checks) == 0 {
		return
	}
	hashes := make([]common.Hash, len(checks))
	for i, c := range checks {
		hashes[i] = c.hash
	}
	receipts, errs, err := ix.client.TransactionReceipts(ctx, hashes)
	if err != nil {
		// The whole batch failed: report it on every check.
		for i := range checks {
			checks[i].err = err
		}
		return
	}
	for i := range checks {
		checks[i].receipt, checks[i].err = receipts[i], errs[i]
	}
}

// updateLifecycles confirms mined txs past the threshold (after a fresh
// receipt check) and resolves timed-out pending txs. During a backward
// catch-up scan, scanFloor is the lowest block already scanned:
// transactions tracked in not-yet-rescanned blocks below it are left
// alone, otherwise confirming (and forgetting) them before their block
// is rescanned would make the rescan re-discover them as new. Pass 0
// during normal operation.
func (ix *Indexer) updateLifecycles(ctx context.Context, headNumber, scanFloor uint64) error {
	var finalized uint64
	if ix.cfg.Confirmations.Tag != "" {
		f, err := ix.finalizedNumber(ctx)
		if err != nil {
			// Keep indexing; confirmations resume when the tag resolves.
			ix.log.Warn("resolving finality tag failed", "tag", ix.cfg.Confirmations.Tag, "err", err)
		}
		finalized = f
	}
	confirmCandidates, staleCandidates := ix.tracker.OnHead(headNumber, finalized)

	// First pass after a restart: resolve restored pending txs right
	// away instead of waiting out their pending_timeout. The list is
	// cleared only after the recheck below succeeds - a transient
	// failure here (session restart) must retry on the next head, not
	// silently drop the recheck for the process lifetime.
	recheckingRestored := len(ix.restoredPending) > 0
	if recheckingRestored {
		ix.log.Info("rechecking restored pending transactions", "count", len(ix.restoredPending))
		stale := make(map[common.Hash]bool, len(staleCandidates))
		for _, h := range staleCandidates {
			stale[h] = true
		}
		for _, h := range ix.restoredPending {
			if !stale[h] {
				staleCandidates = append(staleCandidates, h)
			}
		}
	}

	// The receipt re-verifications ride one batch request; the
	// (single-goroutine) tracker mutations are applied in order after.
	checks := make([]confirmCheck, 0, len(confirmCandidates))
	for _, hash := range confirmCandidates {
		trackedBlock, minedAt, ok := ix.tracker.MinedBlock(hash)
		if !ok || minedAt < scanFloor {
			continue
		}
		checks = append(checks, confirmCheck{hash: hash, tracked: trackedBlock})
	}
	ix.fetchConfirmReceipts(ctx, checks)

	for _, c := range checks {
		hash, trackedBlock, receipt, err := c.hash, c.tracked, c.receipt, c.err
		switch {
		case errors.Is(err, ethereum.NotFound):
			// Vanished from the canonical chain (reorg deeper than our
			// buffer): back to pending, timeout will resolve it if it
			// never returns.
			ix.log.Warn("tx lost from canonical chain before confirmation", "tx", hash)
			ix.emitAll(ix.tracker.Demote(hash))
		case err != nil:
			return fmt.Errorf("receipt check for %s: %w", hash, err)
		case receipt.BlockHash != trackedBlock:
			// Re-mined in a different block than we tracked.
			ix.log.Warn("tx moved blocks before confirmation", "tx", hash, "block", receipt.BlockHash)
			ix.emitAll(ix.tracker.Demote(hash))
			failed := receipt.Status == types.ReceiptStatusFailed
			minedAt := receipt.BlockNumber.Uint64()
			ix.emitAll(ix.tracker.OnMined(hash, receipt.BlockHash, minedAt, nil, failed, ix.confirmationsFor(minedAt), ix.tagFinalized()))
		default:
			ix.emitAll(ix.tracker.Confirm(hash, headNumber))
		}
	}

	if err := ix.recheckPendingTxs(ctx, staleCandidates); err != nil {
		return err
	}
	if recheckingRestored {
		ix.restoredPending = nil
	}
	return nil
}

// recheckPendingTxs resolves the fate of tracked pending transactions:
// two batch requests for the whole set - all receipts, then mempool
// existence for the receipt-less rest.
func (ix *Indexer) recheckPendingTxs(ctx context.Context, hashes []common.Hash) error {
	if len(hashes) == 0 {
		return nil
	}
	receipts, rerrs, err := ix.client.TransactionReceipts(ctx, hashes)
	if err != nil {
		return fmt.Errorf("pending recheck batch: %w", err)
	}
	var unmined []common.Hash
	for i, hash := range hashes {
		switch {
		case rerrs[i] == nil:
			ix.promotePendingTx(hash, receipts[i])
		case errors.Is(rerrs[i], ethereum.NotFound):
			unmined = append(unmined, hash)
		default:
			return fmt.Errorf("receipt check for pending %s: %w", hash, rerrs[i])
		}
	}
	if len(unmined) == 0 {
		return nil
	}

	// No receipt: still in the mempool, or gone?
	_, terrs, err := ix.client.TransactionsByHash(ctx, unmined)
	if err != nil {
		return fmt.Errorf("stale pending batch check: %w", err)
	}
	for i, hash := range unmined {
		switch {
		case errors.Is(terrs[i], ethereum.NotFound):
			ix.emitAll(ix.tracker.OnDropStale(hash, false))
		case terrs[i] != nil:
			return fmt.Errorf("stale pending check for %s: %w", hash, terrs[i])
		default:
			// Known to the node but unmined (in the pool, or in reorg
			// limbo): keep waiting with a fresh timeout.
			ix.emitAll(ix.tracker.OnDropStale(hash, true))
		}
	}
	return nil
}

// promotePendingTx applies a found receipt to a tracked pending tx,
// recovering ERC20 legs from the receipt logs.
func (ix *Indexer) promotePendingTx(hash common.Hash, receipt *types.Receipt) {
	transfers := ix.tracker.NativeLegs(hash)
	for _, lg := range receipt.Logs {
		if match, ok := ix.matcher.MatchLog(*lg); ok {
			transfers = append(transfers, match)
		}
	}
	failed := receipt.Status == types.ReceiptStatusFailed
	ix.log.Info("pending tx found mined, promoting", "tx", hash, "block", receipt.BlockNumber)
	minedAt := receipt.BlockNumber.Uint64()
	ix.emitAll(ix.tracker.OnMined(hash, receipt.BlockHash, minedAt, transfers, failed, ix.confirmationsFor(minedAt), ix.tagFinalized()))
}

var _ headerFetcher = (*Indexer)(nil)

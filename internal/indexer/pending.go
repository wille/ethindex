package indexer

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"

	"github.com/wille/ethindex/internal/metrics"
)

const (
	pendingHashBuffer  = 4096
	dropReportInterval = time.Minute
	// pendingLookupBatch caps how many queued pending hashes one worker
	// coalesces into a single batch lookup.
	pendingLookupBatch = 16
	// mempoolProgressInterval paces the snapshot's heartbeat and
	// match-progress logs.
	mempoolProgressInterval = 5 * time.Second
)

// watchPending streams matched pending transactions into out until ctx
// is cancelled or the subscription fails (returned error). It tries a
// full-transaction subscription first, falls back to hash notifications
// with a lookup worker pool, and if neither is supported returns nil so
// the indexer runs in block-only mode. After subscribing (so nothing
// slips between snapshot and stream) the node's current mempool is
// loaded once via txpool_content, where supported.
func (ix *Indexer) watchPending(ctx context.Context, client ChainClient, out chan<- []Match) error {
	txCh := make(chan *types.Transaction, 512)
	sub, err := client.SubscribeFullPendingTransactions(ctx, txCh)
	if err == nil {
		ix.log.Info("pending watcher: full transaction subscription active")
		go ix.loadMempool(ctx, client, out)
		return ix.consumePendingTxs(ctx, sub, txCh, out)
	}
	ix.log.Debug("full pending tx subscription unavailable, trying hashes", "err", err)

	hashCh := make(chan common.Hash, pendingHashBuffer)
	sub, err = client.SubscribePendingTransactions(ctx, hashCh)
	if err != nil {
		ix.log.Warn("pending tx subscriptions unsupported by provider; running in block-only mode", "err", err)
		ix.loadMempool(ctx, client, out) // still worth a one-shot snapshot
		return nil
	}
	ix.log.Info("pending watcher: hash subscription active")
	go ix.loadMempool(ctx, client, out)
	return ix.consumePendingHashes(ctx, client, sub, hashCh, out)
}

// loadMempool matches a one-time snapshot of the node's transaction
// pool, so payments already waiting in the mempool at startup are
// detected without waiting for them to mine. Best effort: most public
// providers do not expose txpool_content. It runs at most once per
// process - a failing snapshot (e.g. a response so large it kills the
// connection) must not repeat on every reconnect.
func (ix *Indexer) loadMempool(ctx context.Context, client ChainClient, out chan<- []Match) {
	if !ix.mempoolSnapshot.CompareAndSwap(false, true) {
		return
	}
	start := time.Now()
	pending, queued, err := client.TxPoolStatus(ctx)
	if err != nil {
		// A provider without txpool_status won't serve txpool_content
		// either - skip the expensive call.
		ix.log.Info("mempool unavailable", "err", err)
		return
	}
	ix.log.Info("loading mempool", "pending", pending, "queued", queued)

	// The snapshot is one monolithic call - on a busy mainnet node it
	// downloads and decodes for a long time with nothing to show for
	// it, so a heartbeat proves the indexer is alive, not stuck.
	fetchDone := make(chan struct{})
	go func() {
		tick := time.NewTicker(mempoolProgressInterval)
		defer tick.Stop()
		for {
			select {
			case <-fetchDone:
				return
			case <-ctx.Done():
				return
			case <-tick.C:
				ix.log.Info("still fetching mempool snapshot", "elapsed", time.Since(start).Round(time.Second))
			}
		}
	}()
	txs, err := client.TxPoolContent(ctx)
	close(fetchDone)
	if err != nil {
		ix.log.Debug("mempool snapshot unavailable", "err", err)
		return
	}

	matched := 0
	lastReport := time.Now()
	for i, tx := range txs {
		if time.Since(lastReport) >= mempoolProgressInterval {
			lastReport = time.Now()
			ix.log.Info("matching mempool transactions",
				"progress", fmt.Sprintf("%.0f%%", float64(i)/float64(len(txs))*100),
				"matched", matched,
			)
		}
		matches := ix.matcher.MatchBlockTx(tx)
		if len(matches) == 0 {
			continue
		}
		select {
		case out <- matches:
			matched++
		case <-ctx.Done():
			return
		}
	}
	ix.log.Info("mempool loaded", "txs", len(txs), "matched", matched, "duration", time.Since(start).Round(time.Millisecond))
}

func (ix *Indexer) consumePendingTxs(ctx context.Context, sub ethereum.Subscription, txCh <-chan *types.Transaction, out chan<- []Match) error {
	defer sub.Unsubscribe()
	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-sub.Err():
			return err
		case tx := <-txCh:
			if tx == nil {
				continue
			}
			if matches := ix.matcher.MatchTx(tx); len(matches) > 0 {
				select {
				case out <- matches:
				case <-ctx.Done():
					return nil
				}
			}
		}
	}
}

// consumePendingHashes fans hash notifications out to a small worker
// pool doing batched transaction lookups. The subscription channel is
// drained without blocking: if the workers fall behind, hashes are
// dropped and counted - the block scan remains the source of truth.
func (ix *Indexer) consumePendingHashes(ctx context.Context, client ChainClient, sub ethereum.Subscription, hashCh <-chan common.Hash, out chan<- []Match) error {
	defer sub.Unsubscribe()

	work := make(chan common.Hash, pendingHashBuffer)
	var wg sync.WaitGroup
	workerCtx, cancelWorkers := context.WithCancel(ctx)
	defer func() {
		cancelWorkers()
		wg.Wait()
	}()

	emit := func(matches []Match) bool {
		select {
		case out <- matches:
			return true
		case <-workerCtx.Done():
			return false
		}
	}
	// lookupBatch resolves a burst of queued hashes in one request. A
	// failed batch drops the burst rather than hammer the provider -
	// the block scan remains the source of truth for anything missed.
	// NotFound per hash is normal churn (evicted before the lookup).
	lookupBatch := func(hashes []common.Hash) bool {
		txs, errs, err := client.TransactionsByHash(workerCtx, hashes)
		if err != nil {
			ix.log.Debug("batched pending lookup failed, dropping burst", "count", len(hashes), "err", err)
			return true
		}
		for i, h := range hashes {
			if errs[i] != nil {
				if !errors.Is(errs[i], ethereum.NotFound) {
					ix.log.Debug("pending tx lookup failed", "tx", h, "err", errs[i])
				}
				continue
			}
			if matches := ix.matcher.MatchBlockTx(*txs[i]); len(matches) > 0 {
				if !emit(matches) {
					return false
				}
			}
		}
		return true
	}

	for range max(1, ix.cfg.Concurrency) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-workerCtx.Done():
					return
				case h := <-work:
					// Coalesce whatever burst is already queued into one
					// batch request - zero added latency, since only
					// hashes that were waiting anyway ride along.
					hashes := []common.Hash{h}
				drain:
					for len(hashes) < pendingLookupBatch {
						select {
						case more := <-work:
							hashes = append(hashes, more)
						default:
							break drain
						}
					}
					if !lookupBatch(hashes) {
						return
					}
				}
			}
		}()
	}

	var dropped uint64
	report := time.NewTicker(dropReportInterval)
	defer report.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-sub.Err():
			return err
		case <-report.C:
			if dropped > 0 {
				metrics.RecordPendingDropped(ix.cfg.Name, dropped)
				ix.log.Warn("pending watcher overloaded, dropped hash notifications", "count", dropped)
				dropped = 0
			}
		case h := <-hashCh:
			select {
			case work <- h:
			default:
				dropped++
			}
		}
	}
}

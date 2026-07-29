package indexer

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"math/big"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/wille/ethindex/internal/config"
	"github.com/wille/ethindex/internal/event"
	"github.com/wille/ethindex/internal/storage"
)

// fakeSub is a controllable ethereum.Subscription.
type fakeSub struct {
	errCh chan error
	once  sync.Once
}

func newFakeSub() *fakeSub           { return &fakeSub{errCh: make(chan error, 1)} }
func (s *fakeSub) Err() <-chan error { return s.errCh }
func (s *fakeSub) Unsubscribe()      { s.once.Do(func() { close(s.errCh) }) }

// fakeClient serves a scripted chain to the indexer.
type fakeClient struct {
	mu       sync.Mutex
	chainID  *big.Int
	blocks   map[common.Hash]*Block
	byNumber map[uint64]*Block
	receipts map[common.Hash]*types.Receipt
	logs     map[common.Hash][]types.Log // keyed by block hash
	pendTxs  map[common.Hash]*types.Transaction
	served   map[common.Hash]struct{} // blocks fetched by hash

	heads       chan<- *types.Header
	headSubs    int // subscription generation, so harnesses can await a fresh one
	headSub     *fakeSub
	pending     chan<- *types.Transaction
	pendingHash chan<- common.Hash
	pendSub     *fakeSub

	noPending        bool            // full+hash pending subscriptions unsupported
	hashPending      bool            // full pending unsupported, hash subscription works
	failBlocks       map[uint64]bool // BlockByNumber errors for these numbers
	maxLogRange      uint64          // eth_getLogs range cap (0 = unlimited)
	noBlockReceipts  bool            // eth_getBlockReceipts unsupported
	batchErr         error           // transient error served by every batch method
	receiptFailures  int             // fail this many BlockReceipts calls transiently
	poolTxs          []BlockTx       // served by TxPoolContent
	blockReceiptHits int             // how many BlockReceipts calls were made
	filterLogsHits   int             // how many FilterLogs calls were made
	headerRefsHits   int             // how many HeaderRefsByNumbers batches were made
	batchHits        int             // how many batch requests were made
	multiBatchHits   int             // how many multi-block batch requests were made
	finalizedNum     uint64          // served by BlockNumberByTag (0 = unsupported)
}

// BlockBundleByNumber composes the fake's individual calls into one
// "batch" result, mirroring the real client's semantics.
func (f *fakeClient) BlockBundleByNumber(ctx context.Context, number uint64) (BlockBundle, error) {
	f.mu.Lock()
	f.batchHits++
	f.mu.Unlock()
	if err := f.transientBatchErr(); err != nil {
		return BlockBundle{}, err
	}
	block, err := f.BlockByNumber(ctx, number)
	if err != nil {
		return BlockBundle{}, err
	}
	receipts, rerr := f.BlockReceipts(ctx, block.Hash)
	return BlockBundle{Block: block, Receipts: receipts, ReceiptsErr: rerr}, nil
}

// BlockBundles mirrors the real client's multi-block batch: per-block
// errors, outer error only when the provider rejects the batch.
func (f *fakeClient) BlockBundles(ctx context.Context, numbers []uint64, withReceipts bool) ([]BlockBundle, []error, error) {
	f.mu.Lock()
	f.multiBatchHits++
	f.mu.Unlock()
	if err := f.transientBatchErr(); err != nil {
		return nil, nil, err
	}
	bundles := make([]BlockBundle, len(numbers))
	errs := make([]error, len(numbers))
	for i, n := range numbers {
		block, err := f.BlockByNumber(ctx, n)
		if err != nil {
			errs[i] = err
			continue
		}
		bundles[i] = BlockBundle{Block: block}
		if withReceipts {
			bundles[i].Receipts, bundles[i].ReceiptsErr = f.BlockReceipts(ctx, block.Hash)
		}
	}
	return bundles, errs, nil
}

func (f *fakeClient) BlockBundleByHash(ctx context.Context, hash common.Hash) (BlockBundle, error) {
	f.mu.Lock()
	f.batchHits++
	f.mu.Unlock()
	if err := f.transientBatchErr(); err != nil {
		return BlockBundle{}, err
	}
	block, err := f.BlockByHash(ctx, hash)
	if err != nil {
		return BlockBundle{}, err
	}
	receipts, rerr := f.BlockReceipts(ctx, hash)
	return BlockBundle{Block: block, Receipts: receipts, ReceiptsErr: rerr}, nil
}

func (f *fakeClient) TransactionReceipts(ctx context.Context, hashes []common.Hash) ([]*types.Receipt, []error, error) {
	f.mu.Lock()
	f.batchHits++
	f.mu.Unlock()
	if err := f.transientBatchErr(); err != nil {
		return nil, nil, err
	}
	receipts := make([]*types.Receipt, len(hashes))
	errs := make([]error, len(hashes))
	for i, h := range hashes {
		receipts[i], errs[i] = f.TransactionReceipt(ctx, h)
	}
	return receipts, errs, nil
}

func (f *fakeClient) BlockNumberByTag(context.Context, string) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.finalizedNum == 0 {
		return 0, errors.New("finality tags unsupported")
	}
	return f.finalizedNum, nil
}

func (f *fakeClient) setFinalized(n uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.finalizedNum = n
}

func newFakeClient() *fakeClient {
	return &fakeClient{
		chainID:  testChainID,
		blocks:   map[common.Hash]*Block{},
		byNumber: map[uint64]*Block{},
		receipts: map[common.Hash]*types.Receipt{},
		logs:     map[common.Hash][]types.Log{},
		pendTxs:  map[common.Hash]*types.Transaction{},
		served:   map[common.Hash]struct{}{},
	}
}

func (f *fakeClient) ChainID(context.Context) (*big.Int, error) { return f.chainID, nil }

func (f *fakeClient) SubscribeNewHead(_ context.Context, ch chan<- *types.Header) (ethereum.Subscription, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.heads = ch
	f.headSubs++
	f.headSub = newFakeSub()
	return f.headSub, nil
}

func (f *fakeClient) SubscribeFullPendingTransactions(_ context.Context, ch chan<- *types.Transaction) (ethereum.Subscription, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.noPending || f.hashPending {
		return nil, errors.New("notifications not supported")
	}
	f.pending = ch
	f.pendSub = newFakeSub()
	return f.pendSub, nil
}

func (f *fakeClient) SubscribePendingTransactions(_ context.Context, ch chan<- common.Hash) (ethereum.Subscription, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.hashPending {
		return nil, errors.New("notifications not supported")
	}
	f.pendingHash = ch
	f.pendSub = newFakeSub()
	return f.pendSub, nil
}

func (f *fakeClient) BlockByHash(_ context.Context, hash common.Hash) (*Block, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if b, ok := f.blocks[hash]; ok {
		f.served[hash] = struct{}{}
		return b, nil
	}
	return nil, ethereum.NotFound
}

// servedBlock reports whether the indexer has fetched a block by hash.
func (f *fakeClient) servedBlock(hash common.Hash) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.served[hash]
	return ok
}

func (f *fakeClient) BlockByNumber(_ context.Context, number uint64) (*Block, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failBlocks[number] {
		return nil, errors.New("injected block fetch failure")
	}
	if b, ok := f.byNumber[number]; ok {
		return b, nil
	}
	return nil, ethereum.NotFound
}

func (f *fakeClient) transientBatchErr() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.batchErr
}

func (f *fakeClient) TransactionReceipt(_ context.Context, hash common.Hash) (*types.Receipt, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if r, ok := f.receipts[hash]; ok {
		return r, nil
	}
	return nil, ethereum.NotFound
}

// TransactionsByHash mirrors the real client's batch: per-item errors,
// outer error only when the provider rejects the batch. Pending txs are
// converted to the BlockTx shape; mined txs are looked up in blocks.
func (f *fakeClient) TransactionsByHash(_ context.Context, hashes []common.Hash) ([]*BlockTx, []error, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.multiBatchHits++
	if f.batchErr != nil {
		return nil, nil, f.batchErr
	}
	txs := make([]*BlockTx, len(hashes))
	errs := make([]error, len(hashes))
	signer := types.LatestSignerForChainID(testChainID)
	for i, h := range hashes {
		if tx, ok := f.pendTxs[h]; ok {
			from, err := types.Sender(signer, tx)
			if err != nil {
				errs[i] = err
				continue
			}
			txs[i] = &BlockTx{Hash: h, From: from, To: tx.To(), Value: (*hexutil.Big)(tx.Value()), Input: tx.Data(), Nonce: hexutil.Uint64(tx.Nonce())}
			continue
		}
		// Mined txs are known via their receipt's block.
		if r, ok := f.receipts[h]; ok {
			if b, bok := f.blocks[r.BlockHash]; bok {
				for _, btx := range b.Transactions {
					if btx.Hash == h {
						cp := btx
						txs[i] = &cp
						break
					}
				}
			}
		}
		if txs[i] == nil {
			errs[i] = ethereum.NotFound
		}
	}
	return txs, errs, nil
}

func (f *fakeClient) HeaderRefsByNumbers(_ context.Context, numbers []uint64) ([]*Block, []error, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.multiBatchHits++
	f.headerRefsHits++
	if f.batchErr != nil {
		return nil, nil, f.batchErr
	}
	blocks := make([]*Block, len(numbers))
	errs := make([]error, len(numbers))
	for i, n := range numbers {
		if b, ok := f.byNumber[n]; ok {
			blocks[i] = &Block{Hash: b.Hash, ParentHash: b.ParentHash, Number: b.Number, Time: b.Time}
		} else {
			errs[i] = ethereum.NotFound
		}
	}
	return blocks, errs, nil
}

func (f *fakeClient) FilterLogsBatch(ctx context.Context, queries []ethereum.FilterQuery) ([][]types.Log, []error, error) {
	f.mu.Lock()
	f.multiBatchHits++
	f.mu.Unlock()
	if err := f.transientBatchErr(); err != nil {
		return nil, nil, err
	}
	results := make([][]types.Log, len(queries))
	errs := make([]error, len(queries))
	for i, q := range queries {
		results[i], errs[i] = f.FilterLogs(ctx, q)
	}
	return results, errs, nil
}

func (f *fakeClient) FilterLogs(_ context.Context, q ethereum.FilterQuery) ([]types.Log, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.filterLogsHits++
	if q.FromBlock == nil || q.ToBlock == nil {
		return nil, errors.New("fake only supports number-range queries")
	}
	if f.maxLogRange > 0 && q.ToBlock.Uint64()-q.FromBlock.Uint64()+1 > f.maxLogRange {
		return nil, errors.New("query exceeds max block range")
	}
	// The fake trusts the caller's topic filter and returns everything
	// recorded for the canonical blocks in range; MatchLog re-checks all
	// conditions anyway.
	var out []types.Log
	for n := q.FromBlock.Uint64(); n <= q.ToBlock.Uint64(); n++ {
		b, ok := f.byNumber[n]
		if !ok {
			continue
		}
		for _, lg := range f.logs[b.Hash] {
			lg.BlockNumber = n
			out = append(out, lg)
		}
	}
	return out, nil
}

func (f *fakeClient) TxPoolStatus(context.Context) (uint64, uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.poolTxs == nil {
		return 0, 0, errors.New("txpool_status not supported")
	}
	return uint64(len(f.poolTxs)), 0, nil
}

func (f *fakeClient) TxPoolContent(context.Context) ([]BlockTx, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.poolTxs == nil {
		return nil, errors.New("txpool_content not supported")
	}
	return f.poolTxs, nil
}

func (f *fakeClient) BlockReceipts(_ context.Context, blockHash common.Hash) ([]BlockReceipt, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.noBlockReceipts {
		return nil, errors.New("the method eth_getBlockReceipts does not exist")
	}
	if f.receiptFailures > 0 {
		f.receiptFailures--
		return nil, errors.New("connection reset by peer") // transient
	}
	f.blockReceiptHits++
	b, ok := f.blocks[blockHash]
	if !ok {
		return nil, ethereum.NotFound
	}
	var out []BlockReceipt
	for _, tx := range b.Transactions {
		r, ok := f.receipts[tx.Hash]
		if !ok {
			continue
		}
		br := BlockReceipt{
			TxHash:    tx.Hash,
			Status:    hexutil.Uint64(r.Status),
			BlockHash: blockHash,
		}
		for _, lg := range f.logs[blockHash] {
			if lg.TxHash == tx.Hash {
				lg.BlockHash = blockHash
				br.Logs = append(br.Logs, lg)
			}
		}
		out = append(out, br)
	}
	return out, nil
}

func (f *fakeClient) Close() {}

// fakeBlock pairs the header delivered on the heads channel with the
// raw Block served by BlockByHash/BlockByNumber; header.Hash() is the
// block's identity, exactly as with a real node.
type fakeBlock struct {
	header *types.Header
	block  *Block
}

func (b *fakeBlock) Hash() common.Hash { return b.block.Hash }

// addBlock registers a canonical block built on parent, containing txs.
func (f *fakeClient) addBlock(number uint64, parent common.Hash, txs ...*types.Transaction) *fakeBlock {
	return f.addForkBlock(number, parent, nil, txs...)
}

// addForkBlock is addBlock with extra header bytes so competing blocks
// at the same height get distinct hashes.
func (f *fakeClient) addForkBlock(number uint64, parent common.Hash, extra []byte, txs ...*types.Transaction) *fakeBlock {
	header := &types.Header{
		Number:     new(big.Int).SetUint64(number),
		ParentHash: parent,
		Difficulty: big.NewInt(0),
		Time:       number,
		Extra:      extra,
	}
	hash := header.Hash()
	block := &Block{Hash: hash, ParentHash: parent, Number: hexutil.Uint64(number), Time: hexutil.Uint64(header.Time)}
	signer := types.LatestSignerForChainID(testChainID)
	for _, tx := range txs {
		from, err := types.Sender(signer, tx)
		if err != nil {
			panic(err)
		}
		block.Transactions = append(block.Transactions, BlockTx{
			Hash:  tx.Hash(),
			From:  from,
			To:    tx.To(),
			Value: (*hexutil.Big)(tx.Value()),
			Input: tx.Data(),
			Nonce: hexutil.Uint64(tx.Nonce()),
		})
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.blocks[hash] = block
	f.byNumber[number] = block
	for _, tx := range txs {
		f.receipts[tx.Hash()] = &types.Receipt{
			Status:      types.ReceiptStatusSuccessful,
			TxHash:      tx.Hash(),
			BlockHash:   hash,
			BlockNumber: new(big.Int).SetUint64(number),
		}
	}
	return &fakeBlock{header: header, block: block}
}

func (f *fakeClient) sendHead(b *fakeBlock) {
	f.mu.Lock()
	heads := f.heads
	f.mu.Unlock()
	heads <- b.header
}

// testHarness runs an Indexer against a fakeClient, capturing emitted events.
type testHarness struct {
	client   *fakeClient
	buf      *lockedBuffer
	cancel   context.CancelFunc
	done     chan error
	stopOnce sync.Once
}

// stop shuts the indexer down; safe to call more than once.
func (h *testHarness) stop(t *testing.T) {
	t.Helper()
	h.stopOnce.Do(func() {
		h.cancel()
		select {
		case <-h.done:
		case <-time.After(5 * time.Second):
			t.Error("indexer did not shut down")
		}
	})
}

type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) lines() [][]byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	data := bytes.TrimSpace(b.buf.Bytes())
	if len(data) == 0 {
		return nil
	}
	return bytes.Split(data, []byte("\n"))
}

func startIndexer(t *testing.T, client *fakeClient, confirmations uint64) *testHarness {
	return startIndexerWithStore(t, client, confirmations, nil)
}

func startIndexerWithStore(t *testing.T, client *fakeClient, confirmations uint64, store storage.Storage) *testHarness {
	t.Helper()
	cfg := config.IndexerConfig{
		Name:           "testnet",
		ChainID:        testChainID.Uint64(),
		Confirmations:  config.Confirmations{Depth: confirmations},
		PendingTimeout: 30 * time.Minute,
		TokenList:      []config.ParsedToken{{Address: tokenAddr, Symbol: "TEST", Decimals: 6}},
	}
	return startIndexerWithConfig(t, client, cfg, []common.Address{watchedAddr}, store)
}

// startIndexerWithConfig runs an indexer against the fake and waits for
// its head subscription before returning - sending a head earlier would
// block forever on a nil channel. Mutators run before the indexer starts.
func startIndexerWithConfig(t *testing.T, client *fakeClient, cfg config.IndexerConfig, addresses []common.Address, store storage.Storage, mutate ...func(*Indexer)) *testHarness {
	t.Helper()
	buf := &lockedBuffer{}
	ix := New(cfg, addresses, event.NewEmitter(buf), store)
	// Tests exercise the full restore path by default; production
	// defaults to tip-start (opt in with -resume).
	ix.Resume = true
	ix.dial = func(context.Context) (ChainClient, error) { return client, nil }
	for _, m := range mutate {
		m(ix)
	}

	client.mu.Lock()
	wantSubs := client.headSubs + 1
	client.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- ix.Run(ctx) }()

	h := &testHarness{client: client, buf: buf, cancel: cancel, done: done}
	t.Cleanup(func() { h.stop(t) })

	// Wait for THIS indexer's head subscription to be registered (a
	// previous harness on the same fake may have left a stale one).
	deadline := time.After(5 * time.Second)
	for {
		client.mu.Lock()
		ready := client.headSubs >= wantSubs
		client.mu.Unlock()
		if ready {
			return h
		}
		select {
		case <-deadline:
			t.Fatal("indexer never subscribed to heads")
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// waitForEvents polls until n events are emitted, returning them decoded.
func (h *testHarness) waitForEvents(t *testing.T, n int) []event.Event {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		lines := h.buf.lines()
		if len(lines) >= n {
			out := make([]event.Event, len(lines))
			for i, line := range lines {
				if err := json.Unmarshal(line, &out[i]); err != nil {
					t.Fatalf("bad event line %q: %v", line, err)
				}
			}
			return out
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for %d events, got %d: %s", n, len(lines), h.buf.lines())
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func TestEndToEndNativeTransfer(t *testing.T) {
	key, sender := testKey(t)
	client := newFakeClient()
	h := startIndexer(t, client, 2)

	// The watched address is paid in block 101.
	payment := signedTx(t, key, watchedAddr, big.NewInt(5e18), nil)
	client.pendTxs[payment.Hash()] = payment

	// Pending phase.
	client.mu.Lock()
	pendingCh := client.pending
	client.mu.Unlock()
	pendingCh <- payment
	evs := h.waitForEvents(t, 1)
	if evs[0].Type != event.TypePending || evs[0].From != sender.Hex() || evs[0].Value != "5" {
		t.Fatalf("pending event = %+v", evs[0])
	}
	if evs[0].Indexer != "testnet" || evs[0].ChainID != testChainID.Uint64() {
		t.Fatalf("event not stamped with indexer identity: %+v", evs[0])
	}
	if _, err := time.Parse(time.RFC3339, evs[0].FirstSeen); err != nil {
		t.Fatalf("first_seen %q not RFC3339: %v", evs[0].FirstSeen, err)
	}

	// Mined in block 101.
	b100 := client.addBlock(100, common.Hash{0x99})
	b101 := client.addBlock(101, b100.Hash(), payment)
	client.sendHead(b100)
	client.sendHead(b101)
	evs = h.waitForEvents(t, 2)
	if evs[1].Type != event.TypeMined || evs[1].BlockNumber != 101 || evs[1].Confirmations != 1 {
		t.Fatalf("mined event = %+v", evs[1])
	}

	// Confirmed at 2 confirmations (head 102).
	b102 := client.addBlock(102, b101.Hash())
	client.sendHead(b102)
	evs = h.waitForEvents(t, 3)
	if evs[2].Type != event.TypeConfirmed || evs[2].Confirmations != 2 {
		t.Fatalf("confirmed event = %+v", evs[2])
	}
	if evs[2].TxHash != payment.Hash().Hex() || evs[2].Asset != "ETH" {
		t.Fatalf("confirmed event = %+v", evs[2])
	}
}

func TestEndToEndERC20Log(t *testing.T) {
	key, _ := testKey(t)
	client := newFakeClient()
	client.noPending = true // exercise block-only mode
	h := startIndexer(t, client, 1)

	transfer := signedTx(t, key, tokenAddr, big.NewInt(0), transferCalldata(watchedAddr, big.NewInt(7_500_000)))
	b200 := client.addBlock(200, common.Hash{0x98})
	b201 := client.addBlock(201, b200.Hash(), transfer)

	lg := transferLog(tokenAddr, otherAddr, watchedAddr, big.NewInt(7_500_000), 4)
	lg.TxHash = transfer.Hash()
	lg.BlockHash = b201.Hash()
	client.mu.Lock()
	client.logs[b201.Hash()] = []types.Log{lg}
	client.mu.Unlock()

	client.sendHead(b200)
	client.sendHead(b201)

	// confirmations=1: the threshold is met at discovery, so a single
	// confirmed event is emitted with no mined first.
	evs := h.waitForEvents(t, 1)
	if evs[0].Type != event.TypeConfirmed || evs[0].Asset != "TEST" || evs[0].Value != "7.5" {
		t.Fatalf("confirmed event = %+v", evs[0])
	}
	if evs[0].LogIndex == nil || *evs[0].LogIndex != 4 || evs[0].TokenAddress != tokenAddr.Hex() {
		t.Fatalf("confirmed event = %+v", evs[0])
	}
}

func TestEndToEndReplacedTransaction(t *testing.T) {
	key, _ := testKey(t)
	client := newFakeClient()
	h := startIndexer(t, client, 2)

	// A pending payment to the watched address...
	payment := signedTx(t, key, watchedAddr, big.NewInt(1e18), nil)
	client.mu.Lock()
	pendingCh := client.pending
	client.mu.Unlock()
	pendingCh <- payment
	evs := h.waitForEvents(t, 1)
	if evs[0].Type != event.TypePending {
		t.Fatalf("first event = %+v", evs[0])
	}

	// ...is replaced: a different tx from the same sender and nonce
	// (signedTx always uses nonce 1) mines, paying someone else.
	replacement := signedTx(t, key, otherAddr, big.NewInt(1), nil)
	b400 := client.addBlock(400, common.Hash{0x96})
	b401 := client.addBlock(401, b400.Hash(), replacement)
	client.sendHead(b400)
	client.sendHead(b401)

	evs = h.waitForEvents(t, 2)
	if evs[1].Type != event.TypeReplaced {
		t.Fatalf("second event = %+v", evs[1])
	}
	if evs[1].TxHash != payment.Hash().Hex() || evs[1].ReplacedBy != replacement.Hash().Hex() {
		t.Fatalf("replaced event = %+v", evs[1])
	}
	if evs[1].FirstSeen != evs[0].FirstSeen {
		t.Errorf("first_seen changed: %s vs %s", evs[1].FirstSeen, evs[0].FirstSeen)
	}
}

func TestEndToEndCatchUpLogFallback(t *testing.T) {
	key, _ := testKey(t)
	client := newFakeClient()
	client.noPending = true
	client.maxLogRange = 1 // provider rejects multi-block getLogs ranges
	h := startIndexer(t, client, 3)

	// Establish a tip, then let the chain run ahead with a token
	// transfer buried mid-gap.
	b600 := client.addBlock(600, common.Hash{0x94})
	client.sendHead(b600)

	transfer := signedTx(t, key, tokenAddr, big.NewInt(0), transferCalldata(watchedAddr, big.NewInt(4_000_000)))
	prev := b600
	var b605 *fakeBlock
	for n := uint64(601); n <= 610; n++ {
		if n == 605 {
			prev = client.addBlock(n, prev.Hash(), transfer)
			b605 = prev
			lg := transferLog(tokenAddr, otherAddr, watchedAddr, big.NewInt(4_000_000), 2)
			lg.TxHash = transfer.Hash()
			lg.BlockHash = b605.Hash()
			client.mu.Lock()
			client.logs[b605.Hash()] = []types.Log{lg}
			client.mu.Unlock()
		} else {
			prev = client.addBlock(n, prev.Hash())
		}
	}
	client.sendHead(prev) // head 610 -> gap, catch-up 601-610

	// The range log query fails, the fallback still finds the transfer:
	// discovered at depth 610-605+1=6, past threshold 3 -> a single
	// confirmed event, no mined first.
	evs := h.waitForEvents(t, 1)
	if evs[0].Type != event.TypeConfirmed || evs[0].Asset != "TEST" || evs[0].Value != "4" {
		t.Fatalf("confirmed event = %+v", evs[0])
	}
	if evs[0].BlockNumber != 605 || evs[0].Confirmations != 6 {
		t.Fatalf("confirmed event = %+v", evs[0])
	}
	if evs[0].TxHash != transfer.Hash().Hex() {
		t.Fatalf("confirmed event = %+v", evs[0])
	}
}

func TestEndToEndHashPendingTier(t *testing.T) {
	key, sender := testKey(t)
	client := newFakeClient()
	client.hashPending = true // full-object subscription unsupported
	h := startIndexer(t, client, 2)

	payment := signedTx(t, key, watchedAddr, big.NewInt(3e18), nil)
	client.mu.Lock()
	client.pendTxs[payment.Hash()] = payment
	hashCh := client.pendingHash
	client.mu.Unlock()
	if hashCh == nil {
		t.Fatal("hash subscription never registered")
	}
	hashCh <- payment.Hash()

	// The worker pool resolves the hash via a batched lookup and the
	// pending event flows.
	evs := h.waitForEvents(t, 1)
	if evs[0].Type != event.TypePending || evs[0].From != sender.Hex() || evs[0].Value != "3" {
		t.Fatalf("pending event = %+v", evs[0])
	}
}

func TestEndToEndMempoolSnapshot(t *testing.T) {
	key, sender := testKey(t)
	client := newFakeClient()
	client.noPending = true // snapshot is the only pending source

	// A matching payment is already sitting in the mempool at startup.
	waiting := signedTx(t, key, watchedAddr, big.NewInt(7e18), nil)
	client.poolTxs = []BlockTx{{
		Hash:  waiting.Hash(),
		From:  sender,
		To:    waiting.To(),
		Value: (*hexutil.Big)(waiting.Value()),
		Input: waiting.Data(),
		Nonce: hexutil.Uint64(waiting.Nonce()),
	}}

	h := startIndexer(t, client, 2)
	evs := h.waitForEvents(t, 1)
	if evs[0].Type != event.TypePending || evs[0].TxHash != waiting.Hash().Hex() || evs[0].Value != "7" {
		t.Fatalf("pending event = %+v", evs[0])
	}
}

func TestEndToEndReorgDuringCatchUp(t *testing.T) {
	key, _ := testKey(t)
	client := newFakeClient()
	client.noPending = true
	h := startIndexer(t, client, 3)

	// Tip at 700, then a gap 701-710 with a payment at 705.
	b700 := client.addBlock(700, common.Hash{0x93})
	payment := signedTx(t, key, watchedAddr, big.NewInt(1e18), nil)
	prev := b700
	var b709 *fakeBlock
	for n := uint64(701); n <= 710; n++ {
		if n == 705 {
			prev = client.addBlock(n, prev.Hash(), payment)
		} else {
			prev = client.addBlock(n, prev.Hash())
		}
		if n == 709 {
			b709 = prev
		}
	}
	b710 := prev

	// A competing fork replaces 710 and extends to 711 while the
	// catch-up for 710 is still running: the heads are queued back to
	// back, so drainLive meets the fork mid-scan.
	b710b := client.addForkBlock(710, b709.Hash(), []byte("fork"))
	b711 := client.addBlock(711, b710b.Hash())
	client.sendHead(b700) // extend, tip 700
	client.sendHead(b710) // gap: catch-up 701-710 starts
	client.sendHead(b711) // fork tip, drained during catch-up

	// The payment at 705 is below the fork point and discovered already
	// past the threshold, so it confirms exactly once (no mined event)
	// despite the mid-scan reorg.
	evs := h.waitForEvents(t, 1)
	if evs[0].Type != event.TypeConfirmed || evs[0].BlockNumber != 705 {
		t.Fatalf("confirmed event = %+v", evs[0])
	}
	if evs[0].TxHash != payment.Hash().Hex() {
		t.Fatalf("confirmed event = %+v", evs[0])
	}
	for _, ev := range h.waitForEvents(t, 1)[1:] {
		t.Errorf("unexpected extra event: %+v", ev)
	}

	// The indexer keeps following the fork chain afterwards.
	b712 := client.addBlock(712, b711.Hash())
	client.sendHead(b712)
	deadline := time.After(5 * time.Second)
	for !client.servedBlock(b712.Hash()) {
		select {
		case <-deadline:
			t.Fatal("indexer did not follow the fork chain after catch-up")
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func TestEndToEndBlockReceiptsFallback(t *testing.T) {
	key, _ := testKey(t)
	client := newFakeClient()
	client.noPending = true
	client.noBlockReceipts = true // provider lacks eth_getBlockReceipts
	h := startIndexer(t, client, 1)

	payment := signedTx(t, key, watchedAddr, big.NewInt(6e18), nil)
	b800 := client.addBlock(800, common.Hash{0x92})
	b801 := client.addBlock(801, b800.Hash(), payment)
	client.sendHead(b800)
	client.sendHead(b801)

	// Per-transaction receipt fallback still verifies the transfer;
	// confirmations=1 means instant confirm, no mined event.
	evs := h.waitForEvents(t, 1)
	if evs[0].Type != event.TypeConfirmed || evs[0].Value != "6" {
		t.Fatalf("confirmed event = %+v", evs[0])
	}
}

func TestEndToEndFinalizedTag(t *testing.T) {
	key, _ := testKey(t)
	client := newFakeClient()
	client.noPending = true
	client.setFinalized(850) // node's finalized block

	cfg := config.IndexerConfig{
		Name:           "testnet",
		ChainID:        testChainID.Uint64(),
		Confirmations:  config.Confirmations{Tag: "finalized"},
		PendingTimeout: 30 * time.Minute,
	}
	h := startIndexerWithConfig(t, client, cfg, []common.Address{watchedAddr}, nil, func(ix *Indexer) {
		ix.finalizedTTL = 0 // refresh the finality number on every head
	})

	// Payment mines at 900 - far past any depth threshold, but NOT yet
	// finalized (finalized=850), so no confirmed event may fire.
	payment := signedTx(t, key, watchedAddr, big.NewInt(1e18), nil)
	b899 := client.addBlock(899, common.Hash{0x91})
	b900 := client.addBlock(900, b899.Hash(), payment)
	client.sendHead(b899)
	client.sendHead(b900)
	prev := b900
	for n := uint64(901); n <= 920; n++ {
		prev = client.addBlock(n, prev.Hash())
		client.sendHead(prev)
	}
	evs := h.waitForEvents(t, 1)
	if evs[0].Type != event.TypeMined || evs[0].BlockNumber != 900 {
		t.Fatalf("mined event = %+v", evs[0])
	}

	// Barrier: once block 921 has been fetched, head 920 (and its
	// lifecycle pass with finalized=850) is fully processed - the tx
	// must still be unconfirmed despite being 21 blocks deep.
	b921 := client.addBlock(921, prev.Hash())
	client.sendHead(b921)
	deadline := time.After(5 * time.Second)
	for !client.servedBlock(b921.Hash()) {
		select {
		case <-deadline:
			t.Fatal("head 921 never processed")
		case <-time.After(5 * time.Millisecond):
		}
	}
	if lines := h.buf.lines(); len(lines) != 1 {
		t.Fatalf("tx confirmed by depth despite finalized=850: %s", lines)
	}

	// The node finalizes past the payment's block; the next head picks
	// it up (zero cache TTL in this test).
	client.setFinalized(905)
	client.sendHead(client.addBlock(922, b921.Hash()))

	evs = h.waitForEvents(t, 2)
	if evs[1].Type != event.TypeConfirmed || evs[1].TxHash != payment.Hash().Hex() {
		t.Fatalf("confirmed event = %+v", evs[1])
	}
	// Confirmation happened at head 921 or 922 (never during phase 1,
	// which would show a depth <= 21).
	if evs[1].Confirmations < 22 {
		t.Errorf("confirmations = %d: confirmed before the finalized tag reached the block", evs[1].Confirmations)
	}
}

func TestInstantConfirmPersisted(t *testing.T) {
	store, err := storage.OpenSQLite(filepath.Join(t.TempDir(), "instant.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	key, _ := testKey(t)
	client := newFakeClient()
	client.noPending = true
	h := startIndexerWithStore(t, client, 1, store) // instant confirm at tip

	payment := signedTx(t, key, watchedAddr, big.NewInt(9e18), nil)
	b950 := client.addBlock(950, common.Hash{0x90})
	b951 := client.addBlock(951, b950.Hash(), payment)
	client.sendHead(b950)
	client.sendHead(b951)

	evs := h.waitForEvents(t, 1)
	if evs[0].Type != event.TypeConfirmed {
		t.Fatalf("event = %+v", evs[0])
	}

	// The tx went terminal without ever being stored as mined; the
	// snapshot must have been reconstructed from the events.
	var row *storage.TrackedTx
	deadline := time.After(5 * time.Second)
	for row == nil {
		all, err := loadRow(store, "testnet", payment.Hash().Hex())
		if err != nil {
			t.Fatal(err)
		}
		row = all
		select {
		case <-deadline:
			t.Fatal("confirmed tx never persisted")
		case <-time.After(5 * time.Millisecond):
		}
	}
	if row.Status != "confirmed" || row.BlockNumber != 951 {
		t.Fatalf("row = %+v", row)
	}
	if len(row.Transfers) != 1 || row.Transfers[0].ValueRaw != "9000000000000000000" || row.Transfers[0].Decimals != 18 {
		t.Fatalf("row transfers = %+v", row.Transfers)
	}
}

// loadRow fetches one transaction row regardless of status.
func loadRow(store storage.Storage, indexer, hash string) (*storage.TrackedTx, error) {
	// LoadActiveTransactions excludes terminal rows, so probe via the
	// update path: a status update that reports "row exists".
	updated, err := store.UpdateTransactionStatus(context.Background(), indexer, hash, "confirmed", "")
	if err != nil || !updated {
		return nil, err
	}
	// Row exists; fetch its content by flipping it active briefly.
	if _, err := store.UpdateTransactionStatus(context.Background(), indexer, hash, "mined", ""); err != nil {
		return nil, err
	}
	defer store.UpdateTransactionStatus(context.Background(), indexer, hash, "confirmed", "")
	txs, err := store.LoadActiveTransactions(context.Background(), indexer)
	if err != nil {
		return nil, err
	}
	for _, tx := range txs {
		if tx.Hash == hash {
			tx.Status = "confirmed"
			return &tx, nil
		}
	}
	return nil, nil
}

// manyAddresses returns a watched set exceeding the server-filter
// threshold, including the standard fixture address.
func manyAddresses() []common.Address {
	out := make([]common.Address, 0, serverFilterMaxAddresses+2)
	out = append(out, watchedAddr)
	for i := 0; i <= serverFilterMaxAddresses; i++ {
		out = append(out, common.BigToAddress(big.NewInt(int64(0x10000+i))))
	}
	return out
}

func TestTransferTopicFiltersModes(t *testing.T) {
	small := &Indexer{matcher: NewMatcher([]common.Address{watchedAddr}, []config.ParsedToken{{Address: tokenAddr, Symbol: "TEST", Decimals: 6}}, testChainID, "")}
	if got := small.transferTopicFilters(); len(got) != 2 || len(got[0]) != 3 || len(got[1]) != 2 {
		t.Fatalf("small set filters = %v", got)
	}
	if small.initialLogChunk() != 500 {
		t.Errorf("small chunk = %d", small.initialLogChunk())
	}

	large := &Indexer{matcher: NewMatcher(manyAddresses(), []config.ParsedToken{{Address: tokenAddr, Symbol: "TEST", Decimals: 6}}, testChainID, "")}
	got := large.transferTopicFilters()
	if len(got) != 1 || len(got[0]) != 1 || got[0][0][0] != transferTopic {
		t.Fatalf("large set filters = %v", got)
	}
	if large.initialLogChunk() != 100 {
		t.Errorf("large chunk = %d", large.initialLogChunk())
	}
}

func TestEndToEndClientSideFiltering(t *testing.T) {
	key, _ := testKey(t)
	client := newFakeClient()
	client.noPending = true

	cfg := config.IndexerConfig{
		Name:           "testnet",
		ChainID:        testChainID.Uint64(),
		Confirmations:  config.Confirmations{Depth: 1},
		PendingTimeout: 30 * time.Minute,
		TokenList:      []config.ParsedToken{{Address: tokenAddr, Symbol: "TEST", Decimals: 6}},
	}
	h := startIndexerWithConfig(t, client, cfg, manyAddresses(), nil)

	// An ERC20 transfer to the watched address plus a decoy transfer to
	// an unwatched one in the same transaction - the receipts-derived
	// logs contain both, local matching must keep only the real one.
	transfer := signedTx(t, key, tokenAddr, big.NewInt(0), transferCalldata(watchedAddr, big.NewInt(1_000_000)))
	b100 := client.addBlock(1000, common.Hash{0x89})
	b101 := client.addBlock(1001, b100.Hash(), transfer)
	lg := transferLog(tokenAddr, otherAddr, watchedAddr, big.NewInt(1_000_000), 1)
	lg.TxHash = transfer.Hash()
	lg.BlockHash = b101.Hash()
	decoy := transferLog(tokenAddr, otherAddr, common.BigToAddress(big.NewInt(0xdead)), big.NewInt(5), 2)
	decoy.TxHash = transfer.Hash()
	decoy.BlockHash = b101.Hash()
	client.mu.Lock()
	client.logs[b101.Hash()] = []types.Log{lg, decoy}
	client.mu.Unlock()

	client.sendHead(b100)
	client.sendHead(b101)

	evs := h.waitForEvents(t, 1)
	if evs[0].Type != event.TypeConfirmed || evs[0].Value != "1" || evs[0].To != watchedAddr.Hex() {
		t.Fatalf("event = %+v", evs[0])
	}
	if lines := h.buf.lines(); len(lines) != 1 {
		t.Fatalf("decoy transfer leaked: %s", lines)
	}

	// At the tip in client-filter mode, logs come from block receipts:
	// no getLogs call may have happened.
	client.mu.Lock()
	filterHits, receiptHits := client.filterLogsHits, client.blockReceiptHits
	client.mu.Unlock()
	if filterHits != 0 {
		t.Errorf("eth_getLogs called %d times, want 0 (receipts-derived logs)", filterHits)
	}
	if receiptHits == 0 {
		t.Error("eth_getBlockReceipts never called")
	}
}

func TestEndToEndCatchUpClientMode(t *testing.T) {
	key, _ := testKey(t)
	client := newFakeClient()
	client.noPending = true

	cfg := config.IndexerConfig{
		Name:           "testnet",
		ChainID:        testChainID.Uint64(),
		Confirmations:  config.Confirmations{Depth: 3},
		PendingTimeout: 30 * time.Minute,
		TokenList:      []config.ParsedToken{{Address: tokenAddr, Symbol: "TEST", Decimals: 6}},
	}
	h := startIndexerWithConfig(t, client, cfg, manyAddresses(), nil)

	// Tip at 1200, then a 10-block gap with a token transfer at 1205.
	b1200 := client.addBlock(1200, common.Hash{0x87})
	client.sendHead(b1200)

	transfer := signedTx(t, key, tokenAddr, big.NewInt(0), transferCalldata(watchedAddr, big.NewInt(3_000_000)))
	prev := b1200
	for n := uint64(1201); n <= 1210; n++ {
		if n == 1205 {
			prev = client.addBlock(n, prev.Hash(), transfer)
			lg := transferLog(tokenAddr, otherAddr, watchedAddr, big.NewInt(3_000_000), 1)
			lg.TxHash = transfer.Hash()
			lg.BlockHash = prev.Hash()
			client.mu.Lock()
			client.logs[prev.Hash()] = []types.Log{lg}
			client.mu.Unlock()
		} else {
			prev = client.addBlock(n, prev.Hash())
		}
	}
	client.sendHead(prev) // gap -> receipts-driven catch-up

	// Discovered at depth 1210-1205+1=6 >= 3: instant confirmed.
	evs := h.waitForEvents(t, 1)
	if evs[0].Type != event.TypeConfirmed || evs[0].Value != "3" || evs[0].BlockNumber != 1205 {
		t.Fatalf("event = %+v", evs[0])
	}

	// The whole catch-up ran on prefetched receipts: no getLogs at all.
	client.mu.Lock()
	filterHits, receiptHits := client.filterLogsHits, client.blockReceiptHits
	client.mu.Unlock()
	if filterHits != 0 {
		t.Errorf("eth_getLogs called %d times during client-mode catch-up, want 0", filterHits)
	}
	if receiptHits < 10 {
		t.Errorf("block receipts fetched %d times, want one per backfilled block", receiptHits)
	}
}

func TestMethodUnsupportedClassification(t *testing.T) {
	unsupported := []string{
		"the method eth_getBlockReceipts does not exist/is not available",
		"Method Not Found",
		"this request method is not supported",
		"rpc method is unsupported",
	}
	for _, s := range unsupported {
		if !methodUnsupported(errors.New(s)) {
			t.Errorf("%q should classify as unsupported", s)
		}
	}
	transient := []string{
		"connection reset by peer",
		"block not found",
		"context deadline exceeded",
		"429 Too Many Requests",
	}
	for _, s := range transient {
		if methodUnsupported(errors.New(s)) {
			t.Errorf("%q should classify as transient", s)
		}
	}
	if methodUnsupported(nil) {
		t.Error("nil error classified as unsupported")
	}
}

func TestEndToEndCatchUpTransientReceiptFailure(t *testing.T) {
	oldDelay := fetchRetryDelay
	fetchRetryDelay = time.Millisecond
	t.Cleanup(func() { fetchRetryDelay = oldDelay })

	key, _ := testKey(t)
	client := newFakeClient()
	client.noPending = true
	// More consecutive failures than one retry window: exactly one
	// block's receipts fetch gives up; everything after succeeds.
	client.receiptFailures = fetchRetries + 1

	cfg := config.IndexerConfig{
		Name:           "testnet",
		ChainID:        testChainID.Uint64(),
		Confirmations:  config.Confirmations{Depth: 3},
		PendingTimeout: 30 * time.Minute,
		TokenList:      []config.ParsedToken{{Address: tokenAddr, Symbol: "TEST", Decimals: 6}},
	}
	h := startIndexerWithConfig(t, client, cfg, manyAddresses(), nil)

	b1300 := client.addBlock(1300, common.Hash{0x86})
	client.sendHead(b1300)

	transfer := signedTx(t, key, tokenAddr, big.NewInt(0), transferCalldata(watchedAddr, big.NewInt(4_000_000)))
	prev := b1300
	for n := uint64(1301); n <= 1310; n++ {
		if n == 1305 {
			prev = client.addBlock(n, prev.Hash(), transfer)
			lg := transferLog(tokenAddr, otherAddr, watchedAddr, big.NewInt(4_000_000), 1)
			lg.TxHash = transfer.Hash()
			lg.BlockHash = prev.Hash()
			client.mu.Lock()
			client.logs[prev.Hash()] = []types.Log{lg}
			client.mu.Unlock()
		} else {
			prev = client.addBlock(n, prev.Hash())
		}
	}
	client.sendHead(prev)

	// The transfer is found despite the transient failures - via
	// receipts or the chunk fallback, whichever covered block 1305.
	evs := h.waitForEvents(t, 1)
	if evs[0].Type != event.TypeConfirmed || evs[0].BlockNumber != 1305 {
		t.Fatalf("event = %+v", evs[0])
	}

	// The transient errors must NOT have permanently disabled block
	// receipts: fresh blocks at the tip keep using them.
	client.mu.Lock()
	before := client.blockReceiptHits
	client.mu.Unlock()
	next := client.addBlock(1311, prev.Hash())
	client.sendHead(next)
	deadline := time.After(5 * time.Second)
	for {
		client.mu.Lock()
		after := client.blockReceiptHits
		client.mu.Unlock()
		if after > before {
			break
		}
		select {
		case <-deadline:
			t.Fatal("block receipts permanently disabled by a transient failure")
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func TestEndToEndClientSideFilteringWithoutBlockReceipts(t *testing.T) {
	key, _ := testKey(t)
	client := newFakeClient()
	client.noPending = true
	client.noBlockReceipts = true // degrade to unfiltered getLogs

	cfg := config.IndexerConfig{
		Name:           "testnet",
		ChainID:        testChainID.Uint64(),
		Confirmations:  config.Confirmations{Depth: 1},
		PendingTimeout: 30 * time.Minute,
		TokenList:      []config.ParsedToken{{Address: tokenAddr, Symbol: "TEST", Decimals: 6}},
	}
	h := startIndexerWithConfig(t, client, cfg, manyAddresses(), nil)

	transfer := signedTx(t, key, tokenAddr, big.NewInt(0), transferCalldata(watchedAddr, big.NewInt(2_000_000)))
	b110 := client.addBlock(1100, common.Hash{0x88})
	b111 := client.addBlock(1101, b110.Hash(), transfer)
	lg := transferLog(tokenAddr, otherAddr, watchedAddr, big.NewInt(2_000_000), 1)
	lg.TxHash = transfer.Hash()
	lg.BlockHash = b111.Hash()
	client.mu.Lock()
	client.logs[b111.Hash()] = []types.Log{lg}
	client.mu.Unlock()

	client.sendHead(b110)
	client.sendHead(b111)

	evs := h.waitForEvents(t, 1)
	if evs[0].Type != event.TypeConfirmed || evs[0].Value != "2" {
		t.Fatalf("event = %+v", evs[0])
	}
	client.mu.Lock()
	filterHits := client.filterLogsHits
	client.mu.Unlock()
	if filterHits == 0 {
		t.Error("expected getLogs fallback when block receipts are unsupported")
	}
}

func TestEndToEndReorgToLowerHead(t *testing.T) {
	key, _ := testKey(t)
	client := newFakeClient()
	client.noPending = true
	h := startIndexer(t, client, 3)

	// Canonical chain up to 1520 with a payment at 1515.
	payment := signedTx(t, key, watchedAddr, big.NewInt(1e18), nil)
	prev := client.addBlock(1500, common.Hash{0x84})
	client.sendHead(prev)
	fork := prev // 1500 is the eventual fork point
	for n := uint64(1501); n <= 1520; n++ {
		if n == 1515 {
			prev = client.addBlock(n, prev.Hash(), payment)
		} else {
			prev = client.addBlock(n, prev.Hash())
		}
		client.sendHead(prev)
	}
	// Payment mined then confirmed on the original chain.
	evs := h.waitForEvents(t, 2)
	if evs[0].Type != event.TypeMined || evs[1].Type != event.TypeConfirmed {
		t.Fatalf("events = %+v", evs)
	}

	// Deep reorg onto a fork whose head (1510) is LOWER than our tip
	// (1520): everything after 1500 is replaced.
	fprev := fork
	for n := uint64(1501); n <= 1510; n++ {
		fprev = client.addForkBlock(n, fprev.Hash(), []byte("fork"))
	}
	client.sendHead(fprev) // fork head 1510 -> reorg

	// Then the fork chain advances one block at a time. Each of these
	// must classify as a clean extend - the bug was that the orphaned
	// tip (1520) made every one of them a phantom reorg.
	headerFetchesAfterReorg := -1
	for n := uint64(1511); n <= 1525; n++ {
		fprev = client.addForkBlock(n, fprev.Hash(), []byte("fork"))
		client.sendHead(fprev)
		deadline := time.After(5 * time.Second)
		for !client.servedBlock(fprev.Hash()) {
			select {
			case <-deadline:
				t.Fatalf("fork head %d never processed", n)
			case <-time.After(2 * time.Millisecond):
			}
		}
		client.mu.Lock()
		hits := client.headerRefsHits
		client.mu.Unlock()
		if headerFetchesAfterReorg == -1 {
			headerFetchesAfterReorg = hits // baseline after the real reorg
		} else if hits != headerFetchesAfterReorg {
			t.Fatalf("head %d triggered ancestor walking (phantom reorg): header fetches %d -> %d",
				n, headerFetchesAfterReorg, hits)
		}
	}
}

func TestEndToEndResumeFromStorage(t *testing.T) {
	store, err := storage.OpenSQLite(filepath.Join(t.TempDir(), "resume.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	key, _ := testKey(t)
	client := newFakeClient()
	client.noPending = true

	// First process: the payment mines at block 501 but the process
	// stops before it reaches 3 confirmations.
	h1 := startIndexerWithStore(t, client, 3, store)
	payment := signedTx(t, key, watchedAddr, big.NewInt(2e18), nil)
	b500 := client.addBlock(500, common.Hash{0x95})
	b501 := client.addBlock(501, b500.Hash(), payment)
	client.sendHead(b500)
	client.sendHead(b501)
	evs := h1.waitForEvents(t, 1)
	if evs[0].Type != event.TypeMined {
		t.Fatalf("first event = %+v", evs[0])
	}
	firstSeen := evs[0].FirstSeen
	h1.stop(t)

	// Chain advances while the process is down.
	b502 := client.addBlock(502, b501.Hash())
	b503 := client.addBlock(503, b502.Hash())

	// Second process resumes from storage: the gap path catches up
	// blocks 502-503 and the tx confirms with its original first_seen,
	// without a duplicate mined event.
	h2 := startIndexerWithStore(t, client, 3, store)
	client.sendHead(b503)
	evs = h2.waitForEvents(t, 1)
	if evs[0].Type != event.TypeConfirmed || evs[0].Confirmations != 3 {
		t.Fatalf("resumed event = %+v", evs[0])
	}
	if evs[0].TxHash != payment.Hash().Hex() {
		t.Fatalf("resumed event = %+v", evs[0])
	}
	if evs[0].FirstSeen != firstSeen {
		t.Errorf("first_seen changed across restart: %s vs %s", evs[0].FirstSeen, firstSeen)
	}
	for _, line := range h2.buf.lines() {
		if bytes.Contains(line, []byte(`"mined"`)) {
			t.Errorf("duplicate mined event after resume: %s", line)
		}
	}

	// The stored row reached its terminal status.
	active, err := store.LoadActiveTransactions(context.Background(), "testnet")
	if err != nil {
		t.Fatal(err)
	}
	if len(active) != 0 {
		t.Errorf("confirmed tx still active in storage: %+v", active)
	}
}

func TestStartAtTipWithoutResume(t *testing.T) {
	store, err := storage.OpenSQLite(filepath.Join(t.TempDir(), "tip.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	key, _ := testKey(t)
	client := newFakeClient()
	client.noPending = true

	// First process: a payment mines at 501, then an unrelated payment
	// lands at 502 after the process stopped.
	h1 := startIndexerWithStore(t, client, 3, store)
	pending := signedTx(t, key, watchedAddr, big.NewInt(2e18), nil)
	b500 := client.addBlock(500, common.Hash{0x95})
	b501 := client.addBlock(501, b500.Hash(), pending)
	client.sendHead(b500)
	client.sendHead(b501)
	h1.waitForEvents(t, 1)
	h1.stop(t)

	missed := signedTx(t, key, watchedAddr, big.NewInt(3e18), nil)
	b502 := client.addBlock(502, b501.Hash(), missed)
	b503 := client.addBlock(503, b502.Hash())
	b504 := client.addBlock(504, b503.Hash())

	// Second process without Resume: it starts at the tip, so the
	// payment in the missed block 502 is never discovered - but the
	// in-flight tx from 501 is restored and still confirms.
	cfg := config.IndexerConfig{
		Name:           "testnet",
		ChainID:        testChainID.Uint64(),
		Confirmations:  config.Confirmations{Depth: 3},
		PendingTimeout: 30 * time.Minute,
		TokenList:      []config.ParsedToken{{Address: tokenAddr, Symbol: "TEST", Decimals: 6}},
	}
	h2 := startIndexerWithConfig(t, client, cfg, []common.Address{watchedAddr}, store,
		func(ix *Indexer) { ix.Resume = false })
	client.sendHead(b504)
	evs := h2.waitForEvents(t, 1)
	if evs[0].Type != event.TypeConfirmed || evs[0].TxHash != pending.Hash().Hex() {
		t.Fatalf("event = %+v, want the restored tx confirmed", evs[0])
	}
	for _, line := range h2.buf.lines() {
		if bytes.Contains(line, []byte(missed.Hash().Hex())) {
			t.Errorf("tx from a missed block was caught up without -resume: %s", line)
		}
	}
}

func TestEmitAllFullEventLogs(t *testing.T) {
	client := newFakeClient()
	cfg := config.IndexerConfig{
		Name:           "testnet",
		ChainID:        testChainID.Uint64(),
		Confirmations:  config.Confirmations{Depth: 3},
		PendingTimeout: 30 * time.Minute,
	}
	ix := New(cfg, []common.Address{watchedAddr}, nil, nil)
	ix.dial = func(context.Context) (ChainClient, error) { return client, nil }

	logBuf := &bytes.Buffer{}
	ix.log = slog.New(slog.NewJSONHandler(logBuf, nil))
	ev := event.Event{
		Type: event.TypeMined, Direction: event.DirectionIn,
		TxHash: "0xabc", From: "0xfrom", To: "0xto",
		Asset: "ETH", Decimals: 18, ValueRaw: "1000000000000000000", Value: "1",
		BlockNumber: 100,
	}

	// Full mode: the whole event object, no curated attrs.
	ix.FullEventLogs = true
	ix.emitAll([]event.Event{ev})
	var full map[string]any
	if err := json.Unmarshal(logBuf.Bytes(), &full); err != nil {
		t.Fatalf("log line not JSON: %v: %s", err, logBuf.String())
	}
	evObj, ok := full["event"].(map[string]any)
	if !ok {
		t.Fatalf("no event object in log line: %s", logBuf.String())
	}
	if evObj["tx_hash"] != "0xabc" || evObj["value"] != "1" || evObj["asset"] != "ETH" {
		t.Errorf("event object = %v", evObj)
	}
	if _, curated := full["address"]; curated {
		t.Errorf("curated attrs present in full mode: %s", logBuf.String())
	}

	// Default mode: curated attrs, no event object.
	logBuf.Reset()
	ix.FullEventLogs = false
	ix.emitAll([]event.Event{ev})
	var curated map[string]any
	if err := json.Unmarshal(logBuf.Bytes(), &curated); err != nil {
		t.Fatalf("log line not JSON: %v", err)
	}
	if curated["address"] != "0xto" || curated["tx"] != "0xabc" {
		t.Errorf("curated attrs = %v", curated)
	}
	if _, hasEvent := curated["event"]; hasEvent {
		t.Errorf("event object present in curated mode: %s", logBuf.String())
	}
}

func TestLogValue(t *testing.T) {
	for in, want := range map[string]string{
		"2":                    "2",
		"0.028856":             "0.028856",
		"0.028856123456789012": "0.028856",
		"366.500019":           "366.500019",
		"1.500000":             "1.5",
		"0.000000123":          "0",
		"12345.6789012345":     "12345.678901",
	} {
		if got := logValue(in); got != want {
			t.Errorf("logValue(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestRestoredPendingPromotedAtStartup(t *testing.T) {
	store, err := storage.OpenSQLite(filepath.Join(t.TempDir(), "pending.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	key, sender := testKey(t)
	client := newFakeClient()
	client.noPending = true
	payment := signedTx(t, key, watchedAddr, big.NewInt(2e18), nil)

	// A pending payment persisted by a previous run.
	seen := time.Now().Add(-2 * time.Minute).UTC().Truncate(time.Second)
	if err := store.SaveTransaction(context.Background(), "testnet", storage.TrackedTx{
		Hash:         payment.Hash().Hex(),
		Status:       "pending",
		FirstSeen:    seen,
		PendingSince: seen,
		Sender:       sender.Hex(),
		Nonce:        payment.Nonce(),
		HasNonce:     true,
		Transfers: []storage.Transfer{{
			From: sender.Hex(), To: watchedAddr.Hex(), Direction: "in", Asset: "ETH",
			Decimals: 18, ValueRaw: "2000000000000000000", Value: "2",
			TxSender: sender.Hex(), TxNonce: payment.Nonce(),
		}},
	}); err != nil {
		t.Fatal(err)
	}

	// It mined at block 501 while the process was down. The new process
	// starts at the tip (Resume off) and never scans that block - the
	// first-head recheck must find it via its receipt.
	b500 := client.addBlock(500, common.Hash{0x95})
	b501 := client.addBlock(501, b500.Hash(), payment)
	b502 := client.addBlock(502, b501.Hash())

	cfg := config.IndexerConfig{
		Name:           "testnet",
		ChainID:        testChainID.Uint64(),
		Confirmations:  config.Confirmations{Depth: 3},
		PendingTimeout: 30 * time.Minute,
	}
	h := startIndexerWithConfig(t, client, cfg, []common.Address{watchedAddr}, store,
		func(ix *Indexer) { ix.Resume = false })
	client.sendHead(b502)

	evs := h.waitForEvents(t, 1)
	if evs[0].Type != event.TypeMined || evs[0].TxHash != payment.Hash().Hex() {
		t.Fatalf("event = %+v, want the restored pending tx mined", evs[0])
	}
	if evs[0].BlockNumber != 501 {
		t.Errorf("block = %d, want the unscanned inclusion block 501", evs[0].BlockNumber)
	}
	if evs[0].FirstSeen != seen.Format(time.RFC3339) {
		t.Errorf("first_seen = %s, want the original %s", evs[0].FirstSeen, seen.Format(time.RFC3339))
	}
}

// TestCatchupMultiBlockBatching drives a catch-up with batch_blocks
// set and verifies blocks arrive via multi-block batch requests.
func TestCatchupMultiBlockBatching(t *testing.T) {
	store, err := storage.OpenSQLite(filepath.Join(t.TempDir(), "batch.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.SaveIndexerState(context.Background(), "testnet", storage.IndexerState{LastProcessed: 499}); err != nil {
		t.Fatal(err)
	}

	key, _ := testKey(t)
	client := newFakeClient()
	client.noPending = true
	payment := signedTx(t, key, watchedAddr, big.NewInt(2e18), nil)

	parent := common.Hash{0x95}
	var blocks []*fakeBlock
	for n := uint64(500); n <= 520; n++ {
		var txs []*types.Transaction
		if n == 505 {
			txs = append(txs, payment)
		}
		b := client.addBlock(n, parent, txs...)
		parent = b.Hash()
		blocks = append(blocks, b)
	}

	cfg := config.IndexerConfig{
		Name:           "testnet",
		ChainID:        testChainID.Uint64(),
		Confirmations:  config.Confirmations{Depth: 3},
		PendingTimeout: 30 * time.Minute,
		Concurrency:    2,
		BatchBlocks:    4,
	}
	h := startIndexerWithConfig(t, client, cfg, []common.Address{watchedAddr}, store)
	client.sendHead(blocks[len(blocks)-1])

	// Discovered 16 blocks deep: past the threshold, single confirmed.
	evs := h.waitForEvents(t, 1)
	if evs[0].Type != event.TypeConfirmed || evs[0].TxHash != payment.Hash().Hex() {
		t.Fatalf("event = %+v, want the deep payment confirmed", evs[0])
	}
	client.mu.Lock()
	hits := client.multiBatchHits
	client.mu.Unlock()
	if hits == 0 {
		t.Error("catch-up never used multi-block batch requests")
	}
}

// TestCatchupAgeCap verifies blocks older than max_catchup_age are
// skipped: the newest-first scan stops at the first too-old block.
func TestCatchupAgeCap(t *testing.T) {
	store, err := storage.OpenSQLite(filepath.Join(t.TempDir(), "cap.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.SaveIndexerState(context.Background(), "testnet", storage.IndexerState{LastProcessed: 499}); err != nil {
		t.Fatal(err)
	}

	key, _ := testKey(t)
	client := newFakeClient()
	client.noPending = true
	oldPayment := signedTx(t, key, watchedAddr, big.NewInt(1e18), nil)
	newPayment := signedTx(t, key, watchedAddr, big.NewInt(2e18), nil)

	// Fake block timestamps equal their numbers (epoch seconds); a cap
	// whose cutoff lands between 501 and 502 must skip 500-501.
	b500 := client.addBlock(500, common.Hash{0x95})
	b501 := client.addBlock(501, b500.Hash(), oldPayment)
	b502 := client.addBlock(502, b501.Hash())
	b503 := client.addBlock(503, b502.Hash(), newPayment)
	b504 := client.addBlock(504, b503.Hash())

	cfg := config.IndexerConfig{
		Name:           "testnet",
		ChainID:        testChainID.Uint64(),
		Confirmations:  config.Confirmations{Depth: 10},
		PendingTimeout: 30 * time.Minute,
		MaxCatchupAge:  time.Since(time.Unix(501, 0).Add(500 * time.Millisecond)),
	}
	h := startIndexerWithConfig(t, client, cfg, []common.Address{watchedAddr}, store)
	client.sendHead(b504)

	evs := h.waitForEvents(t, 1)
	if evs[0].Type != event.TypeMined || evs[0].TxHash != newPayment.Hash().Hex() {
		t.Fatalf("event = %+v, want the in-cap payment mined", evs[0])
	}
	// The scan stops before reaching 501; give it a moment, then make
	// sure the too-old payment never surfaced.
	time.Sleep(300 * time.Millisecond)
	for _, line := range h.buf.lines() {
		if bytes.Contains(line, []byte(oldPayment.Hash().Hex())) {
			t.Errorf("payment older than the cap was indexed: %s", line)
		}
	}
}

// TestInterruptedCatchupResumesGap: a catch-up that dies mid-scan must
// not lose the unscanned range when the next session's first head
// extends the ring tip (which advanced past blocks never scanned).
func TestInterruptedCatchupResumesGap(t *testing.T) {
	oldDelay := fetchRetryDelay
	fetchRetryDelay = time.Millisecond
	t.Cleanup(func() { fetchRetryDelay = oldDelay })

	store, err := storage.OpenSQLite(filepath.Join(t.TempDir(), "interrupted.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.SaveIndexerState(context.Background(), "testnet", storage.IndexerState{LastProcessed: 500}); err != nil {
		t.Fatal(err)
	}

	key, _ := testKey(t)
	client := newFakeClient()
	client.noPending = true
	payment := signedTx(t, key, watchedAddr, big.NewInt(2e18), nil)

	// Payment at 502, deep below the failure point at 510.
	parent := common.Hash{0x95}
	blocks := map[uint64]*fakeBlock{}
	for n := uint64(501); n <= 520; n++ {
		var txs []*types.Transaction
		if n == 502 {
			txs = append(txs, payment)
		}
		b := client.addBlock(n, parent, txs...)
		parent = b.Hash()
		blocks[n] = b
	}
	client.mu.Lock()
	client.failBlocks = map[uint64]bool{510: true}
	client.mu.Unlock()

	h := startIndexerWithStore(t, client, 3, store)
	// Head 520 triggers catchUp(501..520); the scan runs 520 down to
	// 511, dies at 510, and the session tears down with the ring tip at
	// 520 while lastProcessed is still 500.
	client.sendHead(blocks[520])

	// Wait out the failure and the 1s reconnect backoff, then heal the
	// fake and announce 521 - the head that extends the stale tip.
	time.Sleep(1500 * time.Millisecond)
	client.mu.Lock()
	client.failBlocks = nil
	client.mu.Unlock()
	b521 := client.addBlock(521, blocks[520].Hash())
	deadline := time.After(5 * time.Second)
	for {
		client.mu.Lock()
		ready := client.headSubs >= 2
		client.mu.Unlock()
		if ready {
			break
		}
		select {
		case <-deadline:
			t.Fatal("indexer never reconnected")
		case <-time.After(10 * time.Millisecond):
		}
	}
	client.sendHead(b521)

	// The payment at 502 is only found if the gap 501..521 is rescanned.
	evs := h.waitForEvents(t, 1)
	if evs[0].TxHash != payment.Hash().Hex() || evs[0].BlockNumber != 502 {
		t.Fatalf("event = %+v, want the payment from the abandoned range", evs[0])
	}
}

// TestNoDuplicateConfirmAfterRecheck: with -resume, a restored pending
// tx that mined during downtime is promoted (and instantly confirmed)
// by the startup recheck; the catch-up scan then re-reaching its block
// must not emit a second confirmed.
func TestNoDuplicateConfirmAfterRecheck(t *testing.T) {
	store, err := storage.OpenSQLite(filepath.Join(t.TempDir(), "dup.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()
	if err := store.SaveIndexerState(ctx, "testnet", storage.IndexerState{LastProcessed: 500}); err != nil {
		t.Fatal(err)
	}

	key, sender := testKey(t)
	client := newFakeClient()
	client.noPending = true
	payment := signedTx(t, key, watchedAddr, big.NewInt(2e18), nil)

	seen := time.Now().Add(-time.Minute).UTC().Truncate(time.Second)
	if err := store.SaveTransaction(ctx, "testnet", storage.TrackedTx{
		Hash: payment.Hash().Hex(), Status: "pending",
		FirstSeen: seen, PendingSince: seen,
		Sender: sender.Hex(), Nonce: payment.Nonce(), HasNonce: true,
		Transfers: []storage.Transfer{{
			From: sender.Hex(), To: watchedAddr.Hex(), Direction: "in", Asset: "ETH",
			Decimals: 18, ValueRaw: "2000000000000000000", Value: "2",
			TxSender: sender.Hex(), TxNonce: payment.Nonce(),
		}},
	}); err != nil {
		t.Fatal(err)
	}

	// Mined at 505 during downtime, 15 deep at the head - past the
	// threshold of 3, so the recheck instant-confirms it.
	parent := common.Hash{0x95}
	var last *fakeBlock
	for n := uint64(501); n <= 520; n++ {
		var txs []*types.Transaction
		if n == 505 {
			txs = append(txs, payment)
		}
		last = client.addBlock(n, parent, txs...)
		parent = last.Hash()
	}

	h := startIndexerWithStore(t, client, 3, store)
	client.sendHead(last)

	evs := h.waitForEvents(t, 1)
	if evs[0].Type != event.TypeConfirmed || evs[0].TxHash != payment.Hash().Hex() {
		t.Fatalf("event = %+v, want confirmed", evs[0])
	}
	// Let the scan pass block 505, then count confirmed emissions.
	time.Sleep(500 * time.Millisecond)
	confirmed := 0
	for _, line := range h.buf.lines() {
		if bytes.Contains(line, []byte(payment.Hash().Hex())) && bytes.Contains(line, []byte(`"confirmed"`)) {
			confirmed++
		}
	}
	if confirmed != 1 {
		t.Errorf("confirmed emitted %d times, want exactly once", confirmed)
	}
}

// TestDetectionSourceLogged: transaction logs carry source=resume for
// restored transactions, source=catchup for scan discoveries, and no
// source attribute for live traffic.
func TestDetectionSourceLogged(t *testing.T) {
	store, err := storage.OpenSQLite(filepath.Join(t.TempDir(), "source.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()
	if err := store.SaveIndexerState(ctx, "testnet", storage.IndexerState{LastProcessed: 500}); err != nil {
		t.Fatal(err)
	}

	key, sender := testKey(t)
	client := newFakeClient()
	client.noPending = true
	restoredTx := signedTx(t, key, watchedAddr, big.NewInt(1e18), nil)
	scannedTx := signedTx(t, key, watchedAddr, big.NewInt(2e18), nil)
	liveTx := signedTx(t, key, watchedAddr, big.NewInt(3e18), nil)

	// A restored pending tx that mined at 505 during downtime.
	seen := time.Now().Add(-time.Minute).UTC().Truncate(time.Second)
	if err := store.SaveTransaction(ctx, "testnet", storage.TrackedTx{
		Hash: restoredTx.Hash().Hex(), Status: "pending",
		FirstSeen: seen, PendingSince: seen,
		Sender: sender.Hex(), Nonce: restoredTx.Nonce(), HasNonce: true,
		Transfers: []storage.Transfer{{
			From: sender.Hex(), To: watchedAddr.Hex(), Direction: "in", Asset: "ETH",
			Decimals: 18, ValueRaw: "1000000000000000000", Value: "1",
			TxSender: sender.Hex(), TxNonce: restoredTx.Nonce(),
		}},
	}); err != nil {
		t.Fatal(err)
	}

	parent := common.Hash{0x95}
	var last *fakeBlock
	for n := uint64(501); n <= 520; n++ {
		var txs []*types.Transaction
		switch n {
		case 505:
			txs = append(txs, restoredTx)
		case 510:
			txs = append(txs, scannedTx)
		}
		last = client.addBlock(n, parent, txs...)
		parent = last.Hash()
	}

	logBuf := &lockedBuffer{}
	cfg := config.IndexerConfig{
		Name:           "testnet",
		ChainID:        testChainID.Uint64(),
		Confirmations:  config.Confirmations{Depth: 3},
		PendingTimeout: 30 * time.Minute,
	}
	h := startIndexerWithConfig(t, client, cfg, []common.Address{watchedAddr}, store,
		func(ix *Indexer) { ix.log = slog.New(slog.NewJSONHandler(logBuf, nil)) })
	client.sendHead(last)
	h.waitForEvents(t, 2) // restored + scanned, both instant-confirmed

	// A live block after the catch-up completes.
	b521 := client.addBlock(521, last.Hash(), liveTx)
	client.sendHead(b521)
	h.waitForEvents(t, 3)

	sources := map[string]string{} // tx hash -> source attr of its first transaction log
	for _, line := range logBuf.lines() {
		var rec map[string]any
		if err := json.Unmarshal(line, &rec); err != nil {
			continue
		}
		msg, _ := rec["msg"].(string)
		if !strings.HasPrefix(msg, "transaction ") {
			continue
		}
		tx, _ := rec["tx"].(string)
		src, _ := rec["source"].(string)
		if _, seen := sources[tx]; !seen {
			sources[tx] = src
		}
	}
	if got := sources[restoredTx.Hash().Hex()]; got != "resume" {
		t.Errorf("restored tx source = %q, want resume", got)
	}
	if got := sources[scannedTx.Hash().Hex()]; got != "catchup" {
		t.Errorf("scanned tx source = %q, want catchup", got)
	}
	if got := sources[liveTx.Hash().Hex()]; got != "" {
		t.Errorf("live tx source = %q, want none", got)
	}
}

// TestBatchedPendingRecheck: a set of restored pending txs with mixed
// fates (mined / dropped / still waiting) resolves in TWO batch
// requests - all receipts, then existence for the receipt-less - with
// no individual lookups.
func TestBatchedPendingRecheck(t *testing.T) {
	store, err := storage.OpenSQLite(filepath.Join(t.TempDir(), "recheck.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()
	if err := store.SaveIndexerState(ctx, "testnet", storage.IndexerState{LastProcessed: 500}); err != nil {
		t.Fatal(err)
	}

	key, sender := testKey(t)
	client := newFakeClient()
	client.noPending = true
	minedTx := signedTx(t, key, watchedAddr, big.NewInt(1e18), nil)
	droppedTx := signedTx(t, key, watchedAddr, big.NewInt(2e18), nil)
	waitingTx := signedTx(t, key, watchedAddr, big.NewInt(3e18), nil)

	seen := time.Now().Add(-time.Minute).UTC().Truncate(time.Second)
	for i, tx := range []*types.Transaction{minedTx, droppedTx, waitingTx} {
		if err := store.SaveTransaction(ctx, "testnet", storage.TrackedTx{
			Hash: tx.Hash().Hex(), Status: "pending",
			FirstSeen: seen, PendingSince: seen,
			Sender: sender.Hex(), Nonce: tx.Nonce() + uint64(i), HasNonce: true,
			Transfers: []storage.Transfer{{
				From: sender.Hex(), To: watchedAddr.Hex(), Direction: "in", Asset: "ETH",
				Decimals: 18, ValueRaw: tx.Value().String(), Value: "1",
				TxSender: sender.Hex(), TxNonce: tx.Nonce() + uint64(i),
			}},
		}); err != nil {
			t.Fatal(err)
		}
	}

	// minedTx mined at 505 during downtime; waitingTx is still in
	// the mempool; droppedTx is known nowhere.
	b := client.addBlock(501, common.Hash{0x95})
	for n := uint64(502); n <= 510; n++ {
		var txs []*types.Transaction
		if n == 505 {
			txs = append(txs, minedTx)
		}
		b = client.addBlock(n, b.Hash(), txs...)
	}
	client.mu.Lock()
	client.pendTxs[waitingTx.Hash()] = waitingTx
	client.mu.Unlock()

	h := startIndexerWithStore(t, client, 3, store)
	client.mu.Lock()
	client.batchHits, client.multiBatchHits = 0, 0
	client.mu.Unlock()
	client.sendHead(b)

	// minedTx: 6 deep at head 510 with depth 3 - instant confirm.
	// droppedTx: dropped event. waitingTx: no event (timer reset).
	evs := h.waitForEvents(t, 2)
	byHash := map[string]event.Type{}
	for _, ev := range evs {
		byHash[ev.TxHash] = ev.Type
	}
	if byHash[minedTx.Hash().Hex()] != event.TypeConfirmed {
		t.Errorf("mined tx = %v, want confirmed", byHash[minedTx.Hash().Hex()])
	}
	if byHash[droppedTx.Hash().Hex()] != event.TypeDropped {
		t.Errorf("dropped tx = %v, want dropped", byHash[droppedTx.Hash().Hex()])
	}

	client.mu.Lock()
	batches := client.batchHits + client.multiBatchHits
	client.mu.Unlock()
	if batches == 0 {
		t.Error("no batch requests recorded")
	}
}

// TestTransientBatchErrorNoFallback: a failing batch request must NOT
// be retried as individual calls - the failure propagates (session
// backoff) and batches resume once the provider recovers.
func TestTransientBatchErrorNoFallback(t *testing.T) {
	oldDelay := fetchRetryDelay
	fetchRetryDelay = time.Millisecond
	t.Cleanup(func() { fetchRetryDelay = oldDelay })

	store, err := storage.OpenSQLite(filepath.Join(t.TempDir(), "transient.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()
	if err := store.SaveIndexerState(ctx, "testnet", storage.IndexerState{LastProcessed: 500}); err != nil {
		t.Fatal(err)
	}

	key, sender := testKey(t)
	client := newFakeClient()
	client.noPending = true
	client.batchErr = errors.New("502 Bad Gateway")
	payment := signedTx(t, key, watchedAddr, big.NewInt(2e18), nil)

	seen := time.Now().Add(-time.Minute).UTC().Truncate(time.Second)
	if err := store.SaveTransaction(ctx, "testnet", storage.TrackedTx{
		Hash: payment.Hash().Hex(), Status: "pending",
		FirstSeen: seen, PendingSince: seen,
		Sender: sender.Hex(), Nonce: payment.Nonce(), HasNonce: true,
		Transfers: []storage.Transfer{{
			From: sender.Hex(), To: watchedAddr.Hex(), Direction: "in", Asset: "ETH",
			Decimals: 18, ValueRaw: "2000000000000000000", Value: "2",
			TxSender: sender.Hex(), TxNonce: payment.Nonce(),
		}},
	}); err != nil {
		t.Fatal(err)
	}

	b := client.addBlock(501, common.Hash{0x95})
	for n := uint64(502); n <= 510; n++ {
		var txs []*types.Transaction
		if n == 505 {
			txs = append(txs, payment)
		}
		b = client.addBlock(n, b.Hash(), txs...)
	}

	h := startIndexerWithStore(t, client, 3, store)
	client.sendHead(b)

	// The failing batches end the session; give it a failure cycle and
	// assert nothing degraded to individual calls in the meantime.
	time.Sleep(400 * time.Millisecond)
	client.mu.Lock()
	filterHits := client.filterLogsHits
	client.mu.Unlock()
	if filterHits != 0 {
		t.Fatalf("transient batch error fell back to individual log queries: filterLogs=%d", filterHits)
	}
	if lines := h.buf.lines(); len(lines) != 0 {
		t.Fatalf("events emitted while batches failing: %s", lines)
	}

	// Provider recovers: the reconnect must use batches again (the
	// unsupported flag was never set) and resolve the restored tx.
	client.mu.Lock()
	client.batchErr = nil
	client.multiBatchHits = 0
	client.mu.Unlock()

	deadline := time.After(10 * time.Second)
	for {
		client.mu.Lock()
		ready := client.headSubs >= 2
		client.mu.Unlock()
		if ready {
			break
		}
		select {
		case <-deadline:
			t.Fatal("indexer never reconnected")
		case <-time.After(10 * time.Millisecond):
		}
	}
	b511 := client.addBlock(511, b.Hash())
	client.sendHead(b511)

	evs := h.waitForEvents(t, 1)
	if evs[0].Type != event.TypeConfirmed || evs[0].TxHash != payment.Hash().Hex() {
		t.Fatalf("event = %+v, want the restored tx confirmed after recovery", evs[0])
	}
	client.mu.Lock()
	batches := client.multiBatchHits
	client.mu.Unlock()
	if batches == 0 {
		t.Error("batches not used after recovery")
	}
}

// TestRestoredRecheckRetriesAfterFailure: a transient batch failure on
// the first head must not permanently skip the restored-pending
// recheck - the next session's first head retries it instead of
// leaving the transactions to their 30m pending timeout.
func TestRestoredRecheckRetriesAfterFailure(t *testing.T) {
	oldDelay := fetchRetryDelay
	fetchRetryDelay = time.Millisecond
	t.Cleanup(func() { fetchRetryDelay = oldDelay })

	store, err := storage.OpenSQLite(filepath.Join(t.TempDir(), "retry.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()
	if err := store.SaveIndexerState(ctx, "testnet", storage.IndexerState{LastProcessed: 500}); err != nil {
		t.Fatal(err)
	}

	key, sender := testKey(t)
	client := newFakeClient()
	client.noPending = true
	client.batchErr = errors.New("502 Bad Gateway")
	payment := signedTx(t, key, watchedAddr, big.NewInt(2e18), nil)

	seen := time.Now().Add(-time.Minute).UTC().Truncate(time.Second)
	if err := store.SaveTransaction(ctx, "testnet", storage.TrackedTx{
		Hash: payment.Hash().Hex(), Status: "pending",
		FirstSeen: seen, PendingSince: seen,
		Sender: sender.Hex(), Nonce: payment.Nonce(), HasNonce: true,
		Transfers: []storage.Transfer{{
			From: sender.Hex(), To: watchedAddr.Hex(), Direction: "in", Asset: "ETH",
			Decimals: 18, ValueRaw: "2000000000000000000", Value: "2",
			TxSender: sender.Hex(), TxNonce: payment.Nonce(),
		}},
	}); err != nil {
		t.Fatal(err)
	}

	// Mined during downtime, 10 deep: the recheck should instant-confirm
	// it - but the first attempt dies on the failing batches. Without
	// -resume there is no catch-up scan to find it by another route.
	b := client.addBlock(501, common.Hash{0x95})
	for n := uint64(502); n <= 510; n++ {
		var txs []*types.Transaction
		if n == 505 {
			txs = append(txs, payment)
		}
		b = client.addBlock(n, b.Hash(), txs...)
	}

	cfg := config.IndexerConfig{
		Name:           "testnet",
		ChainID:        testChainID.Uint64(),
		Confirmations:  config.Confirmations{Depth: 3},
		PendingTimeout: 30 * time.Minute,
	}
	h := startIndexerWithConfig(t, client, cfg, []common.Address{watchedAddr}, store,
		func(ix *Indexer) { ix.Resume = false })
	client.sendHead(b)

	// Let the failing first pass play out through a session restart.
	time.Sleep(400 * time.Millisecond)
	if lines := h.buf.lines(); len(lines) != 0 {
		t.Fatalf("events emitted while batches failing: %s", lines)
	}

	// Provider recovers; the reconnect's first head must re-run the
	// recheck and resolve the restored tx.
	client.mu.Lock()
	client.batchErr = nil
	client.mu.Unlock()
	deadline := time.After(10 * time.Second)
	for {
		client.mu.Lock()
		ready := client.headSubs >= 2
		client.mu.Unlock()
		if ready {
			break
		}
		select {
		case <-deadline:
			t.Fatal("indexer never reconnected")
		case <-time.After(10 * time.Millisecond):
		}
	}
	b511 := client.addBlock(511, b.Hash())
	client.sendHead(b511)

	evs := h.waitForEvents(t, 1)
	if evs[0].Type != event.TypeConfirmed || evs[0].TxHash != payment.Hash().Hex() {
		t.Fatalf("event = %+v, want the restored tx confirmed on retry", evs[0])
	}
}

func TestFinalityTagUnsupported(t *testing.T) {
	client := newFakeClient() // finalizedNum 0: BlockNumberByTag errors

	cfg := config.IndexerConfig{
		Name:           "testnet",
		ChainID:        testChainID.Uint64(),
		Confirmations:  config.Confirmations{Tag: "safe"},
		PendingTimeout: time.Minute,
	}
	ix := New(cfg, []common.Address{watchedAddr}, event.NewEmitter(&lockedBuffer{}), nil)
	ix.dial = func(context.Context) (ChainClient, error) { return client, nil }

	done := make(chan error, 1)
	go func() { done <- ix.Run(context.Background()) }()
	select {
	case err := <-done:
		if err == nil || !errors.Is(err, errFinalityTag) {
			t.Fatalf("Run returned %v, want finality tag error", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return promptly on unresolvable finality tag (retrying instead of failing fast?)")
	}
}

func TestChainIDMismatch(t *testing.T) {
	client := newFakeClient()
	client.chainID = big.NewInt(999) // config expects testChainID (1)

	cfg := config.IndexerConfig{
		Name:           "testnet",
		ChainID:        testChainID.Uint64(),
		Confirmations:  config.Confirmations{Depth: 1},
		PendingTimeout: time.Minute,
	}
	ix := New(cfg, []common.Address{watchedAddr}, event.NewEmitter(&lockedBuffer{}), nil)
	ix.dial = func(context.Context) (ChainClient, error) { return client, nil }

	done := make(chan error, 1)
	go func() { done <- ix.Run(context.Background()) }()
	select {
	case err := <-done:
		if err == nil || !errors.Is(err, errChainMismatch) {
			t.Fatalf("Run returned %v, want chain mismatch error", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return promptly on chain mismatch (retrying instead of failing fast?)")
	}
}

func TestEndToEndReorg(t *testing.T) {
	key, _ := testKey(t)
	client := newFakeClient()
	client.noPending = true
	h := startIndexer(t, client, 5)

	payment := signedTx(t, key, watchedAddr, big.NewInt(1e18), nil)

	b300 := client.addBlock(300, common.Hash{0x97})
	b301 := client.addBlock(301, b300.Hash(), payment)
	client.sendHead(b300)
	client.sendHead(b301)
	evs := h.waitForEvents(t, 1)
	if evs[0].Type != event.TypeMined || evs[0].BlockNumber != 301 {
		t.Fatalf("mined event = %+v", evs[0])
	}

	// Reorg: a competing chain replaces block 301, re-including the tx
	// in 301' and building 302 on top. addForkBlock overwrites byNumber
	// so the canonical view (HeaderByNumber/BlockByNumber) flips too,
	// and its receipt registration points at the new block hash.
	b301b := client.addForkBlock(301, b300.Hash(), []byte("fork"), payment)
	if b301b.Hash() == b301.Hash() {
		t.Fatal("fork block must have a distinct hash")
	}
	b302 := client.addBlock(302, b301b.Hash())
	client.sendHead(b302)

	evs = h.waitForEvents(t, 3)
	if evs[1].Type != event.TypeReorged {
		t.Fatalf("second event = %+v", evs[1])
	}
	if evs[2].Type != event.TypeMined || evs[2].BlockHash != b301b.Hash().Hex() {
		t.Fatalf("re-mine event = %+v", evs[2])
	}

	// Confirm on the new chain at 5 confirmations (head 305).
	prev := b302
	for n := uint64(303); n <= 305; n++ {
		prev = client.addBlock(n, prev.Hash())
		client.sendHead(prev)
	}
	evs = h.waitForEvents(t, 4)
	if evs[3].Type != event.TypeConfirmed || evs[3].Confirmations != 5 {
		t.Fatalf("confirmed event = %+v", evs[3])
	}
}

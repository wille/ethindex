package indexer

import (
	"context"
	"math/big"
	"runtime/debug"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/ethclient/gethclient"
	"github.com/ethereum/go-ethereum/rpc"
)

// Block is a minimal chain-agnostic block fetched over raw JSON-RPC.
// go-ethereum's typed block decoding rejects transaction types it does
// not know (e.g. OP-stack deposit txs, type 0x7E, present in every Base
// block), so blocks are unmarshalled into this struct instead. Mined
// transactions carry `from` in the RPC response, so no signature
// recovery is needed either.
type Block struct {
	Hash         common.Hash    `json:"hash"`
	ParentHash   common.Hash    `json:"parentHash"`
	Number       hexutil.Uint64 `json:"number"`
	Time         hexutil.Uint64 `json:"timestamp"`
	Transactions []BlockTx      `json:"transactions"`
}

// BlockTx is the subset of a mined transaction the indexer inspects.
type BlockTx struct {
	Hash  common.Hash     `json:"hash"`
	From  common.Address  `json:"from"`
	To    *common.Address `json:"to"`
	Value *hexutil.Big    `json:"value"`
	Input hexutil.Bytes   `json:"input"`
	Nonce hexutil.Uint64  `json:"nonce"`
}

// BlockReceipt is the subset of a receipt the indexer inspects, decoded
// raw for the same reason as Block: typed decoding rejects unknown
// transaction types (OP-stack deposits). Logs carry every event of the
// transaction, letting one eth_getBlockReceipts call double as the log
// source at the tip.
type BlockReceipt struct {
	TxHash    common.Hash    `json:"transactionHash"`
	Status    hexutil.Uint64 `json:"status"`
	BlockHash common.Hash    `json:"blockHash"`
	Logs      []types.Log    `json:"logs"`
}

// ChainClient is the narrow node-facing surface the indexer consumes,
// so tests can substitute a fake.
type ChainClient interface {
	ChainID(ctx context.Context) (*big.Int, error)
	SubscribeNewHead(ctx context.Context, ch chan<- *types.Header) (ethereum.Subscription, error)
	BlockByHash(ctx context.Context, hash common.Hash) (*Block, error)
	BlockByNumber(ctx context.Context, number uint64) (*Block, error)
	TransactionReceipt(ctx context.Context, hash common.Hash) (*types.Receipt, error)

	// SubscribeFullPendingTransactions streams full pending tx objects.
	SubscribeFullPendingTransactions(ctx context.Context, ch chan<- *types.Transaction) (ethereum.Subscription, error)
	// SubscribePendingTransactions streams pending tx hashes.
	SubscribePendingTransactions(ctx context.Context, ch chan<- common.Hash) (ethereum.Subscription, error)
	// TxPoolStatus returns the node's mempool size: pending
	// (executable) and queued (nonce-gapped) transaction counts.
	// Many providers do not expose txpool_*.
	TxPoolStatus(ctx context.Context) (pending, queued uint64, err error)
	// TxPoolContent returns a snapshot of the node's mempool (pending
	// and queued), flattened. Many providers do not expose txpool_*.
	TxPoolContent(ctx context.Context) ([]BlockTx, error)
	// BlockReceipts returns all receipts of one block in a single call
	// (eth_getBlockReceipts). Not supported by every provider.
	BlockReceipts(ctx context.Context, blockHash common.Hash) ([]BlockReceipt, error)
	// BlockNumberByTag resolves a consensus block tag ("finalized",
	// "safe") to its block number.
	BlockNumberByTag(ctx context.Context, tag string) (uint64, error)

	// BlockBundleByNumber and BlockBundleByHash fetch a block and its
	// receipts in ONE JSON-RPC batch request - a single round trip,
	// which matters against remote providers with tens of milliseconds
	// of latency. The bundle's ReceiptsErr is set when only the
	// receipts half failed.
	BlockBundleByNumber(ctx context.Context, number uint64) (BlockBundle, error)
	BlockBundleByHash(ctx context.Context, hash common.Hash) (BlockBundle, error)
	// BlockBundles fetches several blocks (and, with withReceipts,
	// their receipts) in ONE JSON-RPC batch request - the catch-up
	// workers' bulk fetch. The error slice is per block; the outer
	// error means the whole batch failed.
	BlockBundles(ctx context.Context, numbers []uint64, withReceipts bool) ([]BlockBundle, []error, error)
	// TransactionReceipts fetches many receipts in one batch request.
	// The error slice is per transaction (ethereum.NotFound for null).
	TransactionReceipts(ctx context.Context, hashes []common.Hash) ([]*types.Receipt, []error, error)
	// TransactionsByHash fetches many transactions in one batch
	// request, decoded into the raw BlockTx shape (the response carries
	// `from`, so no signature recovery is needed and unusual tx types
	// survive). The error slice is per transaction (ethereum.NotFound
	// for null).
	TransactionsByHash(ctx context.Context, hashes []common.Hash) ([]*BlockTx, []error, error)
	// HeaderRefsByNumbers fetches many block headers (bodies excluded)
	// in one batch request, for reorg ancestor walks.
	HeaderRefsByNumbers(ctx context.Context, numbers []uint64) ([]*Block, []error, error)
	// FilterLogsBatch runs several eth_getLogs queries in one batch
	// request. The error slice is per query.
	FilterLogsBatch(ctx context.Context, queries []ethereum.FilterQuery) ([][]types.Log, []error, error)

	Close()
}

// BlockBundle is a block plus its receipts, fetched together.
type BlockBundle struct {
	Block       *Block
	Receipts    []BlockReceipt
	ReceiptsErr error // set when the receipts half of the batch failed
}

// rpcClient splits traffic across two connections: the WebSocket
// carries only subscriptions, while all request/response calls (blocks,
// logs, receipts, mempool) go over HTTP - a connection pool gives
// concurrent catch-up fetches true parallelism, where a single ws
// stream would serialize them.
type rpcClient struct {
	*ethclient.Client // HTTP-backed: all request/response methods
	ws                *ethclient.Client
	wsGeth            *gethclient.Client
	rawWS             *rpc.Client
	rawHTTP           *rpc.Client
}

// userAgent identifies ethindex (and its version) to RPC providers.
// The version is stamped into the binary from the module's git tag on
// release builds; source builds report "dev".
func userAgent() string {
	version := "dev"
	if info, ok := debug.ReadBuildInfo(); ok && info.Main.Version != "" && info.Main.Version != "(devel)" {
		version = info.Main.Version
	}
	return "ethindex/" + version
}

// Dial connects the WebSocket (subscriptions) and HTTP (queries)
// endpoints. The ws message size limit is raised well past the default
// as a safety net for large subscription payloads.
func Dial(ctx context.Context, wsURL, httpURL string) (ChainClient, error) {
	ua := rpc.WithHeader("User-Agent", userAgent())
	rawWS, err := rpc.DialOptions(ctx, wsURL, rpc.WithWebsocketMessageSizeLimit(512*1024*1024), ua)
	if err != nil {
		return nil, err
	}
	rawHTTP, err := rpc.DialOptions(ctx, httpURL, ua)
	if err != nil {
		rawWS.Close()
		return nil, err
	}
	return &rpcClient{
		Client:  ethclient.NewClient(rawHTTP),
		ws:      ethclient.NewClient(rawWS),
		wsGeth:  gethclient.New(rawWS),
		rawWS:   rawWS,
		rawHTTP: rawHTTP,
	}, nil
}

// BlockByHash fetches a full block as raw JSON (see Block).
func (c *rpcClient) BlockByHash(ctx context.Context, hash common.Hash) (*Block, error) {
	return c.getBlock(ctx, "eth_getBlockByHash", hash)
}

// BlockByNumber fetches a full canonical block as raw JSON (see Block).
func (c *rpcClient) BlockByNumber(ctx context.Context, number uint64) (*Block, error) {
	return c.getBlock(ctx, "eth_getBlockByNumber", hexutil.Uint64(number))
}

func (c *rpcClient) getBlock(ctx context.Context, method string, arg any) (*Block, error) {
	var block *Block
	if err := c.rawHTTP.CallContext(ctx, &block, method, arg, true); err != nil {
		return nil, err
	}
	if block == nil {
		return nil, ethereum.NotFound
	}
	return block, nil
}

func (c *rpcClient) SubscribeNewHead(ctx context.Context, ch chan<- *types.Header) (ethereum.Subscription, error) {
	return c.ws.SubscribeNewHead(ctx, ch)
}

func (c *rpcClient) SubscribeFullPendingTransactions(ctx context.Context, ch chan<- *types.Transaction) (ethereum.Subscription, error) {
	return c.wsGeth.SubscribeFullPendingTransactions(ctx, ch)
}

func (c *rpcClient) SubscribePendingTransactions(ctx context.Context, ch chan<- common.Hash) (ethereum.Subscription, error) {
	return c.wsGeth.SubscribePendingTransactions(ctx, ch)
}

// TxPoolStatus fetches the mempool's pending/queued counts.
func (c *rpcClient) TxPoolStatus(ctx context.Context) (uint64, uint64, error) {
	var res struct {
		Pending hexutil.Uint64 `json:"pending"`
		Queued  hexutil.Uint64 `json:"queued"`
	}
	if err := c.rawHTTP.CallContext(ctx, &res, "txpool_status"); err != nil {
		return 0, 0, err
	}
	return uint64(res.Pending), uint64(res.Queued), nil
}

// TxPoolContent fetches txpool_content (over HTTP - mainnet snapshots
// can be enormous) and flattens both pools. The tx objects are decoded
// into the same minimal shape as block transactions (the response
// includes `from`, so no signature recovery is needed).
func (c *rpcClient) TxPoolContent(ctx context.Context) ([]BlockTx, error) {
	var res struct {
		Pending map[common.Address]map[string]BlockTx `json:"pending"`
		Queued  map[common.Address]map[string]BlockTx `json:"queued"`
	}
	if err := c.rawHTTP.CallContext(ctx, &res, "txpool_content"); err != nil {
		return nil, err
	}
	var out []BlockTx
	for _, pool := range []map[common.Address]map[string]BlockTx{res.Pending, res.Queued} {
		for _, byNonce := range pool {
			for _, tx := range byNonce {
				out = append(out, tx)
			}
		}
	}
	return out, nil
}

// BlockReceipts fetches every receipt of a block in one round trip.
func (c *rpcClient) BlockReceipts(ctx context.Context, blockHash common.Hash) ([]BlockReceipt, error) {
	var receipts []BlockReceipt
	if err := c.rawHTTP.CallContext(ctx, &receipts, "eth_getBlockReceipts", blockHash); err != nil {
		return nil, err
	}
	if receipts == nil {
		return nil, ethereum.NotFound
	}
	return receipts, nil
}

// BlockNumberByTag resolves "finalized"/"safe" to a block number.
func (c *rpcClient) BlockNumberByTag(ctx context.Context, tag string) (uint64, error) {
	var head *struct {
		Number hexutil.Uint64 `json:"number"`
	}
	if err := c.rawHTTP.CallContext(ctx, &head, "eth_getBlockByNumber", tag, false); err != nil {
		return 0, err
	}
	if head == nil {
		return 0, ethereum.NotFound
	}
	return uint64(head.Number), nil
}

// blockBundle runs the two-element batch shared by the ByNumber and
// ByHash variants.
func (c *rpcClient) blockBundle(ctx context.Context, blockMethod string, blockArg, receiptsArg any) (BlockBundle, error) {
	var (
		block    *Block
		receipts []BlockReceipt
	)
	batch := []rpc.BatchElem{
		{Method: blockMethod, Args: []any{blockArg, true}, Result: &block},
		{Method: "eth_getBlockReceipts", Args: []any{receiptsArg}, Result: &receipts},
	}
	if err := c.rawHTTP.BatchCallContext(ctx, batch); err != nil {
		return BlockBundle{}, err
	}
	if batch[0].Error != nil {
		return BlockBundle{}, batch[0].Error
	}
	if block == nil {
		return BlockBundle{}, ethereum.NotFound
	}
	out := BlockBundle{Block: block, Receipts: receipts, ReceiptsErr: batch[1].Error}
	if out.ReceiptsErr == nil && receipts == nil && len(block.Transactions) > 0 {
		out.ReceiptsErr = ethereum.NotFound
	}
	return out, nil
}

func (c *rpcClient) BlockBundleByNumber(ctx context.Context, number uint64) (BlockBundle, error) {
	n := hexutil.Uint64(number)
	return c.blockBundle(ctx, "eth_getBlockByNumber", n, n)
}

func (c *rpcClient) BlockBundleByHash(ctx context.Context, hash common.Hash) (BlockBundle, error) {
	return c.blockBundle(ctx, "eth_getBlockByHash", hash, hash)
}

// BlockBundles fetches many blocks, each with its receipts when
// withReceipts is set, in a single batch request.
func (c *rpcClient) BlockBundles(ctx context.Context, numbers []uint64, withReceipts bool) ([]BlockBundle, []error, error) {
	per := 1
	if withReceipts {
		per = 2
	}
	blocks := make([]*Block, len(numbers))
	receipts := make([][]BlockReceipt, len(numbers))
	batch := make([]rpc.BatchElem, 0, per*len(numbers))
	for i, n := range numbers {
		arg := hexutil.Uint64(n)
		batch = append(batch, rpc.BatchElem{
			Method: "eth_getBlockByNumber",
			Args:   []any{arg, true},
			Result: &blocks[i],
		})
		if withReceipts {
			batch = append(batch, rpc.BatchElem{
				Method: "eth_getBlockReceipts",
				Args:   []any{arg},
				Result: &receipts[i],
			})
		}
	}
	if err := c.rawHTTP.BatchCallContext(ctx, batch); err != nil {
		return nil, nil, err
	}
	bundles := make([]BlockBundle, len(numbers))
	errs := make([]error, len(numbers))
	for i := range numbers {
		switch {
		case batch[per*i].Error != nil:
			errs[i] = batch[per*i].Error
			continue
		case blocks[i] == nil:
			errs[i] = ethereum.NotFound
			continue
		}
		bundles[i] = BlockBundle{Block: blocks[i]}
		if withReceipts {
			bundles[i].Receipts = receipts[i]
			bundles[i].ReceiptsErr = batch[per*i+1].Error
			if bundles[i].ReceiptsErr == nil && receipts[i] == nil && len(blocks[i].Transactions) > 0 {
				bundles[i].ReceiptsErr = ethereum.NotFound
			}
		}
	}
	return bundles, errs, nil
}

// TransactionReceipts fetches many receipts in one batch request.
func (c *rpcClient) TransactionReceipts(ctx context.Context, hashes []common.Hash) ([]*types.Receipt, []error, error) {
	receipts := make([]*types.Receipt, len(hashes))
	batch := make([]rpc.BatchElem, len(hashes))
	for i, h := range hashes {
		batch[i] = rpc.BatchElem{
			Method: "eth_getTransactionReceipt",
			Args:   []any{h},
			Result: &receipts[i],
		}
	}
	if err := c.rawHTTP.BatchCallContext(ctx, batch); err != nil {
		return nil, nil, err
	}
	errs := make([]error, len(hashes))
	for i := range batch {
		switch {
		case batch[i].Error != nil:
			errs[i] = batch[i].Error
		case receipts[i] == nil:
			errs[i] = ethereum.NotFound
		}
	}
	return receipts, errs, nil
}

// TransactionsByHash fetches many transactions in one batch request,
// decoded into the raw BlockTx shape.
func (c *rpcClient) TransactionsByHash(ctx context.Context, hashes []common.Hash) ([]*BlockTx, []error, error) {
	txs := make([]*BlockTx, len(hashes))
	batch := make([]rpc.BatchElem, len(hashes))
	for i, h := range hashes {
		batch[i] = rpc.BatchElem{
			Method: "eth_getTransactionByHash",
			Args:   []any{h},
			Result: &txs[i],
		}
	}
	if err := c.rawHTTP.BatchCallContext(ctx, batch); err != nil {
		return nil, nil, err
	}
	errs := make([]error, len(hashes))
	for i := range batch {
		switch {
		case batch[i].Error != nil:
			errs[i] = batch[i].Error
		case txs[i] == nil:
			errs[i] = ethereum.NotFound
		}
	}
	return txs, errs, nil
}

// HeaderRefsByNumbers fetches many block headers (fullTx=false, so the
// bodies stay home) in one batch request.
func (c *rpcClient) HeaderRefsByNumbers(ctx context.Context, numbers []uint64) ([]*Block, []error, error) {
	blocks := make([]*Block, len(numbers))
	batch := make([]rpc.BatchElem, len(numbers))
	for i, n := range numbers {
		batch[i] = rpc.BatchElem{
			Method: "eth_getBlockByNumber",
			Args:   []any{hexutil.Uint64(n), false},
			Result: &blocks[i],
		}
	}
	if err := c.rawHTTP.BatchCallContext(ctx, batch); err != nil {
		return nil, nil, err
	}
	errs := make([]error, len(numbers))
	for i := range batch {
		switch {
		case batch[i].Error != nil:
			errs[i] = batch[i].Error
		case blocks[i] == nil:
			errs[i] = ethereum.NotFound
		}
	}
	return blocks, errs, nil
}

// filterArg encodes an ethereum.FilterQuery as the eth_getLogs
// parameter object (ethclient's encoder is unexported). Only the
// number-range form is supported - the indexer never queries by
// block hash.
func filterArg(q ethereum.FilterQuery) map[string]any {
	arg := map[string]any{
		"topics": q.Topics,
	}
	if len(q.Addresses) > 0 {
		arg["address"] = q.Addresses
	}
	if q.FromBlock != nil {
		arg["fromBlock"] = hexutil.EncodeBig(q.FromBlock)
	}
	if q.ToBlock != nil {
		arg["toBlock"] = hexutil.EncodeBig(q.ToBlock)
	}
	return arg
}

// FilterLogsBatch runs several eth_getLogs queries in one batch request.
func (c *rpcClient) FilterLogsBatch(ctx context.Context, queries []ethereum.FilterQuery) ([][]types.Log, []error, error) {
	results := make([][]types.Log, len(queries))
	batch := make([]rpc.BatchElem, len(queries))
	for i, q := range queries {
		batch[i] = rpc.BatchElem{
			Method: "eth_getLogs",
			Args:   []any{filterArg(q)},
			Result: &results[i],
		}
	}
	if err := c.rawHTTP.BatchCallContext(ctx, batch); err != nil {
		return nil, nil, err
	}
	errs := make([]error, len(queries))
	for i := range batch {
		errs[i] = batch[i].Error
	}
	return results, errs, nil
}

func (c *rpcClient) Close() {
	c.rawWS.Close()
	c.rawHTTP.Close()
}

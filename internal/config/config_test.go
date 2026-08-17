package config

import (
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"

	"github.com/wille/ethindex/internal/event"
)

const validBase = `
addresses:
  - "0x1111111111111111111111111111111111111111"
indexers:
  - name: ethereum
    chain_id: 1
    rpc_url: wss://example.com
`

func TestDefaults(t *testing.T) {
	cfg, err := Parse([]byte(validBase))
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Indexers) != 1 {
		t.Fatalf("indexers = %d, want 1", len(cfg.Indexers))
	}
	ix := cfg.Indexers[0]
	if ix.Confirmations.Depth != 12 {
		t.Errorf("confirmations = %s, want 12", ix.Confirmations)
	}
	if ix.NativeSymbol != "ETH" {
		t.Errorf("native_symbol = %q, want ETH", ix.NativeSymbol)
	}
	if ix.PendingTimeout != 30*time.Minute {
		t.Errorf("pending_timeout = %v, want 30m", ix.PendingTimeout)
	}
	// No built-in token defaults: omitting tokens means native-only.
	if len(ix.TokenList) != 0 {
		t.Errorf("token list = %d entries, want 0", len(ix.TokenList))
	}
}

func TestMultipleIndexers(t *testing.T) {
	cfg, err := Parse([]byte(`
addresses:
  - "0x1111111111111111111111111111111111111111"
indexers:
  - name: ethereum
    chain_id: 1
    rpc_url: wss://example.com
    tokens:
      - address: "0x2222222222222222222222222222222222222222"
        symbol: USDC
        decimals: 6
  - name: base
    chain_id: 8453
    rpc_url: wss://base.example.com
    confirmations: 30
`))
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Indexers) != 2 {
		t.Fatalf("indexers = %d, want 2", len(cfg.Indexers))
	}
	if cfg.Indexers[0].TokenList[0].Symbol != "USDC" || cfg.Indexers[0].TokenList[0].Decimals != 6 {
		t.Errorf("unexpected token: %+v", cfg.Indexers[0].TokenList[0])
	}
	if cfg.Indexers[1].ChainID != 8453 || cfg.Indexers[1].Confirmations.Depth != 30 {
		t.Errorf("unexpected second indexer: %+v", cfg.Indexers[1])
	}
	// Per-entry default still applied alongside explicit overrides.
	if cfg.Indexers[1].PendingTimeout != 30*time.Minute {
		t.Errorf("pending_timeout = %v, want 30m", cfg.Indexers[1].PendingTimeout)
	}
}

func TestConfirmationTags(t *testing.T) {
	cfg, err := Parse([]byte(`
addresses:
  - "0x1111111111111111111111111111111111111111"
indexers:
  - {name: eth, chain_id: 1, rpc_url: "wss://example.com", confirmations: finalized}
  - {name: base, chain_id: 8453, rpc_url: "wss://base.example.com", confirmations: safe}
`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Indexers[0].Confirmations.Tag != "finalized" || cfg.Indexers[0].Confirmations.Depth != 0 {
		t.Errorf("confirmations = %+v", cfg.Indexers[0].Confirmations)
	}
	if cfg.Indexers[1].Confirmations.Tag != "safe" {
		t.Errorf("confirmations = %+v", cfg.Indexers[1].Confirmations)
	}

	if _, err := Parse([]byte(`
addresses: ["0x1111111111111111111111111111111111111111"]
indexers:
  - {name: eth, chain_id: 1, rpc_url: "wss://example.com", confirmations: banana}
`)); err == nil {
		t.Error("invalid confirmations tag accepted")
	}
}

func TestAddressNormalization(t *testing.T) {
	upper, err := Parse([]byte(strings.Replace(validBase,
		"0x1111111111111111111111111111111111111111",
		"0xABCDEFABCDEFABCDEFABCDEFABCDEFABCDEFABCD", 1)))
	if err != nil {
		t.Fatal(err)
	}
	lower, err := Parse([]byte(strings.Replace(validBase,
		"0x1111111111111111111111111111111111111111",
		"0xabcdefabcdefabcdefabcdefabcdefabcdefabcd", 1)))
	if err != nil {
		t.Fatal(err)
	}
	// Casing must not matter: both parse to the same address bytes.
	addr := common.HexToAddress("0xabcdefabcdefabcdefabcdefabcdefabcdefabcd")
	if !upper.Watched.Contains(addr) || !lower.Watched.Contains(addr) {
		t.Errorf("address casing not normalized: upper=%v lower=%v",
			upper.Watched.Contains(addr), lower.Watched.Contains(addr))
	}
}

func TestNamedAddresses(t *testing.T) {
	cfg, err := Parse([]byte(`
addresses:
  - "0x1111111111111111111111111111111111111111"
  - {name: hot-wallet, address: "0x2222222222222222222222222222222222222222"}
indexers:
  - {name: eth, chain_id: 1, rpc_url: "wss://example.com"}
`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Watched.Count() != 2 {
		t.Fatalf("watched = %d, want 2", cfg.Watched.Count())
	}
	unnamed := common.HexToAddress("0x1111111111111111111111111111111111111111")
	named := common.HexToAddress("0x2222222222222222222222222222222222222222")
	if got := cfg.Watched.Name(unnamed); got != "" {
		t.Errorf("unnamed entry label = %q, want empty", got)
	}
	if got := cfg.Watched.Name(named); got != "hot-wallet" {
		t.Errorf("named entry label = %q, want hot-wallet", got)
	}
}

func TestWatchedSetFor(t *testing.T) {
	named := common.HexToAddress("0x1111111111111111111111111111111111111111")
	unnamed := common.HexToAddress("0x2222222222222222222222222222222222222222")
	other := common.HexToAddress("0x3333333333333333333333333333333333333333")
	s := NewWatchedSet(map[common.Address]string{named: "hot", unnamed: ""})

	if got := s.For(other, named, event.DirectionIn); got != "hot" {
		t.Errorf("in = %q, want hot", got)
	}
	if got := s.For(named, other, event.DirectionOut); got != "hot" {
		t.Errorf("out = %q, want hot", got)
	}
	// Self anchors on the recipient, falling back to the sender's label.
	if got := s.For(unnamed, named, event.DirectionSelf); got != "hot" {
		t.Errorf("self to named = %q, want hot", got)
	}
	if got := s.For(named, unnamed, event.DirectionSelf); got != "hot" {
		t.Errorf("self fallback = %q, want hot", got)
	}
	if got := s.For(other, unnamed, event.DirectionIn); got != "" {
		t.Errorf("unnamed in = %q, want empty", got)
	}
}

func TestValidationErrors(t *testing.T) {
	indexers := func(body string) string {
		return "addresses: [\"0x1111111111111111111111111111111111111111\"]\nindexers:\n" + body
	}
	cases := map[string]string{
		"no addresses": `
indexers:
  - {name: eth, chain_id: 1, rpc_url: "wss://example.com"}
`,
		"bad address": `
addresses: ["0x123"]
indexers:
  - {name: eth, chain_id: 1, rpc_url: "wss://example.com"}
`,
		"duplicate address": `
addresses:
  - "0x1111111111111111111111111111111111111111"
  - "0x1111111111111111111111111111111111111111"
indexers:
  - {name: eth, chain_id: 1, rpc_url: "wss://example.com"}
`,
		"object entry missing address": `
addresses:
  - {name: hot-wallet}
indexers:
  - {name: eth, chain_id: 1, rpc_url: "wss://example.com"}
`,
		"object entry bad address": `
addresses:
  - {name: hot-wallet, address: "0x123"}
indexers:
  - {name: eth, chain_id: 1, rpc_url: "wss://example.com"}
`,
		"duplicate across entry forms": `
addresses:
  - "0x1111111111111111111111111111111111111111"
  - {name: hot-wallet, address: "0x1111111111111111111111111111111111111111"}
indexers:
  - {name: eth, chain_id: 1, rpc_url: "wss://example.com"}
`,
		"no indexers":      "addresses: [\"0x1111111111111111111111111111111111111111\"]",
		"missing name":     indexers(`  - {chain_id: 1, rpc_url: "wss://example.com"}`),
		"missing chain id": indexers(`  - {name: eth, rpc_url: "wss://example.com"}`),
		"missing rpc_url":  indexers(`  - {name: eth, chain_id: 1}`),
		"http rpc_url":     indexers(`  - {name: eth, chain_id: 1, rpc_url: "https://example.com"}`),
		"duplicate name": indexers(`  - {name: eth, chain_id: 1, rpc_url: "wss://example.com"}
  - {name: eth, chain_id: 8453, rpc_url: "wss://base.example.com"}`),
		"bad token": indexers(`  - name: eth
    chain_id: 1
    rpc_url: "wss://example.com"
    tokens: [{address: "0xzz", symbol: X, decimals: 1}]`),
		"token no symbol": indexers(`  - name: eth
    chain_id: 1
    rpc_url: "wss://example.com"
    tokens: [{address: "0x2222222222222222222222222222222222222222", decimals: 1}]`),
		"duplicate token": indexers(`  - name: eth
    chain_id: 1
    rpc_url: "wss://example.com"
    tokens:
      - {address: "0x2222222222222222222222222222222222222222", symbol: A, decimals: 1}
      - {address: "0x2222222222222222222222222222222222222222", symbol: B, decimals: 1}`),
	}
	for name, yaml := range cases {
		if _, err := Parse([]byte(yaml)); err == nil {
			t.Errorf("%s: expected error, got none", name)
		}
	}
}

// anvilAccountXpub is the account-level xpub (m/44'/60'/0') of the
// well-known anvil/hardhat test mnemonic ("test test ... junk"), whose
// derived addresses are publicly documented.
const anvilAccountXpub = "xpub6Ce9NcJvTk36xtLSrJLZqE7wtgA5deCeYs7rSQtreh4cj6ByPtrg9sD7V2FNFLPnf8heNP3FGkeV9qwfzvZNSd54JoNXVsXFYSYwHsnJxqP"

func TestConfigWithHDWallet(t *testing.T) {
	cfg, err := Parse([]byte(`
hd_wallets:
  - xpub: "` + anvilAccountXpub + `"
    count: 5
indexers:
  - {name: eth, chain_id: 1, rpc_url: "wss://example.com"}
`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Watched.Count() != 5 {
		t.Fatalf("watched = %d, want 5", cfg.Watched.Count())
	}
	first := common.HexToAddress("0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266")
	if !cfg.Watched.Contains(first) {
		t.Error("first derived anvil address not watched")
	}
	// Unnamed wallets label their addresses with the xpub itself.
	if got := cfg.Watched.Name(first); got != anvilAccountXpub {
		t.Errorf("derived label = %q, want the xpub", got)
	}
}

func TestConfigHDWalletNamed(t *testing.T) {
	cfg, err := Parse([]byte(`
hd_wallets:
  - name: deposits
    xpub: "` + anvilAccountXpub + `"
    count: 3
indexers:
  - {name: eth, chain_id: 1, rpc_url: "wss://example.com"}
`))
	if err != nil {
		t.Fatal(err)
	}
	found := 0
	cfg.Watched.Each(func(a common.Address) {
		if cfg.Watched.Name(a) == "deposits" {
			found++
		}
	})
	if found != 3 {
		t.Errorf("addresses labeled deposits = %d, want all 3", found)
	}
}

func TestConfigHDWalletDuplicateStatic(t *testing.T) {
	_, err := Parse([]byte(`
addresses:
  - "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"
hd_wallets:
  - xpub: "` + anvilAccountXpub + `"
    count: 1
indexers:
  - {name: eth, chain_id: 1, rpc_url: "wss://example.com"}
`))
	if err == nil {
		t.Fatal("overlapping static and derived address accepted")
	}
}

func TestIndexerDefaults(t *testing.T) {
	cfg, err := Parse([]byte(`
addresses:
  - "0x0000000000000000000000000000000000000001"
indexers:
  - {name: eth, chain_id: 1, rpc_url: "wss://example.com"}
`))
	if err != nil {
		t.Fatal(err)
	}
	ix := cfg.Indexers[0]
	if ix.Concurrency != 8 {
		t.Errorf("concurrency default = %d, want 8", ix.Concurrency)
	}
	if ix.BatchBlocks != 10 {
		t.Errorf("batch_blocks default = %d, want 10", ix.BatchBlocks)
	}
	if ix.Confirmations.Depth != 12 {
		t.Errorf("confirmations default = %d, want 12", ix.Confirmations.Depth)
	}
	if ix.PendingTimeout != 30*time.Minute {
		t.Errorf("pending_timeout default = %v, want 30m", ix.PendingTimeout)
	}
	if ix.NativeSymbol != "ETH" {
		t.Errorf("native_symbol default = %q, want ETH", ix.NativeSymbol)
	}
	if ix.MaxCatchupAge != 0 {
		t.Errorf("max_catchup_age default = %v, want 0 (unlimited)", ix.MaxCatchupAge)
	}
}

func TestEnvExpansion(t *testing.T) {
	t.Setenv("ETHINDEX_TEST_HOST", "node.example.com")
	t.Setenv("ETHINDEX_TEST_KEY", "sekrit")

	cfg, err := Parse([]byte(`
database: "$literal-dollar.db"
addresses:
  - "0x0000000000000000000000000000000000000001"
indexers:
  - {name: eth, chain_id: 1, rpc_url: "wss://${ETHINDEX_TEST_HOST}/v2/${ETHINDEX_TEST_KEY}"}
`))
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.Indexers[0].RPCURL; got != "wss://node.example.com/v2/sekrit" {
		t.Errorf("rpc_url = %q", got)
	}
	// A bare $ without braces stays literal.
	if cfg.Database != "$literal-dollar.db" {
		t.Errorf("database = %q, want literal $", cfg.Database)
	}

	// Unset variables are a hard error naming the variable.
	_, err = Parse([]byte(`
addresses:
  - "0x0000000000000000000000000000000000000001"
indexers:
  - {name: eth, chain_id: 1, rpc_url: "wss://${ETHINDEX_TEST_UNSET_VAR}/"}
`))
	if err == nil || !strings.Contains(err.Error(), "ETHINDEX_TEST_UNSET_VAR") {
		t.Fatalf("err = %v, want unset-variable error naming the variable", err)
	}
}

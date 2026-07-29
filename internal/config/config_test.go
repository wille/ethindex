package config

import (
	"strings"
	"testing"
	"time"
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
	if upper.WatchedAddresses[0] != lower.WatchedAddresses[0] {
		t.Errorf("addresses differ: %s vs %s", upper.WatchedAddresses[0], lower.WatchedAddresses[0])
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
	if len(cfg.WatchedAddresses) != 5 {
		t.Fatalf("watched = %d, want 5", len(cfg.WatchedAddresses))
	}
	if cfg.WatchedAddresses[0].Hex() != "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266" {
		t.Errorf("first derived = %s", cfg.WatchedAddresses[0].Hex())
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

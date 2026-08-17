// Package config loads and validates the ethindex YAML configuration.
package config

import (
	"fmt"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"gopkg.in/yaml.v3"

	"github.com/wille/ethindex/internal/hdwallet"
)

// Token describes an ERC20 token to watch for Transfer activity.
type Token struct {
	Address  string `yaml:"address"`
	Symbol   string `yaml:"symbol"`
	Decimals uint8  `yaml:"decimals"`
}

// ParsedToken is a Token with its address parsed.
type ParsedToken struct {
	Address  common.Address
	Symbol   string
	Decimals uint8
}

// Confirmations is when a mined transfer counts as final: either a
// block depth (`confirmations: 12`) or a consensus tag
// (`confirmations: finalized` / `safe`) - the latter asks the node for
// the explicitly finalized/justified block instead of guessing by depth.
type Confirmations struct {
	Depth uint64 // >0 when depth-based
	Tag   string // "finalized" or "safe" when tag-based
}

func (c *Confirmations) UnmarshalYAML(node *yaml.Node) error {
	var depth uint64
	if err := node.Decode(&depth); err == nil {
		c.Depth = depth
		return nil
	}
	var tag string
	if err := node.Decode(&tag); err == nil && (tag == "finalized" || tag == "safe") {
		c.Tag = tag
		return nil
	}
	return fmt.Errorf("confirmations must be a number, %q or %q, got %q", "finalized", "safe", node.Value)
}

// RingDepth is how many recent headers the reorg buffer should hold for
// this finality mode.
func (c Confirmations) RingDepth() uint64 {
	if c.Tag != "" {
		// The finalized tag trails the head by up to ~2 epochs (~95
		// blocks) on mainnet; keep enough history to cover it.
		return 128
	}
	return c.Depth
}

func (c Confirmations) String() string {
	if c.Tag != "" {
		return c.Tag
	}
	return fmt.Sprintf("%d", c.Depth)
}

// IndexerConfig is one chain-watching indexer instance.
type IndexerConfig struct {
	Name    string `yaml:"name"`
	ChainID uint64 `yaml:"chain_id"`
	RPCURL  string `yaml:"rpc_url"`
	// HTTPRPCURL is used for all request/response calls (blocks, logs,
	// receipts, mempool); the WebSocket rpc_url then only carries
	// subscriptions. Derived from rpc_url by scheme swap (wss->https)
	// when omitted. Point it at the ws URL to force everything over the
	// single WebSocket connection.
	HTTPRPCURL     string        `yaml:"http_rpc_url"`
	Confirmations  Confirmations `yaml:"confirmations"`
	PendingTimeout time.Duration `yaml:"pending_timeout"`
	// NativeSymbol is reported as the asset of native-coin transfers on
	// this chain (default "ETH"; e.g. "BNB" on BSC, "POL" on Polygon).
	NativeSymbol string `yaml:"native_symbol"`
	// Concurrency is how many RPC requests may run in parallel against
	// this chain's node: catch-up span fetches and pending-transaction
	// lookups (default 8). Size to the node's rate limits: too much
	// concurrency against a throttled public provider is slower than
	// one request at a time.
	Concurrency int `yaml:"concurrency"`
	// BatchBlocks is how many blocks a catch-up worker fetches per
	// JSON-RPC batch request (default 10; 1 disables multi-block
	// batching). The node must accept batches of 2*batch_blocks
	// elements. In-flight blocks (and catch-up memory) scale with
	// concurrency x batch_blocks - size the pair to the chain's block
	// size: heavy chains (Ethereum, ~1MB per block with receipts) want
	// small batches, thin L2 blocks tolerate large ones.
	BatchBlocks int `yaml:"batch_blocks"`
	// MaxCatchupAge caps how far back a catch-up scans: blocks older
	// than this are skipped with a warning (0 = no cap, scan
	// everything). Transfers in skipped blocks are never indexed.
	MaxCatchupAge time.Duration `yaml:"max_catchup_age"`
	Tokens        []Token       `yaml:"tokens"`

	// TokenList is the parsed form of Tokens, populated by validate().
	TokenList []ParsedToken `yaml:"-"`
}

// AddressEntry is one watched-address config entry: either a bare
// address string, or a mapping with an optional display name
// (`{name: hot-wallet, address: "0x..."}`) printed on matched
// transaction logs and events.
type AddressEntry struct {
	Name    string `yaml:"name"`
	Address string `yaml:"address"`
}

func (e *AddressEntry) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind == yaml.ScalarNode {
		return node.Decode(&e.Address)
	}
	type plain AddressEntry
	return node.Decode((*plain)(e))
}

// Config is the top-level configuration file schema. Addresses are
// global: every indexer watches the same address set on its own chain.
type Config struct {
	// Database is the SQLite database path used to persist matched
	// transactions and indexer progress. Default "ethindex.db".
	Database string `yaml:"database"`
	// Metrics is the listen address for the Prometheus /metrics
	// endpoint (e.g. ":9090"). Empty disables the metrics server.
	Metrics string `yaml:"metrics"`
	// API is the listen address for the consumer events API
	// (e.g. ":8080"). Empty disables the API server.
	API       string            `yaml:"api"`
	Addresses []AddressEntry    `yaml:"addresses"`
	HDWallets []hdwallet.Wallet `yaml:"hd_wallets"`
	Indexers  []IndexerConfig   `yaml:"indexers"`

	// Watched is the parsed and labeled form of Addresses plus every
	// HD-derived address, populated by validate().
	Watched *WatchedSet `yaml:"-"`
}

// Load reads, parses and validates the config file at path.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config: %w", err)
	}
	return Parse(data)
}

// Parse parses and validates raw YAML config bytes, expanding
// ${VAR} environment references first.
func Parse(data []byte) (*Config, error) {
	expanded, err := expandEnv(string(data))
	if err != nil {
		return nil, err
	}
	cfg := &Config{}
	if err := yaml.Unmarshal([]byte(expanded), cfg); err != nil {
		return nil, fmt.Errorf("parsing config: %w", err)
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// envRef matches the explicit ${VAR} form only; a bare $ stays
// literal.
var envRef = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)\}`)

// expandEnv substitutes ${VAR} references with environment variables,
// so secrets (provider API keys in RPC URLs) can stay out of the
// config file. Referencing an unset variable is a hard config error
// rather than a silent empty string that would surface much later as
// a confusing dial failure.
func expandEnv(s string) (string, error) {
	var missing []string
	expanded := envRef.ReplaceAllStringFunc(s, func(ref string) string {
		name := ref[2 : len(ref)-1]
		if v, ok := os.LookupEnv(name); ok {
			return v
		}
		missing = append(missing, name)
		return ref
	})
	if len(missing) > 0 {
		return "", fmt.Errorf("config references unset environment variable(s): %s", strings.Join(missing, ", "))
	}
	return expanded, nil
}

func parseAddress(s string) (common.Address, error) {
	var zero common.Address
	if !common.IsHexAddress(s) {
		return zero, fmt.Errorf("invalid address %q", s)
	}
	return common.HexToAddress(s), nil
}

func (c *Config) validate() error {
	if c.Database == "" {
		c.Database = "ethindex.db"
	}
	if len(c.Addresses) == 0 && len(c.HDWallets) == 0 {
		return fmt.Errorf("at least one watched address or hd wallet is required")
	}
	c.Watched = newWatchedSet(len(c.Addresses))
	for _, entry := range c.Addresses {
		addr, err := parseAddress(entry.Address)
		if err != nil {
			return err
		}
		if !c.Watched.add(addr, c.Watched.intern(entry.Name)) {
			return fmt.Errorf("duplicate watched address %s", addr.Hex())
		}
	}
	for i, w := range c.HDWallets {
		derived, err := w.Derive()
		if err != nil {
			return fmt.Errorf("hd_wallets[%d]: %w", i, err)
		}
		// One interned label covers every address the wallet derives:
		// its name, or the xpub itself when unnamed.
		label := w.Name
		if label == "" {
			label = w.XPub
		}
		li := c.Watched.intern(label)
		for _, addr := range derived {
			if !c.Watched.add(addr, li) {
				return fmt.Errorf("hd_wallets[%d]: derived address %s is already watched", i, addr.Hex())
			}
		}
	}

	if len(c.Indexers) == 0 {
		return fmt.Errorf("at least one indexer is required")
	}
	names := make(map[string]struct{}, len(c.Indexers))
	for i := range c.Indexers {
		ix := &c.Indexers[i]
		if ix.Name == "" {
			return fmt.Errorf("indexer %d: name is required", i)
		}
		if _, dup := names[ix.Name]; dup {
			return fmt.Errorf("duplicate indexer name %q", ix.Name)
		}
		names[ix.Name] = struct{}{}
		if err := ix.validate(); err != nil {
			return fmt.Errorf("indexer %q: %w", ix.Name, err)
		}
	}
	return nil
}

func (ix *IndexerConfig) validate() error {
	if ix.ChainID == 0 {
		return fmt.Errorf("chain_id is required")
	}
	u, err := url.Parse(ix.RPCURL)
	if err != nil || (u.Scheme != "ws" && u.Scheme != "wss") {
		return fmt.Errorf("rpc_url must be a ws:// or wss:// URL, got %q", ix.RPCURL)
	}
	if ix.HTTPRPCURL == "" {
		h := *u
		h.Scheme = map[string]string{"ws": "http", "wss": "https"}[u.Scheme]
		ix.HTTPRPCURL = h.String()
	} else if hu, err := url.Parse(ix.HTTPRPCURL); err != nil || hu.Scheme == "" {
		return fmt.Errorf("http_rpc_url is not a valid URL: %q", ix.HTTPRPCURL)
	}
	if ix.Confirmations.Depth == 0 && ix.Confirmations.Tag == "" {
		ix.Confirmations.Depth = 12
	}
	if ix.PendingTimeout == 0 {
		ix.PendingTimeout = 30 * time.Minute
	}
	if ix.PendingTimeout < 0 {
		return fmt.Errorf("pending_timeout must be positive")
	}
	if ix.Concurrency == 0 {
		ix.Concurrency = 8
	}
	if ix.Concurrency < 0 {
		return fmt.Errorf("concurrency must be positive")
	}
	if ix.NativeSymbol == "" {
		ix.NativeSymbol = "ETH"
	}
	if ix.BatchBlocks == 0 {
		ix.BatchBlocks = 10
	}
	if ix.BatchBlocks < 0 {
		return fmt.Errorf("batch_blocks must be positive")
	}
	if ix.MaxCatchupAge < 0 {
		return fmt.Errorf("max_catchup_age must be positive")
	}

	seenTokens := make(map[common.Address]struct{}, len(ix.Tokens))
	for _, t := range ix.Tokens {
		addr, err := parseAddress(t.Address)
		if err != nil {
			return fmt.Errorf("token %s: %w", t.Symbol, err)
		}
		if t.Symbol == "" {
			return fmt.Errorf("token %s has no symbol", addr.Hex())
		}
		if _, dup := seenTokens[addr]; dup {
			return fmt.Errorf("duplicate token address %s", addr.Hex())
		}
		seenTokens[addr] = struct{}{}
		ix.TokenList = append(ix.TokenList, ParsedToken{Address: addr, Symbol: t.Symbol, Decimals: t.Decimals})
	}
	return nil
}

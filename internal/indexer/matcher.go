package indexer

import (
	"log/slog"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/wille/ethindex/internal/config"
	"github.com/wille/ethindex/internal/event"
)

// transferTopic is keccak256("Transfer(address,address,uint256)").
var transferTopic = common.HexToHash("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")

var (
	methodTransfer     = [4]byte{0xa9, 0x05, 0x9c, 0xbb} // transfer(address,uint256)
	methodTransferFrom = [4]byte{0x23, 0xb8, 0x72, 0xdd} // transferFrom(address,address,uint256)
)

// Match is one detected transfer touching a watched address, in either
// direction. A transaction can contain several matches (e.g. multiple
// ERC20 Transfer logs, or a native leg plus a token leg).
type Match struct {
	TxHash    common.Hash
	From      common.Address
	To        common.Address
	Direction event.Direction // which side is watched
	Asset     string          // "ETH" or token symbol
	Token     *common.Address
	Decimals  uint8
	Value     *big.Int
	LogIndex  *uint // set for mined ERC20 transfers

	// TxSender and TxNonce identify the (sender, nonce) slot the
	// transaction occupies, for replacement detection. TxSender is the
	// transaction signer, which for transferFrom differs from From.
	// Set by MatchTx and MatchBlockTx; unknown (zero) on log matches.
	TxSender common.Address
	TxNonce  uint64
}

// Matcher holds the watched-address and token sets and decodes
// transactions and logs into Matches. It never mutates after being
// built and is safe for concurrent use.
type Matcher struct {
	watched      *config.WatchedSet // shared read-only across indexers
	tokens       map[common.Address]config.ParsedToken
	signer       types.Signer
	nativeSymbol string
	log          *slog.Logger
}

func NewMatcher(watched *config.WatchedSet, tokens []config.ParsedToken, chainID *big.Int, nativeSymbol string) *Matcher {
	if nativeSymbol == "" {
		nativeSymbol = "ETH"
	}
	m := &Matcher{
		watched:      watched,
		tokens:       make(map[common.Address]config.ParsedToken, len(tokens)),
		signer:       types.LatestSignerForChainID(chainID),
		nativeSymbol: nativeSymbol,
		log:          slog.Default(),
	}
	for _, t := range tokens {
		m.tokens[t.Address] = t
	}
	return m
}

// WatchedCount is the number of watched addresses.
func (m *Matcher) WatchedCount() int { return m.watched.Count() }

// TokenAddresses returns the watched token contract addresses,
// for use in log filter queries.
func (m *Matcher) TokenAddresses() []common.Address {
	out := make([]common.Address, 0, len(m.tokens))
	for a := range m.tokens {
		out = append(out, a)
	}
	return out
}

// WatchedTopics returns the watched addresses as left-padded 32-byte
// topics, for use as the Transfer `to` topic in log filter queries.
func (m *Matcher) WatchedTopics() []common.Hash {
	out := make([]common.Hash, 0, m.watched.Count())
	m.watched.Each(func(a common.Address) {
		out = append(out, common.BytesToHash(a.Bytes()))
	})
	return out
}

// MatchTx inspects a full pending transaction and returns any transfers
// it would deliver to a watched address: a native ETH transfer, or
// decoded ERC20 transfer/transferFrom calldata. ERC20 calldata matches
// are best-effort - the actual outcome is determined by the Transfer
// logs once mined. The sender is recovered from the signature.
func (m *Matcher) MatchTx(tx *types.Transaction) []Match {
	if tx.To() == nil {
		return nil // contract creation
	}
	from, err := types.Sender(m.signer, tx)
	if err != nil {
		return nil
	}
	return withTxInfo(m.match(tx.Hash(), from, *tx.To(), tx.Value(), tx.Data()), from, tx.Nonce())
}

// MatchBlockTx inspects a mined transaction from a raw block fetch.
// Mined transactions carry the sender explicitly, so this also handles
// transaction types go-ethereum cannot decode (OP-stack deposits).
func (m *Matcher) MatchBlockTx(tx BlockTx) []Match {
	if tx.To == nil {
		return nil // contract creation
	}
	value := new(big.Int)
	if tx.Value != nil {
		value = (*big.Int)(tx.Value)
	}
	return withTxInfo(m.match(tx.Hash, tx.From, *tx.To, value, tx.Input), tx.From, uint64(tx.Nonce))
}

func withTxInfo(matches []Match, sender common.Address, nonce uint64) []Match {
	for i := range matches {
		matches[i].TxSender = sender
		matches[i].TxNonce = nonce
	}
	return matches
}

// direction classifies a transfer against the watched set; ok is false
// when neither side is watched.
func (m *Matcher) direction(from, to common.Address) (event.Direction, bool) {
	fromWatched := m.watched.Contains(from)
	toWatched := m.watched.Contains(to)
	switch {
	case fromWatched && toWatched:
		return event.DirectionSelf, true
	case toWatched:
		return event.DirectionIn, true
	case fromWatched:
		return event.DirectionOut, true
	}
	return "", false
}

// logZeroValue records a skipped zero-value transfer touching a
// watched address. Zero-value transfers move nothing and are never
// tracked: they are address-poisoning spam, planting lookalike
// addresses in the transfer history of a targeted wallet in the hope
// that a later payout copy-pastes the fake recipient. Logged at debug,
// never indexed - the poison addresses must not spread into events and
// the database.
func (m *Matcher) logZeroValue(hash common.Hash, from, to common.Address, asset string, dir event.Direction) {
	m.log.Debug("ignoring zero value transfer",
		"tx", hash.Hex(), "from", from.Hex(), "to", to.Hex(),
		"asset", asset, "direction", string(dir))
}

// match finds transfers touching watched addresses in either direction:
// the native value leg, and an ERC20 calldata leg when the destination
// is a watched token contract. signer is the transaction sender.
func (m *Matcher) match(hash common.Hash, signer, to common.Address, value *big.Int, data []byte) []Match {
	var out []Match

	if dir, ok := m.direction(signer, to); ok {
		switch {
		case value.Sign() > 0:
			out = append(out, Match{
				TxHash:    hash,
				From:      signer,
				To:        to,
				Direction: dir,
				Asset:     m.nativeSymbol,
				Decimals:  18,
				Value:     new(big.Int).Set(value),
			})
		case len(data) == 0:
			// A plain zero-value native send (calls carrying calldata are
			// ordinary contract interactions, not transfers).
			m.logZeroValue(hash, signer, to, m.nativeSymbol, dir)
		}
	}

	if token, ok := m.tokens[to]; ok {
		if tokenFrom, recipient, amount, ok := decodeERC20Transfer(signer, data); ok {
			if dir, ok := m.direction(tokenFrom, recipient); ok {
				if amount.Sign() == 0 {
					m.logZeroValue(hash, tokenFrom, recipient, token.Symbol, dir)
					return out
				}
				tokenAddr := to
				out = append(out, Match{
					TxHash:    hash,
					From:      tokenFrom,
					To:        recipient,
					Direction: dir,
					Asset:     token.Symbol,
					Token:     &tokenAddr,
					Decimals:  token.Decimals,
					Value:     amount,
				})
			}
		}
	}
	return out
}

// decodeERC20Transfer extracts the token sender, recipient and amount
// from transfer(address,uint256) or transferFrom(address,address,uint256)
// calldata. For transfer the token moves from the transaction signer;
// for transferFrom it moves from the decoded owner argument. Returns
// ok=false for anything else or malformed input.
func decodeERC20Transfer(signer common.Address, data []byte) (from, to common.Address, value *big.Int, ok bool) {
	if len(data) < 4 {
		return common.Address{}, common.Address{}, nil, false
	}
	var method [4]byte
	copy(method[:], data[:4])
	args := data[4:]

	word := func(i int) []byte { return args[32*i : 32*(i+1)] }

	switch method {
	case methodTransfer:
		if len(args) < 64 {
			return common.Address{}, common.Address{}, nil, false
		}
		return signer, common.BytesToAddress(word(0)), new(big.Int).SetBytes(word(1)), true
	case methodTransferFrom:
		if len(args) < 96 {
			return common.Address{}, common.Address{}, nil, false
		}
		return common.BytesToAddress(word(0)), common.BytesToAddress(word(1)), new(big.Int).SetBytes(word(2)), true
	}
	return common.Address{}, common.Address{}, nil, false
}

// MatchLog inspects a mined log and returns a Match if it is an ERC20
// Transfer on a watched token contract with a watched address on either
// side.
func (m *Matcher) MatchLog(lg types.Log) (Match, bool) {
	token, ok := m.tokens[lg.Address]
	if !ok {
		return Match{}, false
	}
	if len(lg.Topics) != 3 || lg.Topics[0] != transferTopic {
		return Match{}, false
	}
	from := common.BytesToAddress(lg.Topics[1].Bytes())
	to := common.BytesToAddress(lg.Topics[2].Bytes())
	dir, ok := m.direction(from, to)
	if !ok {
		return Match{}, false
	}
	value := new(big.Int).SetBytes(lg.Data)
	if value.Sign() == 0 {
		m.logZeroValue(lg.TxHash, from, to, token.Symbol, dir)
		return Match{}, false
	}
	idx := lg.Index
	tokenAddr := lg.Address
	return Match{
		TxHash:    lg.TxHash,
		From:      from,
		To:        to,
		Direction: dir,
		Asset:     token.Symbol,
		Token:     &tokenAddr,
		Decimals:  token.Decimals,
		Value:     value,
		LogIndex:  &idx,
	}, true
}

// Package event defines the transfer lifecycle event schema and its
// delivery sinks (stdout NDJSON, the API's live-subscriber hub).
package event

import (
	"encoding/json"
	"io"
	"sync"
	"time"
)

// Type is the lifecycle stage of a detected transfer.
type Type string

const (
	TypePending   Type = "pending"   // seen in the mempool
	TypeMined     Type = "mined"     // included in a block
	TypeConfirmed Type = "confirmed" // reached the confirmation threshold
	TypeReorged   Type = "reorged"   // containing block left the canonical chain
	TypeDropped   Type = "dropped"   // evicted from the mempool without being mined
	TypeFailed    Type = "failed"    // mined but reverted (receipt status 0)
	TypeReplaced  Type = "replaced"  // another tx from the same sender consumed its nonce
)

// Direction relates a transfer to the watched address set.
type Direction string

const (
	DirectionIn   Direction = "in"   // recipient is watched
	DirectionOut  Direction = "out"  // sender is watched
	DirectionSelf Direction = "self" // both sides are watched
)

// Event is one detected-transfer lifecycle update. The same object is
// logged, streamed over the events API and printed as NDJSON with
// -print.
type Event struct {
	Type           Type      `json:"type"`
	Indexer        string    `json:"indexer"`
	ChainID        uint64    `json:"chain_id"`
	Direction      Direction `json:"direction"`
	Timestamp      string    `json:"timestamp"`
	FirstSeen      string    `json:"first_seen"`
	FirstSeenBlock uint64    `json:"first_seen_block,omitempty"`
	// MinedAt is when the transaction was first seen included in a
	// block; empty while pending, immutable across reorgs.
	MinedAt  string `json:"mined_at,omitempty"`
	TxHash   string `json:"tx_hash"`
	LogIndex *uint  `json:"log_index,omitempty"`
	From     string `json:"from"`
	To       string `json:"to"`
	// Wallet is the configured name of the watched side's wallet
	// (name-or-xpub for HD wallets), resolved from config at emission -
	// never persisted, so renames apply to replayed history too.
	Wallet        string `json:"wallet,omitempty"`
	Asset         string `json:"asset"`
	TokenAddress  string `json:"token_address,omitempty"`
	Decimals      uint8  `json:"decimals"`
	ValueRaw      string `json:"value_raw"`
	Value         string `json:"value"`
	BlockNumber   uint64 `json:"block_number,omitempty"`
	BlockHash     string `json:"block_hash,omitempty"`
	Confirmations uint64 `json:"confirmations"`
	// ReplacedBy is the hash of the mined transaction that consumed
	// this transaction's nonce. Set on "replaced" events only.
	ReplacedBy string `json:"replaced_by,omitempty"`
}

// Emitter writes events as NDJSON to a writer, safe for concurrent use.
type Emitter struct {
	mu  sync.Mutex
	enc *json.Encoder
	now func() time.Time
}

// NewEmitter returns an Emitter writing to w.
func NewEmitter(w io.Writer) *Emitter {
	return &Emitter{enc: json.NewEncoder(w), now: time.Now}
}

// Emit writes a single event, stamping Timestamp if the producer has
// not already.
func (e *Emitter) Emit(ev Event) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if ev.Timestamp == "" {
		ev.Timestamp = e.now().UTC().Format(TimeLayout)
	}
	return e.enc.Encode(ev)
}

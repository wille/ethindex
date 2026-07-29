// Package storage defines the persistence interface for ethindex and
// its data transfer types. Implementations store matched transactions
// with their latest status and per-indexer progress so a restarted
// process resumes where it left off.
package storage

import (
	"context"
	"time"
)

// Transfer is one persisted transfer leg of a tracked transaction.
type Transfer struct {
	From         string
	To           string
	Direction    string
	Asset        string
	TokenAddress string
	Decimals     uint8
	ValueRaw     string
	Value        string // decimal-adjusted, for consumer convenience
	LogIndex     *uint  // nil for the native-coin leg
	TxSender     string
	TxNonce      uint64
}

// TrackedTx is a persisted snapshot of one matched transaction.
type TrackedTx struct {
	Hash           string
	Status         string // pending|mined|confirmed|dropped|failed|replaced
	BlockHash      string
	BlockNumber    uint64
	FirstSeen      time.Time
	FirstSeenBlock uint64
	MinedAt        time.Time // when first seen included; zero while pending
	PendingSince   time.Time
	Sender         string
	Nonce          uint64
	HasNonce       bool
	Authoritative  bool
	Transfers      []Transfer
	ReplacedBy     string
}

// ActiveStatuses are the non-terminal transaction statuses that must be
// reloaded on restart.
var ActiveStatuses = []string{"pending", "mined"}

// LegUpdate is one transfer-leg row together with its transaction's
// lifecycle state, as returned by the change-feed queries backing the
// events API.
type LegUpdate struct {
	Indexer        string
	TxHash         string
	Status         string
	BlockHash      string
	BlockNumber    uint64
	FirstSeen      time.Time
	FirstSeenBlock uint64
	MinedAt        time.Time
	ReplacedBy     string
	UpdatedAt      time.Time
	Transfer
}

// LegCursor is a keyset position in the change feed, which is ordered
// by (updated_at, tx_hash, log_index). An empty TxHash means "strictly
// after UpdatedAt" - the form of a consumer-supplied timestamp cursor;
// paging within one read fills all three fields from the last row.
type LegCursor struct {
	UpdatedAt time.Time
	TxHash    string
	LogIndex  int64
}

// HeaderRef is a persisted recent canonical header.
type HeaderRef struct {
	Number     uint64 `json:"number"`
	Hash       string `json:"hash"`
	ParentHash string `json:"parent_hash"`
}

// IndexerState is one indexer's resumable progress.
type IndexerState struct {
	LastProcessed uint64
	RecentHeaders []HeaderRef
}

// Storage persists matched transactions and indexer progress. All
// methods are safe for concurrent use by multiple indexers.
type Storage interface {
	// SaveTransaction upserts the full latest snapshot of a transaction.
	SaveTransaction(ctx context.Context, indexer string, tx TrackedTx) error
	// UpdateTransactionStatus records a terminal status transition for
	// a transaction whose snapshot is already stored. Returns false when
	// no such row exists (the caller then stores a full snapshot).
	UpdateTransactionStatus(ctx context.Context, indexer, txHash, status, replacedBy string) (bool, error)
	// LoadActiveTransactions returns every non-terminal transaction for
	// an indexer.
	LoadActiveTransactions(ctx context.Context, indexer string) ([]TrackedTx, error)
	// LoadLegsUpdatedSince returns up to limit transfer-leg rows
	// changed after the cursor position, ascending by (updated_at,
	// tx_hash, log_index). indexer == "" spans all indexers.
	LoadLegsUpdatedSince(ctx context.Context, indexer string, cursor LegCursor, limit int) ([]LegUpdate, error)
	// LoadLatestLegs returns the limit most recently updated
	// transfer-leg rows, ascending by update order. indexer == ""
	// spans all indexers.
	LoadLatestLegs(ctx context.Context, indexer string, limit int) ([]LegUpdate, error)
	// SaveIndexerState upserts an indexer's progress.
	SaveIndexerState(ctx context.Context, indexer string, st IndexerState) error
	// LoadIndexerState returns an indexer's progress, or nil if none is
	// stored yet.
	LoadIndexerState(ctx context.Context, indexer string) (*IndexerState, error)

	Close() error
}

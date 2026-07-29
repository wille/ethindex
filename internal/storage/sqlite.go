package storage

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

// The transactions table is normalized to one row per transfer leg,
// each carrying its transaction's full lifecycle - so consumers can
// query payments by address directly (WHERE to_address = ?) with no
// JSON unpacking and no joins. log_index -1 marks the native-coin leg
// (a transaction has at most one).
const schema = `
CREATE TABLE IF NOT EXISTS transactions (
	indexer          TEXT    NOT NULL,
	tx_hash          TEXT    NOT NULL,
	log_index        INTEGER NOT NULL,
	status           TEXT    NOT NULL,
	direction        TEXT    NOT NULL,
	asset            TEXT    NOT NULL,
	token_address    TEXT    NOT NULL DEFAULT '',
	from_address     TEXT    NOT NULL,
	to_address       TEXT    NOT NULL,
	decimals         INTEGER NOT NULL,
	value_raw        TEXT    NOT NULL,
	value            TEXT    NOT NULL,
	block_hash       TEXT    NOT NULL DEFAULT '',
	block_number     INTEGER NOT NULL DEFAULT 0,
	first_seen       TEXT    NOT NULL,
	first_seen_block INTEGER NOT NULL DEFAULT 0,
	mined_at         TEXT    NOT NULL DEFAULT '',
	pending_since    TEXT    NOT NULL,
	sender           TEXT    NOT NULL DEFAULT '',
	nonce            INTEGER NOT NULL DEFAULT 0,
	has_nonce        INTEGER NOT NULL DEFAULT 0,
	authoritative    INTEGER NOT NULL DEFAULT 0,
	replaced_by      TEXT    NOT NULL DEFAULT '',
	updated_at       TEXT    NOT NULL,
	PRIMARY KEY (indexer, tx_hash, log_index)
);
CREATE INDEX IF NOT EXISTS idx_transactions_status  ON transactions (indexer, status);
CREATE INDEX IF NOT EXISTS idx_transactions_to      ON transactions (indexer, to_address);
CREATE INDEX IF NOT EXISTS idx_transactions_from    ON transactions (indexer, from_address);
CREATE INDEX IF NOT EXISTS idx_transactions_updated ON transactions (indexer, updated_at);
CREATE INDEX IF NOT EXISTS idx_transactions_updated_global ON transactions (updated_at);

CREATE TABLE IF NOT EXISTS indexer_state (
	indexer        TEXT PRIMARY KEY,
	last_processed INTEGER NOT NULL,
	headers        TEXT    NOT NULL,
	updated_at     TEXT    NOT NULL
);
`

// nativeLegIndex is the log_index sentinel for the native-coin leg.
const nativeLegIndex = -1

// sqliteStorage implements Storage on a local SQLite database.
type sqliteStorage struct {
	db *sql.DB
}

// OpenSQLite opens (creating if needed) a SQLite database at path and
// ensures the schema exists.
func OpenSQLite(path string) (Storage, error) {
	// WAL for concurrent indexers, busy_timeout so writers wait instead
	// of failing when the database is momentarily locked.
	dsn := fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=synchronous(NORMAL)", path)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("opening sqlite database %s: %w", path, err)
	}
	if _, err := db.Exec(schema); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("initializing sqlite schema: %w", err)
	}
	return &sqliteStorage{db: db}, nil
}

// timeLayout is RFC3339 with fixed-width nanoseconds. RFC3339Nano
// trims trailing zeros, which breaks the lexicographic-equals-
// chronological ordering the change-feed cursor queries rely on.
const timeLayout = "2006-01-02T15:04:05.000000000Z07:00"

func formatTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(timeLayout)
}

func nowString() string {
	return time.Now().UTC().Format(timeLayout)
}

func parseTime(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, nil
	}
	return time.Parse(time.RFC3339Nano, s)
}

// SaveTransaction replaces the transaction's rows with one per leg.
// Delete-then-insert, not upsert: leg identities change when pending
// calldata guesses are superseded by authoritative logs, and an upsert
// would leave the stale rows behind.
func (s *sqliteStorage) SaveTransaction(ctx context.Context, indexer string, tx TrackedTx) error {
	dbtx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	// Rollback after Commit is a documented no-op error; ignored.
	defer func() { _ = dbtx.Rollback() }()

	if _, err := dbtx.ExecContext(ctx,
		`DELETE FROM transactions WHERE indexer = ? AND tx_hash = ?`,
		indexer, tx.Hash,
	); err != nil {
		return err
	}
	now := nowString()
	for _, leg := range tx.Transfers {
		logIndex := int64(nativeLegIndex)
		if leg.LogIndex != nil {
			logIndex = int64(*leg.LogIndex)
		}
		if _, err := dbtx.ExecContext(ctx, `
			INSERT INTO transactions (
				indexer, tx_hash, log_index, status, direction, asset,
				token_address, from_address, to_address, decimals,
				value_raw, value, block_hash, block_number, first_seen,
				first_seen_block, mined_at, pending_since, sender, nonce,
				has_nonce, authoritative, replaced_by, updated_at
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			indexer, tx.Hash, logIndex, tx.Status, leg.Direction, leg.Asset,
			leg.TokenAddress, leg.From, leg.To, leg.Decimals,
			leg.ValueRaw, leg.Value, tx.BlockHash, tx.BlockNumber, formatTime(tx.FirstSeen),
			tx.FirstSeenBlock, formatTime(tx.MinedAt), formatTime(tx.PendingSince), tx.Sender, tx.Nonce,
			tx.HasNonce, tx.Authoritative, tx.ReplacedBy, now,
		); err != nil {
			return err
		}
	}
	return dbtx.Commit()
}

// UpdateTransactionStatus records a terminal transition on every leg
// row of the transaction.
func (s *sqliteStorage) UpdateTransactionStatus(ctx context.Context, indexer, txHash, status, replacedBy string) (bool, error) {
	res, err := s.db.ExecContext(ctx, `
		UPDATE transactions SET status = ?, replaced_by = ?, updated_at = ?
		WHERE indexer = ? AND tx_hash = ?`,
		status, replacedBy, nowString(),
		indexer, txHash,
	)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n > 0, err
}

// LoadActiveTransactions reassembles the per-leg rows into
// per-transaction DTOs.
func (s *sqliteStorage) LoadActiveTransactions(ctx context.Context, indexer string) ([]TrackedTx, error) {
	placeholders := strings.Repeat("?,", len(ActiveStatuses))
	args := []any{indexer}
	for _, st := range ActiveStatuses {
		args = append(args, st)
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT tx_hash, log_index, status, direction, asset, token_address,
		       from_address, to_address, decimals, value_raw, value,
		       block_hash, block_number, first_seen, first_seen_block,
		       mined_at, pending_since, sender, nonce, has_nonce,
		       authoritative, replaced_by
		FROM transactions
		WHERE indexer = ? AND status IN (`+placeholders[:len(placeholders)-1]+`)
		ORDER BY tx_hash, log_index`,
		args...,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var (
		out     []TrackedTx
		current *TrackedTx
	)
	for rows.Next() {
		var (
			hash, firstSeen, minedAt, pendingSince string
			logIndex                               int64
			tx                                     TrackedTx
			leg                                    Transfer
		)
		if err := rows.Scan(
			&hash, &logIndex, &tx.Status, &leg.Direction, &leg.Asset, &leg.TokenAddress,
			&leg.From, &leg.To, &leg.Decimals, &leg.ValueRaw, &leg.Value,
			&tx.BlockHash, &tx.BlockNumber, &firstSeen, &tx.FirstSeenBlock,
			&minedAt, &pendingSince, &tx.Sender, &tx.Nonce, &tx.HasNonce,
			&tx.Authoritative, &tx.ReplacedBy,
		); err != nil {
			return nil, err
		}
		if logIndex != nativeLegIndex {
			idx := uint(logIndex)
			leg.LogIndex = &idx
		}
		// The tx signer and nonce are transaction-level columns; mirror
		// them back onto each leg for a faithful roundtrip.
		leg.TxSender = tx.Sender
		leg.TxNonce = tx.Nonce
		if current == nil || current.Hash != hash {
			tx.Hash = hash
			if tx.FirstSeen, err = parseTime(firstSeen); err != nil {
				return nil, fmt.Errorf("tx %s: parsing first_seen: %w", hash, err)
			}
			if tx.MinedAt, err = parseTime(minedAt); err != nil {
				return nil, fmt.Errorf("tx %s: parsing mined_at: %w", hash, err)
			}
			if tx.PendingSince, err = parseTime(pendingSince); err != nil {
				return nil, fmt.Errorf("tx %s: parsing pending_since: %w", hash, err)
			}
			out = append(out, tx)
			current = &out[len(out)-1]
		}
		current.Transfers = append(current.Transfers, leg)
	}
	return out, rows.Err()
}

// legColumns is the change-feed projection shared by
// LoadLegsUpdatedSince and LoadLatestLegs.
const legColumns = `indexer, tx_hash, log_index, status, direction, asset,
	token_address, from_address, to_address, decimals, value_raw, value,
	block_hash, block_number, first_seen, first_seen_block, mined_at,
	replaced_by, updated_at`

func scanLegUpdate(rows *sql.Rows) (LegUpdate, error) {
	var (
		leg                         LegUpdate
		logIndex                    int64
		firstSeen, minedAt, updated string
	)
	if err := rows.Scan(
		&leg.Indexer, &leg.TxHash, &logIndex, &leg.Status, &leg.Direction,
		&leg.Asset, &leg.TokenAddress, &leg.From, &leg.To, &leg.Decimals,
		&leg.ValueRaw, &leg.Value, &leg.BlockHash, &leg.BlockNumber,
		&firstSeen, &leg.FirstSeenBlock, &minedAt, &leg.ReplacedBy, &updated,
	); err != nil {
		return leg, err
	}
	if logIndex != nativeLegIndex {
		idx := uint(logIndex)
		leg.LogIndex = &idx
	}
	var err error
	if leg.FirstSeen, err = parseTime(firstSeen); err != nil {
		return leg, fmt.Errorf("tx %s: parsing first_seen: %w", leg.TxHash, err)
	}
	if leg.MinedAt, err = parseTime(minedAt); err != nil {
		return leg, fmt.Errorf("tx %s: parsing mined_at: %w", leg.TxHash, err)
	}
	if leg.UpdatedAt, err = parseTime(updated); err != nil {
		return leg, fmt.Errorf("tx %s: parsing updated_at: %w", leg.TxHash, err)
	}
	return leg, nil
}

func collectLegUpdates(rows *sql.Rows) ([]LegUpdate, error) {
	defer rows.Close()
	var out []LegUpdate
	for rows.Next() {
		leg, err := scanLegUpdate(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, leg)
	}
	return out, rows.Err()
}

func (s *sqliteStorage) LoadLegsUpdatedSince(ctx context.Context, indexer string, cursor LegCursor, limit int) ([]LegUpdate, error) {
	var (
		where string
		args  []any
	)
	after := formatTime(cursor.UpdatedAt)
	if cursor.TxHash == "" {
		where = `updated_at > ?`
		args = append(args, after)
	} else {
		// Keyset paging: resume exactly after the last row of the
		// previous page, even when many rows share one updated_at.
		where = `(updated_at > ? OR (updated_at = ? AND (tx_hash > ? OR (tx_hash = ? AND log_index > ?))))`
		args = append(args, after, after, cursor.TxHash, cursor.TxHash, cursor.LogIndex)
	}
	if indexer != "" {
		where += ` AND indexer = ?`
		args = append(args, indexer)
	}
	args = append(args, limit)
	rows, err := s.db.QueryContext(ctx, `
		SELECT `+legColumns+`
		FROM transactions
		WHERE `+where+`
		ORDER BY updated_at, tx_hash, log_index
		LIMIT ?`,
		args...,
	)
	if err != nil {
		return nil, err
	}
	return collectLegUpdates(rows)
}

func (s *sqliteStorage) LoadLatestLegs(ctx context.Context, indexer string, limit int) ([]LegUpdate, error) {
	where, args := `1=1`, []any{}
	if indexer != "" {
		where = `indexer = ?`
		args = append(args, indexer)
	}
	args = append(args, limit)
	rows, err := s.db.QueryContext(ctx, `
		SELECT `+legColumns+`
		FROM transactions
		WHERE `+where+`
		ORDER BY updated_at DESC, tx_hash DESC, log_index DESC
		LIMIT ?`,
		args...,
	)
	if err != nil {
		return nil, err
	}
	out, err := collectLegUpdates(rows)
	if err != nil {
		return nil, err
	}
	// Newest-first fetch, ascending result.
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return out, nil
}

func (s *sqliteStorage) SaveIndexerState(ctx context.Context, indexer string, st IndexerState) error {
	headers, err := encodeHeaders(st.RecentHeaders)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `
		INSERT INTO indexer_state (indexer, last_processed, headers, updated_at)
		VALUES (?, ?, ?, ?)
		ON CONFLICT (indexer) DO UPDATE SET
			last_processed = excluded.last_processed,
			headers = excluded.headers,
			updated_at = excluded.updated_at`,
		indexer, st.LastProcessed, headers,
		nowString(),
	)
	return err
}

func (s *sqliteStorage) LoadIndexerState(ctx context.Context, indexer string) (*IndexerState, error) {
	var (
		st      IndexerState
		headers string
	)
	err := s.db.QueryRowContext(ctx, `
		SELECT last_processed, headers FROM indexer_state WHERE indexer = ?`,
		indexer,
	).Scan(&st.LastProcessed, &headers)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if st.RecentHeaders, err = decodeHeaders(headers); err != nil {
		return nil, err
	}
	return &st, nil
}

func (s *sqliteStorage) Close() error {
	return s.db.Close()
}

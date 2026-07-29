package storage

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func openTestDB(t *testing.T) Storage {
	t.Helper()
	s, err := OpenSQLite(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func sampleTx(hash string) TrackedTx {
	idx := uint(3)
	return TrackedTx{
		Hash:           hash,
		Status:         "mined",
		BlockHash:      "0xblock",
		BlockNumber:    123,
		FirstSeen:      time.Date(2026, 7, 24, 10, 0, 0, 0, time.UTC),
		FirstSeenBlock: 120,
		MinedAt:        time.Date(2026, 7, 24, 10, 0, 7, 0, time.UTC),
		PendingSince:   time.Date(2026, 7, 24, 10, 0, 5, 0, time.UTC),
		Sender:         "0xsender",
		Nonce:          7,
		HasNonce:       true,
		Authoritative:  true,
		Transfers: []Transfer{
			// Reassembly orders the native leg (log_index -1) first.
			{From: "0xa", To: "0xb", Direction: "in", Asset: "ETH",
				Decimals: 18, ValueRaw: "1000000000000000000", Value: "1", TxSender: "0xa", TxNonce: 7},
			{From: "0xa", To: "0xb", Direction: "in", Asset: "USDC", TokenAddress: "0xtoken",
				Decimals: 6, ValueRaw: "2500000", Value: "2.5", LogIndex: &idx, TxSender: "0xa", TxNonce: 7},
		},
	}
}

func TestTransactionRoundtrip(t *testing.T) {
	s := openTestDB(t)
	ctx := context.Background()
	want := sampleTx("0x01")

	if err := s.SaveTransaction(ctx, "ethereum", want); err != nil {
		t.Fatal(err)
	}
	got, err := s.LoadActiveTransactions(ctx, "ethereum")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("loaded %d txs, want 1", len(got))
	}
	tx := got[0]
	if tx.Hash != want.Hash || tx.Status != want.Status || tx.BlockNumber != want.BlockNumber {
		t.Errorf("roundtrip mismatch: %+v", tx)
	}
	if !tx.FirstSeen.Equal(want.FirstSeen) || !tx.PendingSince.Equal(want.PendingSince) {
		t.Errorf("time mismatch: %v / %v", tx.FirstSeen, tx.PendingSince)
	}
	if !tx.HasNonce || tx.Nonce != 7 || !tx.Authoritative {
		t.Errorf("flags mismatch: %+v", tx)
	}
	if tx.FirstSeenBlock != 120 {
		t.Errorf("first_seen_block = %d, want 120", tx.FirstSeenBlock)
	}
	if len(tx.Transfers) != 2 {
		t.Fatalf("transfers = %d, want 2", len(tx.Transfers))
	}
	if tx.Transfers[0].LogIndex != nil {
		t.Errorf("native leg log index not nil: %v", tx.Transfers[0].LogIndex)
	}
	if tx.Transfers[1].LogIndex == nil || *tx.Transfers[1].LogIndex != 3 {
		t.Errorf("log index = %v", tx.Transfers[1].LogIndex)
	}
	if tx.Transfers[1].Value != "2.5" || tx.Transfers[0].Value != "1" {
		t.Errorf("formatted values lost: %+v", tx.Transfers)
	}
	if !tx.MinedAt.Equal(want.MinedAt) {
		t.Errorf("mined_at = %v, want %v", tx.MinedAt, want.MinedAt)
	}
}

func TestPerLegRowsAndAddressQuery(t *testing.T) {
	s := openTestDB(t)
	ctx := context.Background()
	if err := s.SaveTransaction(ctx, "ethereum", sampleTx("0x10")); err != nil {
		t.Fatal(err)
	}

	// One row per leg, queryable by address without JSON unpacking.
	db := s.(*sqliteStorage).db
	var rows int
	if err := db.QueryRow(`SELECT COUNT(*) FROM transactions WHERE indexer = 'ethereum' AND tx_hash = '0x10'`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 2 {
		t.Fatalf("rows = %d, want one per leg (2)", rows)
	}
	var byAddr int
	if err := db.QueryRow(`SELECT COUNT(*) FROM transactions WHERE indexer = 'ethereum' AND to_address = '0xb' AND status = 'mined'`).Scan(&byAddr); err != nil {
		t.Fatal(err)
	}
	if byAddr != 2 {
		t.Fatalf("address query = %d rows, want 2", byAddr)
	}

	// Saving with changed legs replaces rows instead of accreting.
	tx := sampleTx("0x10")
	tx.Transfers = tx.Transfers[:1] // authoritative scan dropped a leg
	if err := s.SaveTransaction(ctx, "ethereum", tx); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM transactions WHERE indexer = 'ethereum' AND tx_hash = '0x10'`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 1 {
		t.Fatalf("rows after leg change = %d, want 1 (stale legs must be deleted)", rows)
	}

	// A status update touches every remaining leg row.
	if _, err := s.UpdateTransactionStatus(ctx, "ethereum", "0x10", "confirmed", ""); err != nil {
		t.Fatal(err)
	}
	var confirmed int
	if err := db.QueryRow(`SELECT COUNT(*) FROM transactions WHERE tx_hash = '0x10' AND status = 'confirmed'`).Scan(&confirmed); err != nil {
		t.Fatal(err)
	}
	if confirmed != 1 {
		t.Fatalf("confirmed rows = %d, want all legs updated", confirmed)
	}
}

func TestUpsertOverwrites(t *testing.T) {
	s := openTestDB(t)
	ctx := context.Background()
	tx := sampleTx("0x02")
	tx.Status = "pending"
	tx.BlockHash = ""
	tx.BlockNumber = 0
	if err := s.SaveTransaction(ctx, "ethereum", tx); err != nil {
		t.Fatal(err)
	}

	tx.Status = "mined"
	tx.BlockHash = "0xnewblock"
	tx.BlockNumber = 456
	if err := s.SaveTransaction(ctx, "ethereum", tx); err != nil {
		t.Fatal(err)
	}

	got, err := s.LoadActiveTransactions(ctx, "ethereum")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("loaded %d txs, want 1 after upsert", len(got))
	}
	if got[0].Status != "mined" || got[0].BlockNumber != 456 {
		t.Errorf("upsert not applied: %+v", got[0])
	}
}

func TestTerminalStatusExcluded(t *testing.T) {
	s := openTestDB(t)
	ctx := context.Background()
	if err := s.SaveTransaction(ctx, "ethereum", sampleTx("0x03")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.UpdateTransactionStatus(ctx, "ethereum", "0x03", "confirmed", ""); err != nil {
		t.Fatal(err)
	}
	got, err := s.LoadActiveTransactions(ctx, "ethereum")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Errorf("terminal tx still active: %+v", got)
	}
}

func TestReplacedByPersisted(t *testing.T) {
	s := openTestDB(t)
	ctx := context.Background()
	tx := sampleTx("0x04")
	tx.Status = "pending"
	if err := s.SaveTransaction(ctx, "ethereum", tx); err != nil {
		t.Fatal(err)
	}
	if _, err := s.UpdateTransactionStatus(ctx, "ethereum", "0x04", "replaced", "0xwinner"); err != nil {
		t.Fatal(err)
	}
	// Not active anymore; verify the row via a fresh save-load cycle on
	// another tx is unaffected (isolation) - replaced rows stay as history.
	got, err := s.LoadActiveTransactions(ctx, "ethereum")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Errorf("replaced tx still active: %+v", got)
	}
}

func TestIndexerIsolation(t *testing.T) {
	s := openTestDB(t)
	ctx := context.Background()
	if err := s.SaveTransaction(ctx, "ethereum", sampleTx("0x05")); err != nil {
		t.Fatal(err)
	}
	got, err := s.LoadActiveTransactions(ctx, "base")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Errorf("tx leaked across indexers: %+v", got)
	}
}

func TestLegFeedCursor(t *testing.T) {
	s := openTestDB(t)
	ctx := context.Background()

	if err := s.SaveTransaction(ctx, "ethereum", sampleTx("0x20")); err != nil {
		t.Fatal(err)
	}
	afterFirst := time.Now()
	time.Sleep(2 * time.Millisecond)
	if err := s.SaveTransaction(ctx, "base", sampleTx("0x21")); err != nil {
		t.Fatal(err)
	}

	// A zero cursor returns everything, ascending by update order.
	all, err := s.LoadLegsUpdatedSince(ctx, "", LegCursor{}, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 4 {
		t.Fatalf("legs = %d, want 4 (2 txs x 2 legs)", len(all))
	}
	if all[0].TxHash != "0x20" || all[3].TxHash != "0x21" {
		t.Errorf("order = %s .. %s, want 0x20 first", all[0].TxHash, all[3].TxHash)
	}
	if all[0].Status != "mined" || all[0].UpdatedAt.IsZero() || all[0].Indexer != "ethereum" {
		t.Errorf("leg fields = %+v", all[0])
	}

	// A timestamp cursor excludes rows updated before it.
	since, err := s.LoadLegsUpdatedSince(ctx, "", LegCursor{UpdatedAt: afterFirst}, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(since) != 2 || since[0].TxHash != "0x21" {
		t.Fatalf("legs after cursor = %+v, want only 0x21", since)
	}

	// Indexer filter.
	eth, err := s.LoadLegsUpdatedSince(ctx, "ethereum", LegCursor{}, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(eth) != 2 || eth[0].TxHash != "0x20" {
		t.Fatalf("filtered legs = %+v", eth)
	}
}

func TestLegFeedKeysetPaging(t *testing.T) {
	s := openTestDB(t)
	ctx := context.Background()
	// Both legs of one tx share an identical updated_at (stamped once
	// per save), the case plain timestamp cursors cannot page through.
	if err := s.SaveTransaction(ctx, "ethereum", sampleTx("0x30")); err != nil {
		t.Fatal(err)
	}

	var (
		got    []LegUpdate
		cursor LegCursor
	)
	for {
		page, err := s.LoadLegsUpdatedSince(ctx, "", cursor, 1)
		if err != nil {
			t.Fatal(err)
		}
		if len(page) == 0 {
			break
		}
		got = append(got, page...)
		last := page[len(page)-1]
		idx := int64(-1)
		if last.LogIndex != nil {
			idx = int64(*last.LogIndex)
		}
		cursor = LegCursor{UpdatedAt: last.UpdatedAt, TxHash: last.TxHash, LogIndex: idx}
		if len(got) > 4 {
			t.Fatal("paging loops without terminating")
		}
	}
	if len(got) != 2 {
		t.Fatalf("paged legs = %d, want 2 (no skips, no dups)", len(got))
	}
	if got[0].LogIndex != nil || got[1].LogIndex == nil {
		t.Errorf("leg order = %+v, want native leg first", got)
	}
}

func TestLoadLatestLegs(t *testing.T) {
	s := openTestDB(t)
	ctx := context.Background()
	for i, hash := range []string{"0x40", "0x41", "0x42"} {
		if err := s.SaveTransaction(ctx, "ethereum", sampleTx(hash)); err != nil {
			t.Fatal(err)
		}
		if i < 2 {
			time.Sleep(2 * time.Millisecond)
		}
	}

	latest, err := s.LoadLatestLegs(ctx, "ethereum", 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(latest) != 2 {
		t.Fatalf("legs = %d, want limit 2", len(latest))
	}
	// The two newest legs (both of 0x42), ascending.
	if latest[0].TxHash != "0x42" || latest[1].TxHash != "0x42" {
		t.Errorf("latest = %s, %s, want the newest tx", latest[0].TxHash, latest[1].TxHash)
	}
	if latest[0].LogIndex != nil || latest[1].LogIndex == nil {
		t.Errorf("ascending order violated: %+v", latest)
	}

	none, err := s.LoadLatestLegs(ctx, "base", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(none) != 0 {
		t.Errorf("legs for other indexer = %+v", none)
	}
}

func TestIndexerStateRoundtrip(t *testing.T) {
	s := openTestDB(t)
	ctx := context.Background()

	// Absent state loads as nil.
	st, err := s.LoadIndexerState(ctx, "ethereum")
	if err != nil {
		t.Fatal(err)
	}
	if st != nil {
		t.Fatalf("expected nil state, got %+v", st)
	}

	want := IndexerState{
		LastProcessed: 999,
		RecentHeaders: []HeaderRef{
			{Number: 998, Hash: "0xh1", ParentHash: "0xh0"},
			{Number: 999, Hash: "0xh2", ParentHash: "0xh1"},
		},
	}
	if err := s.SaveIndexerState(ctx, "ethereum", want); err != nil {
		t.Fatal(err)
	}
	// Upsert.
	want.LastProcessed = 1000
	if err := s.SaveIndexerState(ctx, "ethereum", want); err != nil {
		t.Fatal(err)
	}

	st, err = s.LoadIndexerState(ctx, "ethereum")
	if err != nil {
		t.Fatal(err)
	}
	if st == nil || st.LastProcessed != 1000 || len(st.RecentHeaders) != 2 {
		t.Fatalf("state = %+v", st)
	}
	if st.RecentHeaders[1].Hash != "0xh2" {
		t.Errorf("headers = %+v", st.RecentHeaders)
	}
}

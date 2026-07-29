package indexer

import (
	"math/big"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/wille/ethindex/internal/config"
	"github.com/wille/ethindex/internal/event"
	"github.com/wille/ethindex/internal/storage"
)

type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

func newTestTracker() (*Tracker, *fakeClock) {
	clock := &fakeClock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	return NewTracker(config.Confirmations{Depth: 3}, 30*time.Minute, clock.now), clock
}

func ethMatch(txHash byte) []Match {
	return []Match{{
		TxHash:   common.Hash{txHash},
		From:     otherAddr,
		To:       watchedAddr,
		Asset:    "ETH",
		Decimals: 18,
		Value:    big.NewInt(1e18),
		TxSender: otherAddr,
		TxNonce:  uint64(txHash), // distinct nonce slot per fixture tx
	}}
}

func tokenMatch(txHash byte, logIndex uint) Match {
	token := tokenAddr
	return Match{
		TxHash:   common.Hash{txHash},
		From:     otherAddr,
		To:       watchedAddr,
		Asset:    "TEST",
		Token:    &token,
		Decimals: 6,
		Value:    big.NewInt(1_000_000),
		LogIndex: &logIndex,
	}
}

func eventTypes(evs []event.Event) []event.Type {
	out := make([]event.Type, len(evs))
	for i, ev := range evs {
		out[i] = ev.Type
	}
	return out
}

func TestPendingToMinedToConfirmed(t *testing.T) {
	tr, _ := newTestTracker()
	hash := common.Hash{0xaa}
	block := common.Hash{0xbb}

	evs := tr.OnPending(ethMatch(0xaa))
	if len(evs) != 1 || evs[0].Type != event.TypePending {
		t.Fatalf("pending events = %v", eventTypes(evs))
	}
	if evs[0].Value != "1" || evs[0].ValueRaw != "1000000000000000000" {
		t.Errorf("value = %s raw = %s", evs[0].Value, evs[0].ValueRaw)
	}

	// Duplicate pending notification is ignored.
	if evs := tr.OnPending(ethMatch(0xaa)); evs != nil {
		t.Errorf("duplicate pending emitted %v", eventTypes(evs))
	}

	evs = tr.OnMined(hash, block, 100, ethMatch(0xaa), false, 1, 0)
	if len(evs) != 1 || evs[0].Type != event.TypeMined || evs[0].BlockNumber != 100 || evs[0].Confirmations != 1 {
		t.Fatalf("mined events = %+v", evs)
	}

	// Re-processing the same block (rescan overlap) is a no-op.
	if evs := tr.OnMined(hash, block, 100, ethMatch(0xaa), false, 1, 0); evs != nil {
		t.Errorf("duplicate mined emitted %v", eventTypes(evs))
	}

	// Head 101: 2 confirmations, below threshold 3.
	confirm, stale := tr.OnHead(101, 0)
	if len(confirm) != 0 || len(stale) != 0 {
		t.Fatalf("premature candidates: %v %v", confirm, stale)
	}

	// Head 102: 3 confirmations reached.
	confirm, _ = tr.OnHead(102, 0)
	if len(confirm) != 1 || confirm[0] != hash {
		t.Fatalf("confirm candidates = %v", confirm)
	}
	evs = tr.Confirm(hash, 102)
	if len(evs) != 1 || evs[0].Type != event.TypeConfirmed || evs[0].Confirmations != 3 {
		t.Fatalf("confirmed events = %+v", evs)
	}
	if tr.Len() != 0 {
		t.Errorf("tracker not empty after confirm: %d", tr.Len())
	}
}

func TestMinedWithoutPending(t *testing.T) {
	tr, _ := newTestTracker()
	evs := tr.OnMined(common.Hash{0xcc}, common.Hash{0xbb}, 50, ethMatch(0xcc), false, 1, 0)
	if len(evs) != 1 || evs[0].Type != event.TypeMined {
		t.Fatalf("events = %v", eventTypes(evs))
	}
}

func TestFailedTx(t *testing.T) {
	tr, _ := newTestTracker()
	tr.OnPending(ethMatch(0xdd))
	evs := tr.OnMined(common.Hash{0xdd}, common.Hash{0xbb}, 50, ethMatch(0xdd), true, 1, 0)
	if len(evs) != 1 || evs[0].Type != event.TypeFailed {
		t.Fatalf("events = %v", eventTypes(evs))
	}
	if tr.Len() != 0 {
		t.Error("failed tx still tracked")
	}
}

func TestMinedWithNoMatchingTransfers(t *testing.T) {
	tr, _ := newTestTracker()
	// Pending ERC20 calldata match whose mined logs show no transfer to
	// a watched address (e.g. amount rerouted by the contract).
	tr.OnPending([]Match{tokenMatch(0xee, 0)})
	evs := tr.OnMined(common.Hash{0xee}, common.Hash{0xbb}, 50, nil, false, 1, 0)
	if evs != nil {
		t.Fatalf("events = %v", eventTypes(evs))
	}
	if tr.Len() != 0 {
		t.Error("non-delivering tx still tracked")
	}
}

func TestReorgDemoteAndRemine(t *testing.T) {
	tr, _ := newTestTracker()
	hash := common.Hash{0x01}
	tr.OnMined(hash, common.Hash{0xb1}, 100, ethMatch(0x01), false, 1, 0)

	evs := tr.OnReorg(99) // ancestor below the mined block
	if len(evs) != 1 || evs[0].Type != event.TypeReorged {
		t.Fatalf("reorg events = %v", eventTypes(evs))
	}

	// Re-included in a new block on the canonical chain.
	evs = tr.OnMined(hash, common.Hash{0xb2}, 101, nil, false, 1, 0)
	if len(evs) != 1 || evs[0].Type != event.TypeMined || evs[0].BlockNumber != 101 {
		t.Fatalf("re-mine events = %+v", evs)
	}
	// Transfers were retained across the demote.
	if evs[0].Asset != "ETH" || evs[0].ValueRaw != "1000000000000000000" {
		t.Errorf("transfer legs lost: %+v", evs[0])
	}
}

func TestReorgBelowAncestorUntouched(t *testing.T) {
	tr, _ := newTestTracker()
	tr.OnMined(common.Hash{0x02}, common.Hash{0xb1}, 90, ethMatch(0x02), false, 1, 0)
	if evs := tr.OnReorg(95); evs != nil {
		t.Errorf("tx below ancestor demoted: %v", eventTypes(evs))
	}
}

func TestPendingTimeout(t *testing.T) {
	tr, clock := newTestTracker()
	hash := common.Hash{0x03}
	tr.OnPending(ethMatch(0x03))

	_, stale := tr.OnHead(100, 0)
	if len(stale) != 0 {
		t.Fatal("fresh pending tx reported stale")
	}

	clock.advance(31 * time.Minute)
	_, stale = tr.OnHead(101, 0)
	if len(stale) != 1 || stale[0] != hash {
		t.Fatalf("stale candidates = %v", stale)
	}

	// Node still knows it: timer resets.
	if evs := tr.OnDropStale(hash, true); evs != nil {
		t.Fatalf("still-known tx dropped: %v", eventTypes(evs))
	}
	_, stale = tr.OnHead(102, 0)
	if len(stale) != 0 {
		t.Fatal("timer did not reset")
	}

	// Gone from the node: dropped.
	clock.advance(31 * time.Minute)
	_, stale = tr.OnHead(103, 0)
	if len(stale) != 1 {
		t.Fatal("expected stale candidate after second timeout")
	}
	evs := tr.OnDropStale(hash, false)
	if len(evs) != 1 || evs[0].Type != event.TypeDropped {
		t.Fatalf("drop events = %v", eventTypes(evs))
	}
	if tr.Len() != 0 {
		t.Error("dropped tx still tracked")
	}
}

func TestMultipleERC20Legs(t *testing.T) {
	tr, _ := newTestTracker()
	hash := common.Hash{0x04}
	legs := []Match{tokenMatch(0x04, 2), tokenMatch(0x04, 5)}

	evs := tr.OnMined(hash, common.Hash{0xb1}, 100, legs, false, 1, 0)
	if len(evs) != 2 {
		t.Fatalf("got %d events, want 2", len(evs))
	}
	if *evs[0].LogIndex == *evs[1].LogIndex {
		t.Error("log indexes not distinct")
	}
	for _, ev := range evs {
		if ev.TokenAddress == "" || ev.Asset != "TEST" {
			t.Errorf("bad token event: %+v", ev)
		}
	}

	confirm, _ := tr.OnHead(102, 0)
	if len(confirm) != 1 {
		t.Fatalf("confirm candidates = %v", confirm)
	}
	if evs := tr.Confirm(hash, 102); len(evs) != 2 {
		t.Errorf("confirmed %d legs, want 2", len(evs))
	}
}

func TestFirstSeenStableAcrossLifecycle(t *testing.T) {
	tr, clock := newTestTracker()
	hash := common.Hash{0x06}
	t0 := clock.t.UTC().Format(time.RFC3339)

	evs := tr.OnPending(ethMatch(0x06))
	if evs[0].FirstSeen != t0 {
		t.Fatalf("pending first_seen = %s, want %s", evs[0].FirstSeen, t0)
	}

	clock.advance(2 * time.Minute)
	evs = tr.OnMined(hash, common.Hash{0xb1}, 100, ethMatch(0x06), false, 1, 0)
	if evs[0].FirstSeen != t0 {
		t.Errorf("mined first_seen = %s, want %s", evs[0].FirstSeen, t0)
	}

	// Reorg demote resets the timeout anchor but not first_seen.
	clock.advance(2 * time.Minute)
	evs = tr.OnReorg(99)
	if evs[0].FirstSeen != t0 {
		t.Errorf("reorged first_seen = %s, want %s", evs[0].FirstSeen, t0)
	}
	evs = tr.OnMined(hash, common.Hash{0xb2}, 101, nil, false, 1, 0)
	if evs[0].FirstSeen != t0 {
		t.Errorf("re-mined first_seen = %s, want %s", evs[0].FirstSeen, t0)
	}

	clock.advance(2 * time.Minute)
	tr.OnHead(103, 0)
	evs = tr.Confirm(hash, 103)
	if evs[0].FirstSeen != t0 {
		t.Errorf("confirmed first_seen = %s, want %s", evs[0].FirstSeen, t0)
	}
}

func TestFirstSeenBlock(t *testing.T) {
	tr, _ := newTestTracker()
	hash := common.Hash{0x16}

	// A mempool entry is in no block: first_seen_block stays 0 and
	// MinedAt unset.
	evs := tr.OnPending(ethMatch(0x16))
	if evs[0].FirstSeenBlock != 0 {
		t.Fatalf("pending first_seen_block = %d, want 0", evs[0].FirstSeenBlock)
	}
	if snap, _ := tr.ExportTx(hash); !snap.MinedAt.IsZero() {
		t.Fatalf("pending mined_at = %v, want zero", snap.MinedAt)
	}
	// Set at first inclusion.
	evs = tr.OnMined(hash, common.Hash{0xb1}, 205, ethMatch(0x16), false, 1, 0)
	if evs[0].FirstSeenBlock != 205 {
		t.Errorf("mined first_seen_block = %d, want 205", evs[0].FirstSeenBlock)
	}
	minedAt, _ := tr.ExportTx(hash)
	if minedAt.MinedAt.IsZero() {
		t.Error("mined_at not set at first inclusion")
	}

	// Immutable across reorg and re-mine in a different block.
	tr.OnReorg(200)
	evs = tr.OnMined(hash, common.Hash{0xb2}, 207, nil, false, 1, 0)
	if evs[0].FirstSeenBlock != 205 {
		t.Errorf("re-mined first_seen_block = %d, want 205", evs[0].FirstSeenBlock)
	}
	if again, _ := tr.ExportTx(hash); !again.MinedAt.Equal(minedAt.MinedAt) {
		t.Errorf("mined_at changed across re-mine: %v -> %v", minedAt.MinedAt, again.MinedAt)
	}

	// First seen mined: the containing block.
	evs = tr.OnMined(common.Hash{0x17}, common.Hash{0xb1}, 300, ethMatch(0x17), false, 1, 0)
	if evs[0].FirstSeenBlock != 300 {
		t.Errorf("mined-first first_seen_block = %d, want 300", evs[0].FirstSeenBlock)
	}

	// Survives export/import.
	snap, ok := tr.ExportTx(hash)
	if !ok || snap.FirstSeenBlock != 205 {
		t.Fatalf("exported first_seen_block = %d, want 205", snap.FirstSeenBlock)
	}
	tr2, _ := newTestTracker()
	if err := tr2.Import([]storage.TrackedTx{snap}); err != nil {
		t.Fatal(err)
	}
	tr2.OnHead(210, 0)
	evs = tr2.Confirm(hash, 210)
	if evs[0].FirstSeenBlock != 205 {
		t.Errorf("first_seen_block lost across import: %d", evs[0].FirstSeenBlock)
	}
}

func TestFirstSeenMinedWithoutPending(t *testing.T) {
	tr, clock := newTestTracker()
	clock.advance(5 * time.Minute)
	want := clock.t.UTC().Format(time.RFC3339)
	evs := tr.OnMined(common.Hash{0x07}, common.Hash{0xb1}, 100, ethMatch(0x07), false, 1, 0)
	if evs[0].FirstSeen != want {
		t.Errorf("first_seen = %s, want %s", evs[0].FirstSeen, want)
	}
}

func TestReplacedPending(t *testing.T) {
	tr, _ := newTestTracker()
	winner := common.Hash{0xcc}
	tr.OnPending(ethMatch(0x08)) // sender=otherAddr nonce=8

	// A different tx from the same sender mines with the same nonce.
	evs := tr.OnNonceUsed(otherAddr, 8, winner)
	if len(evs) != 1 || evs[0].Type != event.TypeReplaced {
		t.Fatalf("events = %v", eventTypes(evs))
	}
	if evs[0].ReplacedBy != winner.Hex() {
		t.Errorf("replaced_by = %s, want %s", evs[0].ReplacedBy, winner.Hex())
	}
	if tr.Len() != 0 {
		t.Error("replaced tx still tracked")
	}
	// Slot is cleaned up: repeating the observation is a no-op.
	if evs := tr.OnNonceUsed(otherAddr, 8, winner); evs != nil {
		t.Errorf("second OnNonceUsed emitted %v", eventTypes(evs))
	}
}

func TestNonceUsedByTrackedTxItself(t *testing.T) {
	tr, _ := newTestTracker()
	tr.OnPending(ethMatch(0x09))
	// The tracked tx mining consumes its own slot - not a replacement.
	if evs := tr.OnNonceUsed(otherAddr, 9, common.Hash{0x09}); evs != nil {
		t.Fatalf("self-mine emitted %v", eventTypes(evs))
	}
	if tr.Len() != 1 {
		t.Error("tx no longer tracked")
	}
}

func TestNonceUsedDifferentSlotUntouched(t *testing.T) {
	tr, _ := newTestTracker()
	tr.OnPending(ethMatch(0x0a))
	if evs := tr.OnNonceUsed(otherAddr, 99, common.Hash{0xcc}); evs != nil {
		t.Fatalf("different nonce emitted %v", eventTypes(evs))
	}
	if evs := tr.OnNonceUsed(watchedAddr, 10, common.Hash{0xcc}); evs != nil {
		t.Fatalf("different sender emitted %v", eventTypes(evs))
	}
}

func TestReplacedAfterReorgDemote(t *testing.T) {
	tr, _ := newTestTracker()
	hash := common.Hash{0x0b}
	tr.OnPending(ethMatch(0x0b))
	tr.OnMined(hash, common.Hash{0xb1}, 100, ethMatch(0x0b), false, 1, 0)

	// While mined, the slot is not considered occupied by a pending tx.
	if evs := tr.OnNonceUsed(otherAddr, 0x0b, common.Hash{0xcc}); evs != nil {
		t.Fatalf("mined tx replaced: %v", eventTypes(evs))
	}

	// After a reorg demotes it back to pending, replacement applies again.
	tr.OnReorg(99)
	evs := tr.OnNonceUsed(otherAddr, 0x0b, common.Hash{0xcc})
	if len(evs) != 1 || evs[0].Type != event.TypeReplaced {
		t.Fatalf("events after demote = %v", eventTypes(evs))
	}
	if tr.Len() != 0 {
		t.Error("replaced tx still tracked")
	}
}

func TestExportImportRoundtrip(t *testing.T) {
	tr, clock := newTestTracker()
	hash := common.Hash{0x0c}
	tr.OnPending([]Match{tokenMatch(0x0c, 4)})
	t0 := clock.t
	clock.advance(time.Minute)
	tr.OnMined(hash, common.Hash{0xb1}, 100, []Match{tokenMatch(0x0c, 4)}, false, 1, 0)

	snap, ok := tr.ExportTx(hash)
	if !ok {
		t.Fatal("export failed")
	}
	if snap.Status != "mined" || snap.BlockNumber != 100 || !snap.Authoritative {
		t.Fatalf("snapshot = %+v", snap)
	}
	if !snap.FirstSeen.Equal(t0) {
		t.Errorf("first_seen = %v, want %v", snap.FirstSeen, t0)
	}

	// A fresh tracker (fresh process) imports the snapshot and the
	// lifecycle continues: threshold 3 reached at head 102.
	tr2, _ := newTestTracker()
	if err := tr2.Import([]storage.TrackedTx{snap}); err != nil {
		t.Fatal(err)
	}
	confirm, _ := tr2.OnHead(102, 0)
	if len(confirm) != 1 || confirm[0] != hash {
		t.Fatalf("confirm candidates after import = %v", confirm)
	}
	evs := tr2.Confirm(hash, 102)
	if len(evs) != 1 || evs[0].Type != event.TypeConfirmed {
		t.Fatalf("events = %v", eventTypes(evs))
	}
	if evs[0].FirstSeen != t0.UTC().Format(time.RFC3339) {
		t.Errorf("first_seen lost across import: %s", evs[0].FirstSeen)
	}
	if evs[0].LogIndex == nil || *evs[0].LogIndex != 4 || evs[0].TokenAddress == "" {
		t.Errorf("transfer leg lost across import: %+v", evs[0])
	}
}

func TestImportRestoresNonceIndex(t *testing.T) {
	tr, _ := newTestTracker()
	tr.OnPending(ethMatch(0x0d)) // sender=otherAddr nonce=0x0d
	snap, ok := tr.ExportTx(common.Hash{0x0d})
	if !ok {
		t.Fatal("export failed")
	}
	if snap.Status != "pending" || !snap.HasNonce {
		t.Fatalf("snapshot = %+v", snap)
	}

	tr2, _ := newTestTracker()
	if err := tr2.Import([]storage.TrackedTx{snap}); err != nil {
		t.Fatal(err)
	}
	// Replacement detection still works after restart.
	evs := tr2.OnNonceUsed(otherAddr, 0x0d, common.Hash{0xcc})
	if len(evs) != 1 || evs[0].Type != event.TypeReplaced {
		t.Fatalf("events = %v", eventTypes(evs))
	}
}

func TestInstantConfirmAtThreshold(t *testing.T) {
	tr, _ := newTestTracker() // depth threshold 3

	// Depth at discovery below threshold: normal mined event.
	evs := tr.OnMined(common.Hash{0x10}, common.Hash{0xb1}, 100, ethMatch(0x10), false, 2, 0)
	if len(evs) != 1 || evs[0].Type != event.TypeMined {
		t.Fatalf("events = %v", eventTypes(evs))
	}

	// At/past threshold: a single confirmed event, tracking already over.
	evs = tr.OnMined(common.Hash{0x11}, common.Hash{0xb1}, 100, ethMatch(0x11), false, 3, 0)
	if len(evs) != 1 || evs[0].Type != event.TypeConfirmed || evs[0].Confirmations != 3 {
		t.Fatalf("events = %+v", evs)
	}
	if _, tracked := tr.ExportTx(common.Hash{0x11}); tracked {
		t.Error("instantly confirmed tx still tracked")
	}

	// A failed tx never instant-confirms.
	evs = tr.OnMined(common.Hash{0x12}, common.Hash{0xb1}, 100, ethMatch(0x12), true, 9, 0)
	if len(evs) != 1 || evs[0].Type != event.TypeFailed {
		t.Fatalf("events = %v", eventTypes(evs))
	}
}

func TestInstantConfirmTagMode(t *testing.T) {
	tr := NewTracker(config.Confirmations{Tag: "finalized"}, 30*time.Minute, nil)

	// Block above the finalized number: mined.
	evs := tr.OnMined(common.Hash{0x13}, common.Hash{0xb1}, 900, ethMatch(0x13), false, 5, 850)
	if len(evs) != 1 || evs[0].Type != event.TypeMined {
		t.Fatalf("events = %v", eventTypes(evs))
	}
	// Block at/below the finalized number: instant confirmed.
	evs = tr.OnMined(common.Hash{0x14}, common.Hash{0xb1}, 850, ethMatch(0x14), false, 55, 850)
	if len(evs) != 1 || evs[0].Type != event.TypeConfirmed {
		t.Fatalf("events = %v", eventTypes(evs))
	}
	// Unknown finality number: never instant-confirm.
	evs = tr.OnMined(common.Hash{0x15}, common.Hash{0xb1}, 10, ethMatch(0x15), false, 999, 0)
	if len(evs) != 1 || evs[0].Type != event.TypeMined {
		t.Fatalf("events = %v", eventTypes(evs))
	}
}

func TestDemote(t *testing.T) {
	tr, _ := newTestTracker()
	hash := common.Hash{0x05}
	tr.OnMined(hash, common.Hash{0xb1}, 100, ethMatch(0x05), false, 1, 0)

	evs := tr.Demote(hash)
	if len(evs) != 1 || evs[0].Type != event.TypeReorged {
		t.Fatalf("demote events = %v", eventTypes(evs))
	}
	if !tr.IsPending(hash) {
		t.Error("demoted tx not pending")
	}
	if evs := tr.Demote(hash); evs != nil {
		t.Errorf("double demote emitted %v", eventTypes(evs))
	}
}

// Tombstones: late arrivals must not resurrect terminal transactions,
// but legitimate re-mines after reorgs must still be tracked.
func TestTombstones(t *testing.T) {
	tr, _ := newTestTracker() // depth 3
	block := common.Hash{0xb1}

	// Confirmed is final: neither a late pending echo nor a rescan
	// re-discovery may resurrect it.
	confirmed := common.Hash{0x01}
	tr.OnPending(ethMatch(0x01))
	tr.OnMined(confirmed, block, 100, ethMatch(0x01), false, 5, 0) // instant confirm
	if evs := tr.OnPending(ethMatch(0x01)); evs != nil {
		t.Errorf("confirmed tx resurrected by pending echo: %v", eventTypes(evs))
	}
	if evs := tr.OnMined(confirmed, common.Hash{0xb2}, 100, ethMatch(0x01), false, 5, 0); evs != nil {
		t.Errorf("confirmed tx re-emitted on rescan: %v", eventTypes(evs))
	}
	if tr.Len() != 0 {
		t.Errorf("tracker len = %d after resurrection attempts", tr.Len())
	}

	// Failed: pending echo suppressed, but a re-mine (reorg, tx now
	// succeeds) is a real payment and must go through.
	failed := common.Hash{0x02}
	tr.OnPending(ethMatch(0x02))
	tr.OnMined(failed, block, 100, ethMatch(0x02), true, 1, 0)
	if evs := tr.OnPending(ethMatch(0x02)); evs != nil {
		t.Errorf("failed tx resurrected by pending echo: %v", eventTypes(evs))
	}
	if evs := tr.OnMined(failed, common.Hash{0xb3}, 101, ethMatch(0x02), false, 1, 0); len(evs) != 1 || evs[0].Type != event.TypeMined {
		t.Errorf("failed tx re-mine suppressed: %v", eventTypes(evs))
	}

	// Replaced: the nonce is consumed, a pending echo is stale.
	tr.OnPending(ethMatch(0x03))
	tr.OnNonceUsed(otherAddr, 0x03, common.Hash{0x04})
	if evs := tr.OnPending(ethMatch(0x03)); evs != nil {
		t.Errorf("replaced tx resurrected by pending echo: %v", eventTypes(evs))
	}

	// Dropped: re-broadcast is a genuine resurrection, allowed.
	dropped := common.Hash{0x05}
	tr.OnPending(ethMatch(0x05))
	tr.OnDropStale(dropped, false)
	if evs := tr.OnPending(ethMatch(0x05)); len(evs) != 1 || evs[0].Type != event.TypePending {
		t.Errorf("dropped tx re-broadcast not re-tracked: %v", eventTypes(evs))
	}
}

func TestTombstoneEviction(t *testing.T) {
	tr, _ := newTestTracker()
	for i := range tombstoneCap + 10 {
		var hash common.Hash
		hash[0], hash[1], hash[2] = byte(i), byte(i>>8), byte(i>>16)
		tr.bury(hash, event.TypeConfirmed)
	}
	if len(tr.tombstones) != tombstoneCap || len(tr.tombstoneFIFO) != tombstoneCap {
		t.Errorf("tombstones = %d fifo = %d, want capped at %d", len(tr.tombstones), len(tr.tombstoneFIFO), tombstoneCap)
	}
	// The oldest were evicted.
	if _, ok := tr.tombstones[common.Hash{0, 0, 0}]; ok {
		t.Error("oldest tombstone not evicted")
	}
}

// TestTombstoneRebury: a hash whose tombstone was cleared by a re-mine
// and buried again leaves a stale FIFO entry behind; eviction must not
// honor the stale entry, or the re-buried (young) tombstone dies at
// its ORIGINAL burial's age.
func TestTombstoneRebury(t *testing.T) {
	tr, _ := newTestTracker()
	victim := common.Hash{0xfe, 0xed}

	// Original burial: this FIFO entry will go stale.
	tr.bury(victim, event.TypeFailed)

	// Age it: cap-1 fillers bury after the victim.
	for i := range tombstoneCap - 1 {
		var hash common.Hash
		hash[10], hash[11] = byte(i), byte(i>>8)
		tr.bury(hash, event.TypeConfirmed)
	}

	// The victim resurrects (re-mine clears a non-confirmed tombstone)
	// and ends terminal again - a YOUNG burial, while its stale entry
	// still heads the FIFO.
	tr.OnMined(victim, common.Hash{0xb1}, 100, ethMatch(0xfe), false, 1, 0)
	tr.Confirm(victim, 200)

	// The next burial pushes the map over cap. Eviction must skip the
	// victim's stale head entry and evict the oldest live filler; the
	// young re-buried victim survives.
	tr.bury(common.Hash{0xaa, 0xbb}, event.TypeConfirmed)
	if ts, ok := tr.tombstones[victim]; !ok || ts.typ != event.TypeConfirmed {
		t.Errorf("stale FIFO entry evicted the live re-buried tombstone (%v %v)", ts, ok)
	}
	var filler0 common.Hash
	if _, ok := tr.tombstones[filler0]; ok {
		// filler0 has hash[10]=0,hash[11]=0 -> the zero-adjacent filler
		// was the oldest live tombstone and should be the one evicted.
		t.Error("oldest live tombstone not evicted")
	}
}

func TestMempoolWait(t *testing.T) {
	tr, clock := newTestTracker()
	hash := common.Hash{0x06}

	tr.OnPending(ethMatch(0x06))
	clock.advance(7 * time.Second)
	tr.OnMined(hash, common.Hash{0xb1}, 100, ethMatch(0x06), false, 1, 0)

	wait, ok := tr.MempoolWait(hash)
	if !ok || wait != 7*time.Second {
		t.Fatalf("wait = %v ok = %v, want 7s in the mempool", wait, ok)
	}

	// Immutable across reorg and re-mine, like MinedAt.
	tr.Demote(hash)
	clock.advance(30 * time.Second)
	tr.OnMined(hash, common.Hash{0xb2}, 101, ethMatch(0x06), false, 1, 0)
	if wait, ok := tr.MempoolWait(hash); !ok || wait != 7*time.Second {
		t.Errorf("wait after re-mine = %v ok = %v, want the original 7s", wait, ok)
	}

	// A tx discovered at mining was never in the mempool.
	blockOnly := common.Hash{0x07}
	tr.OnMined(blockOnly, common.Hash{0xb1}, 100, ethMatch(0x07), false, 1, 0)
	if _, ok := tr.MempoolWait(blockOnly); ok {
		t.Error("block-discovered tx reported a mempool wait")
	}
}

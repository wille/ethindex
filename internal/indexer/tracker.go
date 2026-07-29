package indexer

import (
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/wille/ethindex/internal/config"
	"github.com/wille/ethindex/internal/event"
	"github.com/wille/ethindex/internal/storage"
)

type txState int

const (
	statePending txState = iota
	stateMined
)

// trackedTx is one transaction being followed through its lifecycle.
// It can carry several transfer legs (e.g. multiple ERC20 Transfer logs).
type trackedTx struct {
	Hash      common.Hash
	Transfers []Match
	// Authoritative is true once Transfers come from mined logs and
	// receipts rather than best-effort pending calldata decoding.
	Authoritative bool
	State         txState
	Block         common.Hash
	BlockNum      uint64
	// FirstSeen is when the indexer first observed this tx.
	// FirstSeenBlock is the first block it was seen included in (0
	// while only known from the mempool) and MinedAt when that
	// happened. Once set, immutable across reorgs and re-mines.
	FirstSeen      time.Time
	FirstSeenBlock uint64
	MinedAt        time.Time
	// pendingSince anchors the pending-timeout check. Unlike FirstSeen
	// it resets whenever the tx (re-)enters the pending state or a
	// stale recheck finds it still alive.
	pendingSince time.Time
	// Sender/Nonce identify the nonce slot for replacement detection.
	// Known only for txs first seen in the mempool (hasNonce).
	Sender   common.Address
	Nonce    uint64
	hasNonce bool
}

// nonceKey is one (sender, nonce) slot; at most one tx occupying it can
// ever mine.
type nonceKey struct {
	sender common.Address
	nonce  uint64
}

// tombstoneCap bounds the terminal-transaction memory (FIFO eviction).
const tombstoneCap = 8192

// Tracker is the pure lifecycle state machine. Every input returns the
// events to emit; it performs no I/O and is driven from a single
// goroutine. The clock is injected for tests.
type Tracker struct {
	txs map[common.Hash]*trackedTx
	// byNonce indexes tracked *pending* txs by nonce slot, so a mined
	// tx consuming a slot reveals which tracked txs it replaced. Multiple
	// tracked txs can compete for one slot (original + matching speed-up).
	byNonce       map[nonceKey][]common.Hash
	confirmations config.Confirmations
	timeout       time.Duration
	now           func() time.Time

	// tombstones remember recently forgotten (terminal) transactions and
	// how they ended, so late arrivals - a slow mempool snapshot echo, a
	// catch-up rescan of an instantly-confirmed block - cannot resurrect
	// them. Asymmetric on purpose: a duplicate event is noise, a missed
	// re-mined payment is lost money, so OnMined only honors `confirmed`
	// (final by definition) while OnPending also honors failed/replaced
	// (they hold a receipt / a consumed nonce and can never be pending).
	tombstones    map[common.Hash]tombstone
	tombstoneFIFO []tombstoneRef
	tombstoneSeq  uint64
}

// tombstone is one terminal record; seq ties it to its FIFO entry so
// stale FIFO entries (superseded burials, tombstones cleared by a
// legitimate re-mine) evict nothing.
type tombstone struct {
	typ event.Type
	seq uint64
}

// tombstoneRef is one FIFO eviction-order entry.
type tombstoneRef struct {
	hash common.Hash
	seq  uint64
}

func NewTracker(confirmations config.Confirmations, pendingTimeout time.Duration, now func() time.Time) *Tracker {
	if now == nil {
		now = time.Now
	}
	return &Tracker{
		txs:           make(map[common.Hash]*trackedTx),
		byNonce:       make(map[nonceKey][]common.Hash),
		confirmations: confirmations,
		timeout:       pendingTimeout,
		now:           now,
		tombstones:    make(map[common.Hash]tombstone),
	}
}

// bury records how a forgotten transaction ended. Every burial gets a
// fresh sequence number and FIFO entry; eviction only honors FIFO
// entries whose sequence still matches the live tombstone, so entries
// left behind by re-buried or re-mined hashes cannot evict a live
// tombstone early.
func (t *Tracker) bury(hash common.Hash, typ event.Type) {
	t.tombstoneSeq++
	t.tombstones[hash] = tombstone{typ: typ, seq: t.tombstoneSeq}
	t.tombstoneFIFO = append(t.tombstoneFIFO, tombstoneRef{hash: hash, seq: t.tombstoneSeq})
	for len(t.tombstones) > tombstoneCap && len(t.tombstoneFIFO) > 0 {
		ref := t.tombstoneFIFO[0]
		t.tombstoneFIFO = t.tombstoneFIFO[1:]
		if ts, ok := t.tombstones[ref.hash]; ok && ts.seq == ref.seq {
			delete(t.tombstones, ref.hash)
		}
	}
	// Compact stale FIFO entries when they accumulate (heavy re-bury or
	// resurrection churn), keeping the FIFO bounded.
	if len(t.tombstoneFIFO) > 2*tombstoneCap {
		live := t.tombstoneFIFO[:0]
		for _, ref := range t.tombstoneFIFO {
			if ts, ok := t.tombstones[ref.hash]; ok && ts.seq == ref.seq {
				live = append(live, ref)
			}
		}
		t.tombstoneFIFO = live
	}
}

// indexNonce registers a pending tx's nonce slot.
func (t *Tracker) indexNonce(tx *trackedTx) {
	if !tx.hasNonce {
		return
	}
	key := nonceKey{tx.Sender, tx.Nonce}
	t.byNonce[key] = append(t.byNonce[key], tx.Hash)
}

// unindexNonce removes a tx from its nonce slot (on any transition out
// of the pending state).
func (t *Tracker) unindexNonce(tx *trackedTx) {
	if !tx.hasNonce {
		return
	}
	key := nonceKey{tx.Sender, tx.Nonce}
	hashes := t.byNonce[key]
	for i, h := range hashes {
		if h == tx.Hash {
			hashes = append(hashes[:i], hashes[i+1:]...)
			break
		}
	}
	if len(hashes) == 0 {
		delete(t.byNonce, key)
	} else {
		t.byNonce[key] = hashes
	}
}

// events builds one event per transfer leg of a tracked tx.
func (t *Tracker) events(tx *trackedTx, typ event.Type, confirmations uint64) []event.Event {
	out := make([]event.Event, 0, len(tx.Transfers))
	for _, m := range tx.Transfers {
		ev := event.Event{
			Type:           typ,
			Direction:      m.Direction,
			FirstSeen:      tx.FirstSeen.UTC().Format(time.RFC3339),
			FirstSeenBlock: tx.FirstSeenBlock,
			TxHash:         m.TxHash.Hex(),
			LogIndex:       m.LogIndex,
			From:           m.From.Hex(),
			To:             m.To.Hex(),
			Asset:          m.Asset,
			Decimals:       m.Decimals,
			ValueRaw:       m.Value.String(),
			Value:          event.FormatUnits(m.Value, m.Decimals),
			Confirmations:  confirmations,
		}
		if m.Token != nil {
			ev.TokenAddress = m.Token.Hex()
		}
		if !tx.MinedAt.IsZero() {
			ev.MinedAt = tx.MinedAt.UTC().Format(time.RFC3339)
		}
		if tx.State == stateMined {
			ev.BlockNumber = tx.BlockNum
			ev.BlockHash = tx.Block.Hex()
		}
		out = append(out, ev)
	}
	return out
}

// OnPending records a transaction seen in the mempool.
func (t *Tracker) OnPending(matches []Match) []event.Event {
	if len(matches) == 0 {
		return nil
	}
	hash := matches[0].TxHash
	if _, ok := t.txs[hash]; ok {
		return nil // already tracked (duplicate notification or already mined)
	}
	// A late pending notification (slow mempool snapshot) for a tx that
	// already ended: confirmed/failed hold a receipt and replaced lost
	// its nonce - none can be pending again. Dropped may genuinely
	// re-enter the mempool via re-broadcast, so it is re-tracked.
	if ts, dead := t.tombstones[hash]; dead && ts.typ != event.TypeDropped {
		return nil
	}
	now := t.now()
	tx := &trackedTx{
		Hash:         hash,
		Transfers:    matches,
		State:        statePending,
		FirstSeen:    now,
		pendingSince: now,
		Sender:       matches[0].TxSender,
		Nonce:        matches[0].TxNonce,
		hasNonce:     true,
	}
	t.txs[hash] = tx
	t.indexNonce(tx)
	return t.events(tx, event.TypePending, 0)
}

// confirmedAt reports whether a block at the given depth (or compared
// to the node-reported finalized number in tag mode) already satisfies
// the confirmation threshold.
func (t *Tracker) confirmedAt(blockNum, confirmations, finalized uint64) bool {
	if t.confirmations.Tag != "" {
		return finalized > 0 && blockNum <= finalized
	}
	return confirmations >= t.confirmations.Depth
}

// OnMined records a transaction found in a canonical block. The
// transfers argument is authoritative (from logs/receipts) and replaces
// any calldata-derived pending legs. failed indicates receipt status 0.
// confirmations is the depth at discovery (1 at the tip; more when the
// block was found during a backward catch-up scan) and finalized the
// cached finality number (0 outside tag mode). A transaction discovered
// already past the threshold emits a single confirmed event - no mined
// first - since its finality is a fact at discovery.
func (t *Tracker) OnMined(hash common.Hash, blockHash common.Hash, blockNum uint64, transfers []Match, failed bool, confirmations, finalized uint64) []event.Event {
	// Confirmed is final: a rescan re-discovering the tx (instant
	// confirm during catch-up, reorg replay) must not resurrect it.
	// Other terminal states can legitimately re-mine after a reorg -
	// that is a real payment, so the tombstone is cleared instead.
	if ts, dead := t.tombstones[hash]; dead {
		if ts.typ == event.TypeConfirmed {
			return nil
		}
		delete(t.tombstones, hash)
	}
	tx, ok := t.txs[hash]
	if !ok {
		now := t.now()
		tx = &trackedTx{Hash: hash, FirstSeen: now, pendingSince: now}
		t.txs[hash] = tx
	} else if tx.State == stateMined && tx.Block == blockHash {
		return nil // already processed in this block (rescan overlap)
	}
	if tx.State == statePending {
		t.unindexNonce(tx)
	}
	if tx.FirstSeenBlock == 0 {
		tx.FirstSeenBlock = blockNum // first block it was seen included in
		tx.MinedAt = t.now()
	}
	if len(transfers) > 0 {
		tx.Transfers = transfers
		tx.Authoritative = true
	}
	tx.State = stateMined
	tx.Block = blockHash
	tx.BlockNum = blockNum

	if confirmations == 0 {
		confirmations = 1
	}
	if failed {
		evs := t.events(tx, event.TypeFailed, confirmations)
		delete(t.txs, hash)
		t.bury(hash, event.TypeFailed)
		return evs
	}
	if !tx.Authoritative {
		// A pending calldata match that mined successfully but produced
		// no matching Transfer log - nothing was actually delivered to a
		// watched address. Forget it silently; buried as failed-like so
		// a late pending echo cannot re-track it.
		delete(t.txs, hash)
		t.bury(hash, event.TypeFailed)
		return nil
	}
	if t.confirmedAt(blockNum, confirmations, finalized) {
		evs := t.events(tx, event.TypeConfirmed, confirmations)
		delete(t.txs, hash)
		t.bury(hash, event.TypeConfirmed)
		return evs
	}
	return t.events(tx, event.TypeMined, confirmations)
}

// MempoolWait returns how long a transaction sat in the mempool before
// its first inclusion. Both anchors already exist: FirstSeen (never
// mutated) and MinedAt (set once, immutable across re-mines). hasNonce
// discriminates mempool-seen txs - it is only ever set by OnPending -
// so block-discovered transactions report no wait instead of 0s.
func (t *Tracker) MempoolWait(hash common.Hash) (time.Duration, bool) {
	tx, ok := t.txs[hash]
	if !ok || !tx.hasNonce || tx.MinedAt.IsZero() {
		return 0, false
	}
	return tx.MinedAt.Sub(tx.FirstSeen), true
}

// OnHead scans tracked txs at a new head. It returns mined txs that
// have reached the confirmation threshold (the caller verifies each
// against a fresh receipt before calling Confirm or Demote) and pending
// txs older than the timeout (the caller rechecks them against the node
// and calls OnDropStale). finalized is the node-reported finalized (or
// safe) block number, used instead of depth when the tracker runs in
// tag mode; 0 means unknown, confirming nothing this round.
func (t *Tracker) OnHead(headNumber, finalized uint64) (confirmCandidates, staleCandidates []common.Hash) {
	cutoff := t.now().Add(-t.timeout)
	for hash, tx := range t.txs {
		switch tx.State {
		case stateMined:
			var depth uint64
			if headNumber >= tx.BlockNum {
				depth = headNumber - tx.BlockNum + 1
			}
			if t.confirmedAt(tx.BlockNum, depth, finalized) {
				confirmCandidates = append(confirmCandidates, hash)
			}
		case statePending:
			if tx.pendingSince.Before(cutoff) {
				staleCandidates = append(staleCandidates, hash)
			}
		}
	}
	return confirmCandidates, staleCandidates
}

// Confirm finalizes a mined tx whose inclusion the caller has just
// re-verified, emitting confirmed events and forgetting it.
func (t *Tracker) Confirm(hash common.Hash, headNumber uint64) []event.Event {
	tx, ok := t.txs[hash]
	if !ok || tx.State != stateMined || headNumber < tx.BlockNum {
		return nil
	}
	evs := t.events(tx, event.TypeConfirmed, headNumber-tx.BlockNum+1)
	delete(t.txs, hash)
	t.bury(hash, event.TypeConfirmed)
	return evs
}

// IsPending reports whether a tx is tracked and still pending.
func (t *Tracker) IsPending(hash common.Hash) bool {
	tx, ok := t.txs[hash]
	return ok && tx.State == statePending
}

// NativeLegs returns the native-ETH transfer legs recorded for a
// tracked tx. Unlike ERC20 calldata guesses, a native leg's value is
// committed by the transaction itself, so it stays valid once the tx
// mines successfully.
func (t *Tracker) NativeLegs(hash common.Hash) []Match {
	tx, ok := t.txs[hash]
	if !ok {
		return nil
	}
	var out []Match
	for _, m := range tx.Transfers {
		if m.Token == nil {
			out = append(out, m)
		}
	}
	return out
}

// MinedBlock reports whether a tracked tx is currently in the mined
// state, and if so which block (hash and number) it was mined in.
func (t *Tracker) MinedBlock(hash common.Hash) (common.Hash, uint64, bool) {
	tx, ok := t.txs[hash]
	if !ok || tx.State != stateMined {
		return common.Hash{}, 0, false
	}
	return tx.Block, tx.BlockNum, true
}

// OnDropStale resolves a stale-pending recheck: if the node no longer
// knows the tx it is dropped, otherwise its timeout timer resets.
func (t *Tracker) OnDropStale(hash common.Hash, stillKnown bool) []event.Event {
	tx, ok := t.txs[hash]
	if !ok || tx.State != statePending {
		return nil
	}
	if stillKnown {
		tx.pendingSince = t.now()
		return nil
	}
	evs := t.events(tx, event.TypeDropped, 0)
	t.unindexNonce(tx)
	delete(t.txs, hash)
	t.bury(hash, event.TypeDropped)
	return evs
}

// OnNonceUsed reports that a mined transaction consumed a nonce slot.
// Any tracked pending tx occupying the same slot under a different hash
// can never mine - it was replaced. Emits replaced events (carrying the
// winning hash) and forgets those txs.
func (t *Tracker) OnNonceUsed(sender common.Address, nonce uint64, byHash common.Hash) []event.Event {
	var evs []event.Event
	// Copy: unindexNonce mutates the slot slice while we iterate it.
	occupants := append([]common.Hash(nil), t.byNonce[nonceKey{sender, nonce}]...)
	for _, hash := range occupants {
		if hash == byHash {
			continue
		}
		tx, ok := t.txs[hash]
		if !ok || tx.State != statePending {
			continue
		}
		replaced := t.events(tx, event.TypeReplaced, 0)
		for i := range replaced {
			replaced[i].ReplacedBy = byHash.Hex()
		}
		evs = append(evs, replaced...)
		t.unindexNonce(tx)
		delete(t.txs, hash)
		t.bury(hash, event.TypeReplaced)
	}
	return evs
}

// OnReorg demotes every tx mined in a block above the common ancestor
// back to pending and emits reorged events. Re-inclusion is detected by
// the subsequent rescan calling OnMined again.
func (t *Tracker) OnReorg(ancestorNumber uint64) []event.Event {
	var evs []event.Event
	for _, tx := range t.txs {
		if tx.State != stateMined || tx.BlockNum <= ancestorNumber {
			continue
		}
		evs = append(evs, t.events(tx, event.TypeReorged, 0)...)
		tx.State = statePending
		tx.Block = common.Hash{}
		tx.BlockNum = 0
		tx.pendingSince = t.now()
		t.indexNonce(tx)
	}
	return evs
}

// Demote moves a single mined tx back to pending (used when the
// pre-confirmation receipt check disagrees with our tracked block).
func (t *Tracker) Demote(hash common.Hash) []event.Event {
	tx, ok := t.txs[hash]
	if !ok || tx.State != stateMined {
		return nil
	}
	evs := t.events(tx, event.TypeReorged, 0)
	tx.State = statePending
	tx.Block = common.Hash{}
	tx.BlockNum = 0
	tx.pendingSince = t.now()
	t.indexNonce(tx)
	return evs
}

// Len reports the number of currently tracked transactions.
func (t *Tracker) Len() int { return len(t.txs) }

// ExportTx snapshots one tracked transaction for persistence.
func (t *Tracker) ExportTx(hash common.Hash) (storage.TrackedTx, bool) {
	tx, ok := t.txs[hash]
	if !ok {
		return storage.TrackedTx{}, false
	}
	status := "pending"
	if tx.State == stateMined {
		status = "mined"
	}
	out := storage.TrackedTx{
		Hash:           tx.Hash.Hex(),
		Status:         status,
		BlockHash:      tx.Block.Hex(),
		BlockNumber:    tx.BlockNum,
		FirstSeen:      tx.FirstSeen,
		FirstSeenBlock: tx.FirstSeenBlock,
		MinedAt:        tx.MinedAt,
		PendingSince:   tx.pendingSince,
		Sender:         tx.Sender.Hex(),
		Nonce:          tx.Nonce,
		HasNonce:       tx.hasNonce,
		Authoritative:  tx.Authoritative,
	}
	if tx.State == statePending {
		out.BlockHash = ""
	}
	for _, m := range tx.Transfers {
		leg := storage.Transfer{
			From:      m.From.Hex(),
			To:        m.To.Hex(),
			Direction: string(m.Direction),
			Asset:     m.Asset,
			Decimals:  m.Decimals,
			ValueRaw:  m.Value.String(),
			Value:     event.FormatUnits(m.Value, m.Decimals),
			LogIndex:  m.LogIndex,
			TxSender:  m.TxSender.Hex(),
			TxNonce:   m.TxNonce,
		}
		if m.Token != nil {
			leg.TokenAddress = m.Token.Hex()
		}
		out.Transfers = append(out.Transfers, leg)
	}
	return out, true
}

// Import restores persisted transactions into an empty tracker without
// emitting events. Malformed rows are skipped and reported.
func (t *Tracker) Import(txs []storage.TrackedTx) error {
	var firstErr error
	for _, stx := range txs {
		tx := &trackedTx{
			Hash:           common.HexToHash(stx.Hash),
			Block:          common.HexToHash(stx.BlockHash),
			BlockNum:       stx.BlockNumber,
			FirstSeen:      stx.FirstSeen,
			FirstSeenBlock: stx.FirstSeenBlock,
			MinedAt:        stx.MinedAt,
			pendingSince:   stx.PendingSince,
			Sender:         common.HexToAddress(stx.Sender),
			Nonce:          stx.Nonce,
			hasNonce:       stx.HasNonce,
			Authoritative:  stx.Authoritative,
		}
		switch stx.Status {
		case "pending":
			tx.State = statePending
		case "mined":
			tx.State = stateMined
		default:
			if firstErr == nil {
				firstErr = fmt.Errorf("tx %s: not an active status: %q", stx.Hash, stx.Status)
			}
			continue
		}
		ok := true
		for _, leg := range stx.Transfers {
			value, valid := new(big.Int).SetString(leg.ValueRaw, 10)
			if !valid {
				if firstErr == nil {
					firstErr = fmt.Errorf("tx %s: invalid value %q", stx.Hash, leg.ValueRaw)
				}
				ok = false
				break
			}
			m := Match{
				TxHash:    tx.Hash,
				From:      common.HexToAddress(leg.From),
				To:        common.HexToAddress(leg.To),
				Direction: event.Direction(leg.Direction),
				Asset:     leg.Asset,
				Decimals:  leg.Decimals,
				Value:     value,
				LogIndex:  leg.LogIndex,
				TxSender:  common.HexToAddress(leg.TxSender),
				TxNonce:   leg.TxNonce,
			}
			if leg.TokenAddress != "" {
				token := common.HexToAddress(leg.TokenAddress)
				m.Token = &token
			}
			tx.Transfers = append(tx.Transfers, m)
		}
		if !ok {
			continue
		}
		t.txs[tx.Hash] = tx
		if tx.State == statePending {
			t.indexNonce(tx)
		}
	}
	return firstErr
}

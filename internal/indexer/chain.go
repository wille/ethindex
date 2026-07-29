package indexer

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

// headRef is a compact record of a seen canonical header.
type headRef struct {
	Number     uint64
	Hash       common.Hash
	ParentHash common.Hash
}

// headKind classifies how a new head relates to the tracked chain tip.
type headKind int

const (
	headExtend headKind = iota // parent is our tip: process just this block
	headGap                    // heads were missed: scan [tip+1, head.Number]
	headReorg                  // parent mismatch: find ancestor and rescan
	headStale                  // at or below tip with a known hash: ignore
)

// chainState tracks recently seen canonical headers in a ring buffer so
// reorgs can be detected and walked back to a common ancestor.
type chainState struct {
	refs  []headRef // ring buffer, indexed by number % len
	tip   *headRef
	depth uint64
}

func newChainState(confirmations uint64) *chainState {
	depth := uint64(64)
	if confirmations+16 > depth {
		depth = confirmations + 16
	}
	return &chainState{refs: make([]headRef, depth), depth: depth}
}

func (c *chainState) record(h headRef) {
	c.refs[h.Number%c.depth] = h
	if c.tip == nil || h.Number >= c.tip.Number {
		tip := h
		c.tip = &tip
	}
}

// at returns the recorded header for a block number, if still buffered.
func (c *chainState) at(number uint64) (headRef, bool) {
	r := c.refs[number%c.depth]
	return r, r.Number == number && r.Hash != (common.Hash{})
}

// floor is the lowest block number still guaranteed to be in the buffer.
func (c *chainState) floor() uint64 {
	if c.tip == nil || c.tip.Number < c.depth-1 {
		return 0
	}
	return c.tip.Number - (c.depth - 1)
}

// resetTip moves the tip to the entry recorded at number. After a
// reorg onto a chain whose head is LOWER than the old tip, the old tip
// is orphaned - leaving it in place makes every subsequent head look
// like yet another reorg (it compares below the stale tip against
// orphaned buffer entries) until the chain outgrows it.
func (c *chainState) resetTip(number uint64) {
	if r, ok := c.at(number); ok {
		c.tip = &r
	}
}

// snapshot returns the buffered headers in ascending block order, for
// persistence.
func (c *chainState) snapshot() []headRef {
	if c.tip == nil {
		return nil
	}
	var out []headRef
	for n := c.floor(); n <= c.tip.Number; n++ {
		if r, ok := c.at(n); ok {
			out = append(out, r)
		}
	}
	return out
}

// classify decides how to treat a newly received head. The caller
// records processed blocks via record(); classify never mutates state.
func (c *chainState) classify(h *types.Header) headKind {
	if c.tip == nil {
		return headExtend
	}
	num := h.Number.Uint64()
	switch {
	case h.ParentHash == c.tip.Hash && num == c.tip.Number+1:
		return headExtend
	case num > c.tip.Number+1:
		return headGap
	case num <= c.tip.Number:
		if r, ok := c.at(num); ok && r.Hash == h.Hash() {
			return headStale
		}
		return headReorg
	default:
		return headReorg
	}
}

// headerFetcher is the subset of the client needed to walk back a reorg.
type headerFetcher interface {
	// headerRefsByNumbers resolves several canonical headers in one
	// batch request, with per-number errors.
	headerRefsByNumbers(numbers []uint64) ([]headRef, []error, error)
}

// ancestorChunk is how many candidate headers one findAncestor round
// trip covers. Shallow reorgs (the overwhelmingly common case) still
// resolve in a single request; a deep one costs depth/chunk requests
// instead of depth.
const ancestorChunk = 8

// findAncestor walks the canonical chain backwards from fromNumber,
// comparing against the ring buffer, and returns the highest block
// number at which both chains agree. Headers are fetched in chunks of
// ancestorChunk. If the fork is deeper than the buffer, it returns the
// buffer floor and deep=true.
func (c *chainState) findAncestor(fetch headerFetcher, fromNumber uint64) (ancestor uint64, deep bool, err error) {
	floor := c.floor()
	chunk := make([]uint64, 0, ancestorChunk)
	locals := make([]headRef, 0, ancestorChunk)
	// check resolves the accumulated chunk; found reports a match.
	check := func() (uint64, bool, error) {
		remotes, errs, err := fetch.headerRefsByNumbers(chunk)
		if err != nil {
			return 0, false, err
		}
		for i := range chunk {
			if errs[i] != nil {
				return 0, false, errs[i]
			}
			if remotes[i].Hash == locals[i].Hash {
				return chunk[i], true, nil
			}
		}
		chunk = chunk[:0]
		locals = locals[:0]
		return 0, false, nil
	}
	for n := fromNumber; n > floor; n-- {
		if local, ok := c.at(n); ok {
			chunk = append(chunk, n)
			locals = append(locals, local)
		}
		if len(chunk) >= ancestorChunk {
			if a, found, err := check(); err != nil || found {
				return a, false, err
			}
		}
	}
	// The final partial chunk (the walk may end on numbers the ring
	// never held).
	if len(chunk) > 0 {
		if a, found, err := check(); err != nil || found {
			return a, false, err
		}
	}
	return floor, true, nil
}

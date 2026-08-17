package config

import (
	"github.com/ethereum/go-ethereum/common"

	"github.com/wille/ethindex/internal/event"
)

// WatchedSet is the watched addresses with their wallet labels, built
// once from config and shared read-only by every indexer and the API.
// Labels are interned: each address carries one int32 into a small
// label table (one entry per named wallet or named static address), so
// the set stays compact with millions of derived addresses.
type WatchedSet struct {
	idx     map[common.Address]int32
	labels  []string // labels[0] is the empty label
	byLabel map[string]int32
}

func newWatchedSet(capacity int) *WatchedSet {
	return &WatchedSet{
		idx:     make(map[common.Address]int32, capacity),
		labels:  []string{""},
		byLabel: map[string]int32{},
	}
}

// NewWatchedSet builds a set from explicit address labels ("" means
// unnamed). Config loading populates the set from the addresses and
// hd_wallets sections; this constructor exists for tests.
func NewWatchedSet(names map[common.Address]string) *WatchedSet {
	s := newWatchedSet(len(names))
	for addr, name := range names {
		s.add(addr, s.intern(name))
	}
	return s
}

// intern adds a label to the table and returns its index; the empty
// label is always index 0 and repeated labels share one entry.
func (s *WatchedSet) intern(label string) int32 {
	if label == "" {
		return 0
	}
	if i, ok := s.byLabel[label]; ok {
		return i
	}
	s.labels = append(s.labels, label)
	i := int32(len(s.labels) - 1)
	s.byLabel[label] = i
	return i
}

// add records a watched address pointing at an interned label index,
// reporting false when the address is already present.
func (s *WatchedSet) add(addr common.Address, label int32) bool {
	if _, dup := s.idx[addr]; dup {
		return false
	}
	s.idx[addr] = label
	return true
}

// Count is the number of watched addresses.
func (s *WatchedSet) Count() int { return len(s.idx) }

// Contains reports whether addr is watched.
func (s *WatchedSet) Contains(addr common.Address) bool {
	_, ok := s.idx[addr]
	return ok
}

// Name returns the wallet label of a watched address, or "" when the
// address is unnamed or not watched.
func (s *WatchedSet) Name(addr common.Address) string {
	return s.labels[s.idx[addr]]
}

// Each calls fn for every watched address, in no particular order.
func (s *WatchedSet) Each(fn func(common.Address)) {
	for a := range s.idx {
		fn(a)
	}
}

// For returns the wallet label of the transfer side dir marks as
// watched: the recipient for in/self (self falls back to the sender's
// label), the sender for out. Empty when the watched side is unnamed.
func (s *WatchedSet) For(from, to common.Address, dir event.Direction) string {
	if dir == event.DirectionOut {
		return s.Name(from)
	}
	if name := s.Name(to); name != "" {
		return name
	}
	if dir == event.DirectionSelf {
		return s.Name(from)
	}
	return ""
}

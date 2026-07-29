package event

import "sync"

// TimeLayout is the fixed-width timestamp format used for event
// timestamps and SSE event ids. Unlike RFC3339Nano it never trims
// trailing zeros, so lexicographic order equals chronological order -
// required for cursor comparisons in SQL and on clients.
const TimeLayout = "2006-01-02T15:04:05.000000000Z07:00"

// Sink receives lifecycle events. Emit must be safe for concurrent use.
type Sink interface {
	Emit(Event) error
}

// Multi fans events out to every non-nil sink. It returns nil when no
// sinks remain and the sink itself when there is exactly one.
func Multi(sinks ...Sink) Sink {
	var out multiSink
	for _, s := range sinks {
		if s != nil {
			out = append(out, s)
		}
	}
	switch len(out) {
	case 0:
		return nil
	case 1:
		return out[0]
	}
	return out
}

type multiSink []Sink

func (m multiSink) Emit(ev Event) error {
	var first error
	for _, s := range m {
		if err := s.Emit(ev); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// Hub broadcasts events to live subscribers (API stream connections).
// A subscriber that stops draining its channel is dropped once its
// buffer fills - a stuck consumer must never block indexing. Dropped
// and canceled subscribers see their channel closed; stream consumers
// reconnect and catch up from the database.
type Hub struct {
	mu   sync.Mutex
	subs map[uint64]chan Event
	next uint64
}

// NewHub returns an empty Hub.
func NewHub() *Hub {
	return &Hub{subs: make(map[uint64]chan Event)}
}

// Subscribe registers a subscriber with the given channel buffer.
// cancel is idempotent and safe to call after the hub dropped the
// subscriber.
func (h *Hub) Subscribe(buffer int) (<-chan Event, func()) {
	ch := make(chan Event, buffer)
	h.mu.Lock()
	id := h.next
	h.next++
	h.subs[id] = ch
	h.mu.Unlock()
	cancel := func() {
		h.mu.Lock()
		defer h.mu.Unlock()
		if _, ok := h.subs[id]; ok {
			delete(h.subs, id)
			close(ch)
		}
	}
	return ch, cancel
}

// Subscribers returns the number of live subscribers.
func (h *Hub) Subscribers() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.subs)
}

// Emit delivers ev to every subscriber, dropping any whose buffer is
// full. Never blocks and never fails.
func (h *Hub) Emit(ev Event) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	for id, ch := range h.subs {
		select {
		case ch <- ev:
		default:
			delete(h.subs, id)
			close(ch)
		}
	}
	return nil
}

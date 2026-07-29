package event

import (
	"errors"
	"testing"
)

func TestHubFanOut(t *testing.T) {
	h := NewHub()
	a, cancelA := h.Subscribe(4)
	b, cancelB := h.Subscribe(4)
	defer cancelA()
	defer cancelB()

	if err := h.Emit(Event{TxHash: "0x1"}); err != nil {
		t.Fatal(err)
	}
	if ev := <-a; ev.TxHash != "0x1" {
		t.Errorf("subscriber a got %+v", ev)
	}
	if ev := <-b; ev.TxHash != "0x1" {
		t.Errorf("subscriber b got %+v", ev)
	}
}

func TestHubDropsSlowSubscriber(t *testing.T) {
	h := NewHub()
	slow, cancelSlow := h.Subscribe(1)
	fast, cancelFast := h.Subscribe(4)
	defer cancelSlow()
	defer cancelFast()

	// The slow subscriber never drains; its 1-slot buffer fills on the
	// first event and the second must drop it without blocking.
	h.Emit(Event{TxHash: "0x1"})
	h.Emit(Event{TxHash: "0x2"})

	if h.Subscribers() != 1 {
		t.Fatalf("subscribers = %d, want the slow one dropped", h.Subscribers())
	}
	<-slow // buffered event
	if _, ok := <-slow; ok {
		t.Error("slow subscriber channel not closed")
	}
	if ev := <-fast; ev.TxHash != "0x1" {
		t.Errorf("fast subscriber got %+v", ev)
	}
	if ev := <-fast; ev.TxHash != "0x2" {
		t.Errorf("fast subscriber got %+v", ev)
	}
	// cancel after drop must not panic (double close guard).
	cancelSlow()
}

func TestHubCancel(t *testing.T) {
	h := NewHub()
	ch, cancel := h.Subscribe(1)
	cancel()
	cancel() // idempotent
	if _, ok := <-ch; ok {
		t.Error("canceled channel not closed")
	}
	if h.Subscribers() != 0 {
		t.Errorf("subscribers = %d after cancel", h.Subscribers())
	}
	h.Emit(Event{TxHash: "0x1"}) // must not panic on the closed channel
}

type errSink struct{ err error }

func (s errSink) Emit(Event) error { return s.err }

type countSink struct{ n int }

func (s *countSink) Emit(Event) error { s.n++; return nil }

func TestMulti(t *testing.T) {
	if Multi() != nil {
		t.Error("empty Multi should be nil")
	}
	if Multi(nil, nil) != nil {
		t.Error("all-nil Multi should be nil")
	}
	single := &countSink{}
	if Multi(nil, single) != Sink(single) {
		t.Error("single-sink Multi should return the sink itself")
	}

	boom := errors.New("boom")
	a, b := &countSink{}, &countSink{}
	m := Multi(a, errSink{boom}, b)
	if err := m.Emit(Event{}); err != boom {
		t.Errorf("err = %v, want first sink error", err)
	}
	if a.n != 1 || b.n != 1 {
		t.Errorf("sinks after error: a=%d b=%d, want both emitted", a.n, b.n)
	}
}

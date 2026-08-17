package api

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"

	"github.com/wille/ethindex/internal/config"
	"github.com/wille/ethindex/internal/event"
	"github.com/wille/ethindex/internal/storage"
)

func newTestServer(t *testing.T) (*httptest.Server, storage.Storage, *event.Hub) {
	t.Helper()
	store, err := storage.OpenSQLite(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	hub := event.NewHub()
	// sampleTx's incoming leg pays 0xb, named here so synthesized
	// events carry a wallet label.
	watched := config.NewWatchedSet(map[common.Address]string{
		common.HexToAddress("0xb"): "test-wallet",
	})
	srv := New(store, hub, map[string]uint64{"ethereum": 1, "base": 8453}, watched)
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return ts, store, hub
}

func sampleTx(hash, status string, block uint64) storage.TrackedTx {
	idx := uint(3)
	return storage.TrackedTx{
		Hash:         hash,
		Status:       status,
		BlockNumber:  block,
		FirstSeen:    time.Date(2026, 7, 26, 10, 0, 0, 0, time.UTC),
		PendingSince: time.Date(2026, 7, 26, 10, 0, 0, 0, time.UTC),
		Transfers: []storage.Transfer{
			{From: "0xa", To: "0xb", Direction: "in", Asset: "USDC", TokenAddress: "0xtoken",
				Decimals: 6, ValueRaw: "2500000", Value: "2.5", LogIndex: &idx},
		},
	}
}

// frame is one parsed SSE frame.
type frame struct {
	id, typ string
	ev      event.Event
}

// readFrames reads n SSE data frames from a live stream.
func readFrames(t *testing.T, r *bufio.Reader, n int) []frame {
	t.Helper()
	var (
		out []frame
		cur frame
	)
	for len(out) < n {
		line, err := r.ReadString('\n')
		if err != nil {
			t.Fatalf("stream ended after %d frames: %v", len(out), err)
		}
		line = strings.TrimRight(line, "\n")
		switch {
		case strings.HasPrefix(line, "id: "):
			cur.id = strings.TrimPrefix(line, "id: ")
		case strings.HasPrefix(line, "event: "):
			cur.typ = strings.TrimPrefix(line, "event: ")
		case strings.HasPrefix(line, "data: "):
			if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &cur.ev); err != nil {
				t.Fatalf("bad data frame %q: %v", line, err)
			}
			out = append(out, cur)
			cur = frame{}
		}
	}
	return out
}

func openStream(t *testing.T, url string) (*bufio.Reader, func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		cancel()
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		cancel()
		t.Fatalf("content type = %q", ct)
	}
	return bufio.NewReader(resp.Body), func() { cancel(); resp.Body.Close() }
}

func waitForSubscriber(t *testing.T, hub *event.Hub) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for hub.Subscribers() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("stream never subscribed to the hub")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestLiveStream(t *testing.T) {
	ts, _, hub := newTestServer(t)
	r, done := openStream(t, ts.URL+"/v1/events")
	defer done()
	waitForSubscriber(t, hub)

	hub.Emit(event.Event{Type: event.TypeMined, Indexer: "ethereum", TxHash: "0x1",
		Timestamp: time.Now().UTC().Format(event.TimeLayout)})
	hub.Emit(event.Event{Type: event.TypeConfirmed, Indexer: "ethereum", TxHash: "0x1",
		Timestamp: time.Now().UTC().Format(event.TimeLayout)})

	frames := readFrames(t, r, 2)
	if frames[0].typ != "mined" || frames[1].typ != "confirmed" {
		t.Errorf("types = %s, %s", frames[0].typ, frames[1].typ)
	}
	if frames[0].ev.TxHash != "0x1" || frames[0].id == "" || frames[0].id != frames[0].ev.Timestamp {
		t.Errorf("frame = %+v", frames[0])
	}
}

func TestLiveStreamIndexerFilter(t *testing.T) {
	ts, _, hub := newTestServer(t)
	r, done := openStream(t, ts.URL+"/v1/events?indexer=base")
	defer done()
	waitForSubscriber(t, hub)

	hub.Emit(event.Event{Type: event.TypeMined, Indexer: "ethereum", TxHash: "0xeth"})
	hub.Emit(event.Event{Type: event.TypeMined, Indexer: "base", TxHash: "0xbase"})

	frames := readFrames(t, r, 1)
	if frames[0].ev.TxHash != "0xbase" {
		t.Errorf("got %+v, want the base event only", frames[0].ev)
	}
}

func TestBackfillThenLive(t *testing.T) {
	ts, store, hub := newTestServer(t)
	ctx := context.Background()

	before := time.Now().Add(-time.Minute)
	if err := store.SaveTransaction(ctx, "ethereum", sampleTx("0x1", "mined", 100)); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveIndexerState(ctx, "ethereum", storage.IndexerState{LastProcessed: 104}); err != nil {
		t.Fatal(err)
	}

	r, done := openStream(t, ts.URL+"/v1/events?lastEventId="+before.UTC().Format(time.RFC3339Nano))
	defer done()

	// Backfilled event synthesized from the stored row.
	frames := readFrames(t, r, 1)
	got := frames[0].ev
	if got.Type != event.TypeMined || got.TxHash != "0x1" || got.Value != "2.5" {
		t.Errorf("backfilled event = %+v", got)
	}
	if got.ChainID != 1 {
		t.Errorf("chain id = %d, want mapped from config", got.ChainID)
	}
	if got.Confirmations != 5 {
		t.Errorf("confirmations = %d, want 104-100+1", got.Confirmations)
	}
	if frames[0].id != got.Timestamp || got.Timestamp == "" {
		t.Errorf("id = %q, timestamp = %q", frames[0].id, got.Timestamp)
	}

	// The stream switches to live delivery after the backfill.
	waitForSubscriber(t, hub)
	hub.Emit(event.Event{Type: event.TypeConfirmed, Indexer: "ethereum", TxHash: "0x1",
		Timestamp: time.Now().UTC().Format(event.TimeLayout)})
	live := readFrames(t, r, 1)
	if live[0].typ != "confirmed" {
		t.Errorf("live frame = %+v", live[0])
	}
}

func TestBackfillCursorExcludesOldRows(t *testing.T) {
	ts, store, hub := newTestServer(t)
	ctx := context.Background()

	if err := store.SaveTransaction(ctx, "ethereum", sampleTx("0xold", "confirmed", 90)); err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * time.Millisecond)
	cursor := time.Now()
	time.Sleep(2 * time.Millisecond)
	if err := store.SaveTransaction(ctx, "ethereum", sampleTx("0xnew", "mined", 100)); err != nil {
		t.Fatal(err)
	}

	r, done := openStream(t, ts.URL+"/v1/events?lastEventId="+cursor.UTC().Format(time.RFC3339Nano))
	defer done()
	frames := readFrames(t, r, 1)
	if frames[0].ev.TxHash != "0xnew" {
		t.Errorf("backfill = %+v, want only rows after the cursor", frames[0].ev)
	}
	// Nothing else buffered: the next thing on the wire is live-phase
	// output, verified by emitting a sentinel and reading exactly it.
	waitForSubscriber(t, hub)
	hub.Emit(event.Event{Type: event.TypeMined, Indexer: "ethereum", TxHash: "0xsentinel"})
	if next := readFrames(t, r, 1); next[0].ev.TxHash != "0xsentinel" {
		t.Errorf("unexpected extra frame %+v", next[0].ev)
	}
}

func TestLatestEndpoint(t *testing.T) {
	ts, store, _ := newTestServer(t)
	ctx := context.Background()
	if err := store.SaveTransaction(ctx, "ethereum", sampleTx("0x1", "confirmed", 100)); err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * time.Millisecond)
	if err := store.SaveTransaction(ctx, "base", sampleTx("0x2", "pending", 0)); err != nil {
		t.Fatal(err)
	}

	resp, err := http.Get(ts.URL + "/v1/events/latest?limit=1")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var events []event.Event
	if err := json.NewDecoder(resp.Body).Decode(&events); err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].TxHash != "0x2" || events[0].Type != event.TypePending {
		t.Errorf("latest = %+v, want the newest row", events)
	}
	if events[0].ChainID != 8453 {
		t.Errorf("chain id = %d", events[0].ChainID)
	}
	if events[0].Wallet != "test-wallet" {
		t.Errorf("wallet = %q, want config-resolved name", events[0].Wallet)
	}
}

func TestBadRequests(t *testing.T) {
	ts, _, _ := newTestServer(t)
	for _, url := range []string{
		"/v1/events?lastEventId=not-a-time",
		"/v1/events/latest?limit=zero",
		"/v1/events/latest?limit=-1",
	} {
		resp, err := http.Get(ts.URL + url)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("%s: status = %d, want 400", url, resp.StatusCode)
		}
	}
}

// Package api serves the consumer-facing events API: a live SSE stream
// of lifecycle events with database-backed catch-up, inspired by
// NBXplorer's event API. Delivery is at-least-once: reconnecting with
// the last received event id replays every transfer leg updated since,
// at its current status - consumers deduplicate by
// (tx_hash, log_index, type).
package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/ethereum/go-ethereum/common"

	"github.com/wille/ethindex/internal/config"
	"github.com/wille/ethindex/internal/event"
	"github.com/wille/ethindex/internal/storage"
)

const (
	// subscriberBuffer is the per-connection live event buffer; a
	// consumer that falls this far behind is disconnected and catches
	// up from the database on reconnect.
	subscriberBuffer = 256
	// backfillPage is the keyset page size for catch-up queries.
	backfillPage = 500
	// defaultLatestLimit and maxLatestLimit bound /v1/events/latest.
	defaultLatestLimit = 10
	maxLatestLimit     = 1000
)

// Server serves the events API over plain net/http.
type Server struct {
	store   storage.Storage
	hub     *event.Hub
	chains  map[string]uint64 // indexer name -> chain id, from config
	watched *config.WatchedSet
	log     *slog.Logger

	// pingInterval is the SSE keep-alive comment cadence; shortened in
	// tests.
	pingInterval time.Duration
}

// New builds a Server streaming live events from hub and backfilling
// from store. chains maps indexer names to their chain ids and watched
// carries the wallet labels - both config-derived, which the database
// does not record.
func New(store storage.Storage, hub *event.Hub, chains map[string]uint64, watched *config.WatchedSet) *Server {
	return &Server{
		store:        store,
		hub:          hub,
		chains:       chains,
		watched:      watched,
		log:          slog.Default().With("component", "api"),
		pingInterval: 15 * time.Second,
	}
}

// Handler returns the API route mux.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/events", s.handleEvents)
	mux.HandleFunc("GET /v1/events/latest", s.handleLatest)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	return mux
}

func httpError(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

// handleEvents is the SSE stream. The event id is the event timestamp;
// reconnecting with Last-Event-ID (or ?lastEventId=) replays all legs
// updated since that instant from the database, then goes live. No
// cursor means live-only.
func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	indexer := r.URL.Query().Get("indexer")
	cursorStr := r.Header.Get("Last-Event-ID")
	if cursorStr == "" {
		cursorStr = r.URL.Query().Get("lastEventId")
	}
	var (
		cursor    time.Time
		hasCursor bool
	)
	if cursorStr != "" {
		t, err := time.Parse(time.RFC3339Nano, cursorStr)
		if err != nil {
			httpError(w, http.StatusBadRequest, "invalid lastEventId: expected an RFC3339 timestamp")
			return
		}
		cursor, hasCursor = t, true
	}

	// Subscribe before backfilling: an event landing during the
	// backfill query is then delivered twice at worst, never dropped.
	ch, cancel := s.hub.Subscribe(subscriberBuffer)
	defer cancel()

	rc := http.NewResponseController(w)
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no") // nginx: do not buffer the stream
	w.WriteHeader(http.StatusOK)
	rc.Flush()

	if hasCursor {
		if !s.backfill(w, rc, r, indexer, cursor) {
			return
		}
	}

	ping := time.NewTicker(s.pingInterval)
	defer ping.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case ev, ok := <-ch:
			if !ok {
				// Dropped by the hub for falling behind; ending the
				// stream makes the client reconnect and catch up from
				// the database.
				s.log.Warn("dropping slow event stream consumer", "remote", r.RemoteAddr)
				return
			}
			if indexer != "" && ev.Indexer != indexer {
				continue
			}
			if writeFrame(w, ev) != nil {
				return
			}
			rc.Flush()
		case <-ping.C:
			// Keep-alive comment so idle connections survive proxies.
			if _, err := fmt.Fprint(w, ": ping\n\n"); err != nil {
				return
			}
			rc.Flush()
		}
	}
}

// backfill streams stored legs updated after since, paging by keyset
// until drained. Returns false when the stream should end.
func (s *Server) backfill(w http.ResponseWriter, rc *http.ResponseController, r *http.Request, indexer string, since time.Time) bool {
	confs := &confSource{store: s.store, heads: make(map[string]uint64)}
	cursor := storage.LegCursor{UpdatedAt: since}
	for {
		legs, err := s.store.LoadLegsUpdatedSince(r.Context(), indexer, cursor, backfillPage)
		if err != nil {
			s.log.Error("loading catch-up events", "err", err)
			return false
		}
		for _, leg := range legs {
			if writeFrame(w, s.synthesize(r.Context(), leg, confs)) != nil {
				return false
			}
		}
		rc.Flush()
		if len(legs) < backfillPage {
			return true
		}
		last := legs[len(legs)-1]
		cursor = storage.LegCursor{UpdatedAt: last.UpdatedAt, TxHash: last.TxHash, LogIndex: legIndex(last)}
	}
}

// handleLatest returns the most recent events as a plain JSON array,
// synthesized from the newest transfer-leg rows - a cursor-free peek.
func (s *Server) handleLatest(w http.ResponseWriter, r *http.Request) {
	indexer := r.URL.Query().Get("indexer")
	limit := defaultLatestLimit
	if v := r.URL.Query().Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			httpError(w, http.StatusBadRequest, "invalid limit")
			return
		}
		limit = min(n, maxLatestLimit)
	}
	legs, err := s.store.LoadLatestLegs(r.Context(), indexer, limit)
	if err != nil {
		s.log.Error("loading latest events", "err", err)
		httpError(w, http.StatusInternalServerError, "loading events failed")
		return
	}
	confs := &confSource{store: s.store, heads: make(map[string]uint64)}
	events := make([]event.Event, 0, len(legs))
	for _, leg := range legs {
		events = append(events, s.synthesize(r.Context(), leg, confs))
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(events); err != nil {
		s.log.Debug("writing latest events response", "err", err)
	}
}

// synthesize turns a stored transfer-leg row into the event a live
// subscriber would have received at its latest transition. Timestamp
// (the event id) is the row's updated_at.
func (s *Server) synthesize(ctx context.Context, leg storage.LegUpdate, confs *confSource) event.Event {
	ev := event.Event{
		Type:           event.Type(leg.Status),
		Indexer:        leg.Indexer,
		ChainID:        s.chains[leg.Indexer],
		Direction:      event.Direction(leg.Direction),
		Timestamp:      leg.UpdatedAt.UTC().Format(event.TimeLayout),
		FirstSeenBlock: leg.FirstSeenBlock,
		TxHash:         leg.TxHash,
		LogIndex:       leg.LogIndex,
		From:           leg.From,
		To:             leg.To,
		Asset:          leg.Asset,
		TokenAddress:   leg.TokenAddress,
		Decimals:       leg.Decimals,
		ValueRaw:       leg.ValueRaw,
		Value:          leg.Value,
		BlockNumber:    leg.BlockNumber,
		BlockHash:      leg.BlockHash,
		Confirmations:  confs.confirmations(ctx, leg),
		ReplacedBy:     leg.ReplacedBy,
	}
	// The wallet label is config-derived, resolved at read time rather
	// than persisted - renames apply to replayed history too.
	ev.Wallet = s.watched.For(common.HexToAddress(leg.From), common.HexToAddress(leg.To), ev.Direction)
	if !leg.FirstSeen.IsZero() {
		ev.FirstSeen = leg.FirstSeen.UTC().Format(time.RFC3339)
	}
	if !leg.MinedAt.IsZero() {
		ev.MinedAt = leg.MinedAt.UTC().Format(time.RFC3339)
	}
	return ev
}

// confSource computes best-effort confirmation counts for synthesized
// events from each indexer's persisted last processed block, loaded
// lazily once per request.
type confSource struct {
	store storage.Storage
	heads map[string]uint64
}

func (c *confSource) confirmations(ctx context.Context, leg storage.LegUpdate) uint64 {
	if leg.BlockNumber == 0 {
		return 0
	}
	head, ok := c.heads[leg.Indexer]
	if !ok {
		if st, err := c.store.LoadIndexerState(ctx, leg.Indexer); err == nil && st != nil {
			head = st.LastProcessed
		}
		c.heads[leg.Indexer] = head
	}
	if head < leg.BlockNumber {
		return 0
	}
	return head - leg.BlockNumber + 1
}

func legIndex(leg storage.LegUpdate) int64 {
	if leg.LogIndex == nil {
		return -1
	}
	return int64(*leg.LogIndex)
}

// writeFrame writes one SSE frame: id (the event timestamp), event
// (the lifecycle type) and data (the event JSON).
func writeFrame(w http.ResponseWriter, ev event.Event) error {
	data, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(w, "id: %s\nevent: %s\ndata: %s\n\n", ev.Timestamp, ev.Type, data)
	return err
}

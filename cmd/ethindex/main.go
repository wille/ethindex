// Command ethindex watches one or more EVM chains for native and ERC20
// transfers touching a configured set of addresses, emitting lifecycle
// events to the database, the events API and (with -print) NDJSON on
// stdout. Logs go to stderr.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/lmittmann/tint"
	"github.com/mattn/go-isatty"
	"golang.org/x/sync/errgroup"

	"github.com/wille/ethindex/internal/api"
	"github.com/wille/ethindex/internal/config"
	"github.com/wille/ethindex/internal/event"
	"github.com/wille/ethindex/internal/indexer"
	"github.com/wille/ethindex/internal/metrics"
	"github.com/wille/ethindex/internal/storage"
)

func main() {
	configPath := flag.String("config", "config.yaml", "path to the YAML config file")
	logLevel := flag.String("log-level", "info", "log level: debug, info, warn, error")
	jsonLogs := flag.Bool("json", false, "log in JSON format instead of colorized text")
	printEvents := flag.Bool("print", false, "print every lifecycle event as JSON on stdout")
	resume := flag.Bool("resume", false, "catch up blocks missed while the process was down instead of starting at the chain tip")
	flag.Parse()

	var level slog.Level
	if err := level.UnmarshalText([]byte(*logLevel)); err != nil {
		fmt.Fprintf(os.Stderr, "invalid log level %q\n", *logLevel)
		os.Exit(1)
	}
	var handler slog.Handler
	if *jsonLogs {
		handler = slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
			Level: level,
			// Durations serialize as integer nanoseconds by default;
			// float seconds with millisecond precision query better.
			ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
				if a.Value.Kind() == slog.KindDuration {
					a.Value = slog.Float64Value(math.Round(a.Value.Duration().Seconds()*1000) / 1000)
				}
				return a
			},
		})
	} else {
		handler = tint.NewTextHandler(os.Stderr, &tint.Options{
			Level:      level,
			TimeFormat: time.TimeOnly,
			NoColor:    !isatty.IsTerminal(os.Stderr.Fd()),
		})
	}
	slog.SetDefault(slog.New(handler))

	slog.Info("starting", "config", *configPath)

	cfg, err := config.Load(*configPath)
	if err != nil {
		slog.Error("loading config", "path", *configPath, "err", err)
		os.Exit(1)
	}
	slog.Info("configuration loaded", "addresses", cfg.Watched.Count(), "indexers", len(cfg.Indexers))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	store, err := storage.OpenSQLite(cfg.Database)
	if err != nil {
		slog.Error("opening database", "path", cfg.Database, "err", err)
		os.Exit(1)
	}
	slog.Info("database open", "path", cfg.Database)

	if cfg.Metrics != "" {
		mux := http.NewServeMux()
		mux.Handle("/metrics", metrics.Handler())
		mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		})
		go func() {
			slog.Info("metrics server listening", "addr", cfg.Metrics)
			if err := http.ListenAndServe(cfg.Metrics, mux); err != nil {
				slog.Error("metrics server failed", "err", err)
			}
		}()
	}

	// Announce the shutdown the moment the signal lands; the stages
	// that follow log themselves as they complete.
	go func() {
		<-ctx.Done()
		slog.Info("shutting down, stopping indexers")
	}()

	// Events fan out to the optional stdout NDJSON stream (logs go to
	// stderr, so -print owns stdout) and the API's live subscribers.
	var sinks []event.Sink
	if *printEvents {
		sinks = append(sinks, event.NewEmitter(os.Stdout))
	}
	if cfg.API != "" {
		hub := event.NewHub()
		sinks = append(sinks, hub)
		chains := make(map[string]uint64, len(cfg.Indexers))
		for _, entry := range cfg.Indexers {
			chains[entry.Name] = entry.ChainID
		}
		srv := api.New(store, hub, chains, cfg.Watched)
		go func() {
			slog.Info("api server listening", "addr", cfg.API)
			if err := http.ListenAndServe(cfg.API, srv.Handler()); err != nil {
				slog.Error("api server failed", "err", err)
			}
		}()
	}

	slog.Info("starting indexers", "count", len(cfg.Indexers))
	g, gctx := errgroup.WithContext(ctx)
	for _, entry := range cfg.Indexers {
		ix := indexer.New(entry, cfg.Watched, event.Multi(sinks...), store)
		ix.Resume = *resume
		ix.FullEventLogs = *jsonLogs
		g.Go(func() error { return ix.Run(gctx) })
	}

	err = g.Wait()
	slog.Info("all indexers stopped")
	slog.Info("closing database")
	if closeErr := store.Close(); closeErr != nil {
		slog.Error("closing database", "err", closeErr)
	}
	if err != nil {
		slog.Error("exited with error", "err", err)
		os.Exit(1)
	}
	slog.Info("shutdown complete")
}

// Command llm-gateway is the campus LLM API gateway: it terminates
// OpenAI-compatible requests from student code, authenticates API keys
// against a snapshot document, enforces usage limits, forwards to the
// configured upstream model server, and meters usage into a local spool.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/pnuops/pickle-llm-gateway/internal/config"
	"github.com/pnuops/pickle-llm-gateway/internal/limits"
	"github.com/pnuops/pickle-llm-gateway/internal/server"
	"github.com/pnuops/pickle-llm-gateway/internal/snapshot"
	"github.com/pnuops/pickle-llm-gateway/internal/spool"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))

	cfg, err := config.FromEnv()
	if err != nil {
		log.Error("startup failed", "error", err)
		os.Exit(1)
	}
	store, err := snapshot.Open(cfg.SnapshotPath, log, snapshot.Options{
		KnownUpstreams:       cfg.UpstreamRefs(),
		AllowGenerationReset: cfg.AllowGenerationReset,
	})
	if err != nil {
		log.Error("startup failed", "error", err)
		os.Exit(1)
	}
	sp, err := spool.Open(cfg.SpoolDir)
	if err != nil {
		log.Error("startup failed", "error", err)
		os.Exit(1)
	}
	srv := server.New(cfg, store, limits.New(nil), sp, log)

	hs := &http.Server{
		Addr:              cfg.Listen,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		// Bound the request-read phase. Slots (gateway-wide in-flight + per-key
		// concurrency) are taken before the body is read, so an unbounded read
		// would let a slow client hold them for as long as nginx allows; chat
		// bodies are small, so a request that cannot be read in 30s is stuck.
		ReadTimeout: 30 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	hup := make(chan os.Signal, 1)
	signal.Notify(hup, syscall.SIGHUP)

	go func() {
		t := time.NewTicker(cfg.SnapshotPollInterval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				store.Refresh()
			case <-hup:
				store.Refresh()
			}
		}
	}()

	// Usage-spool retention: prune once at startup and daily thereafter.
	go func() {
		prune := func() {
			if err := sp.Prune(time.Now(), cfg.SpoolRetentionDays); err != nil {
				log.Error("spool prune failed", "error", err)
			}
		}
		prune()
		t := time.NewTicker(24 * time.Hour)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				prune()
			}
		}
	}()

	serveErr := make(chan error, 1)
	go func() { serveErr <- hs.ListenAndServe() }()
	log.Info("llm-gateway listening", "addr", cfg.Listen, "generation", store.Generation())

	select {
	case <-ctx.Done():
	case err := <-serveErr:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("listener failed", "error", err)
			os.Exit(1)
		}
	}

	// Drain in-flight requests before closing the spool: a handler still
	// running when the spool is closed loses its usage record. The grace is a
	// balance — long enough that ordinary requests finish and record, short
	// enough that a deploy is not blocked by a maximal-length stream (which is
	// cut at the grace, its handler then recording a canceled event).
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	_ = hs.Shutdown(shutdownCtx)
	if err := sp.Close(); err != nil {
		log.Error("spool close failed", "error", err)
	}
	log.Info("llm-gateway stopped")
}

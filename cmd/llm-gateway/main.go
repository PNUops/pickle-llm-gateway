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

	"github.com/pnuops/pickle-llm-gateway/internal/bodies"
	"github.com/pnuops/pickle-llm-gateway/internal/config"
	"github.com/pnuops/pickle-llm-gateway/internal/limits"
	"github.com/pnuops/pickle-llm-gateway/internal/reporter"
	"github.com/pnuops/pickle-llm-gateway/internal/server"
	"github.com/pnuops/pickle-llm-gateway/internal/snapshot"
	"github.com/pnuops/pickle-llm-gateway/internal/spool"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	startedAt := time.Now()

	cfg, err := config.FromEnv()
	if err != nil {
		log.Error("startup failed", "error", err)
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var source snapshot.Source
	var controlSource *snapshot.HTTPSource
	if cfg.SnapshotSource == config.SourceHTTP {
		controlSource = snapshot.NewHTTPSource(cfg.ControlBaseURL, cfg.ControlToken, cfg.SnapshotPath, cfg.ControlTimeout)
		source = controlSource
	} else {
		source = snapshot.NewFileSource(cfg.SnapshotPath)
	}
	store, err := snapshot.Open(ctx, source, cfg.SnapshotPath, log, snapshot.Options{
		KnownUpstreams:       cfg.UpstreamRefs(),
		AllowGenerationReset: cfg.AllowGenerationReset,
		// A hand-maintained file is held to the letter so a typo cannot pass;
		// a document from the control plane may carry members a newer api
		// added, and refusing it would stop revocations arriving.
		FromControlPlane: cfg.SnapshotSource == config.SourceHTTP,
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
	// Declared here so the sync self-report can read them; both stay nil when
	// the corresponding channel is off.
	var rep *reporter.Reporter
	var sink *bodies.Sink
	if controlSource != nil {
		// The api never calls the gateway, so the poll is the only channel
		// carrying what the gateway can say about itself. Everything here is a
		// claim the control plane may display but must not act on.
		controlSource.SetGauges(func() snapshot.SyncGauges {
			g := snapshot.SyncGauges{
				InFlight:        srv.InFlight(),
				MaxInFlight:     cfg.MaxInFlight,
				UpstreamRefs:    cfg.UpstreamRefs(),
				RejectedEntries: store.RejectedEntries(),
				ReloadFailures:  store.ReloadFailures(),
				LastError:       store.LastError(),
				StartedAt:       startedAt,
			}
			if sink != nil {
				g.BodiesDropped = sink.Dropped()
			}
			if rep != nil {
				g.UsageShipFailures = rep.ShipFailures()
			}
			return g
		})
	}

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

	hup := make(chan os.Signal, 1)
	signal.Notify(hup, syscall.SIGHUP)

	go func() {
		t := time.NewTicker(cfg.SnapshotPollInterval)
		defer t.Stop()
		refresh := func() {
			// Bound one poll so a hung control plane cannot stall the loop past
			// the next tick.
			pollCtx, cancel := context.WithTimeout(ctx, cfg.ControlTimeout)
			defer cancel()
			store.Refresh(pollCtx)
		}
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				refresh()
			case <-hup:
				refresh()
			}
		}
	}()

	// Body capture rides its own channel, and only exists when there is a
	// control plane to deliver to: captured text is never written to this
	// host's disk, so without delivery there is nowhere for it to go and
	// nothing is captured. Individual keys still have to opt in.
	if cfg.BodyCapture {
		sink = bodies.New(cfg.ControlBaseURL, cfg.ControlToken,
			cfg.BodyQueueSize, cfg.BodyBatchSize, cfg.ControlTimeout, log)
		srv.SetBodySink(sink)
		go sink.Run(ctx)
		log.Info("body capture channel enabled (per-key opt-in still required)")
	}

	// Usage reporting: walk the spool and ship batches to the control plane.
	// The spool is written regardless, so enabling this later ships what
	// already accumulated.
	var shipped func(day string) bool
	if cfg.UsagePush {
		rep = reporter.New(cfg.SpoolDir, cfg.ControlBaseURL, cfg.ControlToken,
			cfg.UsageBatchSize, cfg.ControlTimeout, log)
		go rep.Run(ctx, cfg.UsagePushInterval)
		// Retention must not delete a day the reporter never confirmed: with
		// shipping on, an unreported day file is the only copy of that usage.
		shipped = func(day string) bool {
			offsets, err := rep.ShippedThrough()
			if err != nil {
				return false // unknown: keep the file
			}
			_, ok := offsets[day]
			return ok
		}
	}

	// Usage-spool retention: prune once at startup and daily thereafter.
	go func() {
		prune := func() {
			if err := sp.Prune(time.Now(), cfg.SpoolRetentionDays, shipped); err != nil {
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

	// Operator surface on its own listener, so metrics can never be reached
	// through the student-facing route.
	var adminSrv *http.Server
	if cfg.AdminListen != "" {
		adminSrv = &http.Server{
			Addr:              cfg.AdminListen,
			Handler:           srv.AdminHandler(),
			ReadHeaderTimeout: 10 * time.Second,
			ReadTimeout:       15 * time.Second,
		}
		go func() {
			if err := adminSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Error("admin listener failed", "error", err)
			}
		}()
		log.Info("admin listener up", "addr", cfg.AdminListen)
	}

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
	if adminSrv != nil {
		_ = adminSrv.Shutdown(shutdownCtx)
	}
	if err := sp.Close(); err != nil {
		log.Error("spool close failed", "error", err)
	}
	log.Info("llm-gateway stopped")
}

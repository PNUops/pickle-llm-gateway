package server

import (
	"net/http"
	"sync"
	"sync/atomic"

	"github.com/pnuops/pickle-llm-gateway/internal/spool"
)

// counters are the process-lifetime totals behind the metrics surface. They
// answer the one question the usage spool cannot: what is happening right now.
// Completed requests are reconstructable from the spool afterwards; in-flight
// and queued work only exists while it is happening.
type counters struct {
	byStatus           sync.Map // status string -> *atomic.Int64
	inputTokens        atomic.Int64
	outputTokens       atomic.Int64
	upstreamRetries    atomic.Int64
	upstreamFallbacks  atomic.Int64
	bodiesCaptured     atomic.Int64
	spoolWriteFailures atomic.Int64
}

func (c *counters) observe(ev spool.Event) {
	status := ev.Status
	if status == "" {
		status = "UNKNOWN"
	}
	v, _ := c.byStatus.LoadOrStore(status, new(atomic.Int64))
	v.(*atomic.Int64).Add(1)
	c.inputTokens.Add(int64(ev.InputTokens))
	c.outputTokens.Add(int64(ev.OutputTokens))
}

func (c *counters) statusMap() map[string]int64 {
	out := map[string]int64{}
	c.byStatus.Range(func(k, v any) bool {
		out[k.(string)] = v.(*atomic.Int64).Load()
		return true
	})
	return out
}

// handleMetrics serves the operational gauges. It is registered only on the
// admin listener, which is bound separately (loopback or bridge-internal) —
// the student-facing listener never carries it, so no edge rule has to
// remember to hide it.
func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeAPIError(w, errMethod)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	writeJSON(w, map[string]any{
		"inFlight":               s.InFlight(),
		"maxInFlight":            s.cfg.MaxInFlight,
		"passthroughInFlight":    s.PassthroughInFlight(),
		"passthroughMaxInFlight": s.cfg.PassthroughMaxInFlight,
		"generation":             s.store.Generation(),
		"reloadFailures":         s.store.ReloadFailures(),
		"rejectedEntries":        s.store.RejectedEntries(),
		"bodiesDropped":          s.bodies.Dropped(),
		"requestsByStatus":       s.metrics.statusMap(),
		"inputTokens":            s.metrics.inputTokens.Load(),
		"outputTokens":           s.metrics.outputTokens.Load(),
		"upstreamRetries":        s.metrics.upstreamRetries.Load(),
		"upstreamFallbacks":      s.metrics.upstreamFallbacks.Load(),
		"bodiesCaptured":         s.metrics.bodiesCaptured.Load(),
		"spoolWriteFailures":     s.metrics.spoolWriteFailures.Load(),
	})
}

// AdminHandler is the operator-facing surface: metrics and the same health
// view. It is served on its own listener so it cannot be reached through the
// public route by accident.
func (s *Server) AdminHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", s.handleMetrics)
	mux.HandleFunc("/healthz", s.handleHealthz)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		writeAPIError(w, errNotFound)
	})
	return mux
}

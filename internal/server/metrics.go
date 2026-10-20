package server

import (
	"net/http"
	"sync"
	"sync/atomic"

	"github.com/pnuops/pickle-llm-gateway/internal/snapshot"
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
	// Streams whose cost was read from the vendor's generation record, and
	// those whose lookup found none and were recorded without one.
	streamCostFilled  atomic.Int64
	streamCostMissing atomic.Int64
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
		"chatLargeBodiesInUse":   s.chatLarge.inUse(),
		"chatLargeBodySlots":     s.cfg.ChatLargeBodySlots,
		"generation":             s.store.Generation(),
		"activeKeysWithoutChat":  s.activeKeysWithoutChat(),
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
		"streamCostFilled":       s.metrics.streamCostFilled.Load(),
		"streamCostMissing":      s.metrics.streamCostMissing.Load(),
		"vendorModelCatalog":     s.catalogGauges(),
	})
}

// activeKeysWithoutChat counts the usable keys in the applied document that
// lack the chat grant. Every key that existed when the grant was introduced
// was given it, so on a healthy document this is the number of keys an
// approver deliberately issued without chat, and a jump to every key means
// the document came from a control plane that does not know the grant (a
// cached document after a restart, or the control plane rolled back alone).
// It is a signal for whoever is deploying, not a switch: nothing here turns
// enforcement off when it is high.
func (s *Server) activeKeysWithoutChat() int {
	doc, _, _ := s.store.Current()
	now := s.now()
	n := 0
	for i := range doc.Keys {
		k := &doc.Keys[i]
		if k.Status != snapshot.KeyActive || (k.ExpiresAt != nil && now.After(*k.ExpiresAt)) {
			continue
		}
		if !k.AllowsEndpoint(snapshot.EndpointChat) {
			n++
		}
	}
	return n
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

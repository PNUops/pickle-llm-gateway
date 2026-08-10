// Package server terminates the student-facing OpenAI-compatible surface:
// GET /v1/models and POST /v1/chat/completions, plus a local health probe.
// Every request is authenticated against the snapshot and metered into the
// usage spool. Request and response bodies are never logged.
package server

import (
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/pnuops/pickle-llm-gateway/internal/bodies"
	"github.com/pnuops/pickle-llm-gateway/internal/config"
	"github.com/pnuops/pickle-llm-gateway/internal/limits"
	"github.com/pnuops/pickle-llm-gateway/internal/snapshot"
	"github.com/pnuops/pickle-llm-gateway/internal/spool"
)

// Server carries the wired dependencies for the HTTP surface.
type Server struct {
	cfg      *config.Config
	store    *snapshot.Store
	limiter  *limits.Limiter
	spool    *spool.Writer
	bodies   *bodies.Sink
	health   *upstreamHealth
	metrics  *counters
	log      *slog.Logger
	client   *http.Client
	inFlight chan struct{}
	now      func() time.Time
}

// New wires a Server. The shared HTTP client waits bounded time for upstream
// response headers; total request duration is capped per request via context.
func New(cfg *config.Config, store *snapshot.Store, limiter *limits.Limiter, sp *spool.Writer, log *slog.Logger) *Server {
	return &Server{
		cfg:     cfg,
		store:   store,
		limiter: limiter,
		spool:   sp,
		log:     log,
		client: &http.Client{
			Transport: &http.Transport{
				ResponseHeaderTimeout: cfg.UpstreamHeaderWait,
				MaxIdleConnsPerHost:   16,
				IdleConnTimeout:       90 * time.Second,
			},
		},
		inFlight: make(chan struct{}, cfg.MaxInFlight),
		health:   newUpstreamHealth(nil),
		metrics:  &counters{},
		now:      time.Now,
	}
}

// InFlight reports how many requests are occupying a gateway-wide slot right
// now. It is a gauge for the control-plane handshake and the health surface;
// nothing acts on it.
func (s *Server) InFlight() int { return len(s.inFlight) }

// SetBodySink enables prompt/response capture for keys that opted in. Without
// a sink no capture happens at all, whatever the snapshot says — the channel
// has to exist before anything is collected, which is what keeps captured text
// off this host's disk.
func (s *Server) SetBodySink(sink *bodies.Sink) { s.bodies = sink }

// Handler builds the route table.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealthz)
	mux.HandleFunc("/v1/models", s.handleModels)
	mux.HandleFunc("/v1/models/", s.handleModels)
	mux.HandleFunc("/v1/chat/completions", s.handleChat)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		writeAPIError(w, errNotFound)
	})
	return mux
}

// handleHealthz reports enough to see the failure the design guards against: a
// snapshot that has gone stale because reloads keep failing. It stays a
// liveness probe — it never calls the upstream (that would meter per probe).
func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeAPIError(w, errMethod)
		return
	}
	failures := s.store.ReloadFailures()
	body := map[string]any{
		"status":              "ok",
		"generation":          s.store.Generation(),
		"snapshotAgeSeconds":  int64(s.now().Sub(s.store.LoadedAt()).Seconds()),
		"snapshotReloadStuck": failures > 0,
	}
	if failures > 0 {
		body["status"] = "degraded"
		body["reloadFailures"] = failures
		body["lastError"] = s.store.LastError()
	}
	// Dropped entries are a different failure from a stuck reload: the document
	// loaded, and the gateway is enforcing less than it says. Nothing else on
	// this host would show it.
	if dropped := s.store.RejectedEntries(); dropped > 0 {
		body["status"] = "degraded"
		body["rejectedEntries"] = dropped
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	writeJSON(w, body)
}

// authenticate resolves the bearer token to a key entry, refusing anything
// but an ACTIVE, unexpired key. The lookup comes from the caller so a request
// authenticates against the same snapshot view it does everything else with.
// The auth scheme is matched case-insensitively (RFC 7235).
func (s *Server) authenticate(r *http.Request, lookup func(string) *snapshot.Key) (*snapshot.Key, *apiError) {
	h := r.Header.Get("Authorization")
	const scheme = "bearer "
	if len(h) <= len(scheme) || !strings.EqualFold(h[:len(scheme)], scheme) {
		return nil, &errMissingKey
	}
	token := strings.TrimSpace(h[len(scheme):])
	if token == "" {
		return nil, &errMissingKey
	}
	key := lookup(snapshot.HashToken(token))
	if key == nil {
		return nil, &errInvalidKey
	}
	switch key.Status {
	case snapshot.KeyRevoked:
		return nil, &errKeyRevoked
	case snapshot.KeySuspended:
		return nil, &errKeySuspended
	}
	if key.ExpiresAt != nil && s.now().After(*key.ExpiresAt) {
		return nil, &errKeyExpired
	}
	return key, nil
}

type modelEntry struct {
	ID      string `json:"id"`
	Object  string `json:"object"`
	Created int64  `json:"created"`
	OwnedBy string `json:"owned_by"`
}

// modelsCreatedEpoch is a stable non-zero `created` value for the models
// surface. The real models have no meaningful creation timestamp and 0 renders
// as 1970 in clients, so a fixed recent epoch (2026-01-01 UTC) is used.
const modelsCreatedEpoch = 1767225600

func entryFor(m *snapshot.Model) modelEntry {
	return modelEntry{ID: m.PublicName, Object: "model", Created: modelsCreatedEpoch, OwnedBy: "pnu-cloud"}
}

func (s *Server) handleModels(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeAPIError(w, errMethod)
		return
	}
	doc, keyLookup, modelLookup := s.store.Current()
	if !doc.ServiceEnabled {
		writeAPIError(w, errServiceDisabled)
		return
	}
	key, authErr := s.authenticate(r, keyLookup)
	if authErr != nil {
		writeAPIError(w, *authErr)
		return
	}

	// GET /v1/models/{id}: single-model retrieve, which several SDK helpers
	// call. A missing or not-allowed id is a 404, never a disclosure of a
	// restricted model's existence.
	if id := strings.TrimPrefix(r.URL.Path, "/v1/models/"); id != "" && id != r.URL.Path {
		m := modelLookup(id)
		if m == nil || !key.AllowsModel(m) {
			writeAPIError(w, errModelNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		writeJSON(w, entryFor(m))
		return
	}

	list := struct {
		Object string       `json:"object"`
		Data   []modelEntry `json:"data"`
	}{Object: "list", Data: []modelEntry{}}
	for i := range doc.Models {
		m := &doc.Models[i]
		if !key.AllowsModel(m) {
			continue
		}
		list.Data = append(list.Data, entryFor(m))
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	writeJSON(w, list)
}

// resolved limits for one key, falling back to the gateway defaults.
func (s *Server) keyLimits(k *snapshot.Key) (rpm, tpm, conc int) {
	rpm, tpm, conc = k.Limits.Rpm, k.Limits.Tpm, k.Limits.Concurrency
	if rpm <= 0 {
		rpm = s.cfg.DefaultRpm
	}
	if tpm <= 0 {
		tpm = s.cfg.DefaultTpm
	}
	if conc <= 0 {
		conc = s.cfg.DefaultConcurrency
	}
	return rpm, tpm, conc
}

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
		now:      time.Now,
	}
}

// Handler builds the route table.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealthz)
	mux.HandleFunc("/v1/models", s.handleModels)
	mux.HandleFunc("/v1/chat/completions", s.handleChat)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		writeAPIError(w, errNotFound)
	})
	return mux
}

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeAPIError(w, errMethod)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_, _ = w.Write([]byte(`{"status":"ok","generation":` + itoa64(s.store.Generation()) + `}` + "\n"))
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

func (s *Server) handleModels(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeAPIError(w, errMethod)
		return
	}
	doc, keyLookup, _ := s.store.Current()
	if !doc.ServiceEnabled {
		writeAPIError(w, errServiceDisabled)
		return
	}
	key, authErr := s.authenticate(r, keyLookup)
	if authErr != nil {
		writeAPIError(w, *authErr)
		return
	}
	type modelEntry struct {
		ID      string `json:"id"`
		Object  string `json:"object"`
		Created int64  `json:"created"`
		OwnedBy string `json:"owned_by"`
	}
	list := struct {
		Object string       `json:"object"`
		Data   []modelEntry `json:"data"`
	}{Object: "list", Data: []modelEntry{}}
	for _, m := range doc.Models {
		if !key.Allows(m.PublicName) {
			continue
		}
		list.Data = append(list.Data, modelEntry{ID: m.PublicName, Object: "model", OwnedBy: "pnu-cloud"})
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

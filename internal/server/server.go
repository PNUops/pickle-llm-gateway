// Package server terminates the student-facing OpenAI-compatible surface:
// GET /v1/models and POST /v1/chat/completions, the Anthropic-compatible
// POST /v1/messages (messages.go), the passthrough routes in
// passthrough.go, and a local health probe. Every request is authenticated
// against the snapshot and metered into the usage spool. Request and response
// bodies are never logged.
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
	// The passthrough surface's own transport and large-body pool. Separate
	// from chat's so that neither its header wait nor its large answers can
	// move what chat does, and so the memory its large bodies can hold is one
	// multiplication with a bounded slot count in it — see config. Small
	// passthrough requests take no slot here, only the gateway-wide one.
	passthroughClient *http.Client
	passthroughLarge  *largeBodySlots
	// chatLarge admits chat bodies and non-streamed chat responses above the
	// ordinary bound; nil when the pool is configured off.
	chatLarge *largeBodySlots
	// costs holds the generation lookups of streams that ended without a
	// cost; see generation_cost.go.
	costs *generationCosts
	// catalog is the vendor's model catalog the list shows paid models
	// from; see catalog.go.
	catalog *catalogState
	now     func() time.Time
	// writeBudget is how long an answer of n bytes may take to reach the
	// client (see largeWriteBudget). A field so tests can fix it rather than
	// race a machine-speed-dependent deadline.
	writeBudget func(n int) time.Duration
}

// New wires a Server. The shared HTTP client waits bounded time for upstream
// response headers; total request duration is capped per request via context.
func New(cfg *config.Config, store *snapshot.Store, limiter *limits.Limiter, sp *spool.Writer, log *slog.Logger) *Server {
	m := &counters{}
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
		passthroughClient: &http.Client{
			Transport: &http.Transport{
				ResponseHeaderTimeout: cfg.PassthroughHeaderWait,
				MaxIdleConnsPerHost:   16,
				IdleConnTimeout:       90 * time.Second,
				// Ask for an identity body so the declared length reaches
				// the response: a gzip answer arrives with ContentLength -1,
				// which sends every large answer down the growing-buffer
				// path instead of the sized read the memory budget assumes.
				DisableCompression: true,
			},
		},
		passthroughLarge: newLargeBodySlots(cfg.PassthroughMaxInFlight),
		chatLarge:        newLargeBodySlots(cfg.ChatLargeBodySlots),
		health:           newUpstreamHealth(nil),
		metrics:          m,
		costs:            newGenerationCosts(m),
		catalog:          newCatalogState(),
		now:              time.Now,
		writeBudget:      largeWriteBudget,
	}
}

// InFlight reports how many requests are occupying a gateway-wide slot right
// now. It is a gauge for the control-plane handshake and the health surface;
// nothing acts on it.
func (s *Server) InFlight() int { return len(s.inFlight) }

// PassthroughInFlight is how many of the passthrough surface's large-body
// slots are taken. The name predates the split: a small passthrough request
// takes a gateway-wide slot only and is counted by InFlight, not here.
func (s *Server) PassthroughInFlight() int { return s.passthroughLarge.inUse() }

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
	// The Anthropic Messages surface. Nothing under it is served beyond the
	// route itself: the token-counting route has nothing upstream to forward
	// to, and it and every other subpath are answered as unknown, in this
	// surface's own envelope so an Anthropic client can read the refusal.
	mux.HandleFunc("/v1/messages", s.handleMessages)
	mux.HandleFunc("/v1/messages/", s.handleMessagesUnknown)
	// The OpenAI Responses surface. Every subpath reads a stored response,
	// and nothing is stored, so all of them are answered as unknown.
	mux.HandleFunc("/v1/responses", s.handleResponses)
	mux.HandleFunc("/v1/responses/", s.handleResponsesUnknown)
	// The passthrough surface. Every pattern here is exact, so none of them
	// competes with the "/v1/models/" subtree above: a request for
	// /v1/models/{id} still matches the more specific subtree pattern, and
	// /v1/images/models matches its own exact pattern rather than either.
	mux.HandleFunc("/v1/images", s.handlePassthrough(routeImages))
	mux.HandleFunc("/v1/images/generations", s.handlePassthrough(routeImageGenerations))
	mux.HandleFunc("/v1/images/models", s.handlePassthrough(routeImageModels))
	mux.HandleFunc("/v1/embeddings", s.handlePassthrough(routeEmbeddings))
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
//
// A refusal still returns the key when one was resolved. Whose key was
// suspended, revoked or expired is the whole value of recording the refusal —
// without it the event says only that somebody was turned away. Only a token
// that matched nothing (or was never presented) has no owner to name.
func (s *Server) authenticate(r *http.Request, lookup func(string) *snapshot.Key) (*snapshot.Key, *apiError) {
	token, missing := bearerToken(r)
	if missing != nil {
		return nil, missing
	}
	return s.authenticateToken(token, lookup)
}

// bearerToken reads the key from Authorization: Bearer, the one place an
// OpenAI-compatible client sends it.
func bearerToken(r *http.Request) (string, *apiError) {
	if token := bearerValue(r); token != "" {
		return token, nil
	}
	return "", &errMissingKey
}

// bearerValue is the token in Authorization: Bearer, or "" when there is none.
func bearerValue(r *http.Request) string {
	h := r.Header.Get("Authorization")
	const scheme = "bearer "
	if len(h) <= len(scheme) || !strings.EqualFold(h[:len(scheme)], scheme) {
		return ""
	}
	return strings.TrimSpace(h[len(scheme):])
}

// authenticateToken is authenticate for a token already read off the request.
func (s *Server) authenticateToken(token string, lookup func(string) *snapshot.Key) (*snapshot.Key, *apiError) {
	key := lookup(snapshot.HashToken(token))
	if key == nil {
		return nil, &errInvalidKey
	}
	switch key.Status {
	case snapshot.KeyRevoked:
		return key, &errKeyRevoked
	case snapshot.KeySuspended:
		return key, &errKeySuspended
	}
	if key.ExpiresAt != nil && s.now().After(*key.ExpiresAt) {
		return key, &errKeyExpired
	}
	return key, nil
}

type modelEntry struct {
	ID      string `json:"id"`
	Object  string `json:"object"`
	Created int64  `json:"created"`
	OwnedBy string `json:"owned_by"`
	// ContextLength is the one field past the standard four, carried for
	// paid models only. SDKs that do not know it ignore it.
	ContextLength int `json:"context_length,omitempty"`
}

// modelsCreatedEpoch is a stable non-zero `created` value for the models
// surface. The real models have no meaningful creation timestamp and 0 renders
// as 1970 in clients, so a fixed recent epoch (2026-01-01 UTC) is used.
const modelsCreatedEpoch = 1767225600

func entryFor(m *snapshot.Model) modelEntry {
	return modelEntry{ID: m.PublicName, Object: "model", Created: modelsCreatedEpoch, OwnedBy: "pnu-cloud"}
}

// vendorEntryFor is a paid model as the vendor's catalog describes it. The
// owner is the name's vendor segment, which is what the name already says.
func vendorEntryFor(v *vendorModel) modelEntry {
	created := v.Created
	if created <= 0 {
		created = modelsCreatedEpoch
	}
	owner, _, _ := strings.Cut(strings.TrimLeft(v.ID, "~"), "/")
	return modelEntry{ID: v.ID, Object: "model", Created: created, OwnedBy: owner, ContextLength: v.ContextLength}
}

// anthropicModelEntry is one model in the shape Anthropic's models API uses,
// which its SDKs and agent clients read when they send anthropic-version.
type anthropicModelEntry struct {
	Type        string `json:"type"`
	ID          string `json:"id"`
	DisplayName string `json:"display_name"`
	CreatedAt   string `json:"created_at"`
	// The two context fields of that shape, carried for paid models only and
	// left out when the vendor's catalog does not give them.
	MaxInputTokens int `json:"max_input_tokens,omitempty"`
	MaxTokens      int `json:"max_tokens,omitempty"`
}

// modelsCreatedAt is modelsCreatedEpoch in the RFC 3339 form that shape uses.
var modelsCreatedAt = time.Unix(modelsCreatedEpoch, 0).UTC().Format(time.RFC3339)

func anthropicEntryFor(m *snapshot.Model) anthropicModelEntry {
	return anthropicModelEntry{Type: "model", ID: m.PublicName, DisplayName: m.PublicName, CreatedAt: modelsCreatedAt}
}

func anthropicVendorEntryFor(v *vendorModel) anthropicModelEntry {
	e := vendorEntryFor(v)
	name := v.Name
	if name == "" {
		name = v.ID
	}
	return anthropicModelEntry{Type: "model", ID: v.ID, DisplayName: name,
		CreatedAt:      time.Unix(e.Created, 0).UTC().Format(time.RFC3339),
		MaxInputTokens: v.ContextLength, MaxTokens: v.MaxOutput}
}

// servableModel is the one judgement both halves of the models route make:
// the model a name resolves to for this key, or nil when the key could not
// call it. It has to agree with chat. A catalogued name is judged by its
// entry; any other name chat would serve through passthrough exists here too,
// but only for a key that could actually call it (a credential for the
// passthrough upstream), and for anyone else it is nil, same as a name that
// was never real.
func servableModel(doc *snapshot.Document, key *snapshot.Key, modelLookup func(string) *snapshot.Model, name string) *snapshot.Model {
	m := modelLookup(name)
	if m == nil {
		if p := passthroughModel(doc, name); p != nil && key.CredentialFor(p.UpstreamRef) != "" {
			m = p
		}
	}
	if m == nil || !key.AllowsModel(m) || !key.ServesCreditModel(m) {
		return nil
	}
	return m
}

// vendorEndpointsSuffix ends the vendor's per-model provider listing,
// /models/{author}/{slug}/endpoints. It is the vendor's own surface, not one
// of the general model APIs, so it is not supported; without this the
// retrieve would read the whole path as a model name and could answer it.
const vendorEndpointsSuffix = "/endpoints"

// The models route answers two client families from one path, and the
// anthropic-version header is what tells them apart: an Anthropic client
// sends it on every request and an OpenAI one never does. The list and the
// refusals both follow it, so neither family is handed a shape it cannot read.
// What the list contains is the same either way.
func (s *Server) handleModels(w http.ResponseWriter, r *http.Request) {
	anthropic := r.Header.Get("Anthropic-Version") != ""
	fail := func(e apiError) {
		if anthropic {
			writeAnthropicError(w, e, "")
			return
		}
		writeAPIError(w, e)
	}
	id := strings.TrimPrefix(r.URL.Path, "/v1/models/")
	retrieve := id != "" && id != r.URL.Path
	// Answered before authentication, like every other unsupported path.
	// A trailing slash and any letter case are the same route to a client.
	if lower := strings.ToLower(strings.TrimRight(id, "/")); retrieve &&
		strings.Count(lower, "/") >= 2 && strings.HasSuffix(lower, vendorEndpointsSuffix) {
		fail(errNotFound)
		return
	}
	if r.Method != http.MethodGet {
		fail(errMethod)
		return
	}
	doc, keyLookup, modelLookup := s.store.Current()
	if !doc.ServiceEnabled {
		fail(errServiceDisabled)
		return
	}
	// An Anthropic client reads its key the way /v1/messages does, x-api-key
	// included, so the SDK's model listing works with the same configuration
	// as its requests.
	readToken := bearerToken
	if anthropic {
		readToken = messagesToken
	}
	token, authErr := readToken(r)
	var key *snapshot.Key
	if authErr == nil {
		key, authErr = s.authenticateToken(token, keyLookup)
	}
	if authErr != nil {
		fail(*authErr)
		return
	}
	catalog := s.catalog.forRef(doc.PassthroughRef)

	// GET /v1/models/{id}: single-model retrieve, which several SDK helpers
	// call. A missing or not-allowed id is a 404, never a disclosure of a
	// restricted model's existence. The money fence applies here too, and it
	// answers 404 rather than 403 like chat does: this surface already refuses
	// to confirm that a model it will not serve exists, and a restricted key
	// learning which names are real would undo that.
	if retrieve {
		m := servableModel(&doc, key, modelLookup, id)
		if m == nil {
			fail(errModelNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		// A passthrough name the catalog knows is described by it; any other
		// one keeps the plain entry.
		var v *vendorModel
		if catalog != nil && modelLookup(id) == nil {
			v = catalog.byID[id]
		}
		switch {
		case anthropic && v != nil:
			writeJSON(w, anthropicVendorEntryFor(v))
		case anthropic:
			writeJSON(w, anthropicEntryFor(m))
		case v != nil:
			writeJSON(w, vendorEntryFor(v))
		default:
			writeJSON(w, entryFor(m))
		}
		return
	}

	var models []*snapshot.Model
	for i := range doc.Models {
		m := &doc.Models[i]
		if !key.AllowsModel(m) || !key.ServesCreditModel(m) {
			continue
		}
		models = append(models, m)
	}
	// Paid models follow the catalogued ones, in name order. A name the
	// snapshot catalogues is listed once, by its own entry above.
	var paid []*vendorModel
	if catalog != nil {
		for i := range catalog.models {
			v := &catalog.models[i]
			if modelLookup(v.ID) != nil || servableModel(&doc, key, modelLookup, v.ID) == nil || !listableFor(key, v) {
				continue
			}
			paid = append(paid, v)
		}
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	if anthropic {
		// One page holds the whole list, so there is never a next one; the
		// two ids are the page's ends, null on an empty page.
		page := struct {
			Data    []anthropicModelEntry `json:"data"`
			HasMore bool                  `json:"has_more"`
			FirstID *string               `json:"first_id"`
			LastID  *string               `json:"last_id"`
		}{Data: make([]anthropicModelEntry, 0, len(models)+len(paid))}
		for _, m := range models {
			page.Data = append(page.Data, anthropicEntryFor(m))
		}
		for _, v := range paid {
			page.Data = append(page.Data, anthropicVendorEntryFor(v))
		}
		if n := len(page.Data); n > 0 {
			page.FirstID, page.LastID = &page.Data[0].ID, &page.Data[n-1].ID
		}
		writeJSON(w, page)
		return
	}
	list := struct {
		Object string       `json:"object"`
		Data   []modelEntry `json:"data"`
	}{Object: "list", Data: make([]modelEntry, 0, len(models)+len(paid))}
	for _, m := range models {
		list.Data = append(list.Data, entryFor(m))
	}
	for _, v := range paid {
		list.Data = append(list.Data, vendorEntryFor(v))
	}
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

// SpoolWriteFailures is how many usage events could not be written to the
// outbox. Reported to the control plane, which cannot otherwise tell quiet
// accounting loss from a quiet week.
func (s *Server) SpoolWriteFailures() int64 { return s.metrics.spoolWriteFailures.Load() }

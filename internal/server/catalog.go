package server

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pnuops/pickle-llm-gateway/internal/snapshot"
)

// The passthrough upstream serves any name its own catalog carries, and the
// snapshot does not enumerate that catalog. So the models list would show only
// the self-served and catalogued models while chat, and the single-model
// retrieve, answer for every paid name a key may reach. To make the list
// answer the same question, the gateway keeps its own copy of the vendor's
// public model catalog and filters it per key with the retrieve's predicate.
//
// The catalog is public and unauthenticated, so no credential is ever sent
// for it: an authenticated read could come back filtered by one account's
// settings, which is not what any key should be shown.
//
// Load and staleness: one GET every vendorCatalogInterval. Measured
// 2026-10-21, the full catalog (every output modality) is 686 models and about
// 1.0 MB, served in 0.16 s from the vendor's CDN with max-age=120. That is 48
// reads a day, against the 288 the upstream probe already makes. The cost of
// a stale copy is small and one-sided: a new model is callable before it is
// listed, and a retired one stays listed until the next read, then fails at
// the vendor as it would have anyway.
//
// Failure keeps the last good copy, however old: a day-old catalog is nearly
// right, while an empty one would silently shrink every list. Before the
// first success there is no copy, and the list is what it was before this
// existed — the snapshot's models only.

// vendorCatalogInterval is the wait between successful reads.
const vendorCatalogInterval = 30 * time.Minute

// vendorCatalogRetry is the first wait after a failed read; it doubles on
// each further failure up to vendorCatalogInterval.
const vendorCatalogRetry = time.Minute

// vendorCatalogTimeout bounds one read, body included.
const vendorCatalogTimeout = 30 * time.Second

// vendorCatalogMaxBytes bounds one catalog body: sixteen times the measured
// size, so ordinary growth does not turn into a stuck copy.
const vendorCatalogMaxBytes = 16 << 20

// vendorCatalogQuery asks for every output modality. Without it the vendor
// lists text models only, which leaves out embedding, image, speech and
// rerank models the passthrough routes serve.
const vendorCatalogQuery = "/models?output_modalities=all"

// vendorModel is the part of one catalog entry the list shows: the standard
// fields of the two list shapes and the context length. Price, modalities and
// supported parameters are the vendor's own extensions and are not carried.
type vendorModel struct {
	ID            string
	Name          string
	Created       int64
	ContextLength int
	MaxOutput     int
	// Outputs are the catalog's output modalities, lower-cased. They decide
	// which grant a key needs to see the model; see listableFor.
	Outputs []string
}

// vendorCatalog is one successful read, for one upstream.
type vendorCatalog struct {
	ref       string
	models    []vendorModel // sorted by id
	byID      map[string]*vendorModel
	fetchedAt time.Time
}

type catalogState struct {
	current  atomic.Pointer[vendorCatalog]
	failures atomic.Int64 // consecutive failed reads
	interval time.Duration
	retry    time.Duration
	maxBytes int64
}

func newCatalogState() *catalogState {
	return &catalogState{interval: vendorCatalogInterval, retry: vendorCatalogRetry, maxBytes: vendorCatalogMaxBytes}
}

// forRef is the copy to list from when ref is the passthrough upstream, or
// nil when there is none for it. A copy read from a different upstream is
// never used: the snapshot can move passthrough elsewhere.
func (c *catalogState) forRef(ref string) *vendorCatalog {
	cat := c.current.Load()
	if cat == nil || ref == "" || cat.ref != strings.ToLower(ref) {
		return nil
	}
	return cat
}

// RunVendorCatalog starts the catalog loop. It wakes every retry period and
// reads only when a read is due: there is no copy for the current passthrough
// upstream, or the copy is an interval old, and no failure backoff is still
// running. Waking without reading costs nothing, and it is what lets a
// snapshot that moves passthrough to another upstream get its catalog within
// a minute rather than at the old copy's next refresh. The first read happens
// at once, so a restarted gateway lists paid models again within a request or
// two.
func (s *Server) RunVendorCatalog(ctx context.Context) {
	go func() {
		timer := time.NewTimer(0)
		defer timer.Stop()
		backoff := s.catalog.retry
		var notBefore time.Time
		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
			}
			timer.Reset(s.catalog.retry)
			now := time.Now()
			if now.Before(notBefore) || !s.vendorCatalogDue() {
				continue
			}
			switch s.refreshVendorCatalog(ctx) {
			case catalogRead:
				backoff = s.catalog.retry
				notBefore = time.Time{}
			case catalogFailed:
				notBefore = now.Add(backoff)
				backoff = min(backoff*2, s.catalog.interval)
			}
		}
	}()
}

// vendorCatalogDue reports whether the copy needs reading for the current
// passthrough upstream.
func (s *Server) vendorCatalogDue() bool {
	doc, _, _ := s.store.Current()
	if doc.PassthroughRef == "" {
		return false
	}
	cat := s.catalog.forRef(doc.PassthroughRef)
	return cat == nil || s.now().Sub(cat.fetchedAt) >= s.catalog.interval
}

type catalogOutcome int

const (
	catalogRead    catalogOutcome = iota
	catalogFailed                 // counted, and backed off
	catalogSkipped                // nothing to read, or shutting down
)

// refreshVendorCatalog makes one read for the current passthrough upstream.
// With no passthrough upstream there is nothing to read, which is not a
// failure, and neither is a read cut short by shutdown.
func (s *Server) refreshVendorCatalog(parent context.Context) catalogOutcome {
	doc, _, _ := s.store.Current()
	ref := strings.ToLower(doc.PassthroughRef)
	if ref == "" {
		return catalogSkipped
	}
	up, ok := s.cfg.Upstreams[ref]
	if !ok {
		return catalogSkipped
	}
	ctx, cancel := context.WithTimeout(parent, vendorCatalogTimeout)
	defer cancel()
	models, err := s.fetchVendorCatalog(ctx, up.BaseURL)
	if err != "" {
		if parent.Err() != nil {
			return catalogSkipped
		}
		n := s.catalog.failures.Add(1)
		s.log.Warn("vendor model catalog read failed", "upstream", ref, "reason", err, "consecutiveFailures", n)
		return catalogFailed
	}
	byID := make(map[string]*vendorModel, len(models))
	for i := range models {
		byID[models[i].ID] = &models[i]
	}
	s.catalog.current.Store(&vendorCatalog{ref: ref, models: models, byID: byID, fetchedAt: s.now()})
	s.catalog.failures.Store(0)
	return catalogRead
}

// fetchVendorCatalog reads and parses the catalog, returning a reason on
// failure. An empty catalog is a failure: the vendor never has none, so it
// is more likely a fault than a fact, and keeping the last copy is safer.
func (s *Server) fetchVendorCatalog(ctx context.Context, baseURL string) ([]vendorModel, string) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+vendorCatalogQuery, http.NoBody)
	if err != nil {
		return nil, "invalid_request"
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return nil, "transport"
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		return nil, "status_" + http.StatusText(resp.StatusCode)
	}
	limited := &io.LimitedReader{R: resp.Body, N: s.catalog.maxBytes + 1}
	var body struct {
		Data []struct {
			ID   string `json:"id"`
			Name string `json:"name"`
			// Numbers are read as floats so that one entry written as 1.28e5
			// does not fail the whole read and freeze the copy.
			Created       float64 `json:"created"`
			ContextLength float64 `json:"context_length"`
			TopProvider   struct {
				MaxCompletionTokens float64 `json:"max_completion_tokens"`
			} `json:"top_provider"`
			Architecture struct {
				OutputModalities []string `json:"output_modalities"`
			} `json:"architecture"`
		} `json:"data"`
	}
	if err := json.NewDecoder(limited).Decode(&body); err != nil {
		if limited.N <= 0 {
			return nil, "too_large"
		}
		return nil, "invalid_body"
	}
	seen := make(map[string]bool, len(body.Data))
	models := make([]vendorModel, 0, len(body.Data))
	for _, e := range body.Data {
		if e.ID == "" || seen[e.ID] {
			continue
		}
		seen[e.ID] = true
		outputs := make([]string, 0, len(e.Architecture.OutputModalities))
		for _, o := range e.Architecture.OutputModalities {
			outputs = append(outputs, strings.ToLower(strings.TrimSpace(o)))
		}
		models = append(models, vendorModel{
			ID: e.ID, Name: e.Name, Created: wholeNumber(e.Created),
			ContextLength: int(wholeNumber(e.ContextLength)), MaxOutput: int(wholeNumber(e.TopProvider.MaxCompletionTokens)),
			Outputs: outputs,
		})
	}
	if len(models) == 0 {
		return nil, "empty"
	}
	sort.Slice(models, func(i, j int) bool { return models[i].ID < models[j].ID })
	return models, ""
}

// wholeNumber is a catalog count as a non-negative integer; anything out of
// range reads as unknown (0), which the entries leave out.
func wholeNumber(f float64) int64 {
	if f < 0 || f > 1<<40 || f != f {
		return 0
	}
	return int64(f)
}

// outputCapability is the grant each output modality needs before a model
// producing it is listed. The list is meant to hold what this key can call
// and get an answer from, and a model is reached through the route its output
// belongs to, which is gated by that route's grant. A model producing several
// kinds is listed when any one of them is granted: a text-and-image model is
// usable as a chat model by a key holding chat alone.
//
// An output this table does not know
// (video, the vendor's routing "decisions") is never listed, for the same
// reason.
var outputCapability = map[string]string{
	"text":          snapshot.EndpointChat,
	"image":         snapshot.EndpointImages,
	"embeddings":    snapshot.EndpointEmbeddings,
	"rerank":        snapshot.EndpointRerank,
	"speech":        snapshot.EndpointSpeech,
	"transcription": snapshot.EndpointTranscriptions,
}

// freeVariantSuffix marks the vendor's no-cost variants. They are refused for
// the platform's account by its data policy, so listing them would offer a
// name that always fails.
const freeVariantSuffix = ":free"

// listableFor reports whether the list shows one catalog model to this key,
// past what the retrieve's predicate already decided. The retrieve keeps
// answering for these names, because a direct call reaches the vendor either
// way; the list is narrower on purpose, holding only what is worth calling.
func listableFor(key *snapshot.Key, m *vendorModel) bool {
	lower := strings.ToLower(m.ID)
	// Routers are not supported: what they resolve to is chosen after every
	// fence has run.
	if snapshot.IsRouterModelName(lower) || strings.HasSuffix(lower, freeVariantSuffix) {
		return false
	}
	for _, out := range m.Outputs {
		if capability, ok := outputCapability[out]; ok && key.AllowsEndpoint(capability) {
			return true
		}
	}
	return false
}

// catalogGauges is what the metrics surface shows about the copy.
func (s *Server) catalogGauges() map[string]any {
	out := map[string]any{"consecutiveFailures": s.catalog.failures.Load(), "models": 0}
	if cat := s.catalog.current.Load(); cat != nil {
		out["models"] = len(cat.models)
		out["ageSeconds"] = int64(s.now().Sub(cat.fetchedAt).Seconds())
	}
	return out
}

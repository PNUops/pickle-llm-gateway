package server

import (
	"context"
	"encoding/json"
	"hash/fnv"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/pnuops/pickle-llm-gateway/internal/config"
	"github.com/pnuops/pickle-llm-gateway/internal/snapshot"
)

const (
	upstreamObservationFormat = 1
	probeTimeout              = 5 * time.Second
	maxProbeBodyBytes         = 4 << 20
	maxMissingPublicModels    = 20
)

type probeResult struct {
	status      string
	failureType string
	latency     time.Duration
	modelIDs    map[string]bool
	modelCount  int
	catalog     snapshot.CatalogObservation
}

// RunUpstreamProbes starts one independent read-only loop per configured
// upstream. Probe results never touch upstreamHealth's routing failure and
// cooldown maps; an observation cannot reorder real user traffic.
func (s *Server) RunUpstreamProbes(ctx context.Context) {
	for _, up := range s.cfg.Upstreams {
		up := up
		go s.runProbeLoop(ctx, up)
	}
}

func (s *Server) runProbeLoop(ctx context.Context, up config.Upstream) {
	// Spread refs within the first tenth of their interval. The hash is stable,
	// so restarts preserve the distribution instead of producing a burst.
	jitterWindow := up.ProbeInterval / 10
	if jitterWindow <= 0 {
		jitterWindow = time.Millisecond
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(up.Ref))
	initialDelay := time.Duration(h.Sum64() % uint64(jitterWindow))
	timer := time.NewTimer(initialDelay)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			s.probeOnce(ctx, up)
			timer.Reset(up.ProbeInterval)
		}
	}
}

func (s *Server) probeOnce(parent context.Context, up config.Upstream) {
	ctx, cancel := context.WithTimeout(parent, probeTimeout)
	defer cancel()
	attemptedAt := s.now()
	started := time.Now()
	result := s.fetchModels(ctx, up)
	result.latency = time.Since(started)
	if result.status == activeOK {
		result.catalog = s.compareCatalog(up.Ref, result.modelIDs)
	}
	s.health.recordProbe(up.Ref, attemptedAt.Add(result.latency), result)
}

func (s *Server) fetchModels(ctx context.Context, up config.Upstream) probeResult {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, up.BaseURL+"/models", nil)
	if err != nil {
		return probeResult{status: activeFailed, failureType: failureInvalidResponse}
	}
	if up.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+up.APIKey)
	}
	resp, err := s.client.Do(req)
	if err != nil {
		kind := failureTransport
		if isTimeout(err) || ctx.Err() == context.DeadlineExceeded {
			kind = failureTimeout
		}
		return probeResult{status: activeFailed, failureType: kind}
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		return probeResult{status: activeAuthUnverified}
	}
	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		kind := failureVendorRejected
		switch {
		case resp.StatusCode == http.StatusTooManyRequests:
			kind = failureThrottled
		case resp.StatusCode >= http.StatusInternalServerError:
			kind = failureUpstream
		}
		return probeResult{status: activeFailed, failureType: kind}
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxProbeBodyBytes+1))
	if err != nil || len(raw) > maxProbeBodyBytes {
		return probeResult{status: activeFailed, failureType: failureInvalidResponse}
	}
	var body struct {
		Data []struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &body); err != nil || body.Data == nil {
		return probeResult{status: activeFailed, failureType: failureInvalidResponse}
	}
	ids := make(map[string]bool, len(body.Data))
	for _, model := range body.Data {
		if model.ID != "" {
			ids[model.ID] = true
		}
	}
	return probeResult{status: activeOK, modelIDs: ids, modelCount: len(ids)}
}

type expectedModel struct {
	publicName    string
	upstreamModel string
}

func (s *Server) compareCatalog(ref string, actual map[string]bool) snapshot.CatalogObservation {
	doc, _, _ := s.store.Current()
	ref = strings.ToLower(ref)
	passthrough := strings.ToLower(doc.PassthroughRef) == ref
	expected := make([]expectedModel, 0)
	seen := map[string]bool{}
	for _, model := range doc.Models {
		if model.UpstreamRef != ref && model.FallbackRef != ref {
			continue
		}
		key := model.PublicName + "\x00" + model.UpstreamModel
		if seen[key] {
			continue
		}
		seen[key] = true
		expected = append(expected, expectedModel{publicName: model.PublicName, upstreamModel: model.UpstreamModel})
	}
	if len(expected) == 0 {
		return snapshot.CatalogObservation{Status: catalogNotApplicable}
	}
	sort.Slice(expected, func(i, j int) bool { return expected[i].publicName < expected[j].publicName })
	missing := make([]string, 0)
	missingCount := 0
	expectedIDs := make(map[string]bool, len(expected))
	for _, model := range expected {
		expectedIDs[model.upstreamModel] = true
		if actual[model.upstreamModel] {
			continue
		}
		missingCount++
		if len(missing) < maxMissingPublicModels {
			missing = append(missing, model.publicName)
		}
	}
	unexpectedCount := 0
	if !passthrough {
		for modelID := range actual {
			if !expectedIDs[modelID] {
				unexpectedCount++
			}
		}
	}
	status := catalogMatch
	if missingCount > 0 || unexpectedCount > 0 {
		status = catalogMismatch
	}
	return snapshot.CatalogObservation{
		Status:               status,
		ExpectedModelCount:   len(expected),
		MissingModelCount:    missingCount,
		UnexpectedModelCount: unexpectedCount,
		MissingPublicModels:  missing,
	}
}

func (h *upstreamHealth) recordProbe(ref string, at time.Time, result probeResult) {
	h.mu.Lock()
	defer h.mu.Unlock()
	o := h.observation(ref)
	o.active.Status = result.status
	o.active.LastAttemptAt = at
	o.active.LatencyMs = result.latency.Milliseconds()
	if result.status == activeOK {
		o.active.LastSuccessAt = at
		o.active.ConsecutiveFailures = 0
		o.active.ModelCount = result.modelCount
		o.catalog = result.catalog
		return
	}
	if result.status == activeAuthUnverified {
		// Reachability succeeded, but /models did not. It therefore clears the
		// down-streak without pretending that a catalog observation succeeded;
		// LastSuccessAt continues to name the last complete 200 response.
		o.active.ConsecutiveFailures = 0
		return
	}
	o.active.LastFailureAt = at
	o.active.LastFailureType = result.failureType
	o.active.ConsecutiveFailures++
}

// UpstreamObservations returns a stable, ref-sorted copy for the control-plane
// self-report. Configured refs appear even before their first passive request
// or active probe, which is how the api distinguishes UNKNOWN from absent.
func (s *Server) UpstreamObservations() (int, []snapshot.UpstreamObservation) {
	refs := s.cfg.UpstreamRefs()
	sort.Strings(refs)
	s.health.mu.Lock()
	defer s.health.mu.Unlock()
	result := make([]snapshot.UpstreamObservation, 0, len(refs))
	for _, ref := range refs {
		o := s.health.observation(ref)
		passive := o.passive
		if until, ok := s.health.coolUntil[ref]; ok {
			if s.health.now().Before(until) {
				passive.CooldownUntil = until
			} else {
				delete(s.health.coolUntil, ref)
			}
		}
		catalog := o.catalog
		catalog.MissingPublicModels = append([]string(nil), catalog.MissingPublicModels...)
		active := o.active
		active.IntervalSeconds = int64(s.cfg.Upstreams[ref].ProbeInterval / time.Second)
		result = append(result, snapshot.UpstreamObservation{
			Ref: ref, Passive: passive, Active: active, Catalog: catalog,
		})
	}
	return upstreamObservationFormat, result
}

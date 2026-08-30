package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pnuops/pickle-llm-gateway/internal/config"
	"github.com/pnuops/pickle-llm-gateway/internal/limits"
	"github.com/pnuops/pickle-llm-gateway/internal/snapshot"
	"github.com/pnuops/pickle-llm-gateway/internal/spool"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func observationServer(t *testing.T, models string) *Server {
	t.Helper()
	dir := t.TempDir()
	path := dir + "/snapshot.json"
	doc := fmt.Sprintf(`{"generation":1,"serviceEnabled":true,"passthroughRef":"external","models":%s,"keys":[]}`, models)
	if err := os.WriteFile(path, []byte(doc), 0o600); err != nil {
		t.Fatal(err)
	}
	store, err := snapshot.OpenFile(path, slog.New(slog.DiscardHandler), snapshot.Options{
		KnownUpstreams: []string{"external", "local"},
	})
	if err != nil {
		t.Fatal(err)
	}
	sp, err := spool.Open(dir + "/spool")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sp.Close() })
	cfg := &config.Config{
		MaxInFlight: 4, UpstreamHeaderWait: time.Second,
		DefaultRpm: 1, DefaultTpm: 1, DefaultConcurrency: 1,
		Upstreams: map[string]config.Upstream{
			"external": {Ref: "external", BaseURL: "https://external.example/v1", ProbeInterval: 5 * time.Minute},
			"local":    {Ref: "local", BaseURL: "http://127.0.0.1:8000/v1", ProbeInterval: time.Minute},
		},
	}
	return New(cfg, store, limits.New(nil), sp, slog.New(slog.DiscardHandler))
}

func TestActiveProbeKeepsLastSuccessAcrossFailureAndDoesNotCoolRouting(t *testing.T) {
	models := `[{"publicName":"pickle-a","upstreamRef":"local","upstreamModel":"model-a"},` +
		`{"publicName":"pickle-b","upstreamRef":"local","upstreamModel":"model-b"}]`
	s := observationServer(t, models)
	now := time.Date(2026, 8, 30, 1, 2, 3, 0, time.UTC)
	s.now = func() time.Time { return now }
	s.health.now = s.now
	s.client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(
			`{"data":[{"id":"model-a"},{"id":"extra"}]}`)), Header: make(http.Header)}, nil
	})}
	s.probeOnce(context.Background(), s.cfg.Upstreams["local"])

	_, got := s.UpstreamObservations()
	local := got[1]
	if local.Ref != "local" || local.Active.Status != activeOK || local.Active.LastSuccessAt.IsZero() ||
		local.Active.IntervalSeconds != 60 {
		t.Fatalf("successful probe not reported: %+v", local)
	}
	if local.Active.ModelCount != 2 || local.Catalog.Status != catalogMismatch ||
		local.Catalog.MissingModelCount != 1 || local.Catalog.UnexpectedModelCount != 1 {
		t.Fatalf("catalog mismatch not reported: %+v", local)
	}
	if len(local.Catalog.MissingPublicModels) != 1 || local.Catalog.MissingPublicModels[0] != "pickle-b" {
		t.Fatalf("missing public models = %v", local.Catalog.MissingPublicModels)
	}
	lastSuccess := local.Active.LastSuccessAt

	now = now.Add(time.Minute)
	s.client = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("network down")
	})}
	for range 3 {
		s.probeOnce(context.Background(), s.cfg.Upstreams["local"])
	}
	_, got = s.UpstreamObservations()
	local = got[1]
	if local.Active.LastSuccessAt != lastSuccess || local.Active.LastFailureAt.IsZero() {
		t.Fatalf("failure erased the last good observation: %+v", local.Active)
	}
	if local.Active.ConsecutiveFailures != 3 || local.Active.LastFailureType != failureTransport {
		t.Fatalf("active failure state = %+v", local.Active)
	}
	if s.health.cooling("local") {
		t.Fatal("active probe failure changed real-request routing cooldown")
	}

	now = now.Add(time.Minute)
	s.client = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(
			`{"data":[{"id":"model-a"},{"id":"model-b"}]}`)), Header: make(http.Header)}, nil
	})}
	s.probeOnce(context.Background(), s.cfg.Upstreams["local"])
	_, got = s.UpstreamObservations()
	local = got[1]
	if local.Active.Status != activeOK || local.Active.ConsecutiveFailures != 0 ||
		local.Active.LastSuccessAt.Before(now) {
		t.Fatalf("successful probe did not recover active state: %+v", local.Active)
	}
}

func TestProbeFailureTaxonomy(t *testing.T) {
	tests := []struct {
		name string
		do   roundTripFunc
		want string
	}{
		{name: "timeout", do: func(*http.Request) (*http.Response, error) {
			return nil, context.DeadlineExceeded
		}, want: failureTimeout},
		{name: "throttled", do: func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusTooManyRequests,
				Body: io.NopCloser(strings.NewReader("slow")), Header: make(http.Header)}, nil
		}, want: failureThrottled},
		{name: "server error", do: func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusBadGateway,
				Body: io.NopCloser(strings.NewReader("down")), Header: make(http.Header)}, nil
		}, want: failureUpstream},
		{name: "invalid json", do: func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusOK,
				Body: io.NopCloser(strings.NewReader("not-json")), Header: make(http.Header)}, nil
		}, want: failureInvalidResponse},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := observationServer(t, `[]`)
			s.client = &http.Client{Transport: tt.do}
			s.probeOnce(context.Background(), s.cfg.Upstreams["local"])
			_, got := s.UpstreamObservations()
			if got[1].Active.Status != activeFailed ||
				got[1].Active.LastFailureType != tt.want ||
				got[1].Active.ConsecutiveFailures != 1 {
				t.Fatalf("probe result = %+v, want %s", got[1].Active, tt.want)
			}
		})
	}
}

func TestProbeLoopHonorsIntervalAndStopsWithContext(t *testing.T) {
	s := observationServer(t, `[]`)
	up := s.cfg.Upstreams["local"]
	up.ProbeInterval = 20 * time.Millisecond
	var calls atomic.Int64
	s.client = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		calls.Add(1)
		return &http.Response{StatusCode: http.StatusOK,
			Body: io.NopCloser(strings.NewReader(`{"data":[]}`)), Header: make(http.Header)}, nil
	})}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		s.runProbeLoop(ctx, up)
		close(done)
	}()
	deadline := time.Now().Add(time.Second)
	for calls.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if calls.Load() < 2 {
		cancel()
		<-done
		t.Fatalf("probe loop calls = %d, want at least 2", calls.Load())
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("probe loop did not stop after context cancellation")
	}
	stoppedAt := calls.Load()
	time.Sleep(2 * up.ProbeInterval)
	if calls.Load() != stoppedAt {
		t.Fatalf("probe loop continued after cancellation: %d -> %d", stoppedAt, calls.Load())
	}
}

func TestProbeTreatsAuthRefusalAsReachableAndPassthroughAsNotApplicable(t *testing.T) {
	s := observationServer(t, `[]`)
	s.client = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusUnauthorized, Body: io.NopCloser(strings.NewReader("no")), Header: make(http.Header)}, nil
	})}
	s.probeOnce(context.Background(), s.cfg.Upstreams["external"])
	_, got := s.UpstreamObservations()
	if got[0].Ref != "external" || got[0].Active.Status != activeAuthUnverified || got[0].Active.LastAttemptAt.IsZero() {
		t.Fatalf("reachable auth refusal reported as down: %+v", got[0])
	}
	if !got[0].Active.LastSuccessAt.IsZero() {
		t.Fatalf("auth-only reachability was reported as a complete /models success: %+v", got[0].Active)
	}
	if got[0].Catalog.Status != catalogUnknown {
		t.Fatalf("catalog was invented without a model list: %+v", got[0].Catalog)
	}

	s.client = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{"data":[]}`)), Header: make(http.Header)}, nil
	})}
	s.probeOnce(context.Background(), s.cfg.Upstreams["external"])
	_, got = s.UpstreamObservations()
	if got[0].Catalog.Status != catalogNotApplicable {
		t.Fatalf("passthrough-only catalog = %+v", got[0].Catalog)
	}
}

func TestCatalogMismatchBoundsMissingPublicNames(t *testing.T) {
	var models strings.Builder
	models.WriteByte('[')
	for i := range 25 {
		if i > 0 {
			models.WriteByte(',')
		}
		fmt.Fprintf(&models, `{"publicName":"pickle-%02d","upstreamRef":"local","upstreamModel":"model-%02d"}`, i, i)
	}
	models.WriteByte(']')
	s := observationServer(t, models.String())
	got := s.compareCatalog("local", map[string]bool{})
	if got.MissingModelCount != 25 || len(got.MissingPublicModels) != maxMissingPublicModels {
		t.Fatalf("bounded mismatch = %+v", got)
	}
	if got.MissingPublicModels[0] != "pickle-00" || got.MissingPublicModels[19] != "pickle-19" {
		t.Fatalf("missing names are not stable and sorted: %v", got.MissingPublicModels)
	}
}

func TestCuratedCatalogFlagsExtrasButPassthroughUsesSubsetSemantics(t *testing.T) {
	models := `[{"publicName":"pickle-a","upstreamRef":"local",` +
		`"upstreamModel":"model-a","fallbackRef":"external"}]`
	s := observationServer(t, models)

	curated := s.compareCatalog("local", map[string]bool{"model-a": true, "extra": true})
	if curated.Status != catalogMismatch || curated.MissingModelCount != 0 ||
		curated.UnexpectedModelCount != 1 {
		t.Fatalf("curated extra model was not mismatch: %+v", curated)
	}

	passthrough := s.compareCatalog("external", map[string]bool{"model-a": true, "extra": true})
	if passthrough.Status != catalogMatch || passthrough.MissingModelCount != 0 ||
		passthrough.UnexpectedModelCount != 0 {
		t.Fatalf("passthrough extras should be ignored: %+v", passthrough)
	}
}

func TestPassiveObservationRetainsFailureAndCooldownSeparately(t *testing.T) {
	now := time.Date(2026, 8, 30, 0, 0, 0, 0, time.UTC)
	h := newUpstreamHealth(func() time.Time { return now })
	h.recordAttempt("local")
	h.recordPassiveFailure("local", failureUpstream)
	for range 3 {
		h.recordFailure("local")
	}
	o := h.observation("local").passive
	if o.ConsecutiveFailures != 1 || o.LastFailureType != failureUpstream {
		t.Fatalf("passive observation = %+v", o)
	}
	if !h.cooling("local") {
		t.Fatal("routing failure threshold did not create cooldown")
	}
	now = now.Add(time.Second)
	h.recordAttempt("local")
	h.recordSuccess("local")
	o = h.observation("local").passive
	if o.ConsecutiveFailures != 0 || o.LastSuccessAt.IsZero() || h.cooling("local") {
		t.Fatalf("success did not recover passive state: %+v", o)
	}
}

func TestKeyLocalFailuresDoNotBuildSharedAvailabilityStreak(t *testing.T) {
	now := time.Date(2026, 8, 30, 0, 0, 0, 0, time.UTC)
	h := newUpstreamHealth(func() time.Time { return now })
	for _, kind := range []string{
		failureRequestRejected, failureCreditExhausted,
		failureKeyCredential, failureKeyThrottled, failureClientCanceled,
	} {
		h.recordAttempt("external")
		h.recordPassiveFailure("external", kind)
		now = now.Add(time.Second)
	}
	o := h.observation("external").passive
	if o.ConsecutiveFailures != 0 || o.LastFailureType != "" || !o.LastFailureAt.IsZero() {
		t.Fatalf("key-local failures polluted shared availability: %+v", o)
	}
}

func TestCanceledClientDoesNotBuildPassiveFailureStreak(t *testing.T) {
	s := observationServer(t, `[]`)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, ae := s.attempt(ctx, s.cfg.Upstreams["local"], []byte(`{}`), "", false)
	if ae == nil || ae.kind != failureClientCanceled {
		t.Fatalf("canceled attempt = %+v, want %s", ae, failureClientCanceled)
	}
	s.health.recordAttempt("local")
	s.health.recordPassiveFailure("local", ae.kind)
	o := s.health.observation("local").passive
	if o.ConsecutiveFailures != 0 || !o.LastFailureAt.IsZero() {
		t.Fatalf("client cancellation polluted passive health: %+v", o)
	}
}

func TestInvalidUpstreamURLHasBoundedFailureKind(t *testing.T) {
	s := observationServer(t, `[]`)
	up := s.cfg.Upstreams["local"]
	up.BaseURL = "://invalid"
	_, ae := s.attempt(context.Background(), up, []byte(`{}`), "", false)
	if ae == nil || ae.kind != failureInvalidResponse {
		t.Fatalf("invalid URL attempt = %+v, want %s", ae, failureInvalidResponse)
	}
}

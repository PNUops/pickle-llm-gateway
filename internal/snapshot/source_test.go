package snapshot

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// controlPlane is a stand-in for the api side of the sync link: it answers the
// bare generation when the caller is current, and the whole document when it
// is not — the omit-when-unchanged convention the real link will follow.
type controlPlane struct {
	mu       sync.Mutex
	doc      string // full document served when the caller is behind
	gen      int64
	status   int // non-200 to simulate a refusing control plane
	requests []SyncRequest
	authSeen []string
}

func (c *controlPlane) serve(w http.ResponseWriter, r *http.Request) {
	var req SyncRequest
	_ = json.NewDecoder(r.Body).Decode(&req)
	c.mu.Lock()
	c.requests = append(c.requests, req)
	c.authSeen = append(c.authSeen, r.Header.Get("Authorization"))
	doc, gen, status := c.doc, c.gen, c.status
	c.mu.Unlock()

	if status != 0 && status != http.StatusOK {
		w.WriteHeader(status)
		_, _ = w.Write([]byte("refused"))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if req.AppliedGeneration == gen {
		fmt.Fprintf(w, `{"generation":%d}`, gen)
		return
	}
	_, _ = w.Write([]byte(doc))
}

func (c *controlPlane) set(f func(*controlPlane)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	f(c)
}

func (c *controlPlane) lastRequest(t *testing.T) SyncRequest {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.requests) == 0 {
		t.Fatal("control plane was never called")
	}
	return c.requests[len(c.requests)-1]
}

func docAt(gen int64, hash string) string {
	return fmt.Sprintf(`{"generation":%d,"serviceEnabled":true,`+
		`"models":[{"publicName":"pickle-general","upstreamRef":"mock","upstreamModel":"m"}],`+
		`"keys":[{"keyId":"k","tokenHash":"%s","status":"ACTIVE","limits":{}}]}`, gen, hash)
}

func newControlPlane(t *testing.T, gen int64) (*controlPlane, string) {
	t.Helper()
	cp := &controlPlane{doc: docAt(gen, HashToken("pickle-x")), gen: gen}
	srv := httptest.NewServer(http.HandlerFunc(cp.serve))
	t.Cleanup(srv.Close)
	return cp, srv.URL
}

func TestHTTPSourceFetchesAndReportsUnchanged(t *testing.T) {
	cp, url := newControlPlane(t, 7)
	cache := filepath.Join(t.TempDir(), "snapshot.json")
	src := NewHTTPSource(url, "tok", cache, 5*time.Second)

	raw, changed, err := src.Load(context.Background(), 0)
	if err != nil || !changed || len(raw) == 0 {
		t.Fatalf("first load: changed=%v err=%v", changed, err)
	}
	if got := cp.lastRequest(t); got.AppliedGeneration != 0 || got.AgentVersion == "" {
		t.Fatalf("handshake did not carry the applied generation and version: %+v", got)
	}
	// Being current must cost a bare-generation response, not a document.
	_, changed, err = src.Load(context.Background(), 7)
	if err != nil || changed {
		t.Fatalf("current caller was sent a document: changed=%v err=%v", changed, err)
	}
	// The token rides the Authorization header.
	cp.mu.Lock()
	auth := cp.authSeen[0]
	cp.mu.Unlock()
	if auth != "Bearer tok" {
		t.Fatalf("auth header = %q", auth)
	}
}

func TestHTTPSourceCachesForRestart(t *testing.T) {
	cp, url := newControlPlane(t, 7)
	cache := filepath.Join(t.TempDir(), "snapshot.json")
	src := NewHTTPSource(url, "tok", cache, 5*time.Second)
	if _, _, err := src.Load(context.Background(), 0); err != nil {
		t.Fatal(err)
	}
	// Nothing is cached until the caller accepts: a document the store would
	// reject must not become what a later restart loads.
	if _, err := os.Stat(cache); err == nil {
		t.Fatal("document cached before it was accepted")
	}
	src.Accept()
	if _, err := os.Stat(cache); err != nil {
		t.Fatalf("accepted document was not cached: %v", err)
	}

	// A fresh process (new source) whose control plane refuses must still come
	// up on the cache rather than fail closed with no authorization at all.
	cp.set(func(c *controlPlane) { c.status = http.StatusServiceUnavailable })
	restarted := NewHTTPSource(url, "tok", cache, 5*time.Second)
	raw, changed, err := restarted.Load(context.Background(), 0)
	if err != nil || !changed || len(raw) == 0 {
		t.Fatalf("restart did not fall back to the cache: changed=%v err=%v", changed, err)
	}
	// But once it has served something, a later failure is an error — the
	// caller's in-memory state is fresher than the cache.
	if _, _, err := restarted.Load(context.Background(), 7); err == nil {
		t.Fatal("a running gateway swallowed a control-plane failure")
	}
}

func TestHTTPSourceRefusesNon200(t *testing.T) {
	cp, url := newControlPlane(t, 7)
	cp.set(func(c *controlPlane) { c.status = http.StatusUnauthorized })
	src := NewHTTPSource(url, "bad", filepath.Join(t.TempDir(), "snapshot.json"), 5*time.Second)
	if _, _, err := src.Load(context.Background(), 0); err == nil {
		t.Fatal("a 401 was accepted as a document")
	}
}

func TestStoreOverControlPlane(t *testing.T) {
	cp, url := newControlPlane(t, 7)
	dir := t.TempDir()
	cache := filepath.Join(dir, "snapshot.json")
	src := NewHTTPSource(url, "tok", cache, 5*time.Second)

	s, err := Open(context.Background(), src, cache, discard(),
		Options{KnownUpstreams: []string{"mock"}})
	if err != nil {
		t.Fatal(err)
	}
	if s.Generation() != 7 {
		t.Fatalf("generation = %d", s.Generation())
	}
	// A no-change poll leaves the state and the failure counter alone.
	s.Refresh(context.Background())
	if s.Generation() != 7 || s.ReloadFailures() != 0 {
		t.Fatalf("unchanged poll disturbed the state: gen=%d fails=%d", s.Generation(), s.ReloadFailures())
	}
	// A newer document is picked up.
	cp.set(func(c *controlPlane) { c.gen = 9; c.doc = docAt(9, HashToken("pickle-y")) })
	s.Refresh(context.Background())
	if s.Generation() != 9 {
		t.Fatalf("new generation not applied: %d", s.Generation())
	}
	// The high-water guard still holds over this transport.
	cp.set(func(c *controlPlane) { c.gen = 4; c.doc = docAt(4, HashToken("pickle-z")) })
	s.Refresh(context.Background())
	if s.Generation() != 9 {
		t.Fatalf("a rollback was served over the control plane: %d", s.Generation())
	}
	if s.ReloadFailures() == 0 {
		t.Fatal("the refused rollback was not counted as a failure")
	}
}

func TestFileSourceUnchangedDetection(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snapshot.json")
	writeDoc(t, path, docAt(1, HashToken("pickle-x")), time.Now().Add(-time.Hour))
	src := NewFileSource(path)

	if _, changed, err := src.Load(context.Background(), 0); err != nil || !changed {
		t.Fatalf("first load: changed=%v err=%v", changed, err)
	}
	src.Accept()
	if _, changed, err := src.Load(context.Background(), 1); err != nil || changed {
		t.Fatalf("untouched file reported a change: changed=%v err=%v", changed, err)
	}
	writeDoc(t, path, docAt(2, HashToken("pickle-y")), time.Now())
	if _, changed, err := src.Load(context.Background(), 1); err != nil || !changed {
		t.Fatalf("replaced file was not noticed: changed=%v err=%v", changed, err)
	}
	// Not accepted, so the same document is still on offer — which is what
	// keeps a rejection visible instead of reading as "unchanged".
	if _, changed, err := src.Load(context.Background(), 1); err != nil || !changed {
		t.Fatalf("an unaccepted document stopped being offered: changed=%v err=%v", changed, err)
	}
}

// A control plane that answers "unchanged" to a caller with no state leaves
// nothing to serve. Open must say so rather than hand back a Store whose every
// read dereferences nil.
func TestOpenRefusesWhenFirstLoadHasNothing(t *testing.T) {
	cp := &controlPlane{doc: docAt(0, HashToken("pickle-x")), gen: 0}
	srv := httptest.NewServer(http.HandlerFunc(cp.serve))
	defer srv.Close()
	cache := filepath.Join(t.TempDir(), "snapshot.json")
	src := NewHTTPSource(srv.URL, "tok", cache, 5*time.Second)

	// served=0 equals the control plane's generation, so it replies "unchanged".
	if _, err := Open(context.Background(), src, cache, discard(), Options{}); err == nil {
		t.Fatal("Open returned a store with no document")
	}
}

// A document the Store refuses must not end up in the restart cache, or the
// next restart loads it and the gateway refuses to start at all.
func TestRejectedDocumentIsNotCached(t *testing.T) {
	cp, url := newControlPlane(t, 7)
	dir := t.TempDir()
	cache := filepath.Join(dir, "snapshot.json")
	src := NewHTTPSource(url, "tok", cache, 5*time.Second)
	s, err := Open(context.Background(), src, cache, discard(), Options{KnownUpstreams: []string{"mock"}})
	if err != nil {
		t.Fatal(err)
	}
	good, err := os.ReadFile(cache)
	if err != nil {
		t.Fatalf("first document was not cached: %v", err)
	}

	// Now the control plane serves a rollback, which the guard refuses.
	cp.set(func(c *controlPlane) { c.gen = 3; c.doc = docAt(3, HashToken("pickle-y")) })
	s.Refresh(context.Background())
	if s.Generation() != 7 {
		t.Fatalf("rollback was applied: %d", s.Generation())
	}
	after, err := os.ReadFile(cache)
	if err != nil {
		t.Fatal(err)
	}
	if string(after) != string(good) {
		t.Fatal("a refused document overwrote the restart cache")
	}
}

// The reload-failure count is the only staleness signal on the health surface.
// A document that keeps failing must keep failing, not read as healthy after
// one poll — otherwise a revoked key keeps working with nothing to show it.
func TestBadDocumentKeepsCountingFailures(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snapshot.json")
	writeDoc(t, path, docAt(1, HashToken("pickle-x")), time.Now().Add(-time.Hour))
	s, err := OpenFile(path, discard(), Options{KnownUpstreams: []string{"mock"}})
	if err != nil {
		t.Fatal(err)
	}
	writeDoc(t, path, "{broken", time.Now())
	for i := range 3 {
		s.Refresh(context.Background())
		if got := s.ReloadFailures(); got != int64(i+1) {
			t.Fatalf("after %d refreshes the failure count is %d — the staleness signal reset", i+1, got)
		}
	}
	// Fixing the document clears it.
	writeDoc(t, path, docAt(2, HashToken("pickle-y")), time.Now().Add(time.Minute))
	s.Refresh(context.Background())
	if s.ReloadFailures() != 0 || s.Generation() != 2 {
		t.Fatalf("recovery did not clear the signal: fails=%d gen=%d", s.ReloadFailures(), s.Generation())
	}
}

func TestFallbackRefIsValidatedAtLoad(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snapshot.json")
	body := `{"generation":1,"serviceEnabled":true,` +
		`"models":[{"publicName":"pickle-general","upstreamRef":"mock","upstreamModel":"m","fallbackRef":"typo"}],"keys":[]}`
	writeDoc(t, path, body, time.Now())
	if _, err := OpenFile(path, discard(), Options{KnownUpstreams: []string{"mock"}}); err == nil {
		t.Fatal("a fallback naming an unconfigured upstream was accepted")
	}
	body2 := strings.Replace(body, `"fallbackRef":"typo"`, `"fallbackRef":"backup"`, 1)
	writeDoc(t, path, body2, time.Now())
	if _, err := OpenFile(path, discard(), Options{KnownUpstreams: []string{"mock", "backup"}}); err != nil {
		t.Fatalf("a configured fallback was rejected: %v", err)
	}
}

// Strictness follows the writer. A hand-maintained file must fail on a typo;
// a control-plane document must survive a member this gateway does not know,
// or every top-level addition becomes a gateway-before-api deployment.
func TestTopLevelStrictnessFollowsTheSource(t *testing.T) {
	withScopes := `{"generation":1,"serviceEnabled":true,` +
		`"scopes":[{"id":"ws-7","limits":{"rpm":100}}],` +
		`"models":[{"publicName":"pickle-general","upstreamRef":"mock","upstreamModel":"m"}],"keys":[]}`

	// File: an unknown member is a typo and must be caught.
	path := filepath.Join(t.TempDir(), "snapshot.json")
	writeDoc(t, path, withScopes, time.Now())
	if _, err := OpenFile(path, discard(), Options{KnownUpstreams: []string{"mock"}}); err == nil {
		t.Fatal("a hand-maintained file accepted an unknown top-level member")
	}
	// A real typo of a known member is caught for the same reason.
	writeDoc(t, path, `{"generation":1,"serviceEnable":true,"models":[],"keys":[]}`, time.Now())
	if _, err := OpenFile(path, discard(), Options{}); err == nil {
		t.Fatal("a misspelled member was silently ignored in a file")
	}

	// Control plane: the same document must load, ignoring what it does not know.
	cp := &controlPlane{doc: withScopes, gen: 1}
	srv := httptest.NewServer(http.HandlerFunc(cp.serve))
	defer srv.Close()
	cache := filepath.Join(t.TempDir(), "snapshot.json")
	src := NewHTTPSource(srv.URL, "tok", cache, 5*time.Second)
	s, err := Open(context.Background(), src, cache, discard(), Options{
		KnownUpstreams:   []string{"mock"},
		FromControlPlane: true,
	})
	if err != nil {
		t.Fatalf("a future top-level member froze authorization: %v", err)
	}
	if s.Generation() != 1 {
		t.Fatalf("generation = %d", s.Generation())
	}
}

// A value the gateway cannot act on must cost the entry, not the document. The
// alternative — refusing everything — leaves the last good state serving
// indefinitely while the control plane keeps getting 200s, so a revocation
// issued afterwards never takes effect and nothing says so.
func TestControlPlaneDropsUnusableEntriesInsteadOfFreezing(t *testing.T) {
	hash := HashToken("live")
	doc := fmt.Sprintf(`{"generation":2,"serviceEnabled":true,
	  "models":[
	    {"publicName":"pickle-general","upstreamRef":"mock","upstreamModel":"m"},
	    {"publicName":"pickle-future","upstreamRef":"dgx","upstreamModel":"m"}],
	  "keys":[
	    {"keyId":"k-live","tokenHash":"%s","status":"ACTIVE","limits":{}},
	    {"keyId":"k-new","tokenHash":"%s","status":"PENDING_ROTATION","limits":{}}]}`,
		hash, HashToken("newstatus"))

	cp := &controlPlane{doc: doc, gen: 2}
	srv := httptest.NewServer(http.HandlerFunc(cp.serve))
	defer srv.Close()
	cache := filepath.Join(t.TempDir(), "snapshot.json")
	s, err := Open(context.Background(), NewHTTPSource(srv.URL, "tok", cache, 5*time.Second),
		cache, discard(), Options{KnownUpstreams: []string{"mock"}, FromControlPlane: true})
	if err != nil {
		t.Fatalf("one unusable entry refused the whole document: %v", err)
	}
	if s.Generation() != 2 {
		t.Fatalf("generation = %d, want the new document applied", s.Generation())
	}
	if got := s.RejectedEntries(); got != 2 {
		t.Fatalf("rejectedEntries = %d, want 2", got)
	}
	_, byHash, byName := s.Current()
	if byHash(hash) == nil {
		t.Fatal("the usable key was lost along with the unusable one")
	}
	if byHash(HashToken("newstatus")) != nil {
		t.Fatal("a key whose status this build does not know was served anyway")
	}
	if byName("pickle-general") == nil {
		t.Fatal("the usable model was lost")
	}
	if byName("pickle-future") != nil {
		t.Fatal("a model naming an upstream this host has no config for was served")
	}

	// The same document from a file is an operator's edit, and there the loud
	// failure is what they need.
	path := filepath.Join(t.TempDir(), "snapshot.json")
	writeDoc(t, path, doc, time.Now())
	if _, err := OpenFile(path, discard(), Options{KnownUpstreams: []string{"mock"}}); err == nil {
		t.Fatal("a hand-maintained file quietly dropped an entry instead of failing")
	}
}

// Omitting a member is how "nothing changed" is expressed, so a writer that
// drops nulls can express a catastrophe by accident: no serviceEnabled reads as
// maintenance mode, and models without keys reads as every key revoked.
func TestHalfDocumentIsRefused(t *testing.T) {
	cases := map[string]string{
		"serviceEnabled missing": `{"generation":3,"models":[],"keys":[]}`,
		"keys missing":           `{"generation":3,"serviceEnabled":true,"models":[]}`,
		"models missing":         `{"generation":3,"serviceEnabled":true,"keys":[]}`,
	}
	for name, doc := range cases {
		t.Run(name, func(t *testing.T) {
			cp := &controlPlane{doc: doc, gen: 3}
			srv := httptest.NewServer(http.HandlerFunc(cp.serve))
			defer srv.Close()
			cache := filepath.Join(t.TempDir(), "snapshot.json")
			_, err := Open(context.Background(), NewHTTPSource(srv.URL, "tok", cache, 5*time.Second),
				cache, discard(), Options{FromControlPlane: true})
			if err == nil {
				t.Fatal("a half-written document was applied")
			}
		})
	}
}

// The poll is the only channel from gateway to api, so what it does not carry
// the control plane cannot know. A frozen document shows up here as a
// generation standing still; these fields are what says why.
func TestSyncRequestCarriesTheSelfReport(t *testing.T) {
	var got SyncRequest
	observed := time.Date(2026, 8, 30, 2, 3, 4, 0, time.UTC)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&got)
		fmt.Fprint(w, `{"generation":1,"serviceEnabled":true,"models":[],"keys":[]}`)
	}))
	defer srv.Close()
	src := NewHTTPSource(srv.URL, "tok", filepath.Join(t.TempDir(), "cache.json"), 5*time.Second)
	src.SetGauges(func() SyncGauges {
		return SyncGauges{
			InFlight: 3, MaxInFlight: 16,
			UpstreamRefs:    []string{"mock"},
			RejectedEntries: 2, ReloadFailures: 5,
			LastError:                 "unknown status\n\"PENDING\"",
			UpstreamObservationFormat: 1,
			Upstreams: []UpstreamObservation{{
				Ref:    "mock",
				Active: ActiveUpstreamObservation{Status: "OK", LastSuccessAt: observed},
				Catalog: CatalogObservation{Status: "MISMATCH",
					MissingModelCount: 1, MissingPublicModels: []string{"pickle-general"}},
			}},
			LastUsageShipSuccessAt: observed,
			UsageQueueObservedAt:   observed.Add(time.Second),
			OldestUnshippedEventAt: observed.Add(-time.Minute),
			QueuedUsageEvents:      7,
			QueuedUsageBytes:       4096,
			UsageQueueScanFailures: 2,
		}
	})
	if _, _, err := src.Load(context.Background(), 0); err != nil {
		t.Fatal(err)
	}
	if got.SupportedFormat != SupportedFormat {
		t.Fatalf("supportedFormat = %d, want %d — without it the writer cannot tell what this build reads",
			got.SupportedFormat, SupportedFormat)
	}
	if got.MaxInFlight != 16 || got.InFlight != 3 {
		t.Fatalf("load gauges = %d/%d", got.InFlight, got.MaxInFlight)
	}
	if got.RejectedEntries != 2 || got.ReloadFailures != 5 {
		t.Fatalf("failure gauges = %d rejected, %d reload failures", got.RejectedEntries, got.ReloadFailures)
	}
	if len(got.UpstreamRefs) != 1 || got.UpstreamRefs[0] != "mock" {
		t.Fatalf("upstreamRefs = %v, want the host's configured refs", got.UpstreamRefs)
	}
	if strings.ContainsAny(got.LastError, "\n\r") {
		t.Fatalf("lastError carried control characters into someone else's log: %q", got.LastError)
	}
	if got.UpstreamObservationFormat != 1 || len(got.Upstreams) != 1 || got.Upstreams[0].Catalog.MissingModelCount != 1 {
		t.Fatalf("upstream observations were lost: format=%d upstreams=%+v", got.UpstreamObservationFormat, got.Upstreams)
	}
	if got.LastUsageShipSuccessAt != observed || got.UsageQueueObservedAt != observed.Add(time.Second) ||
		got.OldestUnshippedEventAt != observed.Add(-time.Minute) || got.QueuedUsageEvents != 7 ||
		got.QueuedUsageBytes != 4096 || got.UsageQueueScanFailures != 2 {
		t.Fatalf("usage queue gauges were lost: %+v", got)
	}
}

func TestObservationFormatOneSerializesAnAuthoritativeEmptyUpstreamList(t *testing.T) {
	var body []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ = io.ReadAll(r.Body)
		fmt.Fprint(w, `{"generation":1,"serviceEnabled":true,"models":[],"keys":[]}`)
	}))
	defer srv.Close()
	src := NewHTTPSource(srv.URL, "tok", filepath.Join(t.TempDir(), "cache.json"), 5*time.Second)
	src.SetGauges(func() SyncGauges {
		return SyncGauges{UpstreamObservationFormat: 1, Upstreams: []UpstreamObservation{}}
	})
	if _, _, err := src.Load(context.Background(), 0); err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(body, []byte(`"upstreamObservationFormat":1`)) ||
		!bytes.Contains(body, []byte(`"upstreams":[]`)) {
		t.Fatalf("format-1 empty upstream list lost authority: %s", body)
	}
}

package snapshot

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
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
		`"models":[{"publicName":"pnu-general","upstreamRef":"mock","upstreamModel":"m"}],`+
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
	if _, changed, err := src.Load(context.Background(), 1); err != nil || changed {
		t.Fatalf("untouched file reported a change: changed=%v err=%v", changed, err)
	}
	writeDoc(t, path, docAt(2, HashToken("pickle-y")), time.Now())
	if _, changed, err := src.Load(context.Background(), 1); err != nil || !changed {
		t.Fatalf("replaced file was not noticed: changed=%v err=%v", changed, err)
	}
}

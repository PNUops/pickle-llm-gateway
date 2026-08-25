package snapshot

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func discard() *slog.Logger { return slog.New(slog.DiscardHandler) }

func writeDoc(t *testing.T, path, body string, mod time.Time) {
	t.Helper()
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(path, mod, mod); err != nil {
		t.Fatal(err)
	}
}

const validDoc = `{
  "generation": 1,
  "serviceEnabled": true,
  "models": [{"publicName": "pickle-general", "upstreamRef": "mock", "upstreamModel": "m1"}],
  "keys": [{"keyId": "k1", "tokenHash": "%s", "status": "ACTIVE", "limits": {}}]
}`

func TestHashToken(t *testing.T) {
	got := HashToken("abc")
	want := "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
	if got != want {
		t.Fatalf("HashToken(abc) = %s, want %s", got, want)
	}
}

func TestOpenAndLookup(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snapshot.json")
	hash := HashToken("pickle-secret")
	writeDoc(t, path, sprintf(validDoc, hash), time.Now())

	s, err := OpenFile(path, discard(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	doc, lookup, model := s.Current()
	if doc.Generation != 1 || !doc.ServiceEnabled {
		t.Fatalf("unexpected doc: %+v", doc)
	}
	if k := lookup(hash); k == nil || k.KeyID != "k1" {
		t.Fatalf("key lookup failed: %+v", k)
	}
	if lookup(HashToken("wrong")) != nil {
		t.Fatal("lookup on a wrong hash must miss")
	}
	if m := model("pickle-general"); m == nil || m.UpstreamModel != "m1" {
		t.Fatalf("model lookup failed: %+v", m)
	}
}

func TestOpenRejectsInvalidDocuments(t *testing.T) {
	hash := HashToken("x")
	cases := map[string]string{
		"unknown field":        `{"generation":1,"serviceEnabled":true,"models":[],"keys":[],"extra":1}`,
		"bad status":           `{"generation":1,"models":[],"keys":[{"keyId":"k","tokenHash":"` + hash + `","status":"ODD"}]}`,
		"short hash":           `{"generation":1,"models":[],"keys":[{"keyId":"k","tokenHash":"abcd","status":"ACTIVE"}]}`,
		"dup model":            `{"generation":1,"models":[{"publicName":"a","upstreamRef":"r","upstreamModel":"m"},{"publicName":"a","upstreamRef":"r","upstreamModel":"m"}],"keys":[]}`,
		"dup token hash":       `{"generation":1,"models":[],"keys":[{"keyId":"k1","tokenHash":"` + hash + `","status":"ACTIVE"},{"keyId":"k2","tokenHash":"` + hash + `","status":"ACTIVE"}]}`,
		"missing model fields": `{"generation":1,"models":[{"publicName":"a"}],"keys":[]}`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "snapshot.json")
			writeDoc(t, path, body, time.Now())
			if _, err := OpenFile(path, discard(), Options{}); err == nil {
				t.Fatal("Open accepted an invalid document")
			}
		})
	}
}

func TestRefreshPicksUpChangesAndRefusesRollback(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snapshot.json")
	hash := HashToken("pickle-secret")
	base := time.Now().Add(-2 * time.Hour)
	writeDoc(t, path, sprintf(validDoc, hash), base)

	s, err := OpenFile(path, discard(), Options{})
	if err != nil {
		t.Fatal(err)
	}

	next := `{"generation":2,"serviceEnabled":false,"models":[],"keys":[]}`
	writeDoc(t, path, next, base.Add(time.Hour))
	s.Refresh(context.Background())
	if got := s.Generation(); got != 2 {
		t.Fatalf("generation after refresh = %d, want 2", got)
	}

	// An older document must not replace a newer one: that would silently
	// undo a revocation.
	writeDoc(t, path, sprintf(validDoc, hash), base.Add(90*time.Minute))
	s.Refresh(context.Background())
	if got := s.Generation(); got != 2 {
		t.Fatalf("generation went backwards to %d", got)
	}
	doc, _, _ := s.Current()
	if doc.ServiceEnabled {
		t.Fatal("rollback document was served")
	}
}

func TestRefreshKeepsStateOnBrokenFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snapshot.json")
	writeDoc(t, path, sprintf(validDoc, HashToken("k")), time.Now().Add(-time.Hour))
	s, err := OpenFile(path, discard(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	writeDoc(t, path, "{broken", time.Now())
	s.Refresh(context.Background())
	if got := s.Generation(); got != 1 {
		t.Fatalf("generation = %d, want the last good state", got)
	}
}

func TestKeyAllowsModel(t *testing.T) {
	pub := &Model{PublicName: "pickle-general"}
	restricted := &Model{PublicName: "pickle-internal", Visibility: ModelRestricted}
	other := &Model{PublicName: "pickle-code"}

	open := Key{}
	if !open.AllowsModel(pub) {
		t.Fatal("empty allow list must allow a public model")
	}
	if open.AllowsModel(restricted) {
		t.Fatal("empty allow list must NOT allow a restricted model")
	}
	limited := Key{AllowedModels: []string{"pickle-general", "pickle-internal"}}
	if !limited.AllowsModel(pub) || !limited.AllowsModel(restricted) {
		t.Fatal("explicit allow list must allow named models, restricted included")
	}
	if limited.AllowsModel(other) {
		t.Fatal("allow list not honored")
	}
}

func sprintf(format string, args ...any) string { return fmt.Sprintf(format, args...) }

func TestBuildRejectsUnknownUpstream(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snapshot.json")
	body := `{"generation":1,"serviceEnabled":true,"models":[{"publicName":"pickle-general","upstreamRef":"opnai","upstreamModel":"m"}],"keys":[]}`
	writeDoc(t, path, body, time.Now())
	// With a known-upstreams set that does not contain the typo, load fails.
	if _, err := OpenFile(path, discard(), Options{KnownUpstreams: []string{"openai"}}); err == nil {
		t.Fatal("Open accepted a model referencing an unconfigured upstream")
	}
	// Correct ref (case-insensitive) loads.
	body2 := `{"generation":1,"serviceEnabled":true,"models":[{"publicName":"pickle-general","upstreamRef":"OpenAI","upstreamModel":"m"}],"keys":[]}`
	writeDoc(t, path, body2, time.Now())
	if _, err := OpenFile(path, discard(), Options{KnownUpstreams: []string{"openai"}}); err != nil {
		t.Fatalf("Open rejected a valid upstream ref: %v", err)
	}
}

func TestHighWaterRefusesRollbackAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snapshot.json")
	// First process serves generation 5, which persists the high-water.
	writeDoc(t, path, `{"generation":5,"serviceEnabled":true,"models":[],"keys":[]}`, time.Now().Add(-time.Hour))
	s1, err := OpenFile(path, discard(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	if s1.Generation() != 5 {
		t.Fatalf("generation = %d", s1.Generation())
	}
	if _, err := os.Stat(path + ".highwater"); err != nil {
		t.Fatalf("high-water sidecar not written: %v", err)
	}

	// A restart (new Store on the same path) finds an older document restored.
	writeDoc(t, path, `{"generation":3,"serviceEnabled":true,"models":[],"keys":[]}`, time.Now())
	if _, err := OpenFile(path, discard(), Options{}); err == nil {
		t.Fatal("Open served a rolled-back snapshot after restart")
	}
	// The override lets it through.
	if _, err := OpenFile(path, discard(), Options{AllowGenerationReset: true}); err != nil {
		t.Fatalf("override did not permit the reset: %v", err)
	}
}

// A document with no models and no keys is not "an empty campus" — through a
// file it can only be truncation, and applying it revokes everyone at once
// while every failure signal stays at zero. Over the sync link the same bytes
// mean "nothing changed", which the transport filters out before the parser
// ever sees them.
func TestDocumentMissingBothListsIsRefused(t *testing.T) {
	path := filepath.Join(t.TempDir(), "snapshot.json")
	hash := HashToken("live")
	writeDoc(t, path, fmt.Sprintf(`{"generation":1,"serviceEnabled":true,
	  "models":[{"publicName":"pickle-general","upstreamRef":"mock","upstreamModel":"m"}],
	  "keys":[{"keyId":"k","tokenHash":%q,"status":"ACTIVE","limits":{}}]}`, hash), time.Now())
	s, err := OpenFile(path, discard(), Options{})
	if err != nil {
		t.Fatal(err)
	}

	writeDoc(t, path, `{"generation":2,"serviceEnabled":true}`, time.Now().Add(time.Minute))
	s.Refresh(t.Context())

	if s.Generation() != 1 {
		t.Fatalf("generation = %d: a document with nothing in it was applied", s.Generation())
	}
	_, byHash, _ := s.Current()
	if byHash(hash) == nil {
		t.Fatal("every key was revoked by a document that simply omitted them")
	}
	if s.ReloadFailures() == 0 {
		t.Fatal("the refusal left no trace: health would keep saying ok")
	}
	if s.LastError() == "" {
		t.Fatal("no lastError to tell the control plane why")
	}
}

// Startup must survive a control plane that answers "unchanged" to a gateway
// with no state — a plausible first implementation, since the gateway reports
// generation 0 and the api's own generation may match its last known value.
// Refusing to start while holding a good cached document is the wrong trade.
func TestCachedDocumentStartsTheGatewayWhenTheFirstPollSaysUnchanged(t *testing.T) {
	dir := t.TempDir()
	cache := filepath.Join(dir, "snapshot.json")
	hash := HashToken("cached")
	body := fmt.Sprintf(`{"generation":5,"serviceEnabled":true,
	  "models":[{"publicName":"pickle-general","upstreamRef":"mock","upstreamModel":"m"}],
	  "keys":[{"keyId":"k","tokenHash":%q,"status":"ACTIVE","limits":{}}]}`, hash)
	if err := os.WriteFile(cache, []byte(body), 0o640); err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"generation":5}`) // unchanged, to a caller with nothing
	}))
	defer srv.Close()

	src := NewHTTPSource(srv.URL, "tok", cache, 5*time.Second)
	s, err := Open(t.Context(), src, cache, discard(), Options{
		KnownUpstreams: []string{"mock"}, FromControlPlane: true,
	})
	if err != nil {
		t.Fatalf("the gateway refused to start while holding a usable cache: %v", err)
	}
	_, byHash, _ := s.Current()
	if byHash(hash) == nil {
		t.Fatal("started, but not on the cached document")
	}
}

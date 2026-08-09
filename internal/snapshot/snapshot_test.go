package snapshot

import (
	"fmt"
	"log/slog"
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
  "models": [{"publicName": "pnu-general", "upstreamRef": "mock", "upstreamModel": "m1"}],
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

	s, err := Open(path, discard())
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
	if m := model("pnu-general"); m == nil || m.UpstreamModel != "m1" {
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
			if _, err := Open(path, discard()); err == nil {
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

	s, err := Open(path, discard())
	if err != nil {
		t.Fatal(err)
	}

	next := `{"generation":2,"serviceEnabled":false,"models":[],"keys":[]}`
	writeDoc(t, path, next, base.Add(time.Hour))
	s.Refresh()
	if got := s.Generation(); got != 2 {
		t.Fatalf("generation after refresh = %d, want 2", got)
	}

	// An older document must not replace a newer one: that would silently
	// undo a revocation.
	writeDoc(t, path, sprintf(validDoc, hash), base.Add(90*time.Minute))
	s.Refresh()
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
	s, err := Open(path, discard())
	if err != nil {
		t.Fatal(err)
	}
	writeDoc(t, path, "{broken", time.Now())
	s.Refresh()
	if got := s.Generation(); got != 1 {
		t.Fatalf("generation = %d, want the last good state", got)
	}
}

func TestKeyAllows(t *testing.T) {
	open := Key{}
	if !open.Allows("anything") {
		t.Fatal("empty allow list must allow every model")
	}
	limited := Key{AllowedModels: []string{"pnu-general"}}
	if !limited.Allows("pnu-general") || limited.Allows("pnu-code") {
		t.Fatal("allow list not honored")
	}
}

func sprintf(format string, args ...any) string { return fmt.Sprintf(format, args...) }

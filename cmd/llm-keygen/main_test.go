package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"testing"

	"github.com/pnuops/pickle-llm-gateway/internal/snapshot"
)

func TestNewTokenShape(t *testing.T) {
	re := regexp.MustCompile(`^pickle-[0-9A-Za-z]{43}$`)
	seen := map[string]bool{}
	for range 50 {
		tok, err := newToken()
		if err != nil {
			t.Fatal(err)
		}
		if !re.MatchString(tok) {
			t.Fatalf("token shape: %s", tok)
		}
		if seen[tok] {
			t.Fatal("duplicate token")
		}
		seen[tok] = true
	}
}

func TestInsertBumpsGenerationAtomically(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snapshot.json")
	doc := snapshot.Document{Generation: 7, ServiceEnabled: true,
		Models: []snapshot.Model{{PublicName: "pnu-general", UpstreamRef: "mock", UpstreamModel: "m"}}}
	raw, _ := json.Marshal(doc)
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}

	entry := snapshot.Key{KeyID: "k-new", TokenHash: snapshot.HashToken("pickle-x"), Status: snapshot.KeyActive}
	if err := insert(path, entry); err != nil {
		t.Fatal(err)
	}
	out, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var got snapshot.Document
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatal(err)
	}
	if got.Generation != 8 || len(got.Keys) != 1 || got.Keys[0].KeyID != "k-new" {
		t.Fatalf("insert result: %+v", got)
	}

	// A duplicate keyId must be refused, not silently doubled.
	if err := insert(path, entry); err == nil {
		t.Fatal("duplicate keyId accepted")
	}
}

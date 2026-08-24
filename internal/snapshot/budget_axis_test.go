// Validation of the budget axis, the passthrough ref and per-key upstream
// credentials, on both strictness paths.
package snapshot

import (
	"strings"
	"testing"
)

func known(refs ...string) map[string]bool {
	m := map[string]bool{}
	for _, r := range refs {
		m[strings.ToLower(r)] = true
	}
	return m
}

func TestBudgetAxisValidation(t *testing.T) {
	doc := `{
	  "generation": 1, "serviceEnabled": true,
	  "models": [
	    {"publicName": "a", "upstreamRef": "mock", "upstreamModel": "m", "budgetAxis": "CREDIT"},
	    {"publicName": "b", "upstreamRef": "mock", "upstreamModel": "m"},
	    {"publicName": "c", "upstreamRef": "mock", "upstreamModel": "m", "budgetAxis": "MONEY"}
	  ],
	  "keys": []
	}`
	// A hand-maintained file refuses the whole document on an unknown axis.
	if _, err := build([]byte(doc), known("mock"), false); err == nil {
		t.Fatal("file path accepted an unknown budgetAxis")
	}
	// The control plane drops only the entry it cannot act on.
	st, err := build([]byte(doc), known("mock"), true)
	if err != nil {
		t.Fatal(err)
	}
	if len(st.doc.Models) != 2 || st.rejected != 1 {
		t.Fatalf("got %d models, %d rejected", len(st.doc.Models), st.rejected)
	}
	if !st.byPublic["a"].CreditAxis() {
		t.Fatal("explicit CREDIT axis not honored")
	}
	if st.byPublic["b"].CreditAxis() {
		t.Fatal("an absent budgetAxis must mean TOKEN")
	}
}

func TestPassthroughRefValidation(t *testing.T) {
	doc := `{
	  "generation": 1, "serviceEnabled": true, "passthroughRef": "OpenRouter",
	  "models": [], "keys": []
	}`
	// Configured: kept, lowercased.
	st, err := build([]byte(doc), known("openrouter"), true)
	if err != nil {
		t.Fatal(err)
	}
	if st.doc.PassthroughRef != "openrouter" {
		t.Fatalf("passthroughRef = %q", st.doc.PassthroughRef)
	}
	// Not configured on this host: the control-plane path disables passthrough
	// and counts the drop; the file path refuses the document.
	st, err = build([]byte(doc), known("mock"), true)
	if err != nil {
		t.Fatal(err)
	}
	if st.doc.PassthroughRef != "" || st.rejected != 1 {
		t.Fatalf("unconfigured passthroughRef survived: %q, rejected %d", st.doc.PassthroughRef, st.rejected)
	}
	if _, err := build([]byte(doc), known("mock"), false); err == nil {
		t.Fatal("file path accepted an unconfigured passthroughRef")
	}
}

func TestCredentialNormalization(t *testing.T) {
	doc := `{
	  "generation": 1, "serviceEnabled": true, "models": [],
	  "keys": [{"keyId": "k1", "tokenHash": "` + HashToken("t") + `", "status": "ACTIVE",
	            "upstreamCredentials": {"OpenRouter": "sk-1", "empty": ""}}]
	}`
	st, err := build([]byte(doc), nil, true)
	if err != nil {
		t.Fatal(err)
	}
	k := st.byHash[HashToken("t")]
	if got := k.CredentialFor("OPENROUTER"); got != "sk-1" {
		t.Fatalf("CredentialFor is not case-insensitive: %q", got)
	}
	if got := k.CredentialFor("empty"); got != "" {
		t.Fatalf("an empty credential value must read as none, got %q", got)
	}
	var none Key
	if none.CredentialFor("anything") != "" {
		t.Fatal("a key with no credentials must answer none")
	}
}

func TestCredentialRefCollisionDropsTheKey(t *testing.T) {
	// Refs that collide after lowering would leave map iteration deciding
	// which credential gets spent; the entry is unusable and must drop.
	doc := `{
	  "generation": 1, "serviceEnabled": true, "models": [],
	  "keys": [{"keyId": "k1", "tokenHash": "` + HashToken("t") + `", "status": "ACTIVE",
	            "upstreamCredentials": {"OpenRouter": "sk-a", "openrouter": "sk-b"}}]
	}`
	st, err := build([]byte(doc), nil, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(st.doc.Keys) != 0 || st.rejected != 1 {
		t.Fatalf("colliding credential refs survived: %d keys, %d rejected",
			len(st.doc.Keys), st.rejected)
	}
	if _, err := build([]byte(doc), nil, false); err == nil {
		t.Fatal("file path accepted colliding credential refs")
	}
}

func TestModelRefsAreLoweredAtLoad(t *testing.T) {
	doc := `{
	  "generation": 1, "serviceEnabled": true,
	  "models": [{"publicName": "a", "upstreamRef": "Mock", "upstreamModel": "m",
	              "fallbackRef": "BACKUP"}],
	  "keys": []
	}`
	st, err := build([]byte(doc), known("mock", "backup"), true)
	if err != nil {
		t.Fatal(err)
	}
	m := st.byPublic["a"]
	if m.UpstreamRef != "mock" || m.FallbackRef != "backup" {
		t.Fatalf("refs not normalized: %q/%q", m.UpstreamRef, m.FallbackRef)
	}
}

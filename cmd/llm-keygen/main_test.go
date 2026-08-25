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
		Models: []snapshot.Model{{PublicName: "pickle-general", UpstreamRef: "mock", UpstreamModel: "m"}}}
	raw, _ := json.Marshal(doc)
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}

	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatal(err)
	}
	entry := snapshot.Key{KeyID: "k-new", TokenHash: snapshot.HashToken("pickle-x"), Status: snapshot.KeyActive}
	if err := insert(path, entry); err != nil {
		t.Fatal(err)
	}
	// The replacement must keep the original permissions: a mode reset would
	// lock the gateway's service user out of its own snapshot.
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o600 {
		t.Fatalf("insert changed the snapshot mode to %v", fi.Mode().Perm())
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

// writeDoc lays down a document to operate on.
func writeDoc(t *testing.T, doc snapshot.Document) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "snapshot.json")
	raw, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, raw, 0o640); err != nil {
		t.Fatal(err)
	}
	return path
}

func readDoc(t *testing.T, path string) snapshot.Document {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var doc snapshot.Document
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	return doc
}

// Revocation is the operation an incident runs, so it has to behave when the
// operator gets it slightly wrong — and it must leave the entry in place, so
// the gateway can say the key was revoked rather than that it never existed.
func TestRevokeMarksTheKeyAndKeepsIt(t *testing.T) {
	path := writeDoc(t, snapshot.Document{
		Generation: 4, ServiceEnabled: true,
		Keys: []snapshot.Key{
			{KeyID: "k-a", TokenHash: snapshot.HashToken("a"), Status: snapshot.KeyActive},
			{KeyID: "k-b", TokenHash: snapshot.HashToken("b"), Status: snapshot.KeyActive},
		},
	})
	if err := revokeKey(path, "k-a"); err != nil {
		t.Fatal(err)
	}
	doc := readDoc(t, path)
	if doc.Generation != 5 {
		t.Fatalf("generation = %d, want the bump that makes the gateway notice", doc.Generation)
	}
	if len(doc.Keys) != 2 {
		t.Fatalf("revocation removed the entry (%d keys left); the gateway can no longer tell revoked from unknown", len(doc.Keys))
	}
	if doc.Keys[0].Status != snapshot.KeyRevoked || doc.Keys[1].Status != snapshot.KeyActive {
		t.Fatalf("wrong key revoked: %v", doc.Keys)
	}

	// A typo in the id must not silently do nothing, and must not bump the
	// generation — an operator who mistypes has to find out immediately.
	before := readDoc(t, path)
	if err := revokeKey(path, "k-typo"); err == nil {
		t.Fatal("revoking an unknown keyId reported success")
	}
	if err := revokeKey(path, "k-a"); err == nil {
		t.Fatal("revoking an already-revoked key reported success")
	}
	if after := readDoc(t, path); after.Generation != before.Generation {
		t.Fatalf("a failed revocation still bumped the generation: %d -> %d", before.Generation, after.Generation)
	}
}

// The kill switch is the other thing an incident reaches for. Flipping it must
// not disturb the keys, or turning the service back on becomes a second
// incident.
func TestServiceSwitchLeavesEverythingElseAlone(t *testing.T) {
	path := writeDoc(t, snapshot.Document{
		Generation: 9, ServiceEnabled: true,
		Models: []snapshot.Model{{PublicName: "pickle-general", UpstreamRef: "mock", UpstreamModel: "m"}},
		Keys:   []snapshot.Key{{KeyID: "k-a", TokenHash: snapshot.HashToken("a"), Status: snapshot.KeyActive}},
	})
	if err := setService(path, false); err != nil {
		t.Fatal(err)
	}
	doc := readDoc(t, path)
	if doc.ServiceEnabled {
		t.Fatal("the kill switch did not take")
	}
	if doc.Generation != 10 || len(doc.Keys) != 1 || len(doc.Models) != 1 {
		t.Fatalf("the switch disturbed the document: %+v", doc)
	}
	if doc.Keys[0].Status != snapshot.KeyActive {
		t.Fatal("keys changed status when the service was switched off")
	}
	if err := setService(path, false); err == nil {
		t.Fatal("switching off an already-off service reported success")
	}
	if err := setService(path, true); err != nil {
		t.Fatal(err)
	}
	if !readDoc(t, path).ServiceEnabled {
		t.Fatal("the service did not come back on")
	}
}

// Every document change keeps the file readable by the gateway's service user.
// A mode or owner reset here is the failure that looks like a broken gateway.
func TestMaintenanceKeepsFileMode(t *testing.T) {
	path := writeDoc(t, snapshot.Document{
		Generation: 1, ServiceEnabled: true,
		Keys: []snapshot.Key{{KeyID: "k-a", TokenHash: snapshot.HashToken("a"), Status: snapshot.KeyActive}},
	})
	if err := revokeKey(path, "k-a"); err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o640 {
		t.Fatalf("revocation changed the snapshot mode to %v", fi.Mode().Perm())
	}
}

func TestParseSwitch(t *testing.T) {
	for _, on := range []string{"on", "ON", "true", "yes"} {
		if v, err := parseSwitch(on); err != nil || !v {
			t.Fatalf("parseSwitch(%q) = %v, %v", on, v, err)
		}
	}
	for _, off := range []string{"off", "OFF", "false", "no"} {
		if v, err := parseSwitch(off); err != nil || v {
			t.Fatalf("parseSwitch(%q) = %v, %v", off, v, err)
		}
	}
	if _, err := parseSwitch("maybe"); err == nil {
		t.Fatal("an unrecognized switch value was accepted")
	}
}

// The document format promises that a field this build does not know is
// ignored rather than lost — that is what lets the control plane extend an
// entry without a lockstep gateway. A tool that re-serializes entries from Go
// structs breaks that promise the first time an operator revokes a key, and
// once the api writes this document it is data loss between two writers.
func TestMaintenancePreservesFieldsItDoesNotKnow(t *testing.T) {
	path := filepath.Join(t.TempDir(), "snapshot.json")
	raw := `{
  "generation": 3,
  "serviceEnabled": true,
  "models": [{"publicName": "pickle-general", "upstreamRef": "mock", "upstreamModel": "m", "futureField": 7}],
  "keys": [{"keyId": "k-a", "tokenHash": "` + snapshot.HashToken("a") + `", "status": "ACTIVE",
            "limits": {}, "owner": "someone", "scopes": ["ws-7"]}]
}`
	if err := os.WriteFile(path, []byte(raw), 0o640); err != nil {
		t.Fatal(err)
	}
	if err := revokeKey(path, "k-a"); err != nil {
		t.Fatal(err)
	}
	out, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Models []map[string]any `json:"models"`
		Keys   []map[string]any `json:"keys"`
	}
	if err := json.Unmarshal(out, &doc); err != nil {
		t.Fatal(err)
	}
	if doc.Keys[0]["owner"] != "someone" {
		t.Fatalf("revocation erased a field it does not model: %v", doc.Keys[0])
	}
	if _, ok := doc.Keys[0]["scopes"]; !ok {
		t.Fatalf("revocation erased scopes: %v", doc.Keys[0])
	}
	if doc.Keys[0]["status"] != snapshot.KeyRevoked {
		t.Fatalf("status not applied: %v", doc.Keys[0])
	}
	if doc.Models[0]["futureField"] == nil {
		t.Fatalf("an untouched model entry lost a field: %v", doc.Models[0])
	}
}

// A document the gateway would refuse must never reach the file. The gateway's
// response to a refused document is to keep serving its last good state, so
// writing one turns "the key is revoked" into "the tool said so and the key
// still works" — with the only signal on a health endpoint nobody reads during
// an incident.
func TestMaintenanceRefusesToWriteAnUnloadableDocument(t *testing.T) {
	path := filepath.Join(t.TempDir(), "snapshot.json")
	// No models member at all — the shape the loader refuses.
	raw := `{"generation":1,"serviceEnabled":true,
	         "keys":[{"keyId":"k-a","tokenHash":"` + snapshot.HashToken("a") + `","status":"ACTIVE","limits":{}}]}`
	if err := os.WriteFile(path, []byte(raw), 0o640); err != nil {
		t.Fatal(err)
	}
	// The tool repairs the missing member rather than propagating it, so this
	// succeeds — and what it wrote must load.
	if err := revokeKey(path, "k-a"); err != nil {
		t.Fatal(err)
	}
	out, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := snapshot.Validate(out); err != nil {
		t.Fatalf("the tool wrote a document the gateway refuses: %v", err)
	}

	// A member this tool does not know is refused rather than silently dropped.
	bad := filepath.Join(t.TempDir(), "snapshot.json")
	if err := os.WriteFile(bad, []byte(`{"generation":1,"serviceEnabled":true,"models":[],"keys":[],"scopes":[]}`), 0o640); err != nil {
		t.Fatal(err)
	}
	if err := setService(bad, false); err == nil {
		t.Fatal("a document with an unknown top-level member was rewritten without it")
	}
}

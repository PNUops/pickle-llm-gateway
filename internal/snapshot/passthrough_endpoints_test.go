package snapshot

import (
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
)

// The whole point of the field: empty grants nothing. This is the opposite of
// the two money lists, and getting it backwards would open every new path to
// every money-axis key the moment the gateway learned to serve them.
func TestAllowsEndpointDefaultsClosed(t *testing.T) {
	var k Key
	if k.AllowsEndpoint(EndpointImages) || k.AllowsEndpoint(EndpointEmbeddings) {
		t.Fatal("a key with no list must reach nothing")
	}
	k.PassthroughEndpoints = []string{}
	if k.AllowsEndpoint(EndpointImages) {
		t.Fatal("an explicitly empty list must reach nothing")
	}
	k.PassthroughEndpoints = []string{EndpointImages}
	if !k.AllowsEndpoint(EndpointImages) {
		t.Fatal("a granted capability must be allowed")
	}
	if k.AllowsEndpoint(EndpointEmbeddings) {
		t.Fatal("one grant must not carry another")
	}
}

// A control plane too old to send the member describes a key with no
// passthrough, which is the correct reading and needs no special case.
func TestAbsentMemberDecodesClosed(t *testing.T) {
	var k Key
	if err := json.Unmarshal([]byte(`{"keyId":"k","status":"ACTIVE"}`), &k); err != nil {
		t.Fatal(err)
	}
	if k.PassthroughEndpoints != nil || k.AllowsEndpoint(EndpointImages) {
		t.Fatalf("absent member: %#v", k.PassthroughEndpoints)
	}
}

func TestNormalizeEndpoints(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   []string
		want []string
	}{
		{"lowercased", []string{"IMAGES"}, []string{"images"}},
		{"trimmed", []string{"  images  "}, []string{"images"}},
		{"deduplicated", []string{"images", "IMAGES"}, []string{"images"}},
		{"blanks dropped", []string{"", "   ", "images"}, []string{"images"}},
		{"order kept", []string{"embeddings", "images"}, []string{"embeddings", "images"}},
		{"empty is nil", []string{"", " "}, nil},
		// A token this build does not route is kept rather than filtered: the
		// vocabulary is the control plane's, and nothing here can match it.
		{"unknown kept", []string{"audio", "images"}, []string{"audio", "images"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := normalizeEndpoints(tc.in)
			if len(got) != len(tc.want) {
				t.Fatalf("got %v want %v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("got %v want %v", got, tc.want)
				}
			}
		})
	}
}

// The load path normalizes, and — unlike the money lists — never drops the key
// over this field. Dropping it would deny chat as well, over a capability the
// key may never have used.
func TestLoadNormalizesAndKeepsTheKey(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snapshot.json")
	doc := `{"generation":1,"serviceEnabled":true,"models":[],"keys":[
		{"keyId":"k1","tokenHash":"` + HashToken("t1") + `","status":"ACTIVE",
		 "passthroughEndpoints":["IMAGES","  images ","audio",""]}]}`
	if err := os.WriteFile(path, []byte(doc), 0o600); err != nil {
		t.Fatal(err)
	}
	store, err := OpenFile(path, slog.New(slog.DiscardHandler), Options{})
	if err != nil {
		t.Fatal(err)
	}
	if got := store.RejectedEntries(); got != 0 {
		t.Fatalf("an unreadable capability must not drop the key: %d rejected", got)
	}
	_, lookup, _ := store.Current()
	k := lookup(HashToken("t1"))
	if k == nil {
		t.Fatal("key was dropped")
	}
	if len(k.PassthroughEndpoints) != 2 {
		t.Fatalf("normalized to %v", k.PassthroughEndpoints)
	}
	if !k.AllowsEndpoint(EndpointImages) {
		t.Fatal("a mixed-case grant must still be allowed")
	}
	if k.AllowsEndpoint(EndpointEmbeddings) {
		t.Fatal("an ungranted capability leaked through")
	}
}

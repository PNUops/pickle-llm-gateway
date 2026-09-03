package snapshot

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// The matcher decides what a key may spend money on, so its shape is pinned
// here at the definition rather than only through the server tests.
func TestMatchesCreditModel(t *testing.T) {
	for _, tc := range []struct {
		pattern, name string
		want          bool
	}{
		{"openai/gpt-4o-mini", "openai/gpt-4o-mini", true},
		{"openai/gpt-4o-mini", "openai/gpt-4o", false},
		{"openai/*", "openai/gpt-4o", true},
		{"openai/*", "openai/a/b", true},
		{"openai/*", "anthropic/claude", false},
		// One vendor, not every vendor whose name begins the same way.
		{"openai/*", "openai-mirror/gpt-4o", false},
		// The prefix opens models under a vendor, not the bare segment.
		{"openai/*", "openai/", false},
		// A bare star matches nothing on purpose: "everything" is an empty
		// list, and a second spelling of one state is how state counts grow.
		{"*", "openai/gpt-4o", false},
		{"*", "*", false},
		{"", "openai/gpt-4o", false},
		// A name with no vendor segment still works as an exact entry.
		{"some-model", "some-model", true},
		// Floating aliases. The vendor ships ~vendor/model-latest entries
		// that always resolve to the newest model of a family, and they
		// route through passthrough today, so the fence has to be able to
		// name them.
		{"~anthropic/claude-sonnet-latest", "~anthropic/claude-sonnet-latest", true},
		{"~anthropic/*", "~anthropic/claude-sonnet-latest", true},
		// The two namespaces stay apart in both directions. An alias points
		// at a model that changes under it, so opening a vendor must not
		// open its aliases, and opening the aliases must not open the vendor.
		{"anthropic/*", "~anthropic/claude-sonnet-latest", false},
		{"~anthropic/*", "anthropic/claude-sonnet-4", false},
		// The tilde is a prefix on the entry, not a wildcard of its own.
		{"~", "~anthropic/claude", false},
		{"~/*", "~anthropic/claude", false},
	} {
		if got := MatchesCreditModel(tc.pattern, tc.name); got != tc.want {
			t.Fatalf("MatchesCreditModel(%q, %q) = %v, want %v",
				tc.pattern, tc.name, got, tc.want)
		}
	}
}

// Everything the pattern check accepts must still match when the caller sends
// the name capitalized, because admission lower-cases the name. This is the
// same invariant the reserved-prefix guard pins for the same reason.
func TestCreditPatternsMatchWhateverTheCase(t *testing.T) {
	for _, tc := range []struct{ pattern, sent string }{
		{"openai/*", "OpenAI/GPT-4o"},
		{"openai/gpt-4o-mini", "OpenAI/GPT-4o-Mini"},
		{"some-model", "Some-Model"},
	} {
		if !creditModelPattern.MatchString(tc.pattern) {
			t.Fatalf("pattern %q should be accepted by the shape check", tc.pattern)
		}
		if !MatchesCreditModel(tc.pattern, strings.ToLower(tc.sent)) {
			t.Fatalf("pattern %q did not match %q sent in mixed case", tc.pattern, tc.sent)
		}
	}
	for _, bad := range []string{"*", "OpenAI/*", "openai/*x", "a/b/*", "", " openai/*"} {
		if creditModelPattern.MatchString(bad) {
			t.Fatalf("pattern %q should be refused by the shape check", bad)
		}
	}
}

func openDoc(t *testing.T, body string) *Store {
	t.Helper()
	path := filepath.Join(t.TempDir(), "snapshot.json")
	if err := os.WriteFile(path, []byte(body), 0o640); err != nil {
		t.Fatal(err)
	}
	s, err := OpenFile(path, discard(), Options{
		KnownUpstreams: []string{"mock"}, FromControlPlane: true,
	})
	if err != nil {
		t.Fatalf("document refused: %v", err)
	}
	return s
}

// A pattern this build cannot act on drops the whole key, not just the entry.
//
// Dropping the entry would be worse than useless: remove the only entry and the
// list becomes empty, and an empty list means unrestricted. A key would quietly
// regain every commercial model because its fence was malformed. Dropping the
// key denies service to one key, counts it, and logs why.
func TestUnusableCreditPatternDropsTheKey(t *testing.T) {
	hash := HashToken("fenced")
	body := fmt.Sprintf(`{"generation":1,"serviceEnabled":true,
	  "models":[{"publicName":"pickle-general","upstreamRef":"mock","upstreamModel":"m"}],
	  "keys":[{"keyId":"k","tokenHash":%q,"status":"ACTIVE","limits":{},
	           "creditAllowedModels":["*"]}]}`, hash)
	s := openDoc(t, body)

	_, byHash, _ := s.Current()
	if byHash(hash) != nil {
		t.Fatal("key with an unusable money fence was served; it must be dropped, " +
			"because a dropped entry would leave it unrestricted")
	}
	if s.RejectedEntries() == 0 {
		t.Fatal("the drop was not counted")
	}
}

// The vendor's floating aliases survive the load. This is the half of the fix
// the matcher table cannot show: MatchesCreditModel never sees the pattern
// check, so a case added there passes whether or not the loader accepts the
// entry. Before the pattern admitted a leading tilde, this document lost the
// key entirely and the student's calls stopped authenticating.
func TestFloatingAliasSurvivesLoad(t *testing.T) {
	hash := HashToken("alias")
	body := fmt.Sprintf(`{"generation":1,"serviceEnabled":true,
	  "models":[{"publicName":"pickle-general","upstreamRef":"mock","upstreamModel":"m"}],
	  "keys":[{"keyId":"k","tokenHash":%q,"status":"ACTIVE","limits":{},
	           "creditAllowedModels":["~anthropic/claude-sonnet-latest","~openai/*"]}]}`, hash)
	s := openDoc(t, body)

	_, byHash, _ := s.Current()
	key := byHash(hash)
	if key == nil {
		t.Fatal("a key fenced to a floating alias was dropped at load; the vendor " +
			"lists these and passthrough routes them, so the fence must spell them")
	}
	if s.RejectedEntries() != 0 {
		t.Fatalf("a usable pattern was counted as rejected: %d", s.RejectedEntries())
	}
	for _, want := range []string{"~anthropic/claude-sonnet-latest", "~openai/*"} {
		if !slices.Contains(key.CreditAllowedModels, want) {
			t.Fatalf("entry %q did not survive the load: %v", want, key.CreditAllowedModels)
		}
	}
}

// A blank entry is refused, not skipped. Skipping is what an earlier version of
// the loader did, and it turned ["  "] — a list that says something — into an
// empty list, which says the opposite: unrestricted. The fence has to fail
// closed on every unusable shape, not only the ones that look wrong.
func TestBlankCreditPatternDropsTheKey(t *testing.T) {
	for _, entry := range []string{`""`, `"   "`} {
		hash := HashToken("blank" + entry)
		body := fmt.Sprintf(`{"generation":1,"serviceEnabled":true,
		  "models":[{"publicName":"pickle-general","upstreamRef":"mock","upstreamModel":"m"}],
		  "keys":[{"keyId":"k","tokenHash":%q,"status":"ACTIVE","limits":{},
		           "creditAllowedModels":[%s]}]}`, hash, entry)
		s := openDoc(t, body)

		_, byHash, _ := s.Current()
		if byHash(hash) != nil {
			t.Fatalf("key carrying a blank entry %s was served; skipping the entry "+
				"would leave the list empty, which means unrestricted", entry)
		}
	}
}

// Patterns are lower-cased once at load so every later comparison is against a
// name lowered the same way. A control plane that skipped normalization must
// still get a working fence rather than one that matches nothing.
func TestCreditPatternsLowercaseAtLoad(t *testing.T) {
	hash := HashToken("fenced")
	body := fmt.Sprintf(`{"generation":1,"serviceEnabled":true,
	  "models":[{"publicName":"pickle-general","upstreamRef":"mock","upstreamModel":"m"}],
	  "keys":[{"keyId":"k","tokenHash":%q,"status":"ACTIVE","limits":{},
	           "creditAllowedModels":["OpenAI/*"," anthropic/claude-sonnet-4 "]}]}`, hash)
	s := openDoc(t, body)

	_, byHash, _ := s.Current()
	key := byHash(hash)
	if key == nil {
		t.Fatal("key was dropped")
	}
	want := []string{"openai/*", "anthropic/claude-sonnet-4"}
	if len(key.CreditAllowedModels) != len(want) {
		t.Fatalf("stored %v, want %v", key.CreditAllowedModels, want)
	}
	for i, w := range want {
		if key.CreditAllowedModels[i] != w {
			t.Fatalf("stored %v, want %v", key.CreditAllowedModels, want)
		}
	}
}

// The fence governs the money axis and nothing else. Pinned on the method so a
// later caller cannot lose the guard by testing the axis itself.
func TestAllowsCreditModelIgnoresTokenAxis(t *testing.T) {
	k := &Key{CreditAllowedModels: []string{"openai/*"}}
	token := &Model{PublicName: "pickle-general"}
	credit := &Model{PublicName: "anthropic/claude", BudgetAxis: AxisCredit}

	if !k.AllowsCreditModel(token) {
		t.Fatal("the money fence refused a self-serving model")
	}
	if k.AllowsCreditModel(credit) {
		t.Fatal("the money fence admitted a model outside it")
	}
	open := &Key{}
	if !open.AllowsCreditModel(credit) {
		t.Fatal("an empty fence must restrict nothing")
	}
}

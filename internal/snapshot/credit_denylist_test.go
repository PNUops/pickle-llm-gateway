// The CREDIT-axis deny list: the half of the money fence that takes models
// back out of whatever the allow list leaves open. These tests pin how the two
// lists combine, and — separately from the matcher table, which never sees the
// pattern check — what the loader accepts into the new list.
package snapshot

import (
	"fmt"
	"slices"
	"testing"
)

func creditModel(name string) *Model {
	return &Model{PublicName: name, BudgetAxis: AxisCredit}
}

// A denial wins over an allowance. Without this the wide allowance an approver
// wrote months ago quietly reopens the model somebody closed yesterday, which
// is the whole reason the second list exists.
func TestCreditDenialBeatsAllowance(t *testing.T) {
	k := &Key{
		CreditAllowedModels: []string{"openai/*"},
		CreditDeniedModels:  []string{"openai/abc"},
	}
	if k.AllowsCreditModel(creditModel("openai/abc")) {
		t.Fatal("a denied model was admitted by the vendor-wide allowance it sits inside")
	}
	if !k.AllowsCreditModel(creditModel("openai/other")) {
		t.Fatal("the denial of one model closed the rest of the allowance")
	}
}

// A deny list on its own fences the named models and nothing else. The allow
// list being empty still means "everything", so the two halves have to be read
// independently rather than as one list with two moods.
func TestCreditDenialAloneFencesOnlyItsNames(t *testing.T) {
	k := &Key{CreditDeniedModels: []string{"openai/o1-pro"}}
	if k.AllowsCreditModel(creditModel("openai/o1-pro")) {
		t.Fatal("the denied model was admitted")
	}
	for _, name := range []string{"openai/gpt-4o-mini", "anthropic/claude-sonnet-4"} {
		if !k.AllowsCreditModel(creditModel(name)) {
			t.Fatalf("%q was refused by a deny list that does not name it", name)
		}
	}
}

// Neither list is the state every key was in before either field existed, and
// it has to stay reachable: an empty deny list must not read as "deny all".
func TestNoCreditListsRestrictNothing(t *testing.T) {
	k := &Key{}
	if !k.AllowsCreditModel(creditModel("anthropic/claude-sonnet-4")) {
		t.Fatal("a key carrying neither money list was fenced")
	}
}

// Both lists govern the money axis and nothing else. A deny list that named a
// self-serving model would refuse campus capacity over a money rule, which is
// the confusion the axis guard inside AllowsCreditModel exists to prevent.
func TestCreditDenialIgnoresTokenAxis(t *testing.T) {
	k := &Key{
		CreditAllowedModels: []string{"openai/*"},
		CreditDeniedModels:  []string{"pickle-general"},
	}
	if !k.AllowsCreditModel(&Model{PublicName: "pickle-general"}) {
		t.Fatal("a money-list entry refused a self-serving model")
	}
}

// The pattern check is a separate gate from the matcher, and only a document
// exercises it: a case added to the matcher table passes whether or not the
// loader would ever let that entry through. These are the shapes the loader
// refuses, each one dropping the key that carried it.
func TestUnusableCreditDenyPatternDropsTheKey(t *testing.T) {
	for _, entry := range []string{
		`"*"`, `"openai*"`, `"openai/*gpt*"`, `"openai/**"`, `"openai/*-"`, `""`, `"   "`,
	} {
		hash := HashToken("denied" + entry)
		body := fmt.Sprintf(`{"generation":1,"serviceEnabled":true,
		  "models":[{"publicName":"pickle-general","upstreamRef":"mock","upstreamModel":"m"}],
		  "keys":[{"keyId":"k","tokenHash":%q,"status":"ACTIVE","limits":{},
		           "creditDeniedModels":[%s]}]}`, hash, entry)
		s := openDoc(t, body)

		_, byHash, _ := s.Current()
		if byHash(hash) != nil {
			t.Fatalf("key carrying the unusable deny entry %s was served; dropping "+
				"the entry instead would empty the list, and an empty deny list "+
				"denies nothing — the closed model would silently reopen", entry)
		}
		if s.RejectedEntries() == 0 {
			t.Fatalf("the drop of %s was not counted", entry)
		}
	}
}

// The shapes this round adds have to survive an actual load, in both lists.
// The matcher table cannot show this: it is handed patterns directly, so it
// would stay green while the pattern check dropped every key that used one.
func TestWildcardPatternsSurviveLoad(t *testing.T) {
	hash := HashToken("wildcards")
	body := fmt.Sprintf(`{"generation":1,"serviceEnabled":true,
	  "models":[{"publicName":"pickle-general","upstreamRef":"mock","upstreamModel":"m"}],
	  "keys":[{"keyId":"k","tokenHash":%q,"status":"ACTIVE","limits":{},
	           "creditAllowedModels":["openai/gpt-5-*","~openai/*"],
	           "creditDeniedModels":["openai/*-pro","openai/o1*"]}]}`, hash)
	s := openDoc(t, body)

	_, byHash, _ := s.Current()
	key := byHash(hash)
	if key == nil {
		t.Fatal("a key using end-anchored wildcards was dropped at load")
	}
	if s.RejectedEntries() != 0 {
		t.Fatalf("a usable pattern was counted as rejected: %d", s.RejectedEntries())
	}
	if !slices.Contains(key.CreditDeniedModels, "openai/*-pro") {
		t.Fatalf("the deny list did not survive the load: %v", key.CreditDeniedModels)
	}
	// The field is read from the document under this exact name. A rename on
	// either side leaves the list empty rather than failing, and an empty deny
	// list is an open one — the fence would be gone with nothing to show for it.
	if len(key.CreditDeniedModels) != 2 {
		t.Fatalf("deny entries were lost between the document and the key: %v",
			key.CreditDeniedModels)
	}
}

// The deny list is read from the document under exactly one name, and this is
// what a document that spells it any other way does: nothing. Unknown fields in
// a key entry are ignored on purpose, so a misspelling does not fail the load —
// it arrives as an empty list, and an empty deny list denies nothing. The
// models somebody closed are open again while every screen still shows them
// closed, which is the worst shape this round can fail in.
//
// The mistake this pins is a real one: the database column is snake_case and
// the document field is not, so the column name is the misspelling that comes
// to hand. It is written here as a document the loader accepts and a fence that
// then does not hold.
//
// Nothing else in this repository can catch it. The matcher tests are handed
// patterns directly and never see a document at all, and the server tests are
// worse than useless here: they build a Document as a Go value and marshal it
// with the same struct tags they read it back through, so the two sides always
// agree whatever the tag says. A green server package is not evidence that the
// name matches what the writer sends — only a document written out by hand, as
// below, is.
func TestMisspelledDenyFieldFencesNothing(t *testing.T) {
	hash := HashToken("misspelled")
	body := fmt.Sprintf(`{"generation":1,"serviceEnabled":true,
	  "models":[{"publicName":"pickle-general","upstreamRef":"mock","upstreamModel":"m"}],
	  "keys":[{"keyId":"k","tokenHash":%q,"status":"ACTIVE","limits":{},
	           "credit_denied_models":["openai/o1-pro"]}]}`, hash)
	s := openDoc(t, body)

	_, byHash, _ := s.Current()
	key := byHash(hash)
	if key == nil {
		t.Fatal("the key was dropped; an unknown field in a key entry is ignored, " +
			"not refused, which is what makes this failure a silent one")
	}
	if len(key.CreditDeniedModels) != 0 {
		t.Fatalf("a field named credit_denied_models filled the deny list: %v — "+
			"the document field is creditDeniedModels, and if that changed here "+
			"it has to change on the side that writes the document too",
			key.CreditDeniedModels)
	}
	if !key.AllowsCreditModel(creditModel("openai/o1-pro")) {
		t.Fatal("a misspelled field denied a model, so the name this loader reads " +
			"is no longer the one the writer is being told to send")
	}
}

// Deny entries are lower-cased and trimmed at load like allow entries, so a
// control plane that skipped normalization gets a working fence instead of one
// that silently matches nothing — which on this list means denying nothing.
func TestCreditDenyPatternsNormalizeAtLoad(t *testing.T) {
	hash := HashToken("denynorm")
	body := fmt.Sprintf(`{"generation":1,"serviceEnabled":true,
	  "models":[{"publicName":"pickle-general","upstreamRef":"mock","upstreamModel":"m"}],
	  "keys":[{"keyId":"k","tokenHash":%q,"status":"ACTIVE","limits":{},
	           "creditDeniedModels":["OpenAI/*-PRO"," anthropic/claude-opus-4 "]}]}`, hash)
	s := openDoc(t, body)

	_, byHash, _ := s.Current()
	key := byHash(hash)
	if key == nil {
		t.Fatal("key was dropped")
	}
	want := []string{"openai/*-pro", "anthropic/claude-opus-4"}
	if !slices.Equal(key.CreditDeniedModels, want) {
		t.Fatalf("stored %v, want %v", key.CreditDeniedModels, want)
	}
	// End to end: the capitalized stored pattern denies the model a caller
	// sends, variant suffix and all.
	if key.AllowsCreditModel(creditModel("openai/gpt-5-pro:batch")) {
		t.Fatal("a pattern stored with capitals denied nothing after the load")
	}
}

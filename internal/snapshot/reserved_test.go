package snapshot

import (
	"strings"
	"testing"
)

// The reserved-prefix list only works if every entry is lowercase — the guard
// lowercases the candidate name before comparing, so a mixed-case entry would
// silently never match and a just-retired prefix would become billable
// passthrough. This pins the invariant at its definition.
func TestReservedModelPrefixInvariants(t *testing.T) {
	if len(reservedModelPrefixes) == 0 {
		t.Fatal("no reserved prefixes: every curated name would be passthrough-eligible")
	}
	for _, p := range reservedModelPrefixes {
		if p != strings.ToLower(p) {
			t.Fatalf("reserved prefix %q is not lowercase and would never match", p)
		}
		if !IsReservedModelName(p + "anything") {
			t.Fatalf("name under reserved prefix %q escaped the guard", p)
		}
		if !IsReservedModelName(strings.ToUpper(p) + "anything") {
			t.Fatalf("uppercase variant of reserved prefix %q escaped the guard", p)
		}
		// The vendor marks floating aliases with a leading tilde, so the
		// character is spellable and used. Before it was stripped here, one
		// character turned a curated name into a passthrough candidate.
		if !IsReservedModelName("~" + p + "anything") {
			t.Fatalf("tilde-prefixed variant of reserved prefix %q escaped the guard", p)
		}
		if !IsReservedModelName("~" + strings.ToUpper(p) + "anything") {
			t.Fatalf("tilde plus uppercase variant of %q escaped the guard", p)
		}
	}
	// A tilde on a name that is not reserved stays not reserved: the vendor's
	// real aliases must keep routing.
	for _, name := range []string{"gpt-4o", "vendor/x", "picklegeneral", "pnux", "",
		"~anthropic/claude-sonnet-latest", "~", "~picklegeneral"} {
		if IsReservedModelName(name) {
			t.Fatalf("non-reserved name %q wrongly reserved", name)
		}
	}
}

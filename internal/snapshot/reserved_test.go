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
	}
	for _, name := range []string{"gpt-4o", "vendor/x", "picklegeneral", "pnux", ""} {
		if IsReservedModelName(name) {
			t.Fatalf("non-reserved name %q wrongly reserved", name)
		}
	}
}

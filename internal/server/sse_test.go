package server

import (
	"strings"
	"testing"
)

// An event keeps its field values only up to a small bound, so a stream that
// sends huge event: or id: lines cannot make one event hold several times the
// per-event bound in fields chat never reads.
func TestSSEFieldValuesAreBounded(t *testing.T) {
	var e sseEvent
	if got := e.field([]byte(" message_start ")); got != "message_start" || e.fieldTooLong {
		t.Fatalf("ordinary field: %q %v", got, e.fieldTooLong)
	}
	if got := e.field([]byte(strings.Repeat("x", sseFieldMax+1))); got != "" || !e.fieldTooLong {
		t.Fatalf("oversized field kept: %d bytes, flagged %v", len(got), e.fieldTooLong)
	}
}

package reporter

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestFlushPreservesRawAttemptPresence(t *testing.T) {
	dir := t.TempDir()
	ingest, url := newIngest(t)
	lines := []string{
		`{"eventUuid":"legacy","status":"RATE_LIMITED"}`,
		`{"eventUuid":"unknown","status":"RATE_LIMITED","attempts":null}`,
		`{"eventUuid":"local","status":"RATE_LIMITED","attempts":0}`,
		`{"eventUuid":"remote","status":"OK","attempts":2}`,
	}
	if err := os.WriteFile(filepath.Join(dir, "usage-20260810.jsonl"), []byte(strings.Join(lines, "\n")+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	reporter := New(dir, url, "test-observer-token", 500, 5*time.Second, discard())
	if sent, err := reporter.Flush(context.Background()); err != nil || sent != len(lines) {
		t.Fatalf("sent=%d err=%v, want four raw events", sent, err)
	}
	got := ingest.rawSnapshot()
	if len(got) != len(lines) {
		t.Fatalf("received %d events, want four", len(got))
	}
	for i, wire := range got {
		var event map[string]any
		if err := json.Unmarshal([]byte(wire), &event); err != nil {
			t.Fatal(err)
		}
		value, exists := event["attempts"]
		switch i {
		case 0:
			if exists {
				t.Fatal("legacy missing attempts became a reported count")
			}
		case 1:
			if !exists || value != nil {
				t.Fatal("raw explicit null did not remain unknown")
			}
		case 2, 3:
			want := float64(0)
			if i == 3 {
				want = 2
			}
			if !exists || value != want {
				t.Fatalf("known count = %v, want %v", value, want)
			}
		}
	}
	if sent, err := reporter.Flush(context.Background()); err != nil || sent != 0 {
		t.Fatalf("checkpoint replay sent=%d err=%v, want zero", sent, err)
	}
}

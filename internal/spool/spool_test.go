package spool

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
)

func TestWriteRoundTripAndDailyFiles(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	day1 := time.Date(2026, 8, 10, 23, 50, 0, 0, time.UTC)
	day2 := day1.Add(time.Hour)
	ev1 := Event{EventUUID: NewEventUUID(), KeyID: "k1", PublicModelName: "pnu-general",
		Status: StatusOK, InputTokens: 7, OutputTokens: 5, LatencyMs: 120, TtftMs: 40, RequestedAt: day1}
	ev2 := Event{EventUUID: NewEventUUID(), KeyID: "k1", Status: StatusRateLimited,
		ErrorType: "rate_limit_requests", RequestedAt: day2}
	if err := w.Write(ev1); err != nil {
		t.Fatal(err)
	}
	if err := w.Write(ev2); err != nil {
		t.Fatal(err)
	}

	f1 := filepath.Join(dir, "usage-20260810.jsonl")
	f2 := filepath.Join(dir, "usage-20260811.jsonl")
	for _, f := range []string{f1, f2} {
		if _, err := os.Stat(f); err != nil {
			t.Fatalf("expected spool file %s: %v", f, err)
		}
	}

	raw, err := os.ReadFile(f1)
	if err != nil {
		t.Fatal(err)
	}
	var got Event
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	if got.EventUUID != ev1.EventUUID || got.InputTokens != 7 || got.OutputTokens != 5 || got.Status != StatusOK {
		t.Fatalf("round trip mismatch: %+v", got)
	}
}

// The event schema is the accounting contract: no field may carry prompt or
// response content, so the marshaled key set is pinned here. Adding a field
// means deciding, explicitly, that it is not content.
func TestEventCarriesOnlyAccountingFields(t *testing.T) {
	ev := Event{EventUUID: "u", KeyID: "k", PublicModelName: "m", Status: StatusOK,
		ErrorType: "e", InputTokens: 1, OutputTokens: 2, Estimated: true,
		LatencyMs: 3, TtftMs: 4, RequestedAt: time.Now()}
	raw, err := json.Marshal(ev)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	allowed := map[string]bool{
		"eventUuid": true, "generation": true, "keyId": true, "publicModelName": true,
		"status": true, "errorType": true, "inputTokens": true, "outputTokens": true,
		"estimated": true, "latencyMs": true, "ttftMs": true, "requestedAt": true,
	}
	for k := range m {
		if !allowed[k] {
			t.Fatalf("event carries unexpected field %q", k)
		}
	}
}

func TestNewEventUUID(t *testing.T) {
	re := regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
	seen := map[string]bool{}
	for range 100 {
		id := NewEventUUID()
		if !re.MatchString(id) {
			t.Fatalf("bad uuid form: %s", id)
		}
		if seen[id] {
			t.Fatalf("duplicate uuid: %s", id)
		}
		seen[id] = true
	}
}

func TestWriteAppendsOneLinePerEvent(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	at := time.Date(2026, 8, 10, 1, 0, 0, 0, time.UTC)
	for range 3 {
		if err := w.Write(Event{EventUUID: NewEventUUID(), Status: StatusOK, RequestedAt: at}); err != nil {
			t.Fatal(err)
		}
	}
	f, err := os.Open(filepath.Join(dir, "usage-20260810.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	lines := 0
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if strings.TrimSpace(sc.Text()) == "" {
			t.Fatal("blank line in spool")
		}
		lines++
	}
	if lines != 3 {
		t.Fatalf("got %d lines, want 3", lines)
	}
}

func TestPruneRemovesOldFilesKeepsRecentAndCurrent(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	now := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	// Write into "today" so it becomes the current open file.
	if err := w.Write(Event{EventUUID: NewEventUUID(), Status: StatusOK, RequestedAt: now}); err != nil {
		t.Fatal(err)
	}
	// Two older day-files, one just inside retention, one outside.
	old := filepath.Join(dir, "usage-20260101.jsonl") // >90 days before
	recent := filepath.Join(dir, "usage-20260720.jsonl")
	for _, f := range []string{old, recent} {
		if err := os.WriteFile(f, []byte("{}\n"), 0o640); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Prune(now, 90); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(old); !os.IsNotExist(err) {
		t.Fatal("old file was not pruned")
	}
	if _, err := os.Stat(recent); err != nil {
		t.Fatal("in-retention file was pruned")
	}
	if _, err := os.Stat(filepath.Join(dir, "usage-20260810.jsonl")); err != nil {
		t.Fatal("current file was pruned")
	}
	// Retention 0 disables pruning.
	if err := w.Prune(now, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(recent); err != nil {
		t.Fatal("retention 0 must not prune")
	}
}

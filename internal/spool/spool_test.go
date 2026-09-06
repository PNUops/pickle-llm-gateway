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
	ev1 := Event{EventUUID: NewEventUUID(), KeyID: "k1", PublicModelName: "pickle-general", BudgetAxis: "TOKEN",
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
	if got.EventUUID != ev1.EventUUID || got.BudgetAxis != "TOKEN" || got.InputTokens != 7 || got.OutputTokens != 5 || got.Status != StatusOK {
		t.Fatalf("round trip mismatch: %+v", got)
	}
}

// The event schema is the accounting contract: no field may carry prompt or
// response content, so the marshaled key set is pinned here. Adding a field
// means deciding, explicitly, that it is not content.
func TestEventCarriesOnlyAccountingFields(t *testing.T) {
	ev := Event{EventUUID: "u", KeyID: "k", PublicModelName: "m", BudgetAxis: "CREDIT", Status: StatusOK,
		ErrorType: "e", InputTokens: 1, OutputTokens: 2, Estimated: true,
		LatencyMs: 3, TtftMs: 4, RequestedAt: time.Now(),
		Endpoint: EndpointImages, ServedModelName: "vendor/model", CostUsd: "0.242",
		ImageCount: 1, CachedInputTokens: 1, ReasoningTokens: 1, Streamed: true}
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
		"budgetAxis": true, "status": true, "errorType": true, "inputTokens": true, "outputTokens": true,
		"estimated": true, "latencyMs": true, "ttftMs": true, "requestedAt": true,
		// The seven below were judged one at a time against the rule above.
		// Six are plainly measurements: a route name, a price, a count of
		// images, two token subtotals and a boolean.
		//
		// servedModelName is the one that needed deciding. It is a string
		// taken from an upstream response body, which is where content would
		// come from — but the value is the vendor's identifier for a model,
		// chosen from the vendor's own catalogue, and nothing a caller writes
		// can steer it anywhere else. It is bounded on the way in for the same
		// reason: it comes from the network, so it does not get to be any
		// length it likes.
		"endpoint": true, "servedModelName": true, "costUsd": true,
		"imageCount": true, "cachedInputTokens": true, "reasoningTokens": true,
		"streamed": true,
	}
	for k := range m {
		if !allowed[k] {
			t.Fatalf("event carries unexpected field %q", k)
		}
	}
}

// A price is marshaled as its raw literal, so an unvalidated one produces a
// line that is not valid JSON. The reporter ships it, the api answers 400, and
// a 400 is the answer that makes the reporter skip the batch and move its
// checkpoint past it — taking every healthy event beside it. The guard drops
// the price and keeps the event.
func TestCostLiteralRefusesWhatWouldBreakTheLine(t *testing.T) {
	for _, bad := range []json.Number{
		"1e999", "NaN", "-0.5", "0.1; drop", "", json.Number(strings.Repeat("9", 40)),
		// A degenerate negative exponent is the one that gets past every check
		// above it: Float64 returns exactly zero with no error, so nothing
		// here objects, and the literal travels intact to a reader that keeps
		// the exponent as a scale and has to build that power of ten to round.
		"1E-2147483647", "1e-600000000", "1E+2147483647",
	} {
		if got := CostLiteral(bad); got != "" {
			t.Fatalf("CostLiteral(%q) = %q, want empty", bad, got)
		}
	}
	for _, good := range []json.Number{"0", "0.242", "0.00000001", "12.5"} {
		if got := CostLiteral(good); got != good {
			t.Fatalf("CostLiteral(%q) = %q, want it kept", good, got)
		}
	}
	// Every accepted literal has to survive a round trip, which is the
	// property the guard exists for.
	for _, good := range []json.Number{"0", "0.242", "0.00000001", "12.5"} {
		raw, err := json.Marshal(Event{EventUUID: "u", Status: StatusOK, CostUsd: CostLiteral(good)})
		if err != nil {
			t.Fatalf("marshal %q: %v", good, err)
		}
		var back Event
		if err := json.Unmarshal(raw, &back); err != nil {
			t.Fatalf("unmarshal %q: %v (%s)", good, err, raw)
		}
		if back.CostUsd != good {
			t.Fatalf("round trip %q became %q", good, back.CostUsd)
		}
	}
}

// An old spool line has none of the seven, and that shape stays legal: the
// gateway ships them only after the control plane can store them, so the two
// versions coexist on disk.
func TestOldSpoolLineKeepsTheMetricsGroupAbsent(t *testing.T) {
	old := []byte(`{"eventUuid":"old","status":"OK","inputTokens":1,"outputTokens":2,` +
		`"latencyMs":1,"requestedAt":"2026-08-10T00:00:00Z"}`)
	var ev Event
	if err := json.Unmarshal(old, &ev); err != nil {
		t.Fatal(err)
	}
	if ev.Endpoint != "" || ev.ServedModelName != "" || ev.CostUsd != "" ||
		ev.ImageCount != 0 || ev.CachedInputTokens != 0 || ev.ReasoningTokens != 0 ||
		ev.Streamed {
		t.Fatalf("old event gained a metric: %+v", ev)
	}
}

func TestEventWithoutBudgetAxisKeepsTheFieldAbsent(t *testing.T) {
	// Existing spool files predate budgetAxis. They remain valid accounting
	// events, and a request that never resolved a route has the same shape.
	old := []byte(`{"eventUuid":"old","status":"BAD_REQUEST","inputTokens":0,"outputTokens":0,"latencyMs":1,"requestedAt":"2026-08-10T00:00:00Z"}`)
	var ev Event
	if err := json.Unmarshal(old, &ev); err != nil {
		t.Fatal(err)
	}
	if ev.BudgetAxis != "" {
		t.Fatalf("old event gained budgetAxis %q", ev.BudgetAxis)
	}
	raw, err := json.Marshal(ev)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]any
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}
	if _, present := fields["budgetAxis"]; present {
		t.Fatal("empty budgetAxis was serialized")
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
	if err := w.Prune(now, 90, nil); err != nil {
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
	if err := w.Prune(now, 0, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(recent); err != nil {
		t.Fatal("retention 0 must not prune")
	}
}

// Retention must not delete a day the reporter never confirmed: with shipping
// on, an unreported file is the only copy of that usage.
func TestPruneKeepsUnshippedDays(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	now := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	shipped := filepath.Join(dir, "usage-20260101.jsonl")
	unshipped := filepath.Join(dir, "usage-20260102.jsonl")
	for _, f := range []string{shipped, unshipped} {
		if err := os.WriteFile(f, []byte("{}\n"), 0o640); err != nil {
			t.Fatal(err)
		}
	}
	keep := func(day string) bool { return day == "20260101" }
	if err := w.Prune(now, 90, keep); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(shipped); !os.IsNotExist(err) {
		t.Fatal("a shipped, past-retention file was kept")
	}
	if _, err := os.Stat(unshipped); err != nil {
		t.Fatal("an unshipped file was deleted past retention")
	}
}

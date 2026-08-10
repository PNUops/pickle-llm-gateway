package reporter

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// ingestServer stands in for the control plane's usage endpoint, recording
// what it was sent so a test can assert delivery and re-delivery.
type ingestServer struct {
	mu       sync.Mutex
	received []string // eventUuid, in arrival order
	batches  int
	status   int
	auth     string
}

func (s *ingestServer) handler(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	s.auth = r.Header.Get("Authorization")
	status := s.status
	s.mu.Unlock()
	if status != 0 && status != http.StatusOK {
		w.WriteHeader(status)
		return
	}
	var body struct {
		AgentVersion string            `json:"agentVersion"`
		Events       []json.RawMessage `json:"events"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	s.mu.Lock()
	s.batches++
	for _, raw := range body.Events {
		var ev struct {
			EventUUID string `json:"eventUuid"`
		}
		_ = json.Unmarshal(raw, &ev)
		s.received = append(s.received, ev.EventUUID)
	}
	s.mu.Unlock()
	w.WriteHeader(http.StatusOK)
}

func (s *ingestServer) snapshot() ([]string, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.received...), s.batches
}

func (s *ingestServer) setStatus(code int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.status = code
}

func newIngest(t *testing.T) (*ingestServer, string) {
	t.Helper()
	is := &ingestServer{}
	srv := httptest.NewServer(http.HandlerFunc(is.handler))
	t.Cleanup(srv.Close)
	return is, srv.URL
}

// writeEvents appends n complete event lines to a day file.
func writeEvents(t *testing.T, dir, day string, ids ...string) {
	t.Helper()
	path := filepath.Join(dir, "usage-"+day+".jsonl")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o640)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	for _, id := range ids {
		if _, err := fmt.Fprintf(f, `{"eventUuid":%q,"status":"OK"}`+"\n", id); err != nil {
			t.Fatal(err)
		}
	}
}

func discard() *slog.Logger { return slog.New(slog.DiscardHandler) }

func TestFlushShipsAndCheckpoints(t *testing.T) {
	dir := t.TempDir()
	is, url := newIngest(t)
	writeEvents(t, dir, "20260810", "a", "b")
	writeEvents(t, dir, "20260811", "c")

	r := New(dir, url, "tok", 500, 5*time.Second, discard())
	sent, err := r.Flush(context.Background())
	if err != nil || sent != 3 {
		t.Fatalf("sent=%d err=%v", sent, err)
	}
	got, _ := is.snapshot()
	if len(got) != 3 || got[0] != "a" || got[2] != "c" {
		t.Fatalf("delivered in the wrong order or count: %v", got)
	}
	if _, err := os.Stat(filepath.Join(dir, ".shipped")); err != nil {
		t.Fatalf("no checkpoint written: %v", err)
	}

	// A second pass with nothing new must ship nothing — the checkpoint is
	// what stops the same events going twice.
	sent, err = r.Flush(context.Background())
	if err != nil || sent != 0 {
		t.Fatalf("re-flush shipped %d events (err=%v)", sent, err)
	}

	// New events after the checkpoint ship, and only those.
	writeEvents(t, dir, "20260811", "d")
	sent, err = r.Flush(context.Background())
	if err != nil || sent != 1 {
		t.Fatalf("incremental flush sent=%d err=%v", sent, err)
	}
	got, _ = is.snapshot()
	if len(got) != 4 || got[3] != "d" {
		t.Fatalf("unexpected delivery: %v", got)
	}
}

func TestFlushBatchesAndResumesAfterFailure(t *testing.T) {
	dir := t.TempDir()
	is, url := newIngest(t)
	ids := make([]string, 0, 5)
	for i := range 5 {
		ids = append(ids, fmt.Sprintf("e%d", i))
	}
	writeEvents(t, dir, "20260811", ids...)

	r := New(dir, url, "tok", 2, 5*time.Second, discard())
	if _, err := r.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	_, batches := is.snapshot()
	if batches != 3 { // 2 + 2 + 1
		t.Fatalf("batch size not honored: %d batches", batches)
	}

	// The control plane starts refusing; a new pass over new events fails and
	// the checkpoint must not advance past what was accepted.
	writeEvents(t, dir, "20260811", "f0", "f1")
	is.setStatus(http.StatusServiceUnavailable)
	if _, err := r.Flush(context.Background()); err == nil {
		t.Fatal("a refusing control plane was reported as success")
	}
	is.setStatus(http.StatusOK)
	sent, err := r.Flush(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if sent != 2 {
		t.Fatalf("resume shipped %d events, want the 2 that had failed", sent)
	}
	got, _ := is.snapshot()
	if got[len(got)-1] != "f1" {
		t.Fatalf("resume delivered the wrong tail: %v", got)
	}
}

func TestFlushLeavesPartialLine(t *testing.T) {
	dir := t.TempDir()
	is, url := newIngest(t)
	writeEvents(t, dir, "20260811", "a")
	// A line still being written has no terminator yet.
	path := filepath.Join(dir, "usage-20260811.jsonl")
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o640)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(`{"eventUuid":"partial"`); err != nil {
		t.Fatal(err)
	}
	f.Close()

	r := New(dir, url, "tok", 500, 5*time.Second, discard())
	sent, err := r.Flush(context.Background())
	if err != nil || sent != 1 {
		t.Fatalf("sent=%d err=%v (the fragment must not ship)", sent, err)
	}

	// Once the writer finishes the line, the next pass ships it whole.
	f, err = os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o640)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(",\"status\":\"OK\"}\n"); err != nil {
		t.Fatal(err)
	}
	f.Close()
	sent, err = r.Flush(context.Background())
	if err != nil || sent != 1 {
		t.Fatalf("completed line did not ship: sent=%d err=%v", sent, err)
	}
	got, _ := is.snapshot()
	if got[len(got)-1] != "partial" {
		t.Fatalf("unexpected delivery: %v", got)
	}
}

func TestCorruptCheckpointRestarts(t *testing.T) {
	dir := t.TempDir()
	is, url := newIngest(t)
	writeEvents(t, dir, "20260811", "a", "b")
	if err := os.WriteFile(filepath.Join(dir, ".shipped"), []byte("{not json"), 0o640); err != nil {
		t.Fatal(err)
	}
	r := New(dir, url, "tok", 500, 5*time.Second, discard())
	sent, err := r.Flush(context.Background())
	if err != nil || sent != 2 {
		t.Fatalf("a corrupt checkpoint stalled shipping: sent=%d err=%v", sent, err)
	}
	got, _ := is.snapshot()
	if len(got) != 2 {
		t.Fatalf("unexpected delivery: %v", got)
	}
}

func TestAuthHeaderAndVersion(t *testing.T) {
	dir := t.TempDir()
	is, url := newIngest(t)
	writeEvents(t, dir, "20260811", "a")
	r := New(dir, url, "sekret", 500, 5*time.Second, discard())
	if _, err := r.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	is.mu.Lock()
	auth := is.auth
	is.mu.Unlock()
	if auth != "Bearer sekret" {
		t.Fatalf("auth header = %q", auth)
	}
}

// A request that starts before UTC midnight and finishes after it appends to
// yesterday's file after today's has already shipped. A single "we are past
// that day" cursor would skip those events forever.
func TestLateWriteToAnEarlierDayStillShips(t *testing.T) {
	dir := t.TempDir()
	is, url := newIngest(t)
	writeEvents(t, dir, "20260811", "today-1")
	r := New(dir, url, "tok", 500, 5*time.Second, discard())
	if sent, err := r.Flush(context.Background()); err != nil || sent != 1 {
		t.Fatalf("sent=%d err=%v", sent, err)
	}
	// Now the straddling request lands in yesterday's file.
	writeEvents(t, dir, "20260810", "straddler")
	sent, err := r.Flush(context.Background())
	if err != nil || sent != 1 {
		t.Fatalf("a late write to an earlier day was skipped: sent=%d err=%v", sent, err)
	}
	got, _ := is.snapshot()
	if got[len(got)-1] != "straddler" {
		t.Fatalf("unexpected delivery: %v", got)
	}
	// And it is not shipped twice.
	if sent, err := r.Flush(context.Background()); err != nil || sent != 0 {
		t.Fatalf("re-flush shipped %d", sent)
	}
}

// A file matching the glob but not named for a date must not become the
// checkpoint and stall every real file behind it.
func TestStrayFileDoesNotStallShipping(t *testing.T) {
	dir := t.TempDir()
	is, url := newIngest(t)
	writeEvents(t, dir, "20260811", "real-1")
	if err := os.WriteFile(filepath.Join(dir, "usage-backup.jsonl"),
		[]byte(`{"eventUuid":"stray"}`+"\n"), 0o640); err != nil {
		t.Fatal(err)
	}
	r := New(dir, url, "tok", 500, 5*time.Second, discard())
	if _, err := r.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	writeEvents(t, dir, "20260812", "real-2")
	sent, err := r.Flush(context.Background())
	if err != nil || sent != 1 {
		t.Fatalf("a stray file stalled shipping: sent=%d err=%v", sent, err)
	}
	got, _ := is.snapshot()
	for _, id := range got {
		if id == "stray" {
			t.Fatal("a file this package never wrote was shipped")
		}
	}
}

func TestShippedThroughReportsDays(t *testing.T) {
	dir := t.TempDir()
	_, url := newIngest(t)
	writeEvents(t, dir, "20260811", "a")
	r := New(dir, url, "tok", 500, 5*time.Second, discard())
	if _, err := r.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	days, err := r.ShippedThrough()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := days["20260811"]; !ok {
		t.Fatalf("shipped day not reported: %v", days)
	}
	if _, ok := days["20260810"]; ok {
		t.Fatalf("a day that never shipped was reported: %v", days)
	}
}

// A batch the control plane refuses permanently must not become a permanent
// blockage. The checkpoint sits in front of it, so stopping there would hold
// every later event — and every later day — behind one bad batch until
// retention deleted the lot.
func TestPermanentRefusalSkipsTheBatchAndKeepsGoing(t *testing.T) {
	dir := t.TempDir()
	is, url := newIngest(t)
	writeEvents(t, dir, "20260810", "a", "b")
	writeEvents(t, dir, "20260811", "c")

	r := New(dir, url, "tok", 2, 5*time.Second, discard())
	is.setStatus(http.StatusBadRequest)
	if _, err := r.Flush(context.Background()); err != nil {
		t.Fatalf("a permanent refusal was reported as a retryable failure: %v", err)
	}
	if got := r.ShipFailures(); got != 2 {
		t.Fatalf("shipFailures = %d, want the two refused batches counted", got)
	}

	// The refused events are gone, but the channel moved on: what comes next
	// is delivered rather than queued behind them forever.
	is.setStatus(http.StatusOK)
	writeEvents(t, dir, "20260811", "d")
	sent, err := r.Flush(context.Background())
	if err != nil || sent != 1 {
		t.Fatalf("sent=%d err=%v, want the later event delivered", sent, err)
	}
	got, _ := is.snapshot()
	if len(got) != 1 || got[0] != "d" {
		t.Fatalf("delivered %v, want only the event written after the refusal", got)
	}
}

// A refusal that means "not now" must still be retried, or a restarting api
// would cost every event in flight at that moment.
func TestTransientRefusalIsRetried(t *testing.T) {
	for _, status := range []int{http.StatusTooManyRequests, http.StatusRequestTimeout, http.StatusBadGateway} {
		dir := t.TempDir()
		is, url := newIngest(t)
		writeEvents(t, dir, "20260810", "a")
		r := New(dir, url, "tok", 500, 5*time.Second, discard())
		is.setStatus(status)
		if _, err := r.Flush(context.Background()); err == nil {
			t.Fatalf("HTTP %d was treated as a permanent refusal", status)
		}
		if got := r.ShipFailures(); got != 0 {
			t.Fatalf("HTTP %d counted as a dropped batch", status)
		}
		is.setStatus(http.StatusOK)
		sent, err := r.Flush(context.Background())
		if err != nil || sent != 1 {
			t.Fatalf("HTTP %d: retry sent=%d err=%v", status, sent, err)
		}
	}
}

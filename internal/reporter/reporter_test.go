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
	if status != 0 && (status < 200 || status >= 300) {
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
	if status != 0 {
		w.WriteHeader(status)
		return
	}
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

func TestQueueGaugesTrackBacklogAndLastSuccess(t *testing.T) {
	dir := t.TempDir()
	is, url := newIngest(t)
	path := filepath.Join(dir, "usage-20260830.jsonl")
	newer := time.Date(2026, 8, 30, 1, 0, 0, 0, time.UTC)
	oldest := newer.Add(-time.Minute)
	raw := fmt.Sprintf(`{"eventUuid":"a","status":"OK","requestedAt":%q}`+"\n"+
		`{"eventUuid":"b","status":"OK","requestedAt":%q}`+"\n",
		newer.Format(time.RFC3339), oldest.Format(time.RFC3339))
	if err := os.WriteFile(path, []byte(raw), 0o640); err != nil {
		t.Fatal(err)
	}
	r := New(dir, url, "tok", 500, 5*time.Second, discard())
	r.refreshQueueGauges()
	g := r.QueueGauges()
	if g.QueuedEvents != 2 || g.QueuedBytes != int64(len(raw)) ||
		!g.OldestUnshippedAt.Equal(oldest) || g.QueueObservedAt.IsZero() ||
		g.QueueScanFailures != 0 {
		t.Fatalf("initial queue gauges = %+v", g)
	}
	if _, err := r.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	g = r.QueueGauges()
	if g.QueuedEvents != 0 || g.QueuedBytes != 0 || !g.OldestUnshippedAt.IsZero() || g.LastSuccessAt.IsZero() {
		t.Fatalf("post-flush queue gauges = %+v", g)
	}
	got, _ := is.snapshot()
	if len(got) != 2 {
		t.Fatalf("delivered events = %v", got)
	}
}

func TestQueueGaugeScanFailureDoesNotMasqueradeAsAnEmptyQueue(t *testing.T) {
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, ".shipped"), 0o700); err != nil {
		t.Fatal(err)
	}
	r := New(dir, "http://unused.invalid", "tok", 500, 5*time.Second, discard())
	r.refreshQueueGauges()
	g := r.QueueGauges()
	if g.QueueScanFailures != 1 || !g.QueueObservedAt.IsZero() {
		t.Fatalf("failed scan looked observed: %+v", g)
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

// The checkpoint is read by the retention goroutine while the shipping
// goroutine writes it. Two goroutines on one Go map is not a race you survive
// — it is a fatal runtime error that kills the process with no defer, no
// shutdown, nothing. This drives both sides at once.
func TestCheckpointIsSafeForTheRetentionGoroutine(t *testing.T) {
	dir := t.TempDir()
	_, url := newIngest(t)
	for _, day := range []string{"20260801", "20260802", "20260803"} {
		ids := make([]string, 200)
		for i := range ids {
			ids[i] = day + "-" + string(rune('a'+i%26)) + string(rune('a'+i/26))
		}
		writeEvents(t, dir, day, ids...)
	}
	r := New(dir, url, "tok", 5, 5*time.Second, discard())

	var wg sync.WaitGroup
	stop := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			if _, err := r.ShippedThrough(); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	if _, err := r.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	close(stop)
	wg.Wait()
}

// A control plane that answers 404 or 401 is not refusing this batch — it is
// telling us the route or the credential is wrong. Skipping the batch there
// walks the checkpoint to the end of the spool and lets retention delete
// everything; and since the api half of this link does not exist yet, 404 is
// the first answer this code will ever get.
func TestConfigurationFaultsDoNotDestroyTheSpool(t *testing.T) {
	for _, status := range []int{http.StatusUnauthorized, http.StatusForbidden,
		http.StatusNotFound, http.StatusMethodNotAllowed, http.StatusInternalServerError} {
		dir := t.TempDir()
		is, url := newIngest(t)
		writeEvents(t, dir, "20260810", "a", "b", "c")
		r := New(dir, url, "tok", 1, 5*time.Second, discard())
		is.setStatus(status)

		if _, err := r.Flush(context.Background()); err == nil {
			t.Fatalf("HTTP %d was treated as a batch fault; the events would be skipped and the file deleted", status)
		}
		if got := r.ShipFailures(); got != 0 {
			t.Fatalf("HTTP %d counted as dropped batches (%d)", status, got)
		}
		offsets, err := r.ShippedThrough()
		if err != nil {
			t.Fatal(err)
		}
		if off := offsets["20260810"]; off != 0 {
			t.Fatalf("HTTP %d advanced the checkpoint to %d; retention would now delete unshipped events", status, off)
		}

		// And once the route exists, everything is still there.
		is.setStatus(http.StatusOK)
		sent, err := r.Flush(context.Background())
		if err != nil || sent != 3 {
			t.Fatalf("HTTP %d: after recovery sent=%d err=%v, want all three", status, sent, err)
		}
	}
}

// Retention asks the reporter whether a day is safe to delete. A day whose
// first batch shipped and whose tail did not is in the checkpoint map, so
// asking only "is this day known" would delete the events that never went.
func TestFullyShippedIsFalseForAPartiallyShippedDay(t *testing.T) {
	dir := t.TempDir()
	is, url := newIngest(t)
	writeEvents(t, dir, "20260810", "a", "b", "c", "d")
	r := New(dir, url, "tok", 2, 5*time.Second, discard())

	// First batch lands, the second fails: the day is half shipped.
	is.setStatus(http.StatusOK)
	if _, err := r.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !r.FullyShipped("20260810") {
		t.Fatal("a fully shipped day was reported as unsafe to delete")
	}
	// New events arrive after the checkpoint; the day is no longer complete.
	writeEvents(t, dir, "20260810", "e", "f")
	if r.FullyShipped("20260810") {
		t.Fatal("a day with unshipped events was reported as safe to delete")
	}
	if r.FullyShipped("20260811") {
		t.Fatal("a day that was never shipped at all was reported as safe to delete")
	}
}

// A framework answering 201 or 204 is an ordinary thing. Reading only 200 and
// 202 as success would make the control plane store every batch and the
// gateway retry it forever, on a five-minute ceiling, silently.
func TestAnyTwoHundredIsAcceptance(t *testing.T) {
	for _, status := range []int{http.StatusOK, http.StatusCreated, http.StatusAccepted, http.StatusNoContent} {
		dir := t.TempDir()
		is, url := newIngest(t)
		writeEvents(t, dir, "20260810", "a")
		is.setStatus(status)
		r := New(dir, url, "tok", 500, 5*time.Second, discard())
		if _, err := r.Flush(context.Background()); err != nil {
			t.Fatalf("HTTP %d was read as a failure: %v", status, err)
		}
		offsets, _ := r.ShippedThrough()
		if offsets["20260810"] == 0 {
			t.Fatalf("HTTP %d did not advance the checkpoint; the batch would be sent forever", status)
		}
	}
}

// A full disk truncates a write mid-line and the next append lands after it.
// Forwarding that line makes the api refuse the batch as malformed — a batch
// fault, which the gateway skips, taking up to 500 good events with the one
// bad line.
func TestTruncatedSpoolLineDoesNotCostTheBatch(t *testing.T) {
	dir := t.TempDir()
	is, url := newIngest(t)
	path := filepath.Join(dir, "usage-20260810.jsonl")
	body := `{"eventUuid":"a","status":"OK"}` + "\n" +
		`{"eventUuid":"b","statu` + "\n" + // truncated by a full disk
		`{"eventUuid":"c","status":"OK"}` + "\n"
	if err := os.WriteFile(path, []byte(body), 0o640); err != nil {
		t.Fatal(err)
	}
	r := New(dir, url, "tok", 500, 5*time.Second, discard())
	sent, err := r.Flush(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if sent != 2 {
		t.Fatalf("sent %d events, want the two good ones", sent)
	}
	got, _ := is.snapshot()
	if len(got) != 2 || got[0] != "a" || got[1] != "c" {
		t.Fatalf("delivered %v, want the events either side of the bad line", got)
	}
}

package bodies

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func discard() *slog.Logger { return slog.New(slog.DiscardHandler) }

// record builds a record of roughly n bytes of response text.
func record(id string, n int) *Record {
	return &Record{EventUUID: id, KeyID: "k", Response: strings.Repeat("a", n)}
}

// A nil sink is the "capture disabled" value and every method has to survive
// it: the request path calls Offer on every request whether or not capture is
// configured.
func TestNilSinkIsSafe(t *testing.T) {
	var s *Sink
	if s.Enabled() {
		t.Fatal("a nil sink reported itself enabled")
	}
	s.Offer(record("a", 10)) // must not panic
	s.Run(context.Background())
	if s.Dropped() != 0 {
		t.Fatal("a nil sink counted a drop")
	}
}

// The queue is bounded by bytes as well as by depth, because records vary by
// three orders of magnitude and a count bounds nothing on a small host. What
// it must never do is drop everything: a small depth means few records, not no
// records.
func TestSmallQueueStillAcceptsAMaximalRecord(t *testing.T) {
	for _, depth := range []int{1, 2, 4, 8, 256} {
		s := New("http://unused", "tok", depth, 10, time.Second, discard())
		s.Offer(record("big", ResponseCapBytes))
		if s.Dropped() != 0 {
			t.Fatalf("queue depth %d dropped a single maximal record: capture would be configured and collect nothing", depth)
		}
		if len(s.queue) != 1 {
			t.Fatalf("queue depth %d did not hold the record", depth)
		}
	}
}

// Two bounds, and both have to hold: the depth stops a flood of small records,
// the byte bound stops a handful of large ones.
func TestQueueDropsOnBothBounds(t *testing.T) {
	t.Run("depth", func(t *testing.T) {
		s := New("http://unused", "tok", 2, 10, time.Second, discard())
		for i := range 5 {
			s.Offer(record(string(rune('a'+i)), 1))
		}
		if s.Dropped() != 3 {
			t.Fatalf("dropped %d, want the 3 that did not fit a depth of 2", s.Dropped())
		}
	})
	t.Run("bytes", func(t *testing.T) {
		// A deep queue whose byte bound is reached first.
		s := New("http://unused", "tok", 64, 10, time.Second, discard())
		accepted := 0
		for range 64 {
			before := s.Dropped()
			s.Offer(record("big", ResponseCapBytes))
			if s.Dropped() == before {
				accepted++
			}
		}
		if accepted == 64 {
			t.Fatal("the byte bound never engaged; a full queue would hold far more than the host has")
		}
		if accepted == 0 {
			t.Fatal("the byte bound rejected everything")
		}
		if s.queuedBytes.Load() > s.maxBytes {
			t.Fatalf("queued %d bytes over a bound of %d", s.queuedBytes.Load(), s.maxBytes)
		}
	})
}

type sink struct {
	mu      sync.Mutex
	batches [][]Record
	status  int
}

func (c *sink) handler(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Records []Record `json:"records"`
	}
	_ = json.NewDecoder(r.Body).Decode(&in)
	_, _ = io.Copy(io.Discard, r.Body)
	c.mu.Lock()
	c.batches = append(c.batches, in.Records)
	status := c.status
	c.mu.Unlock()
	if status != 0 {
		w.WriteHeader(status)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (c *sink) seen() [][]Record {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([][]Record(nil), c.batches...)
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timed out waiting")
}

// A batch is cut by bytes as well as by count. Count alone would build a batch
// the receiver refuses for size — and a refused body batch is dropped, not
// retried, so the text is simply lost.
func TestBatchIsCutByBytes(t *testing.T) {
	c := &sink{}
	srv := httptest.NewServer(http.HandlerFunc(c.handler))
	defer srv.Close()
	// A batch count far above what the byte budget allows.
	s := New(srv.URL, "tok", 512, 10_000, 5*time.Second, discard())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.Run(ctx)

	per := ResponseCapBytes
	n := (BatchCapBytes / per) * 3
	for i := range n {
		s.Offer(record(string(rune('a'+i%26)), per))
	}
	waitFor(t, func() bool { return len(c.seen()) >= 2 })
	for i, b := range c.seen() {
		total := 0
		for j := range b {
			total += b[j].size()
		}
		if total > BatchCapBytes+per {
			t.Fatalf("batch %d carried %d bytes, over the budget", i, total)
		}
	}
}

// A delivery the control plane refuses is dropped, not retried — holding
// records for a retry is exactly the accumulation this channel avoids. What it
// must not do is lose the loss: the count is the only trace.
func TestFailedDeliveryIsCounted(t *testing.T) {
	c := &sink{status: http.StatusInternalServerError}
	srv := httptest.NewServer(http.HandlerFunc(c.handler))
	defer srv.Close()
	s := New(srv.URL, "tok", 64, 2, 5*time.Second, discard())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.Run(ctx)

	s.Offer(record("a", 10))
	s.Offer(record("b", 10))
	waitFor(t, func() bool { return s.Dropped() >= 2 })
	if got := s.Dropped(); got != 2 {
		t.Fatalf("dropped count = %d, want the whole refused batch", got)
	}
}

// Any 2xx is acceptance. Reading only 200 and 202 as success would silently
// discard captured text whenever the api answers 201 or 204.
func TestAnySuccessStatusKeepsTheRecords(t *testing.T) {
	for _, status := range []int{http.StatusOK, http.StatusCreated, http.StatusAccepted, http.StatusNoContent} {
		c := &sink{status: status}
		srv := httptest.NewServer(http.HandlerFunc(c.handler))
		s := New(srv.URL, "tok", 64, 1, 5*time.Second, discard())
		ctx, cancel := context.WithCancel(context.Background())
		go s.Run(ctx)
		s.Offer(record("a", 10))
		waitFor(t, func() bool { return len(c.seen()) >= 1 })
		time.Sleep(50 * time.Millisecond)
		if got := s.Dropped(); got != 0 {
			t.Fatalf("HTTP %d counted as a failure (%d dropped)", status, got)
		}
		cancel()
		srv.Close()
	}
}

package server

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pnuops/pickle-llm-gateway/internal/config"
	"github.com/pnuops/pickle-llm-gateway/internal/snapshot"
	"github.com/pnuops/pickle-llm-gateway/internal/spool"
)

const embeddingBody = `{"model":"openai/text-embedding-3-large","input":"hi"}`

// embeddingAnswer is a passthrough answer of about size bytes.
func embeddingAnswer(size int) string {
	return `{"model":"openai/text-embedding-3-large","data":[{"embedding":"` + strings.Repeat("A", size) +
		`"}],"usage":{"prompt_tokens":3,"total_tokens":3}}`
}

// Small requests with small answers take no large-body slot, so many more of
// them run at once than the pool has slots. This is the embedding class the
// two tiers exist for: dozens of small calls at once, each of which used to
// take one of a handful of slots sized for a 4K picture.
func TestSmallPassthroughTakesNoLargeSlot(t *testing.T) {
	const n = 4
	release := make(chan struct{})
	var releaseOnce sync.Once
	unpark := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(unpark)
	h := newHarness(t, passthroughDoc(snapshot.EndpointEmbeddings, snapshot.EndpointImages), func(c *config.Config) {
		c.PassthroughMaxInFlight = 1
	})
	h.mock.set(func(o *mockOpts) { o.block = release })
	statuses := make(chan int, n+1)
	var wg sync.WaitGroup
	wg.Add(n + 1)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			statuses <- h.passthroughStatus(http.MethodPost, "/v1/embeddings", testToken, embeddingBody)
		}()
	}
	// The catalogue read carries no body at all and takes no slot either.
	go func() {
		defer wg.Done()
		statuses <- h.passthroughStatus(http.MethodGet, "/v1/images/models", testToken, "")
	}()
	deadline := time.Now().Add(5 * time.Second)
	for h.mock.blockedCount() < n+1 {
		if time.Now().After(deadline) {
			t.Fatalf("only %d of %d small requests reached the upstream with one slot", h.mock.blockedCount(), n+1)
		}
		time.Sleep(2 * time.Millisecond)
	}
	if got := h.srv.PassthroughInFlight(); got != 0 {
		t.Fatalf("small requests took %d large-body slots", got)
	}
	if got := h.srv.InFlight(); got != n+1 {
		t.Fatalf("gateway-wide in flight %d, want %d", got, n+1)
	}
	unpark()
	wg.Wait()
	close(statuses)
	for st := range statuses {
		if st != http.StatusOK {
			t.Fatalf("a small request failed: %d", st)
		}
	}
}

// A small request is bounded by the gateway-wide cap like chat, and refused
// by it the same way.
func TestSmallPassthroughAnswersToTheGatewayCap(t *testing.T) {
	h := newHarness(t, passthroughDoc(snapshot.EndpointEmbeddings), func(c *config.Config) {
		c.MaxInFlight = 1
	})
	h.srv.inFlight <- struct{}{}
	status, body := h.passthrough(t, http.MethodPost, "/v1/embeddings", testToken, embeddingBody)
	<-h.srv.inFlight
	if status != http.StatusServiceUnavailable || errCode(t, body) != "server_busy" {
		t.Fatalf("gateway cap: %d %s", status, body)
	}
	if h.mock.callCount() != 0 {
		t.Fatal("a refused request reached the upstream")
	}
}

// An answer past the relay bound takes a slot and holds it until the client
// has read it; one under it does not. Both a declared length and an
// undeclared one, which crosses the bound while being read.
func TestLargePassthroughAnswerTakesASlot(t *testing.T) {
	const size = 32 << 20
	big := embeddingAnswer(size)
	for _, declared := range []bool{true, false} {
		h := newHarness(t, passthroughDoc(snapshot.EndpointEmbeddings), func(c *config.Config) {
			c.PassthroughResponseMaxBytes = 48 << 20
			c.PassthroughMaxInFlight = 1
		})
		h.srv.passthroughClient = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			length := int64(-1)
			if declared {
				length = int64(len(big))
			}
			return &http.Response{StatusCode: 200, Header: make(http.Header), Request: r,
				ContentLength: length, Body: io.NopCloser(strings.NewReader(big))}, nil
		})}
		req, _ := http.NewRequest(http.MethodPost, h.gw.URL+"/v1/embeddings", strings.NewReader(embeddingBody))
		req.Header.Set("Authorization", "Bearer "+testToken)
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode != 200 {
			t.Fatalf("declared=%v: status %d", declared, resp.StatusCode)
		}
		// A 32 MiB answer cannot all sit in socket buffers, so the write is
		// still running and the slot still held.
		if n := h.srv.PassthroughInFlight(); n != 1 {
			t.Fatalf("declared=%v: slots in use before the client reads: %d", declared, n)
		}
		k, _ := io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if k < size {
			t.Fatalf("declared=%v: read %d bytes", declared, k)
		}
		deadline := time.Now().Add(5 * time.Second)
		for h.srv.PassthroughInFlight() != 0 {
			if time.Now().After(deadline) {
				t.Fatalf("declared=%v: the slot was not released after the client read the answer", declared)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}

// With every slot taken, an answer past the relay bound is refused by name
// and the request gives back nothing it did not take; one under the bound
// still passes.
func TestLargePassthroughAnswerWithNoSlot(t *testing.T) {
	big := embeddingAnswer(9 << 20)
	for _, declared := range []bool{true, false} {
		h := newHarness(t, passthroughDoc(snapshot.EndpointEmbeddings), func(c *config.Config) {
			c.PassthroughResponseMaxBytes = 16 << 20
			c.PassthroughMaxInFlight = 1
		})
		answer := big
		h.srv.passthroughClient = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			length := int64(-1)
			if declared {
				length = int64(len(answer))
			}
			return &http.Response{StatusCode: 200, Header: make(http.Header), Request: r,
				ContentLength: length, Body: io.NopCloser(strings.NewReader(answer))}, nil
		})}
		held, ok := h.srv.passthroughLarge.acquire("someone-else")
		if !ok {
			t.Fatal("could not take the only slot")
		}
		status, body := h.passthrough(t, http.MethodPost, "/v1/embeddings", testToken, embeddingBody)
		if status != http.StatusBadGateway || errCode(t, body) != "upstream_response_too_large" {
			t.Fatalf("declared=%v: no slot: %d %s", declared, status, body)
		}
		if n := h.srv.PassthroughInFlight(); n != 1 {
			t.Fatalf("declared=%v: slots in use after the refusal: %d", declared, n)
		}
		answer = embeddingAnswer(1 << 10)
		if status, body := h.passthrough(t, http.MethodPost, "/v1/embeddings", testToken, embeddingBody); status != 200 {
			t.Fatalf("declared=%v: small answer with the pool full: %d %s", declared, status, body)
		}
		held()
		if n := h.srv.PassthroughInFlight(); n != 0 {
			t.Fatalf("declared=%v: slots in use at the end: %d", declared, n)
		}
	}
}

// A large answer past the large bound is refused even with a slot, and the
// slot comes back.
func TestLargePassthroughAnswerPastTheCap(t *testing.T) {
	big := embeddingAnswer(9 << 20)
	h := newHarness(t, passthroughDoc(snapshot.EndpointEmbeddings), func(c *config.Config) {
		c.PassthroughResponseMaxBytes = 9 << 20
	})
	h.srv.passthroughClient = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: make(http.Header), Request: r,
			ContentLength: -1, Body: io.NopCloser(strings.NewReader(big))}, nil
	})}
	status, body := h.passthrough(t, http.MethodPost, "/v1/embeddings", testToken, embeddingBody)
	if status != http.StatusBadGateway || errCode(t, body) != "upstream_response_too_large" {
		t.Fatalf("%d %s", status, body)
	}
	if n := h.srv.PassthroughInFlight(); n != 0 {
		t.Fatalf("slots in use after the refusal: %d", n)
	}
}

// The write to the client runs under the deadline chat's large answers get,
// sized to this answer, and the deadline is cleared afterwards.
func TestPassthroughWriteDeadline(t *testing.T) {
	h := newHarness(t, nil, func(c *config.Config) { c.PassthroughResponseMaxBytes = 16 << 20 })
	answer := embeddingAnswer(3 << 20)
	resp := &http.Response{StatusCode: 200, Header: make(http.Header),
		ContentLength: int64(len(answer)), Body: io.NopCloser(strings.NewReader(answer))}
	gate := make(chan struct{})
	close(gate)
	w := &gatedWriter{ResponseRecorder: httptest.NewRecorder(), gate: gate, entered: make(chan struct{})}
	var ev spool.Event
	start := time.Now()
	h.srv.finishPassthrough(w, resp, routeEmbeddings, &passthroughLargeHold{pool: newLargeBodySlots(1)},
		len(embeddingBody), &ev, func() {})
	if ev.Status != spool.StatusOK {
		t.Fatalf("status %s %s", ev.Status, ev.ErrorType)
	}
	if len(w.deadlines) != 2 || !w.deadlines[1].IsZero() {
		t.Fatalf("write deadlines: %v", w.deadlines)
	}
	want := start.Add(largeWriteBudget(len(answer)))
	if d := w.deadlines[0].Sub(want); d < -time.Second || d > time.Second {
		t.Fatalf("deadline %s off the budget by %s", w.deadlines[0], d)
	}
	if largeWriteBudget(len(answer)) >= 10*time.Minute {
		t.Fatal("the budget is no tighter than the request duration")
	}
}

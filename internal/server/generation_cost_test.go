package server

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pnuops/pickle-llm-gateway/internal/config"
)

const (
	testGenerationID   = "gen-1791954752-wmqpHWc3JegdXJaI3hC1"
	creditStreamBody   = `{"model":"vendor-model","stream":true,"messages":[{"role":"user","content":"hi"}]}`
	generationRecordOK = `{"data":{"id":"` + testGenerationID + `","streamed":true,"usage":0.000023,"total_cost":0.000023}}`
)

// generationVendor stands in front of the harness mock: it stamps a
// generation id on every answer, as the vendor does, and serves the
// generation record from a script of answers, the last one repeating.
type generationVendor struct {
	mu      sync.Mutex
	inner   http.HandlerFunc
	answers []generationAnswer
	reads   int
	auths   []string
}

type generationAnswer struct {
	status int
	body   string
}

func (g *generationVendor) handler(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/generation" {
		g.mu.Lock()
		g.auths = append(g.auths, r.Header.Get("Authorization"))
		a := g.answers[min(g.reads, len(g.answers)-1)]
		g.reads++
		g.mu.Unlock()
		if r.URL.Query().Get("id") != testGenerationID {
			a = generationAnswer{400, `{"error":{"message":"bad id","code":400}}`}
		}
		w.WriteHeader(a.status)
		_, _ = io.WriteString(w, a.body)
		return
	}
	w.Header().Set(generationIDHeader, testGenerationID)
	g.mu.Lock()
	inner := g.inner
	g.mu.Unlock()
	inner(w, r)
}

func (g *generationVendor) setInner(f http.HandlerFunc) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.inner = f
}

func (g *generationVendor) readCount() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.reads
}

func generationHarness(t *testing.T, delays []time.Duration, answers ...generationAnswer) (*harness, *generationVendor) {
	t.Helper()
	g := &generationVendor{answers: answers}
	front := httptest.NewServer(http.HandlerFunc(g.handler))
	t.Cleanup(front.Close)
	h := newHarness(t, creditDoc, func(c *config.Config) {
		up := c.Upstreams["mock"]
		up.BaseURL = front.URL
		c.Upstreams["mock"] = up
	})
	g.setInner(h.mock.handler)
	h.srv.costs.delays = delays
	return h, g
}

// The mock's stream reports tokens but no cost, which is what the vendor's
// Messages surface does. The cost is read from the generation record after
// the stream, past the first read's 404, and the wait is not latency.
func TestStreamWithoutCostReadsTheGenerationRecord(t *testing.T) {
	h, g := generationHarness(t, []time.Duration{300 * time.Millisecond, 10 * time.Millisecond},
		generationAnswer{404, `{"error":{"message":"Generation not found","code":404}}`},
		generationAnswer{200, generationRecordOK})
	status, body := h.chat(t, testToken, creditStreamBody)
	if status != 200 || !strings.Contains(string(body), "[DONE]") {
		t.Fatalf("%d %s", status, body)
	}
	h.srv.DrainCostLookups(context.Background())
	ev := lastEvent(t, h)
	if ev.CostUsd != "0.000023" || ev.InputTokens != 7 || ev.OutputTokens != 5 || ev.Estimated {
		t.Fatalf("record: %+v", ev)
	}
	if ev.LatencyMs >= 300 {
		t.Fatalf("latency %dms counts the lookup", ev.LatencyMs)
	}
	g.mu.Lock()
	reads, auths := g.reads, g.auths
	g.mu.Unlock()
	if reads != 2 || auths[0] != "Bearer "+keyCred {
		t.Fatalf("reads %d, auth %v", reads, auths)
	}
	if n := h.srv.metrics.streamCostFilled.Load(); n != 1 {
		t.Fatalf("streamCostFilled %d", n)
	}
}

// Only a money-axis stream has a price to look up; a self-hosted one and a
// non-streamed answer never read the record.
func TestGenerationRecordOnlyForMoneyAxisStreams(t *testing.T) {
	h, g := generationHarness(t, []time.Duration{time.Millisecond}, generationAnswer{200, generationRecordOK})
	if status, body := h.chat(t, testToken, streamRequest); status != 200 {
		t.Fatalf("%d %s", status, body)
	}
	if status, body := h.chat(t, testToken, creditChatBody); status != 200 {
		t.Fatalf("%d %s", status, body)
	}
	h.srv.DrainCostLookups(context.Background())
	if g.readCount() != 0 {
		t.Fatalf("generation record read %d times", g.readCount())
	}
	if evs := h.spoolEvents(t); len(evs) != 2 || evs[0].CostUsd != "" || evs[1].CostUsd != "" {
		t.Fatalf("events: %+v", evs)
	}
}

// A record that never appears, or a refused credential, leaves the event
// without a cost, written once the schedule ends; a refusal is not retried.
func TestGenerationRecordNotFoundWritesWithoutCost(t *testing.T) {
	for _, tc := range []struct {
		name   string
		answer generationAnswer
		reads  int
	}{
		{"never published", generationAnswer{404, `{"error":{"code":404}}`}, 3},
		{"credential refused", generationAnswer{401, `{"error":{"code":401}}`}, 1},
		{"no total_cost", generationAnswer{200, `{"data":{"id":"x"}}`}, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, g := generationHarness(t, []time.Duration{time.Millisecond, time.Millisecond, time.Millisecond}, tc.answer)
			if status, body := h.chat(t, testToken, creditStreamBody); status != 200 {
				t.Fatalf("%d %s", status, body)
			}
			h.srv.DrainCostLookups(context.Background())
			if ev := lastEvent(t, h); ev.CostUsd != "" || ev.InputTokens != 7 {
				t.Fatalf("record: %+v", ev)
			}
			if g.readCount() != tc.reads {
				t.Fatalf("reads %d, want %d", g.readCount(), tc.reads)
			}
			if n := h.srv.metrics.streamCostMissing.Load(); n != 1 {
				t.Fatalf("streamCostMissing %d", n)
			}
		})
	}
}

// Shutdown does not wait out the schedule: once its grace ends, a pending
// lookup writes its event without a cost rather than lose it with the spool.
func TestDrainCostLookupsWritesPendingEvents(t *testing.T) {
	h, _ := generationHarness(t, []time.Duration{time.Hour}, generationAnswer{200, generationRecordOK})
	if status, body := h.chat(t, testToken, creditStreamBody); status != 200 {
		t.Fatalf("%d %s", status, body)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	done := make(chan struct{})
	go func() { h.srv.DrainCostLookups(ctx); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("drain did not return")
	}
	if ev := lastEvent(t, h); ev.CostUsd != "" || ev.KeyID == "" {
		t.Fatalf("record: %+v", ev)
	}
}

// A stream whose own usage carries the price needs no lookup.
func TestStreamWithCostSkipsTheGenerationRecord(t *testing.T) {
	h, g := generationHarness(t, []time.Duration{time.Millisecond}, generationAnswer{200, generationRecordOK})
	// The stream below carries usage with a cost in its final chunk.
	g.setInner(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, `data: {"id":"c1","object":"chat.completion.chunk","model":"`+upstreamModel+
			`","choices":[{"index":0,"delta":{"content":"hi"},"finish_reason":"stop"}]}`+"\n\n")
		_, _ = io.WriteString(w, `data: {"id":"c1","object":"chat.completion.chunk","model":"`+upstreamModel+
			`","choices":[],"usage":{"prompt_tokens":7,"completion_tokens":5,"total_tokens":12,"cost":0.0005}}`+"\n\n")
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
	})
	if status, body := h.chat(t, testToken, creditStreamBody); status != 200 {
		t.Fatalf("%d %s", status, body)
	}
	h.srv.DrainCostLookups(context.Background())
	if ev := lastEvent(t, h); ev.CostUsd != "0.0005" {
		t.Fatalf("record: %+v", ev)
	}
	if g.readCount() != 0 {
		t.Fatalf("generation record read %d times", g.readCount())
	}
}

// On a surface that keeps estimates out of its records, a stream without
// usage loses its token counts but keeps the vendor's billed cost: that figure
// is not an estimate.
func TestGenerationCostSurvivesDroppedEstimate(t *testing.T) {
	h, _ := generationHarness(t, []time.Duration{time.Millisecond}, generationAnswer{200, generationRecordOK})
	h.mock.set(func(o *mockOpts) { o.noUsage = true })
	p := chatVariant(func(p *protocol) { p.noUsageEstimate = true })
	if rec := serveVariant(t, h, p, nil, creditStreamBody); rec.Code != 200 {
		t.Fatalf("%d %s", rec.Code, rec.Body)
	}
	h.srv.DrainCostLookups(context.Background())
	ev := lastEvent(t, h)
	if ev.ErrorType != "upstream_usage_missing" || ev.InputTokens != 0 || ev.Estimated || ev.CostUsd != "0.000023" {
		t.Fatalf("record: %+v", ev)
	}
}

// A pending lookup never holds the client's stream, and past the pending
// bound an event is written at once without a cost. After a drain has begun
// nothing more is admitted.
func TestGenerationLookupsDoNotBlockAndAreBounded(t *testing.T) {
	h, g := generationHarness(t, []time.Duration{time.Hour}, generationAnswer{200, generationRecordOK})
	h.srv.costs.slots = make(chan struct{}, 1)

	start := time.Now()
	if status, body := h.chat(t, testToken, creditStreamBody); status != 200 {
		t.Fatalf("%d %s", status, body)
	}
	if took := time.Since(start); took > 5*time.Second {
		t.Fatalf("the stream waited %s for its lookup", took)
	}
	if evs := h.spoolEvents(t); len(evs) != 0 {
		t.Fatalf("pending lookup already wrote: %+v", evs)
	}
	// The one slot is taken, so this stream is written now, without a cost.
	if status, body := h.chat(t, testToken, creditStreamBody); status != 200 {
		t.Fatalf("%d %s", status, body)
	}
	if evs := h.spoolEvents(t); len(evs) != 1 || evs[0].CostUsd != "" {
		t.Fatalf("overflow event: %+v", evs)
	}
	if n := h.srv.metrics.streamCostMissing.Load(); n != 1 {
		t.Fatalf("streamCostMissing %d", n)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	h.srv.DrainCostLookups(ctx)
	if evs := h.spoolEvents(t); len(evs) != 2 {
		t.Fatalf("after drain: %d events", len(evs))
	}
	// Admission is closed once draining: written at once, nothing read.
	if status, body := h.chat(t, testToken, creditStreamBody); status != 200 {
		t.Fatalf("%d %s", status, body)
	}
	if evs := h.spoolEvents(t); len(evs) != 3 || evs[2].CostUsd != "" {
		t.Fatalf("event after drain: %+v", evs)
	}
	if g.readCount() != 0 {
		t.Fatalf("generation record read %d times", g.readCount())
	}
}

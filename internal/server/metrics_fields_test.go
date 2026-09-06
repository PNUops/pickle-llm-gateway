package server

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/pnuops/pickle-llm-gateway/internal/spool"
)

// The vendor reports what it charged on the same response the tokens come
// from. Reading it is what puts a dollar figure in the accounting, which until
// now had only the vendor's own cumulative meters and so could say how much
// was spent but never on what.
func TestChatRecordsThePriceAndTheTokenBreakdown(t *testing.T) {
	h := newHarness(t, nil, nil)
	h.mock.set(func(o *mockOpts) {
		o.rawResp = `{"id":"c1","object":"chat.completion","model":"` + upstreamModel + `",` +
			`"choices":[{"index":0,"message":{"role":"assistant","content":"hi"},` +
			`"finish_reason":"stop"}],` +
			`"usage":{"prompt_tokens":100,"completion_tokens":40,"total_tokens":140,` +
			`"cost":0.0012345,"prompt_tokens_details":{"cached_tokens":80},` +
			`"completion_tokens_details":{"reasoning_tokens":25}}}`
	})
	if status, body := h.chat(t, testToken, chatBody); status != 200 {
		t.Fatalf("%d %s", status, body)
	}
	evs := h.spoolEvents(t)
	if len(evs) != 1 {
		t.Fatalf("want 1 event, got %d", len(evs))
	}
	ev := evs[0]
	if ev.CostUsd != "0.0012345" {
		t.Fatalf("price %q", ev.CostUsd)
	}
	// The breakdowns are subsets of the counts, never additions to them: 80 of
	// the 100 input tokens were cached, 25 of the 40 output tokens were spent
	// thinking. A reader adding either into a total double-counts.
	if ev.InputTokens != 100 || ev.CachedInputTokens != 80 {
		t.Fatalf("input side: %+v", ev)
	}
	if ev.OutputTokens != 40 || ev.ReasoningTokens != 25 {
		t.Fatalf("output side: %+v", ev)
	}
	if ev.Endpoint != spool.EndpointChat {
		t.Fatalf("route %q", ev.Endpoint)
	}
}

// An estimate has no price and no breakdown, and leaving either behind would
// put a number claiming to be exact next to the flag that says the tokens are
// not.
func TestAnEstimatedEventCarriesNoPrice(t *testing.T) {
	h := newHarness(t, nil, nil)
	h.mock.set(func(o *mockOpts) { o.noUsage = true })
	if status, body := h.chat(t, testToken, chatBody); status != 200 {
		t.Fatalf("%d %s", status, body)
	}
	evs := h.spoolEvents(t)
	if len(evs) != 1 || !evs[0].Estimated {
		t.Fatalf("want one estimated event, got %+v", evs)
	}
	if evs[0].CostUsd != "" || evs[0].CachedInputTokens != 0 || evs[0].ReasoningTokens != 0 {
		t.Fatalf("estimate carried exact-looking values: %+v", evs[0])
	}
}

// TtftMs cannot answer whether a request was streamed: it is stamped when the
// upstream's response headers arrive, so a non-streaming request carries one
// too. That is why the flag exists at all.
func TestStreamedIsRecordedAndIsNotInferableFromTtft(t *testing.T) {
	h := newHarness(t, nil, nil)
	// The delay is what makes the second half of this test mean anything: the
	// mock otherwise answers inside a millisecond and every ttftMs rounds to
	// zero, which would look like the flag was inferable after all.
	h.mock.set(func(o *mockOpts) { o.delay = 20 * time.Millisecond })
	if status, body := h.chat(t, testToken, chatBody); status != 200 {
		t.Fatalf("non-stream: %d %s", status, body)
	}
	streamBody := strings.Replace(chatBody, `"messages"`, `"stream":true,"messages"`, 1)
	if status, body := h.chat(t, testToken, streamBody); status != 200 {
		t.Fatalf("stream: %d %s", status, body)
	}
	evs := h.spoolEvents(t)
	if len(evs) != 2 {
		t.Fatalf("want 2 events, got %d", len(evs))
	}
	if evs[0].Streamed {
		t.Fatal("non-streaming request recorded as streamed")
	}
	if !evs[1].Streamed {
		t.Fatal("streaming request not recorded as streamed")
	}
	if evs[0].TtftMs == 0 {
		t.Fatal("the non-streaming request has no ttftMs, so this test proves nothing")
	}
}

// The served name is written on every metered response rather than only on a
// mismatch. Writing it only when the two differ makes an empty value mean both
// "the vendor reported nothing" and "the same model we asked for".
func TestServedModelIsRecordedEvenWhenItMatches(t *testing.T) {
	h := newHarness(t, nil, nil)
	if status, body := h.chat(t, testToken, chatBody); status != 200 {
		t.Fatalf("%d %s", status, body)
	}
	evs := h.spoolEvents(t)
	if len(evs) != 1 || evs[0].ServedModelName != upstreamModel {
		t.Fatalf("served name: %+v", evs)
	}
	// The client still sees the public name; the record is where the upstream
	// one lives.
	if evs[0].PublicModelName == evs[0].ServedModelName {
		t.Fatal("the public and served names are the same, so this test proves nothing")
	}
}

// The name comes off the network, so it is bounded here rather than left to
// the control plane. An oversized one would otherwise sit in a 90-day spool on
// a small disk.
func TestServedModelNameIsBounded(t *testing.T) {
	h := newHarness(t, nil, nil)
	long := strings.Repeat("z", 400)
	h.mock.set(func(o *mockOpts) {
		o.rawResp = `{"id":"c1","object":"chat.completion","model":"` + long + `",` +
			`"choices":[{"index":0,"message":{"role":"assistant","content":"hi"},` +
			`"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1}}`
	})
	if status, body := h.chat(t, testToken, chatBody); status != 200 {
		t.Fatalf("%d %s", status, body)
	}
	evs := h.spoolEvents(t)
	if len(evs) != 1 || len(evs[0].ServedModelName) != servedModelMax {
		t.Fatalf("served name length %d", len(evs[0].ServedModelName))
	}
}

// A refusal answered before any upstream is contacted still says which route
// it came in on, so "what are people being refused on" stays answerable.
func TestARefusedChatStillCarriesItsRoute(t *testing.T) {
	h := newHarness(t, nil, nil)
	status, _ := h.chat(t, testToken, `{"model":"no-such-model","messages":[]}`)
	if status != http.StatusForbidden && status != http.StatusBadRequest &&
		status != http.StatusNotFound {
		t.Fatalf("unexpected status %d", status)
	}
	evs := h.spoolEvents(t)
	if len(evs) != 1 || evs[0].Endpoint != spool.EndpointChat {
		t.Fatalf("refusal event: %+v", evs)
	}
}

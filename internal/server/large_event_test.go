package server

import (
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/pnuops/pickle-llm-gateway/internal/config"
)

// largeEventStream is a stream whose first event carries content of the
// given size in one `data:` line, the way an image generation streams its
// picture, followed by an ordinary end.
func largeEventStream(size int) string {
	return `data: {"model":"` + upstreamModel + `","choices":[{"index":0,"delta":{"content":"` +
		strings.Repeat("p", size) + `"}}]}` + "\n\n" +
		`data: {"model":"` + upstreamModel + `","choices":[{"index":0,"delta":{},"finish_reason":"stop"}],` +
		`"usage":{"prompt_tokens":3,"completion_tokens":4}}` + "\n\n" +
		"data: [DONE]\n\n"
}

func streamingHarness(t *testing.T, mutate func(*config.Config), stream string) *harness {
	t.Helper()
	h := newHarness(t, nil, mutate)
	h.srv.client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"text/event-stream"}}, Request: r,
			Body: io.NopCloser(strings.NewReader(stream))}, nil
	})}
	return h
}

const largeEventRequest = `{"model":"pickle-general","stream":true,"messages":[{"role":"user","content":"draw"}]}`

// An event past the ordinary bound is relayed whole when a large-body slot is
// free, and the slot is given back once the event has gone out.
func TestLargeStreamEventTakesASlot(t *testing.T) {
	h := streamingHarness(t, largeCfg(2, 2), largeEventStream(9<<20))
	status, body := h.chat(t, testToken, largeEventRequest)
	if status != 200 {
		t.Fatalf("status %d", status)
	}
	if strings.Contains(string(body), "upstream_response_too_large") {
		t.Fatalf("a large event was refused with a slot free: %s", body[max(0, len(body)-300):])
	}
	if !strings.Contains(string(body), strings.Repeat("p", 9<<20)) || !strings.HasSuffix(string(body), "data: [DONE]\n\n") {
		t.Fatalf("the event or the end did not reach the client (%d bytes)", len(body))
	}
	if n := h.srv.chatLarge.inUse(); n != 0 {
		t.Fatalf("the event's slot was not released: %d", n)
	}
	evs := h.spoolEvents(t)
	if ev := evs[len(evs)-1]; ev.ErrorType != "" {
		t.Fatalf("recorded as %+v", ev)
	}
}

// Without the pool, or with the key's share taken, or past the large bound,
// the stream ends with the named error.
func TestLargeStreamEventRefused(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*config.Config)
		size   int
		hold   int
	}{
		{"pool off", largeCfg(0, 0), 9 << 20, 0},
		{"key share taken", largeCfg(4, 1), 9 << 20, 1},
		{"past the large bound", largeCfg(2, 2), 17 << 20, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := streamingHarness(t, tc.mutate, largeEventStream(tc.size))
			for i := 0; i < tc.hold; i++ {
				release, ok := h.srv.chatLarge.acquire("k-test")
				if !ok {
					t.Fatal("could not take the key's slot")
				}
				defer release()
			}
			status, body := h.chat(t, testToken, largeEventRequest)
			if status != 200 || !strings.Contains(string(body), "upstream_response_too_large") {
				t.Fatalf("status %d, tail %s", status, body[max(0, len(body)-300):])
			}
			if n := h.srv.chatLarge.inUse(); n != tc.hold {
				t.Fatalf("slots in use after the stream: %d, want %d", n, tc.hold)
			}
		})
	}
}

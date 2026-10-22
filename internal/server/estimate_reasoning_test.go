package server

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/pnuops/pickle-llm-gateway/internal/config"
)

// With no usage from the upstream the output is estimated from the text that
// came back, and reasoning is part of that text: it is generated and billed
// like the answer, and it comes first, so a thinking answer cut off before its
// content would otherwise be metered as nearly free.
func TestEstimateCountsReasoningText(t *testing.T) {
	reasoning := strings.Repeat("step ", 300) // 1500 bytes
	for _, field := range []string{"reasoning", "reasoning_content"} {
		t.Run("non-stream "+field, func(t *testing.T) {
			h := newHarness(t, nil, nil)
			h.mock.set(func(u *mockOpts) {
				u.rawResp = `{"id":"c","object":"chat.completion","model":"` + upstreamModel +
					`","choices":[{"index":0,"message":{"role":"assistant","content":"","` + field + `":"` + reasoning +
					`"},"finish_reason":"length"}]}`
			})
			if status, body := h.chat(t, testToken, chatBody); status != 200 {
				t.Fatalf("%d %s", status, body)
			}
			evs := h.spoolEvents(t)
			if len(evs) != 1 || !evs[0].Estimated || evs[0].OutputTokens != len(reasoning)/3 {
				t.Fatalf("reasoning-only answer estimated as %+v, want %d output tokens", evs, len(reasoning)/3)
			}
		})
	}
	t.Run("stream", func(t *testing.T) {
		chunk := func(delta string) string {
			return `data: {"id":"c","object":"chat.completion.chunk","model":"` + upstreamModel +
				`","choices":[{"index":0,"delta":` + delta + `}]}` + "\n\n"
		}
		stream := chunk(`{"role":"assistant","reasoning":"`+reasoning+`","reasoning_content":"`+reasoning+`"}`) +
			chunk(`{"content":"ok"}`) + "data: [DONE]\n\n"
		up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = io.WriteString(w, stream)
		}))
		defer up.Close()
		h := newHarness(t, nil, func(c *config.Config) {
			c.Upstreams["mock"] = config.Upstream{Ref: "mock", BaseURL: up.URL, CapField: "max_completion_tokens"}
		})
		status, body := h.chat(t, testToken, `{"model":"pickle-general","stream":true,"messages":[{"role":"user","content":"hi"}]}`)
		if status != 200 {
			t.Fatalf("%d %s", status, body)
		}
		evs := h.spoolEvents(t)
		// The two names carry the same text and are counted once.
		want := (len(reasoning) + len("ok")) / 3
		if len(evs) != 1 || !evs[0].Estimated || evs[0].OutputTokens != want {
			t.Fatalf("stream estimated as %+v, want %d output tokens", evs, want)
		}
	})
}

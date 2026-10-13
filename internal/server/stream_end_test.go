package server

import (
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/pnuops/pickle-llm-gateway/internal/snapshot"
	"github.com/pnuops/pickle-llm-gateway/internal/spool"
)

// A stream that closes cleanly without the upstream's [DONE] ended early.
// The caller is told, and the usage record is not a success — but the usage
// the upstream did report is kept, because it was spent.
func TestStreamWithoutDoneIsNotASuccess(t *testing.T) {
	for _, axis := range []string{snapshot.AxisToken, snapshot.AxisCredit} {
		t.Run(axis, func(t *testing.T) {
			h := newHarness(t, func(d *snapshot.Document) {
				d.Models[0].BudgetAxis = axis
				if axis == snapshot.AxisCredit {
					d.Keys[0].UpstreamCredentials = map[string]string{"mock": upstreamCred}
				}
			}, nil)
			stream := `data: {"model":"` + upstreamModel + `","choices":[{"index":0,"delta":{"content":"partial"}}]}` + "\n\n" +
				`data: {"model":"` + upstreamModel + `","choices":[],"usage":{"prompt_tokens":7,"completion_tokens":5}}` + "\n\n"
			h.srv.client = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header),
					Body: io.NopCloser(strings.NewReader(stream))}, nil
			})}
			status, body := h.chat(t, testToken, `{"model":"pickle-general","stream":true,"messages":[{"role":"user","content":"hi"}]}`)
			if status != http.StatusOK {
				t.Fatalf("status %d", status)
			}
			text := string(body)
			if !strings.Contains(text, "upstream_stream_interrupted") || !strings.HasSuffix(text, "data: [DONE]\n\n") {
				t.Fatalf("caller was not told the stream ended early: %s", text)
			}
			evs := h.spoolEvents(t)
			if len(evs) != 1 {
				t.Fatalf("events: %+v", evs)
			}
			ev := evs[0]
			if ev.Status != spool.StatusUpstreamErr || ev.ErrorType != "upstream_stream_unterminated" {
				t.Fatalf("result = %s/%s", ev.Status, ev.ErrorType)
			}
			if ev.Estimated || ev.InputTokens != 7 || ev.OutputTokens != 5 {
				t.Fatalf("reported usage was lost: %+v", ev)
			}
		})
	}
}

// One event past the relay bound ends the stream with an error naming the
// bound, and its usage record is not a success. The bound used to apply to
// the whole stream; it now applies to the event being assembled, which is
// what the stream holds in memory.
func TestStreamEventOverCapIsReported(t *testing.T) {
	for _, tc := range []struct {
		name  string
		event string
	}{
		{"one long line", `data: {"model":"` + upstreamModel + `","choices":[{"index":0,"delta":{"content":"` +
			strings.Repeat("x", upstreamResponseCapBytes) + `"}}]}` + "\n\n"},
		{"many lines in one event", strings.Repeat("data: "+strings.Repeat("y", 1<<20)+"\n", 9) + "\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, nil, nil)
			stream := `data: {"model":"` + upstreamModel + `","choices":[{"index":0,"delta":{"content":"ok"}}]}` + "\n\n" +
				tc.event + "data: [DONE]\n\n"
			h.srv.client = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header),
					Body: io.NopCloser(strings.NewReader(stream))}, nil
			})}
			status, body := h.chat(t, testToken, `{"model":"pickle-general","stream":true,"messages":[{"role":"user","content":"hi"}]}`)
			if status != http.StatusOK {
				t.Fatalf("status %d", status)
			}
			text := string(body)
			if !strings.Contains(text, "upstream_response_too_large") || !strings.HasSuffix(text, "data: [DONE]\n\n") ||
				strings.Count(text, "data: [DONE]") != 1 {
				t.Fatalf("stream end: %s", text[max(0, len(text)-400):])
			}
			evs := h.spoolEvents(t)
			if len(evs) != 1 || evs[0].Status != spool.StatusUpstreamErr || evs[0].ErrorType != "upstream_response_too_large" {
				t.Fatalf("events: %+v", evs)
			}
		})
	}
}

// A long stream of ordinary events is not bounded in total: its duration cap
// bounds it, and its memory is one event at a time.
func TestLongStreamOfSmallEventsSucceeds(t *testing.T) {
	h := newHarness(t, nil, nil)
	frame := `data: {"model":"` + upstreamModel + `","choices":[{"index":0,"delta":{"content":"` +
		strings.Repeat("x", 4000) + `"}}]}` + "\n\n"
	var stream strings.Builder
	for stream.Len() <= 2*upstreamResponseCapBytes {
		stream.WriteString(frame)
	}
	stream.WriteString(`data: {"model":"` + upstreamModel + `","choices":[],"usage":{"prompt_tokens":7,"completion_tokens":5}}` + "\n\n")
	stream.WriteString("data: [DONE]\n\n")
	h.srv.client = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header),
			Body: io.NopCloser(strings.NewReader(stream.String()))}, nil
	})}
	status, body := h.chat(t, testToken, `{"model":"pickle-general","stream":true,"messages":[{"role":"user","content":"hi"}]}`)
	if status != http.StatusOK || strings.Contains(string(body), "upstream_response_too_large") {
		t.Fatalf("a long stream of small events was cut: %d", status)
	}
	if evs := h.spoolEvents(t); len(evs) != 1 || evs[0].Status != spool.StatusOK || evs[0].OutputTokens != 5 {
		t.Fatalf("events: %+v", evs)
	}
}

// A [DONE] ends the answer even if the upstream misbehaves after it.
func TestStreamAfterDoneIsIgnored(t *testing.T) {
	h := newHarness(t, nil, nil)
	stream := `data: {"model":"` + upstreamModel + `","choices":[{"index":0,"delta":{"content":"ok"}}]}` + "\n\n" +
		`data: {"model":"` + upstreamModel + `","choices":[],"usage":{"prompt_tokens":7,"completion_tokens":5}}` + "\n\n" +
		"data: [DONE]\n\n" + strings.Repeat("x", upstreamResponseCapBytes+10)
	h.srv.client = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header),
			Body: io.NopCloser(strings.NewReader(stream))}, nil
	})}
	_, body := h.chat(t, testToken, `{"model":"pickle-general","stream":true,"messages":[{"role":"user","content":"hi"}]}`)
	text := string(body)
	if strings.Count(text, "data: [DONE]") != 1 || strings.Contains(text, "error") {
		t.Fatalf("finished stream was turned into an error: %s", text)
	}
	if evs := h.spoolEvents(t); len(evs) != 1 || evs[0].Status != spool.StatusOK {
		t.Fatalf("events: %+v", evs)
	}
}

// A bare CR inside a comment or a non-JSON payload would end a line at the
// client and not here, so what follows it would reach the client unread.
// Neither is forwarded.
func TestStreamDoesNotForwardABareCarriageReturn(t *testing.T) {
	h := newHarness(t, nil, nil)
	h.srv.client = streamFrom(": keep\rdata: {\"model\":\"" + upstreamModel + "\"}\n\n" +
		"data: ping\rdata: {\"model\":\"" + upstreamModel + "\"}\n\n" +
		`data: {"model":"` + upstreamModel + `","choices":[{"index":0,"delta":{"content":"ok"}}]}` + "\n\n" +
		"data: [DONE]\n\n")
	_, body := h.chat(t, testToken, streamRequest)
	if strings.Contains(string(body), "\r") || strings.Contains(string(body), upstreamModel) {
		t.Fatalf("a bare CR reached the client: %q", body)
	}
	if !strings.Contains(string(body), `"content":"ok"`) {
		t.Fatalf("the ordinary event was lost: %q", body)
	}
}

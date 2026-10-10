package server

import (
	"errors"
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

// A stream past the relay bound ends with an error naming the bound, and its
// usage record is not a success. Before, the reader returned EOF at the bound
// and the cut answer was recorded OK.
func TestStreamOverCapIsReported(t *testing.T) {
	h := newHarness(t, nil, nil)
	frame := `data: {"model":"` + upstreamModel + `","choices":[{"index":0,"delta":{"content":"` +
		strings.Repeat("x", 4000) + `"}}]}` + "\n\n"
	var stream strings.Builder
	for stream.Len() <= upstreamResponseCapBytes+len(frame) {
		stream.WriteString(frame)
	}
	stream.WriteString(`data: {"model":"` + upstreamModel + `","choices":[],"usage":{"prompt_tokens":7,"completion_tokens":5}}` + "\n\n")
	stream.WriteString("data: [DONE]\n\n")
	h.srv.client = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header),
			Body: io.NopCloser(strings.NewReader(stream.String()))}, nil
	})}
	status, body := h.chat(t, testToken, `{"model":"pickle-general","stream":true,"messages":[{"role":"user","content":"hi"}]}`)
	if status != http.StatusOK {
		t.Fatalf("status %d", status)
	}
	text := string(body)
	if !strings.Contains(text, "upstream_response_too_large") || !strings.HasSuffix(text, "data: [DONE]\n\n") {
		t.Fatalf("stream end: %s", text[max(0, len(text)-400):])
	}
	if strings.Count(text, "data: [DONE]") != 1 {
		t.Fatal("more than one terminator")
	}
	evs := h.spoolEvents(t)
	if len(evs) != 1 || evs[0].Status != spool.StatusUpstreamErr || evs[0].ErrorType != "upstream_response_too_large" {
		t.Fatalf("events: %+v", evs)
	}
}

func TestCappedReader(t *testing.T) {
	exact := &cappedReader{r: strings.NewReader("abcd"), remaining: 4}
	if b, err := io.ReadAll(exact); err != nil || string(b) != "abcd" {
		t.Fatalf("exactly at the bound: %q %v", b, err)
	}
	over := &cappedReader{r: strings.NewReader("abcde"), remaining: 4}
	if b, err := io.ReadAll(over); !errors.Is(err, errResponseOverCap) || string(b) != "abcd" {
		t.Fatalf("past the bound: %q %v", b, err)
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

package server

import (
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/pnuops/pickle-llm-gateway/internal/snapshot"
	"github.com/pnuops/pickle-llm-gateway/internal/spool"
)

func TestStreamResultIncludesPayloadErrors(t *testing.T) {
	const prefix = `data: {"model":"` + upstreamModel + `","choices":[{"index":0,"delta":{"content":"partial answer"}}]}` + "\n\n"
	const usageFrame = `data: {"model":"` + upstreamModel + `","choices":[],"usage":{"prompt_tokens":7,"completion_tokens":5,"cost":0.0012}}` + "\n\n"
	const done = "data: [DONE]\n\n"
	tests := []struct {
		name       string
		stream     string
		failed     bool
		estimated  bool
		forwarded  string
		wantsUsage bool
	}{
		{
			name:   "unified error followed by usage and done",
			stream: prefix + `data: {"model":"` + upstreamModel + `","error":{"code":"server_error","message":"provider failure detail"},"choices":[{"index":0,"delta":{},"finish_reason":"error"}]}` + "\n\n" + usageFrame + done,
			failed: true, forwarded: "provider failure detail",
		},
		{
			name:   "error only first and final event",
			stream: `data: {"error":{"code":500,"message":"provider failure detail"}}`,
			failed: true, estimated: true, forwarded: "provider failure detail",
		},
		{
			name:   "finish reason without error object",
			stream: prefix + `data: {"choices":[{"index":0,"delta":{},"finish_reason":"error"}]}` + "\n\n" + usageFrame + done,
			failed: true, forwarded: `"finish_reason":"error"`,
		},
		{
			name:   "error in later choice",
			stream: prefix + `data: {"choices":[{"index":0,"delta":{},"finish_reason":"stop"},{"index":1,"delta":{},"finish_reason":"error"}]}` + "\n\n" + usageFrame + done,
			failed: true, forwarded: `"finish_reason":"error"`,
		},
		{
			name: "multiline error with carriage returns",
			stream: prefix + "data: {\"model\":\"" + upstreamModel + "\",\r\n" +
				"data: \"error\":{\"message\":\"provider failure detail\"}}\r\n\r\n" + usageFrame + done,
			failed: true, forwarded: "provider failure detail",
		},
		{
			name:   "error with usage and empty choices",
			stream: prefix + `data: {"error":{"message":"provider failure detail"},"choices":[],"usage":{"prompt_tokens":7,"completion_tokens":5,"cost":0.0012}}` + "\n\n" + done,
			failed: true, forwarded: "provider failure detail",
		},
		{
			name:   "error with requested usage",
			stream: prefix + `data: {"error":{"message":"provider failure detail"},"choices":[],"usage":{"prompt_tokens":7,"completion_tokens":5,"cost":0.0012}}` + "\n\n" + done,
			failed: true, forwarded: "provider failure detail", wantsUsage: true,
		},
		{
			name:   "error without usage retains estimate",
			stream: prefix + `data: {"error":{"message":"provider failure detail"}}` + "\n\n" + done,
			failed: true, estimated: true, forwarded: "provider failure detail",
		},
		{
			name:   "stop with null error",
			stream: prefix + `data: {"error":null,"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}` + "\n\n" + usageFrame + done,
		},
		{
			name:   "length termination",
			stream: prefix + `data: {"choices":[{"index":0,"delta":{},"finish_reason":"length"}]}` + "\n\n" + usageFrame + done,
		},
		{
			name:   "content filter termination",
			stream: prefix + `data: {"choices":[{"index":0,"delta":{},"finish_reason":"content_filter"}]}` + "\n\n" + usageFrame + done,
		},
		{
			name:   "tool call content mentioning error",
			stream: prefix + `data: {"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"name":"error","arguments":"{\"error\":true}"}}]},"finish_reason":"tool_calls"}]}` + "\n\n" + usageFrame + done,
		},
	}
	for _, axis := range []string{snapshot.AxisToken, snapshot.AxisCredit} {
		for _, tt := range tests {
			t.Run(axis+"/"+tt.name, func(t *testing.T) {
				h := newHarness(t, func(d *snapshot.Document) {
					d.Models[0].BudgetAxis = axis
					if axis == snapshot.AxisCredit {
						d.Keys[0].UpstreamCredentials = map[string]string{"mock": upstreamCred}
					}
				}, nil)
				calls := 0
				h.srv.client = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
					calls++
					return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header),
						Body: io.NopCloser(strings.NewReader(tt.stream))}, nil
				})}
				request := `{"model":"pickle-general","stream":true,"messages":[{"role":"user","content":"test prompt"}]`
				if tt.wantsUsage {
					request += `,"stream_options":{"include_usage":true}`
				}
				status, body := h.chat(t, testToken, request+"}")
				if status != http.StatusOK {
					t.Fatalf("committed stream status = %d, body = %s", status, body)
				}
				if calls != 1 {
					t.Fatalf("committed stream was retried: %d calls", calls)
				}
				text := string(body)
				if tt.forwarded != "" && !strings.Contains(text, tt.forwarded) {
					t.Fatalf("error payload was not forwarded: %s", body)
				}
				if strings.Contains(text, upstreamModel) {
					t.Fatalf("upstream model was not rewritten: %s", body)
				}
				if strings.Contains(text, `"usage"`) != tt.wantsUsage {
					t.Fatalf("usage forwarding differs from the request: %s", body)
				}
				events := h.spoolEvents(t)
				if len(events) != 1 {
					t.Fatalf("want one usage event, got %+v", events)
				}
				ev := events[0]
				wantStatus, wantError := spool.StatusOK, ""
				if tt.failed {
					wantStatus, wantError = spool.StatusUpstreamErr, "upstream_error"
				}
				if ev.Status != wantStatus || ev.ErrorType != wantError {
					t.Fatalf("result = %s/%s, want %s/%s", ev.Status, ev.ErrorType, wantStatus, wantError)
				}
				if ev.Estimated != tt.estimated {
					t.Fatalf("estimated = %t, want %t", ev.Estimated, tt.estimated)
				}
				if !tt.estimated && (ev.InputTokens != 7 || ev.OutputTokens != 5 || ev.CostUsd != "0.0012") {
					t.Fatalf("reported usage was not preserved: %+v", ev)
				}
				if ev.BudgetAxis != axis || ev.Attempts != 1 || ev.UpstreamRef != "mock" || !ev.Streamed {
					t.Fatalf("request attribution changed: %+v", ev)
				}
				if counts := h.srv.metrics.statusMap(); counts[wantStatus] != 1 || len(counts) != 1 {
					t.Fatalf("metrics result differs from accounting: %+v", counts)
				}
			})
		}
	}
}

// A non-streamed answer carrying the vendor's own error is relayed as it came
// and recorded the way the same error inside a stream is: not a success, with
// the usage it reported kept.
func TestNonStreamPayloadErrorIsNotASuccess(t *testing.T) {
	for _, tc := range []struct{ name, body string }{
		{"error member", `{"id":"c1","model":"` + upstreamModel + `","error":{"code":502,"message":"provider failure detail"},"choices":[],"usage":{"prompt_tokens":7,"completion_tokens":5}}`},
		{"finish reason", `{"id":"c1","model":"` + upstreamModel + `","choices":[{"index":0,"message":{"role":"assistant","content":"par"},"finish_reason":"error"}],"usage":{"prompt_tokens":7,"completion_tokens":5}}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, nil, nil)
			h.mock.set(func(o *mockOpts) { o.rawResp = tc.body })
			status, body := h.chat(t, testToken, chatBody)
			if status != http.StatusOK || strings.Contains(string(body), upstreamModel) {
				t.Fatalf("relay changed: %d %s", status, body)
			}
			evs := h.spoolEvents(t)
			if len(evs) != 1 || evs[0].Status != spool.StatusUpstreamErr || evs[0].ErrorType != "upstream_error" ||
				evs[0].Estimated || evs[0].InputTokens != 7 || evs[0].OutputTokens != 5 {
				t.Fatalf("events: %+v", evs)
			}
		})
	}
	// An error member that is null is no error.
	h := newHarness(t, nil, nil)
	h.mock.set(func(o *mockOpts) {
		o.rawResp = `{"id":"c1","model":"` + upstreamModel + `","error":null,"choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}],"usage":{"prompt_tokens":7,"completion_tokens":5}}`
	})
	h.chat(t, testToken, chatBody)
	if evs := h.spoolEvents(t); len(evs) != 1 || evs[0].Status != spool.StatusOK {
		t.Fatalf("events: %+v", evs)
	}
}

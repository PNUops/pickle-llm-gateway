package server

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/pnuops/pickle-llm-gateway/internal/snapshot"
	"github.com/pnuops/pickle-llm-gateway/internal/spool"
)

// These tests pin chat behaviour that no other test held still, so that moving
// the chat handler onto a shared pipeline cannot change it unnoticed. They
// assert whole byte sequences and orderings on purpose: the property being
// protected is "nothing changed", and a contains-check lets most changes
// through.

// streamFrom answers every upstream call with one fixed stream.
func streamFrom(stream string) *http.Client {
	return &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header),
			Body: io.NopCloser(strings.NewReader(stream))}, nil
	})}
}

const streamRequest = `{"model":"pickle-general","stream":true,"messages":[{"role":"user","content":"hi"}]}`

// mixedFieldStream carries every SSE line kind the chat relay meets: a comment,
// the three non-data fields, an event split over two data lines, an event with
// no data at all, a non-JSON keepalive, and CRLF line ends on one event.
const mixedFieldStream = ": keepalive\n" +
	"event: completion\n" +
	"id: 1\n" +
	"retry: 3000\n" +
	`data: {"id":"c1","model":"` + upstreamModel + `","choices":[{"index":0,"delta":{"content":"안녕"}}]}` + "\n" +
	"\n" +
	"id: 2\n" +
	`data: {"id":"c1","model":"` + upstreamModel + `",` + "\n" +
	`data: "choices":[{"index":0,"delta":{"content":"하세요"}}]}` + "\n" +
	"\n" +
	"event: ping\n" +
	"\n" +
	"data: ping\n" +
	"\n" +
	`data: {"id":"c1","model":"` + upstreamModel + `","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}` + "\r\n" +
	"\r\n" +
	`data: {"id":"c1","model":"` + upstreamModel + `","choices":[],"usage":{"prompt_tokens":7,"completion_tokens":5,"total_tokens":12}}` + "\n" +
	"\n" +
	"data: [DONE]\n" +
	"\n"

// The relay emits data lines only, re-serialised with sorted keys and the
// public model name; event:, id: and retry: are dropped and comments pass.
func TestChatStreamBytesWithMixedSSEFields(t *testing.T) {
	const head = ": keepalive\n\n" +
		`data: {"choices":[{"delta":{"content":"안녕"},"index":0}],"id":"c1","model":"pickle-general"}` + "\n\n" +
		`data: {"choices":[{"delta":{"content":"하세요"},"index":0}],"id":"c1","model":"pickle-general"}` + "\n\n" +
		"data: ping\n\n" +
		`data: {"choices":[{"delta":{},"finish_reason":"stop","index":0}],"id":"c1","model":"pickle-general"}` + "\n\n"
	for _, tc := range []struct {
		name, request, want string
	}{
		{"usage consumed", streamRequest, head + "data: [DONE]\n\n"},
		{"usage forwarded",
			strings.Replace(streamRequest, `"stream":true`, `"stream":true,"stream_options":{"include_usage":true}`, 1),
			head + `data: {"choices":[],"id":"c1","model":"pickle-general","usage":{"completion_tokens":5,"prompt_tokens":7,"total_tokens":12}}` + "\n\n" +
				"data: [DONE]\n\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, nil, nil)
			h.srv.client = streamFrom(mixedFieldStream)
			status, body, hdr := h.chatHeaders(t, testToken, tc.request)
			if status != http.StatusOK {
				t.Fatalf("status %d: %s", status, body)
			}
			if string(body) != tc.want {
				t.Fatalf("stream bytes changed:\n got %q\nwant %q", body, tc.want)
			}
			if got := hdr.Get("Content-Type"); got != "text/event-stream; charset=utf-8" {
				t.Fatalf("Content-Type %q", got)
			}
			if got := hdr.Get("Cache-Control"); got != "no-cache" {
				t.Fatalf("Cache-Control %q", got)
			}
			if hdr.Get("X-Request-Id") == "" || hdr.Get("X-Generation-Id") != "" {
				t.Fatalf("headers: %v", hdr)
			}
			evs := h.spoolEvents(t)
			if len(evs) != 1 || evs[0].Status != spool.StatusOK || evs[0].Estimated ||
				evs[0].InputTokens != 7 || evs[0].OutputTokens != 5 || !evs[0].Streamed {
				t.Fatalf("events: %+v", evs)
			}
			if evs[0].EventUUID != hdr.Get("X-Request-Id") {
				t.Fatalf("request id %q is not the event id %q", hdr.Get("X-Request-Id"), evs[0].EventUUID)
			}
		})
	}
}

// The gateway's own terminal error event, byte for byte, on a stream that
// closed without its terminator.
func TestChatStreamErrorEventBytes(t *testing.T) {
	h := newHarness(t, nil, nil)
	h.srv.client = streamFrom(`data: {"model":"` + upstreamModel + `","choices":[{"index":0,"delta":{"content":"partial"}}]}` + "\n\n")
	_, body := h.chat(t, testToken, streamRequest)
	want := `data: {"choices":[{"delta":{"content":"partial"},"index":0}],"model":"pickle-general"}` + "\n\n" +
		`data: {"error":{"message":"모델 서버 응답이 중간에 끊겼습니다. 응답이 불완전할 수 있습니다.","type":"server_error","code":"upstream_stream_interrupted"}}` + "\n\n" +
		"data: [DONE]\n\n"
	if string(body) != want {
		t.Fatalf("stream bytes changed:\n got %q\nwant %q", body, want)
	}
}

// An upstream that reports its own error and then closes without a terminator
// gets the terminator alone: its error already went out, and a second one from
// the gateway would contradict it.
func TestChatStreamUpstreamErrorWithoutDoneGetsOnlyTheTerminator(t *testing.T) {
	h := newHarness(t, nil, nil)
	h.srv.client = streamFrom(`data: {"error":{"message":"provider failure detail"}}` + "\n\n")
	_, body := h.chat(t, testToken, streamRequest)
	want := `data: {"error":{"message":"provider failure detail"}}` + "\n\n" + "data: [DONE]\n\n"
	if string(body) != want {
		t.Fatalf("stream bytes changed:\n got %q\nwant %q", body, want)
	}
	evs := h.spoolEvents(t)
	if len(evs) != 1 || evs[0].Status != spool.StatusUpstreamErr || evs[0].ErrorType != "upstream_error" {
		t.Fatalf("events: %+v", evs)
	}
}

// The non-streamed answer, byte for byte: re-serialised with sorted keys and
// the public model name, under the gateway's own Content-Type.
func TestChatNonStreamBytes(t *testing.T) {
	h := newHarness(t, nil, nil)
	status, body, hdr := h.chatHeaders(t, testToken, chatBody)
	if status != http.StatusOK {
		t.Fatalf("status %d: %s", status, body)
	}
	want := `{"choices":[{"finish_reason":"stop","index":0,"message":{"content":"안녕하세요","role":"assistant"}}],` +
		`"created":1700000000,"id":"chatcmpl-t1","model":"pickle-general","object":"chat.completion",` +
		`"usage":{"completion_tokens":5,"prompt_tokens":7,"total_tokens":12}}`
	if string(body) != want {
		t.Fatalf("body changed:\n got %s\nwant %s", body, want)
	}
	if got := hdr.Get("Content-Type"); got != "application/json; charset=utf-8" {
		t.Fatalf("Content-Type %q", got)
	}
}

// A refusal's envelope, byte for byte, with its Content-Type and request id.
func TestChatRefusalEnvelopeBytes(t *testing.T) {
	h := newHarness(t, nil, nil)
	status, body, hdr := h.chatHeaders(t, testToken, `{"model":"pickle-general"}`)
	e := errMissingParam("messages")
	if status != e.status {
		t.Fatalf("status %d", status)
	}
	msg, _ := json.Marshal(e.message)
	want := `{"error":{"code":"` + e.code + `","message":` + string(msg) + `,"type":"` + e.typ + `"}}` + "\n"
	if string(body) != want {
		t.Fatalf("envelope changed:\n got %q\nwant %q", body, want)
	}
	if hdr.Get("Content-Type") != "application/json; charset=utf-8" || hdr.Get("X-Request-Id") == "" {
		t.Fatalf("headers: %v", hdr)
	}
}

// Which refusal wins when a request earns two. The handler's order is the
// answer the caller gets, so each pair here is one the pipeline must keep.
func TestChatRefusalOrder(t *testing.T) {
	restricted := func(d *snapshot.Document) {
		d.Keys[0].AllowedModels = []string{"some-other-model"}
	}
	credit := func(d *snapshot.Document) {
		d.Models[0].BudgetAxis = snapshot.AxisCredit
		d.Keys[0].UpstreamCredentials = map[string]string{"mock": upstreamCred}
		d.Keys[0].CreditDeniedModels = []string{"pickle-general"}
	}
	for _, tc := range []struct {
		name   string
		doc    func(*snapshot.Document)
		token  string
		body   string
		status int
		code   string
	}{
		// Authentication comes before the body is read at all.
		{"auth before json", nil, "pickle-nope", `{not json`, 401, "invalid_api_key"},
		// A malformed request answers 400 rather than 403 (the token axis's
		// parameter list runs ahead of the key's model fence).
		{"unsupported parameter before model fence", restricted, testToken,
			`{"model":"pickle-general","logprobs":true,"messages":[]}`, 400, "unsupported_parameter"},
		{"model fence without the parameter", restricted, testToken,
			`{"model":"pickle-general","messages":[]}`, 403, "model_not_allowed"},
		// Both required fields are read before the model resolves.
		{"missing model before missing messages", nil, testToken, `{}`, 400, "missing_parameter"},
		{"missing messages before unknown model", nil, testToken, `{"model":"no-such-model"}`, 400, "missing_parameter"},
		// On the money axis the model fence runs before the output cap is read.
		{"money fence before output cap", credit, testToken,
			`{"model":"pickle-general","max_tokens":-1,"messages":[]}`, 403, "model_not_allowed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, tc.doc, nil)
			status, body := h.chat(t, tc.token, tc.body)
			if status != tc.status || errCode(t, body) != tc.code {
				t.Fatalf("got %d %s, want %d %s", status, body, tc.status, tc.code)
			}
			if h.mock.callCount() != 0 {
				t.Fatal("a refused request reached the upstream")
			}
		})
	}
}

// The stream flag is stamped before the body's own fields are read, so every
// refusal from there on still says a stream was asked for. A body that does
// not parse cannot say anything.
func TestChatStreamedIsRecordedOnEveryBodyRefusal(t *testing.T) {
	for _, tc := range []struct {
		name, body, errorType string
		streamed              bool
	}{
		{"missing messages", `{"model":"pickle-general","stream":true}`, "missing_parameter", true},
		{"missing model", `{"stream":true,"messages":[]}`, "missing_parameter", true},
		{"bad model value", `{"model":7,"stream":true,"messages":[]}`, "missing_parameter", true},
		{"bad candidate list", `{"models":[""],"stream":true,"messages":[]}`, "invalid_parameter_value", true},
		{"unsupported parameter", `{"model":"pickle-general","stream":true,"logprobs":true,"messages":[]}`, "unsupported_parameter", true},
		{"invalid json", `{"model":"pickle-general","stream":true`, "invalid_json", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, nil, nil)
			if status, _ := h.chat(t, testToken, tc.body); status != http.StatusBadRequest {
				t.Fatalf("status %d", status)
			}
			evs := h.spoolEvents(t)
			if len(evs) != 1 || evs[0].ErrorType != tc.errorType || evs[0].Streamed != tc.streamed {
				t.Fatalf("events: %+v", evs)
			}
			if counts := h.srv.metrics.statusMap(); counts[spool.StatusBadRequest] != 1 || len(counts) != 1 {
				t.Fatalf("a refusal was recorded other than once: %+v", counts)
			}
		})
	}
}

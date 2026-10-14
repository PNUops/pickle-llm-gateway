package server

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/pnuops/pickle-llm-gateway/internal/spool"
)

// testNamedStream is the named-event relay with a pass-through reader that
// drops what does not parse, the shape a Messages surface will fill in.
func testNamedStream() *namedStream {
	return &namedStream{
		ends:      map[string]bool{"message_stop": true},
		errorName: "error",
		read: func(e sseEvent, _ *streamResult) ([]byte, bool) {
			return e.data, json.Valid(e.data)
		},
		errorData: anthropicStreamErrorData("req-1"),
	}
}

// relayNamed runs one upstream stream through the shared loop with x and
// returns what the client received and the usage record.
func relayNamed(t *testing.T, x streamExtractor, stream string) (string, spool.Event, int) {
	t.Helper()
	h := newHarness(t, nil, nil)
	rec := httptest.NewRecorder()
	resp := &http.Response{StatusCode: http.StatusOK, Header: make(http.Header),
		Body: io.NopCloser(strings.NewReader(stream))}
	var ev spool.Event
	records := 0
	h.srv.relayStream(rec, resp, x, streamArgs{
		charge:    func(int) {},
		clientCtx: context.Background(),
		upCtx:     context.Background(),
	}, &ev, func() { records++ })
	if got := rec.Header().Get("Content-Type"); got != "text/event-stream; charset=utf-8" {
		t.Fatalf("Content-Type %q", got)
	}
	return rec.Body.String(), ev, records
}

const (
	msgStart = `{"type":"message_start","message":{"id":"m1","model":"x","usage":{"input_tokens":3}}}`
	msgDelta = `{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"hi"}}`
	msgStop  = `{"type":"message_stop"}`
)

// Event names survive the relay, a data payload split over two lines goes out
// on two lines, comments pass, and id: and retry: are not forwarded.
func TestNamedStreamKeepsEventLines(t *testing.T) {
	stream := ": open\n\n" +
		"event: message_start\nid: 7\nretry: 100\ndata: " + msgStart + "\n\n" +
		"event: ping\ndata: {\"type\":\"ping\"}\n\n" +
		"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\r\ndata: \"index\":0}\r\n\r\n" +
		"data: {\"unnamed\":true}\n\n" +
		"event: message_stop\ndata: " + msgStop + "\n\n"
	out, ev, records := relayNamed(t, testNamedStream(), stream)
	want := ": open\n\n" +
		"event: message_start\ndata: " + msgStart + "\n\n" +
		"event: ping\ndata: {\"type\":\"ping\"}\n\n" +
		"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\ndata: \"index\":0}\n\n" +
		"data: {\"unnamed\":true}\n\n" +
		"event: message_stop\ndata: " + msgStop + "\n\n"
	if out != want {
		t.Fatalf("relayed bytes:\n got %q\nwant %q", out, want)
	}
	if ev.Status != spool.StatusOK || records != 1 {
		t.Fatalf("record %s/%s, %d records", ev.Status, ev.ErrorType, records)
	}
}

// The end set is the surface's: a stream that closes without message_stop
// ended early, and anything after it is ignored.
func TestNamedStreamEndSet(t *testing.T) {
	t.Run("no end event", func(t *testing.T) {
		out, ev, _ := relayNamed(t, testNamedStream(),
			"event: message_start\ndata: "+msgStart+"\n\nevent: content_block_delta\ndata: "+msgDelta+"\n\n")
		wantTail := "event: error\ndata: " + string(anthropicErrorBody("api_error", "upstream_stream_interrupted",
			"모델 서버 응답이 중간에 끊겼습니다. 응답이 불완전할 수 있습니다.", "req-1")) + "\n\n"
		if !strings.HasSuffix(out, wantTail) || strings.Count(out, "event: error") != 1 {
			t.Fatalf("stream end: %q", out)
		}
		if ev.Status != spool.StatusUpstreamErr || ev.ErrorType != "upstream_stream_unterminated" {
			t.Fatalf("record %s/%s", ev.Status, ev.ErrorType)
		}
	})
	t.Run("bytes after the end", func(t *testing.T) {
		out, ev, _ := relayNamed(t, testNamedStream(),
			"event: message_stop\ndata: "+msgStop+"\n\nevent: content_block_delta\ndata: "+msgDelta+"\n\n"+
				strings.Repeat("x", upstreamResponseCapBytes+10))
		if out != "event: message_stop\ndata: "+msgStop+"\n\n" || ev.Status != spool.StatusOK {
			t.Fatalf("finished stream changed: %q %s/%s", out, ev.Status, ev.ErrorType)
		}
	})
	t.Run("chat terminator is not an end", func(t *testing.T) {
		_, ev, _ := relayNamed(t, testNamedStream(),
			"event: message_start\ndata: "+msgStart+"\n\ndata: [DONE]\n\n")
		if ev.ErrorType != "upstream_stream_unterminated" {
			t.Fatalf("[DONE] ended a named stream: %s/%s", ev.Status, ev.ErrorType)
		}
	})
}

// An upstream's own error event is forwarded under its name and is the end of
// the stream: the gateway adds neither a second error nor a terminator, and
// nothing the upstream sends after it is relayed or read.
func TestNamedStreamUpstreamError(t *testing.T) {
	errData := `{"type":"error","error":{"type":"overloaded_error","message":"busy"}}`
	out, ev, _ := relayNamed(t, testNamedStream(),
		"event: message_start\ndata: "+msgStart+"\n\nevent: error\ndata: "+errData+"\n\n"+
			"event: content_block_delta\ndata: "+msgDelta+"\n\n"+strings.Repeat("x", upstreamResponseCapBytes+10))
	want := "event: message_start\ndata: " + msgStart + "\n\nevent: error\ndata: " + errData + "\n\n"
	if out != want {
		t.Fatalf("relayed bytes:\n got %q\nwant %q", out, want)
	}
	if ev.Status != spool.StatusUpstreamErr || ev.ErrorType != "upstream_error" {
		t.Fatalf("record %s/%s", ev.Status, ev.ErrorType)
	}
}

// One event past the relay bound ends the stream with the gateway's error in
// the named shape.
func TestNamedStreamEventOverCap(t *testing.T) {
	big := `{"type":"content_block_delta","delta":{"text":"` + strings.Repeat("y", upstreamResponseCapBytes) + `"}}`
	out, ev, _ := relayNamed(t, testNamedStream(),
		"event: message_start\ndata: "+msgStart+"\n\nevent: content_block_delta\ndata: "+big+"\n\n")
	if !strings.HasPrefix(out, "event: message_start\n") ||
		!strings.HasSuffix(out, "\n\n") || !strings.Contains(out, "event: error\ndata: ") ||
		!strings.Contains(out, `"code":"upstream_response_too_large"`) {
		t.Fatalf("stream end: %q", out[max(0, len(out)-300):])
	}
	if ev.ErrorType != errPassthroughResponseTooLarge.code {
		t.Fatalf("record %s/%s", ev.Status, ev.ErrorType)
	}
}

// An event whose name was too long to keep, and one whose data does not
// parse, are dropped rather than forwarded under another name or verbatim.
func TestNamedStreamDropsWhatItCannotVouchFor(t *testing.T) {
	out, ev, _ := relayNamed(t, testNamedStream(),
		"event: "+strings.Repeat("n", sseFieldMax+1)+"\ndata: {\"a\":1}\n\n"+
			"event: content_block_delta\ndata: {\"trunc\n\n"+
			"event: message_stop\ndata: "+msgStop+"\n\n")
	if out != "event: message_stop\ndata: "+msgStop+"\n\n" {
		t.Fatalf("relayed bytes: %q", out)
	}
	if ev.Status != spool.StatusUpstreamErr || ev.ErrorType != "upstream_chunk_unreadable" {
		t.Fatalf("record %s/%s", ev.Status, ev.ErrorType)
	}
}

// The Anthropic envelope, byte for byte, and its type for every refusal the
// gateway can give.
func TestAnthropicErrorEnvelope(t *testing.T) {
	rec := httptest.NewRecorder()
	writeAnthropicError(rec, errCreditPending, "req-9")
	want := `{"error":{"code":"credit_pending","message":` + mustJSON(t, errCreditPending.message) +
		`,"type":"api_error"},"request_id":"req-9","type":"error"}`
	if rec.Code != errCreditPending.status || rec.Body.String() != want {
		t.Fatalf("envelope %d %s\nwant %s", rec.Code, rec.Body.String(), want)
	}
	if rec.Header().Get("Content-Type") != "application/json; charset=utf-8" ||
		rec.Header().Get("Retry-After") != strconv.Itoa(retryAfterSeconds["credit_pending"]) ||
		rec.Header().Get("X-Should-Retry") != "false" {
		t.Fatalf("headers %v", rec.Header())
	}

	anthropicTypes := map[string]bool{
		"invalid_request_error": true, "authentication_error": true, "permission_error": true,
		"not_found_error": true, "rate_limit_error": true, "api_error": true, "request_too_large": true,
		"overloaded_error": true, "timeout_error": true,
	}
	for name, e := range allAPIErrors() {
		got := anthropicErrorType(e)
		if !anthropicTypes[got] {
			t.Fatalf("%s: %q is not an Anthropic error type", name, got)
		}
		switch {
		case e.code == errRequestTooLarge.code:
			if got != "request_too_large" {
				t.Fatalf("%s: %q", name, got)
			}
		case anthropicNoRetry[e.code]:
			if got == "overloaded_error" {
				t.Fatalf("%s: a no-retry refusal typed %q", name, got)
			}
		case e.status == http.StatusNotFound && got != "not_found_error",
			e.status == http.StatusBadRequest && got != "invalid_request_error",
			e.status == http.StatusForbidden && got != "permission_error",
			e.status == http.StatusGatewayTimeout && got != "timeout_error",
			e.status == http.StatusServiceUnavailable && got != "overloaded_error",
			e.status == http.StatusUnauthorized && got != "authentication_error",
			e.status == http.StatusTooManyRequests && got != "rate_limit_error":
			t.Fatalf("%s (%d): %q", name, e.status, got)
		}
	}
}

// A name carrying a bare CR would be one line here and two at the client, a
// way to put an event in front of it that read never saw. Such an event is
// dropped.
func TestNamedStreamDropsANameWithACarriageReturn(t *testing.T) {
	out, _, _ := relayNamed(t, testNamedStream(),
		"event: ping\rdata: {\"injected\":true}\r\revent: message_stop\ndata: {\"type\":\"ping\"}\n\n"+
			"event: message_stop\ndata: "+msgStop+"\n\n")
	if out != "event: message_stop\ndata: "+msgStop+"\n\n" {
		t.Fatalf("relayed bytes: %q", out)
	}
}

// The gateway's own terminal error on the request deadline is a timeout in
// the Anthropic vocabulary, and anything else it ends a stream for is api_error.
func TestNamedStreamGatewayErrorTypes(t *testing.T) {
	n := testNamedStream()
	for code, want := range map[string]string{
		"request_deadline_exceeded":   "timeout_error",
		"upstream_stream_interrupted": "api_error",
		"upstream_response_too_large": "api_error",
	} {
		got := string(n.errorEvent(code, "m"))
		if !strings.HasPrefix(got, "event: error\ndata: ") || !strings.Contains(got, `"type":"`+want+`"`) ||
			!strings.Contains(got, `"code":"`+code+`"`) || !strings.Contains(got, `"request_id":"req-1"`) {
			t.Fatalf("%s: %q", code, got)
		}
	}
}

func mustJSON(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

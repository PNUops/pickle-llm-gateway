package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strconv"
)

// Streams whose events are named. Chat sends data lines only and carries an
// event's kind inside its payload, so its relay drops the event: line. The
// Anthropic Messages stream (and the vendor's schema for it) names every event
// on its event: line, and a client dispatches on that name, so a relay for it
// has to keep the line. Nothing routes here yet; the surface that does brings
// its own metering through read.

// namedStream relays a stream of named events: each forwarded event keeps its
// event: line, and the stream ends on one of the surface's own end events.
// id: and retry: are not forwarded. They are the upstream's instructions to
// the client's reconnection logic, and a reconnect to this gateway is a new
// request, not a resumption of the upstream's.
type namedStream struct {
	// ends are the event names after which the answer is complete.
	ends map[string]bool
	// errorName is the event name an upstream reports a failure with.
	errorName string
	// read meters and rewrites one event's data. It returns the data to send,
	// or false to drop an event this surface cannot vouch for (one that does
	// not parse would carry the upstream's identifiers verbatim).
	read func(e sseEvent, res *streamResult) ([]byte, bool)
	// errorData renders the gateway's own terminal error as event data.
	errorData func(code, msg string) []byte

	res   streamResult
	ended bool
}

func (n *namedStream) event(e sseEvent) []byte {
	if e.fieldTooLong {
		// A name this relay could not keep is a name it cannot dispatch on,
		// so the event is not forwarded under some other name.
		n.res.dropped++
		return nil
	}
	data, ok := n.read(e, &n.res)
	if !ok {
		n.res.dropped++
		return nil
	}
	if e.name == n.errorName {
		n.res.upstreamError = true
	}
	if n.ends[e.name] {
		n.ended = true
	}
	return sseFrame(e.name, data)
}

func (n *namedStream) done() bool { return n.ended }

func (n *namedStream) result() streamResult { return n.res }

func (n *namedStream) errorEvent(code, msg string) []byte {
	return sseFrame(n.errorName, n.errorData(code, msg))
}

// terminator is nil: an upstream's error event already closes a named stream,
// and there is no separate end marker to add after it.
func (n *namedStream) terminator() []byte { return nil }

// sseFrame renders one event. data was joined from its lines with "\n", so it
// is split on the same byte and each piece goes out as its own data line, the
// way it arrived.
func sseFrame(name string, data []byte) []byte {
	var b bytes.Buffer
	if name != "" {
		b.WriteString("event: ")
		b.WriteString(name)
		b.WriteByte('\n')
	}
	for _, line := range bytes.Split(data, []byte("\n")) {
		b.WriteString("data: ")
		b.Write(line)
		b.WriteByte('\n')
	}
	b.WriteByte('\n')
	return b.Bytes()
}

// The Anthropic error envelope. The shape is the vendor's
// ({"type":"error","error":{"type","message"},"request_id"}); request_id is
// the same value as X-Request-Id.
//
// code is an extra member inside error, carrying this gateway's public code.
// The Anthropic vocabulary below is coarser than the gateway's, so refusals
// that share a status and a type but ask different things of the caller —
// credit_pending against credit_unavailable, quota_exhausted against a rate
// limit, endpoint_not_allowed against model_not_allowed — would otherwise be
// indistinguishable to a client. Whether agent clients tolerate the extra
// member is checked when the first Anthropic route opens.

// anthropicErrorType picks the vendor's error type for a refusal. The
// gateway's own types are OpenAI's, four of which Anthropic shares; the rest
// are chosen by status, because that is what an Anthropic client retries on.
func anthropicErrorType(e apiError) string {
	switch e.typ {
	case "authentication_error", "permission_error", "rate_limit_error":
		return e.typ
	case "invalid_request_error":
		if e.status == http.StatusNotFound {
			return "not_found_error"
		}
		return "invalid_request_error"
	}
	switch e.status {
	case http.StatusGatewayTimeout:
		return "timeout_error"
	case http.StatusServiceUnavailable:
		// The type a client backs off and retries on, which is what every
		// 503 here asks of it.
		return "overloaded_error"
	}
	return "api_error"
}

// anthropicStreamErrorType is the type of a terminal error the gateway writes
// into a stream that is already under way.
func anthropicStreamErrorType(code string) string {
	if code == "request_deadline_exceeded" {
		return "timeout_error"
	}
	return "api_error"
}

func anthropicErrorBody(typ, code, msg, requestID string) []byte {
	b, _ := json.Marshal(map[string]any{
		"type": "error",
		"error": map[string]any{
			"type":    typ,
			"message": msg,
			"code":    code,
		},
		"request_id": requestID,
	})
	return b
}

// writeAnthropicError renders a refusal in the Anthropic envelope, with the
// same status and Retry-After the OpenAI-shaped refusal carries.
func writeAnthropicError(w http.ResponseWriter, e apiError, requestID string) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	if seconds := retryAfterSeconds[e.code]; seconds > 0 {
		w.Header().Set("Retry-After", strconv.Itoa(seconds))
	}
	w.WriteHeader(e.status)
	_, _ = w.Write(anthropicErrorBody(anthropicErrorType(e), e.code, e.message, requestID))
}

// anthropicStreamErrorData renders the gateway's terminal stream error for a
// named stream whose error event is "error".
func anthropicStreamErrorData(requestID string) func(code, msg string) []byte {
	return func(code, msg string) []byte {
		return anthropicErrorBody(anthropicStreamErrorType(code), code, msg, requestID)
	}
}

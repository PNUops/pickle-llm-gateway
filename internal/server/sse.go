package server

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"

	"github.com/pnuops/pickle-llm-gateway/internal/bodies"
	"github.com/pnuops/pickle-llm-gateway/internal/spool"
)

// The stream relay shared by every streaming surface. It owns what does not
// depend on the protocol: reading lines, assembling events, the per-event
// memory bound, comment lines, and how a stream's ending is classified. What a
// surface changes — how one event is metered and rewritten, which events end
// the stream, and what the gateway writes after an error — is the
// streamExtractor's.

// sseEvent is one assembled server-sent event. data joins the event's data
// lines with "\n". name, id and retry follow SSE's last-one-wins rule, so an
// event holds at most one of each however many lines carried them.
type sseEvent struct {
	name, id, retry string
	data            []byte
	// badField marks an event that carried an event:, id: or retry: value
	// this relay would not keep: one past sseFieldMax, or one holding a
	// control byte. The value was dropped, so a surface that reads the field
	// must treat the event as malformed rather than as unnamed.
	badField bool
}

// sseFieldMax bounds the event:, id: and retry: values one event keeps. Those
// fields replace rather than accumulate and are not charged to the event's
// bound, so each is held to a size no real event name or id approaches;
// without it one event could keep three more lines of the full line bound.
const sseFieldMax = 1024

// field reads one field value, or marks the event when the value cannot be
// kept. A control byte is refused because this relay splits lines on LF
// alone while a client also ends one at a bare CR: a name carrying a CR would
// be one name here and, once written back out, a second line there, which is
// a way to put an event in front of the client that no extractor ever read.
func (e *sseEvent) field(after []byte) string {
	v := bytes.TrimSpace(after)
	if len(v) > sseFieldMax || bytes.ContainsFunc(v, isControlByte) {
		e.badField = true
		return ""
	}
	return string(v)
}

func isControlByte(r rune) bool { return r < 0x20 || r == 0x7f }

// streamExtractor is one surface's reading of its upstream's stream.
type streamExtractor interface {
	// event meters and rewrites one event and returns the bytes to send, or
	// nil to send nothing.
	event(e sseEvent) []byte
	// done reports whether one of the surface's own end events has been seen.
	done() bool
	// result is read once the stream has ended.
	result() streamResult
	// errorEvent renders the gateway's terminal error, with whatever closing
	// the surface needs after it.
	errorEvent(code, msg string) []byte
	// terminator closes a stream whose upstream reported an error but no end;
	// nil where the surface has nothing to add.
	terminator() []byte
}

type streamResult struct {
	usage         usage
	haveUsage     bool
	contentChars  int
	answer        string // capture only
	dropped       int
	upstreamError bool
}

type streamArgs struct {
	publicName string
	// sentModel is the upstream model name this request asked for, so a
	// vendor-side fallback can be noticed before the name is rewritten.
	sentModel string
	// keyID is for logging and names the key when an oversized event takes a
	// large-body slot; metering goes through charge, which already knows the
	// key and whether this request's axis is metered at all.
	keyID        string
	charge       func(int)
	inputBytes   int
	forwardUsage bool
	capture      *bodies.Record // nil unless the key opted into body capture
	clientCtx    context.Context
	upCtx        context.Context
}

// relayStream commits a 200 and relays the upstream's stream through x,
// recording the request once it ends.
func (s *Server) relayStream(w http.ResponseWriter, resp *http.Response, x streamExtractor,
	a streamArgs, ev *spool.Event, record func()) {
	fl, _ := w.(http.Flusher)
	w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	if fl != nil {
		fl.Flush()
	}
	writeFailed := false
	writeRaw := func(b []byte) {
		if writeFailed || b == nil {
			return
		}
		if _, err := w.Write(b); err != nil {
			writeFailed = true
			return
		}
		if fl != nil {
			fl.Flush()
		}
	}

	// SSE events are framed by blank lines and one event's data may span
	// several `data:` lines, so payloads are assembled per event before the
	// extractor sees them.
	var cur sseEvent
	var dataLines [][]byte
	// eventBytes is what the event being assembled holds so far. One event is
	// what this loop keeps in memory, so it is what the bound applies to.
	eventBytes := 0
	// eventCap is the bound on the event being assembled. An event that
	// outgrows the ordinary bound may go on to the large response bound only
	// by taking a large-body slot, the same rule a non-streamed answer
	// follows: an image generation streams its picture as one event, so a
	// stream needs the same room a non-streamed answer gets, and the pool is
	// what keeps that room a small multiplication.
	eventCap := upstreamResponseCapBytes
	releaseEvent := func() {}
	defer func() { releaseEvent() }()
	resetEventCap := func() {
		releaseEvent()
		releaseEvent, eventCap = func() {}, upstreamResponseCapBytes
	}
	overCap := func(n int) bool {
		if n <= eventCap {
			return false
		}
		largeMax := s.cfg.ChatLargeResponseMaxBytes
		if eventCap == upstreamResponseCapBytes && largeMax > upstreamResponseCapBytes {
			if release, ok := s.chatLarge.acquire(a.keyID); ok {
				releaseEvent, eventCap = release, int(largeMax)
				return n > eventCap
			}
		}
		return true
	}
	flushEvent := func() {
		// An event with no data is not dispatched, as SSE itself specifies.
		if len(dataLines) == 0 {
			cur = sseEvent{}
			resetEventCap()
			return
		}
		cur.data = bytes.Join(dataLines, []byte("\n"))
		e := cur
		cur, dataLines, eventBytes = sseEvent{}, nil, 0
		out := x.event(e)
		// The slot bounds what assembling and converting the event costs and
		// is given back before the write, as the non-stream path does, so a
		// client that reads slowly does not hold it.
		e.data = nil
		resetEventCap()
		writeRaw(out)
	}

	// Bounded per event, not in total. The memory a stream costs is the one
	// event being assembled: lines are gathered until the blank line that
	// ends an event, then joined into one more copy. Measured 2026-10-10,
	// sixteen 60 MiB streams of ordinary chunks peaked at 116 MiB, while one
	// event the size of a non-streamed response costs what that response
	// does. So one event, and one line within it, gets the non-stream bound,
	// or the large response bound with a slot (eventCap above), and the total
	// length is left to the request's duration cap.
	//
	// The bound fails loudly rather than reading as a finished stream, which
	// is what a plain io.LimitReader at a total cap used to do.
	br := bufio.NewReaderSize(resp.Body, 64<<10)
	readLine := func() ([]byte, error) {
		var line []byte
		for {
			frag, err := br.ReadSlice('\n')
			if overCap(len(line) + len(frag) + eventBytes) {
				return nil, errResponseOverCap
			}
			line = append(line, frag...)
			if errors.Is(err, bufio.ErrBufferFull) {
				continue
			}
			return line, err
		}
	}
	var readErr error
	for {
		line, err := readLine()
		trimmed := bytes.TrimRight(line, "\r\n")
		switch {
		case len(trimmed) == 0 && len(line) > 0:
			flushEvent()
		case len(trimmed) == 0:
		case trimmed[0] == ':':
			// Comment lines are keepalives; forward as-is, unless one holds a
			// bare CR, which a client reads as the end of the line and so as
			// the start of a line this relay never parsed.
			if !bytes.ContainsRune(trimmed, '\r') {
				writeRaw(append(trimmed, '\n', '\n'))
			}
		default:
			if after, ok := bytes.CutPrefix(trimmed, []byte("data:")); ok {
				data := bytes.TrimSpace(after)
				dataLines = append(dataLines, data)
				// Charged by what the line costs to keep, not by its payload:
				// an empty `data:` line still holds its own allocation and a
				// slice header, so counting only the data would let a stream
				// of empty lines grow one event without bound.
				eventBytes += len(trimmed) + dataLineOverhead
			} else if after, ok := bytes.CutPrefix(trimmed, []byte("event:")); ok {
				// The three other fields replace rather than accumulate, so
				// an event holds one of each however many lines carried them,
				// and each is kept only up to sseFieldMax.
				cur.name = cur.field(after)
			} else if after, ok := bytes.CutPrefix(trimmed, []byte("id:")); ok {
				cur.id = cur.field(after)
			} else if after, ok := bytes.CutPrefix(trimmed, []byte("retry:")); ok {
				cur.retry = cur.field(after)
			}
			// Any other line is not an SSE field this relay knows and is
			// dropped.
		}
		if writeFailed {
			break
		}
		if x.done() {
			// The answer is complete. Whatever the upstream does after its
			// end event (more bytes past the bound, a reset) must not turn a
			// finished answer into an error, or send a second end.
			break
		}
		if err != nil {
			if !errors.Is(err, io.EOF) {
				readErr = err
			} else {
				flushEvent()
			}
			break
		}
	}
	res := x.result()
	if res.dropped > 0 {
		s.log.Warn("dropped unparseable stream chunks", "keyId", a.keyID, "count", res.dropped)
	}

	// A stream that ends without the upstream's own end event (deadline or an
	// interrupted read) gets a terminal error event so the client can tell a
	// truncated answer from a complete one, rather than guessing from a
	// missing terminator.
	switch {
	case writeFailed || a.clientCtx.Err() != nil:
		ev.Status = spool.StatusCanceled
		ev.ErrorType = "client_disconnected"
	case !x.done() && (errors.Is(readErr, context.DeadlineExceeded) || errors.Is(a.upCtx.Err(), context.DeadlineExceeded)):
		// The gateway's own duration cap, not an upstream fault.
		ev.Status = spool.StatusTimeout
		ev.ErrorType = "request_deadline_exceeded"
		writeRaw(x.errorEvent("request_deadline_exceeded", "요청 전체 시간 상한을 초과해 스트림을 종료했습니다."))
	case errors.Is(readErr, errResponseOverCap):
		// This service's own relay bound, not an upstream fault, and the
		// caller can act on it, so it is named rather than reported as an
		// interrupted stream. The public code is the passthrough's for the
		// same condition.
		ev.Status = spool.StatusUpstreamErr
		ev.ErrorType = errPassthroughResponseTooLarge.code
		writeRaw(x.errorEvent(errPassthroughResponseTooLarge.code,
			"모델 서버 응답의 이벤트 하나가 중계 가능한 크기를 넘어 스트림을 종료했습니다. 응답이 불완전합니다."))
	case readErr != nil:
		ev.Status = spool.StatusUpstreamErr
		ev.ErrorType = "upstream_stream_interrupted"
		writeRaw(x.errorEvent("upstream_stream_interrupted", "모델 서버 응답이 중간에 끊겼습니다. 응답이 불완전할 수 있습니다."))
	case !x.done() && !res.upstreamError:
		// The connection closed cleanly but the upstream never said it was
		// done, so the answer may stop mid-sentence. To the caller this is the
		// same fact as an interrupted read, and gets the same public code; the
		// record keeps the two apart. An upstream that reported its own error
		// before closing is the next case: that error already went out and is
		// the more specific cause.
		ev.Status = spool.StatusUpstreamErr
		ev.ErrorType = "upstream_stream_unterminated"
		writeRaw(x.errorEvent("upstream_stream_interrupted", "모델 서버 응답이 중간에 끊겼습니다. 응답이 불완전할 수 있습니다."))
	case res.dropped > 0:
		// The stream reached its end, but some chunks could not be forwarded,
		// so the answer the student received is incomplete. The upstream's
		// end already went out, so this cannot be signalled inline anymore;
		// record it as degraded rather than let it read as a clean success.
		ev.Status = spool.StatusUpstreamErr
		ev.ErrorType = "upstream_chunk_unreadable"
	case res.upstreamError:
		// HTTP 200 is already committed; an explicit payload error still
		// makes this request a failure even if the stream ends cleanly.
		ev.Status = spool.StatusUpstreamErr
		ev.ErrorType = errUpstream.code
		if !x.done() {
			// The error itself was forwarded; only the end is missing.
			writeRaw(x.terminator())
		}
	default:
		ev.Status = spool.StatusOK
	}
	s.settleUsage(ev, res.usage, res.haveUsage, a.inputBytes, res.contentChars)
	a.charge(ev.InputTokens + ev.OutputTokens)
	if a.capture != nil {
		a.capture.Response, a.capture.ResponseTruncated = capString(res.answer)
		s.bodies.Offer(a.capture)
		s.metrics.bodiesCaptured.Add(1)
	}
	record()
}

// dataLineOverhead is what one buffered `data:` line costs beyond its bytes:
// its slice header in the event's list, and its separator when joined.
const dataLineOverhead = 32

// errResponseOverCap is what the stream reader returns once one event, or
// one line, outgrows the relay bound. It is an error, not EOF, so the loop
// cannot mistake a cut response for a finished one.
var errResponseOverCap = errors.New("upstream response exceeds the relay bound")

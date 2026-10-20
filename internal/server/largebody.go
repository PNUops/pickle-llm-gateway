package server

import (
	"errors"
	"io"
	"net/http"
	"sync"
	"time"
)

// largeBodySlots admits bodies above the ordinary bound. Chat has one pool,
// for requests, non-streamed responses and stream events above it; the
// passthrough surface has another of the same type, for requests and answers
// above it. Each pool is small and counted in total only: one key may hold
// every slot, so the count bounds memory, not fairness between keys. See
// config for the measured numbers.
type largeBodySlots struct {
	mu    sync.Mutex
	total int
	used  int
}

// newLargeBodySlots returns nil for a pool of size zero, which is how the
// large path is turned off.
func newLargeBodySlots(total int) *largeBodySlots {
	if total <= 0 {
		return nil
	}
	return &largeBodySlots{total: total}
}

// acquire takes a slot without waiting. A wait would hold the caller's
// connection, and with it the gateway-wide slot it already has, for a
// resource that is scarce on purpose. The returned release is safe to call
// more than once, so a handler can release early and still defer it.
//
// The key is not counted. The parameter stays so that every caller still
// names whose request it admits, should a share per key come back.
func (l *largeBodySlots) acquire(_ string) (release func(), ok bool) {
	if l == nil {
		return func() {}, false
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.used >= l.total {
		return func() {}, false
	}
	l.used++
	var once sync.Once
	return func() {
		once.Do(func() {
			l.mu.Lock()
			defer l.mu.Unlock()
			l.used--
		})
	}, true
}

// A large answer holds its slot until it has been written to the client, so
// the pool bounds every copy of it, the one waiting on a slow reader included
// (the edge does not buffer responses). What stops a slow reader from holding
// a slot indefinitely is a write deadline sized to the answer: sixty seconds
// plus two seconds per MiB, so a 53 MB 4K picture has about 160 seconds and
// 128 MiB has 316, the longest one slot can be held by a reader. That is a
// floor of about 0.5 MiB/s (4 Mbit/s); a client slower than that is cut off,
// after the vendor has billed the answer, and recorded as gone. The rate is
// the decision: lower it and a slot can be held longer.
const (
	largeWriteFloor      = 60 * time.Second
	largeWriteSecPerMiB  = 2
	largeWriteBytesInMiB = 1 << 20
)

// largeWriteBudget is how long an answer of n bytes may take to write.
func largeWriteBudget(n int) time.Duration {
	return largeWriteFloor + time.Duration(largeWriteSecPerMiB*(n/largeWriteBytesInMiB))*time.Second
}

// writeUnderDeadline runs write with the deadline for an n-byte answer and
// clears it afterwards, so the rest of a stream is not held to it. A writer
// that cannot take a deadline writes without one.
func (s *Server) writeUnderDeadline(w http.ResponseWriter, n int, write func()) {
	rc := http.NewResponseController(w)
	if rc.SetWriteDeadline(time.Now().Add(s.writeBudget(n))) != nil {
		write()
		return
	}
	defer func() { _ = rc.SetWriteDeadline(time.Time{}) }()
	write()
}

// inUse is a gauge for /metrics.
func (l *largeBodySlots) inUse() int {
	if l == nil {
		return 0
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.used
}

// upstreamBody is the request body sent upstream, held so that it can be let
// go of on purpose. A body handed to net/http as a bytes.Reader stays
// reachable through the request's GetBody, and the request stays reachable
// through the response's Request field for as long as the response is read —
// for a stream, up to the request's whole duration. Clearing fields on the
// request would depend on how the transport holds it internally; every reader
// here points at this holder instead, so emptying it frees the bytes however
// many copies of the request the transport kept.
type upstreamBody struct {
	mu sync.Mutex
	b  []byte
}

var errUpstreamBodyReleased = errors.New("upstream request body already released")

func (u *upstreamBody) reader() io.ReadCloser { return &upstreamBodyReader{h: u} }

// release drops the bytes. It is only called once the response headers have
// arrived, when the transport has finished sending them; a read after it
// fails rather than sending a short body. The one case that assumption misses
// is an upstream that starts answering before it has read the whole request,
// whose stream would then be cut. Chat servers parse the full JSON body before
// they answer, so this is accepted rather than guarded.
func (u *upstreamBody) release() {
	u.mu.Lock()
	u.b = nil
	u.mu.Unlock()
}

type upstreamBodyReader struct {
	h   *upstreamBody
	off int
}

func (r *upstreamBodyReader) Read(p []byte) (int, error) {
	r.h.mu.Lock()
	defer r.h.mu.Unlock()
	if r.h.b == nil {
		return 0, errUpstreamBodyReleased
	}
	if r.off >= len(r.h.b) {
		return 0, io.EOF
	}
	n := copy(p, r.h.b[r.off:])
	r.off += n
	return n, nil
}

func (r *upstreamBodyReader) Close() error { return nil }

type upstreamBodyKey struct{}

// releaseUpstreamBody frees the request body behind a response. It is a no-op
// for a response whose request carried none, or that has no request at all.
func releaseUpstreamBody(resp *http.Response) {
	if resp == nil || resp.Request == nil {
		return
	}
	if h, ok := resp.Request.Context().Value(upstreamBodyKey{}).(*upstreamBody); ok {
		h.release()
	}
}

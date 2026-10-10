package server

import (
	"errors"
	"io"
	"net/http"
	"sync"
)

// largeBodySlots admits chat bodies, and non-streamed chat responses, above
// the ordinary bound. The pool is small and counted per key as well as in
// total: the money axis has no per-key concurrency limit of its own, so
// without the per-key share one key could take every slot and refuse every
// other student's large request. See config for the measured numbers.
type largeBodySlots struct {
	mu     sync.Mutex
	total  int
	perKey int
	used   int
	byKey  map[string]int
}

// newLargeBodySlots returns nil for a pool of size zero, which is how the
// large path is turned off.
func newLargeBodySlots(total, perKey int) *largeBodySlots {
	if total <= 0 {
		return nil
	}
	if perKey <= 0 || perKey > total {
		perKey = total
	}
	return &largeBodySlots{total: total, perKey: perKey, byKey: map[string]int{}}
}

// acquire takes a slot without waiting. A wait would hold the caller's
// connection, and with it the gateway-wide slot it already has, for a
// resource that is scarce on purpose. The returned release is safe to call
// more than once, so a handler can release early and still defer it.
func (l *largeBodySlots) acquire(keyID string) (release func(), ok bool) {
	if l == nil {
		return func() {}, false
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.used >= l.total || l.byKey[keyID] >= l.perKey {
		return func() {}, false
	}
	l.used++
	l.byKey[keyID]++
	var once sync.Once
	return func() {
		once.Do(func() {
			l.mu.Lock()
			defer l.mu.Unlock()
			l.used--
			if l.byKey[keyID]--; l.byKey[keyID] <= 0 {
				delete(l.byKey, keyID)
			}
		})
	}, true
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

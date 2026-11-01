package server

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pnuops/pickle-llm-gateway/internal/config"
)

// agentSlots bounds the Messages turns in flight on each self-hosted upstream
// that serves that surface, across every key. A per-key concurrency limit
// cannot do this: eight students with one agent each are eight long prefills
// on one box, each within its own limit.
//
// A turn holds its place from admission until its answer has been relayed in
// full, a stream included, because the sequence it occupies on the upstream is
// held for that long too. One over the bound waits up to the upstream's queue
// wait for a place and is refused after that; the wait holds the caller's
// connection and its gateway-wide in-flight place, which is why it is short.
type agentSlots struct {
	pools map[string]*agentPool // by lowercase upstream ref
}

type agentPool struct {
	sem     chan struct{}
	wait    time.Duration
	waiting atomic.Int64
}

// newAgentSlots makes one pool per upstream that is open on Messages with a
// bound. An upstream without one has no entry and admits every turn.
func newAgentSlots(ups map[string]config.Upstream) *agentSlots {
	a := &agentSlots{pools: map[string]*agentPool{}}
	for ref, up := range ups {
		if up.Messages != config.MessagesOpen || up.MessagesMaxConcurrent <= 0 {
			continue
		}
		a.pools[ref] = &agentPool{sem: make(chan struct{}, up.MessagesMaxConcurrent), wait: up.MessagesQueueWait}
	}
	return a
}

// acquire takes a place on ref's pool, waiting up to the pool's queue wait.
// ok is false when none came free in time or the caller went away first. The
// returned release is safe to call more than once.
func (a *agentSlots) acquire(ctx context.Context, ref string) (release func(), ok bool) {
	p := a.pools[strings.ToLower(ref)]
	if p == nil {
		return func() {}, true
	}
	select {
	case p.sem <- struct{}{}:
		return p.releaser(), true
	default:
	}
	// The queue is no deeper than the pool: a turn behind that many others
	// would wait longer than the queue wait in all but the rarest case, and
	// each waiter holds a connection and a gateway-wide in-flight place.
	if p.wait <= 0 || p.waiting.Load() >= int64(cap(p.sem)) {
		return func() {}, false
	}
	p.waiting.Add(1)
	defer p.waiting.Add(-1)
	timer := time.NewTimer(p.wait)
	defer timer.Stop()
	select {
	case p.sem <- struct{}{}:
		return p.releaser(), true
	case <-timer.C:
		return func() {}, false
	case <-ctx.Done():
		return func() {}, false
	}
}

func (p *agentPool) releaser() func() {
	var once sync.Once
	return func() { once.Do(func() { <-p.sem }) }
}

// snapshot reports each pool's use for the metrics surface.
func (a *agentSlots) snapshot() map[string]map[string]int {
	out := map[string]map[string]int{}
	for ref, p := range a.pools {
		out[ref] = map[string]int{
			"inUse":   len(p.sem),
			"limit":   cap(p.sem),
			"waiting": int(p.waiting.Load()),
		}
	}
	return out
}

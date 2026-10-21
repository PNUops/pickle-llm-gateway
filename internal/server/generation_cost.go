package server

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pnuops/pickle-llm-gateway/internal/config"
	"github.com/pnuops/pickle-llm-gateway/internal/spool"
)

// A streamed answer on a money-axis model does not always carry its price:
// some vendor surfaces put usage in the stream but no cost. The vendor keeps a
// record per generation that does carry it, so once such a stream has ended
// its cost is read from there before the usage event is written. This is a
// rule for every stream, not for one route: whichever surface relayed it, a
// stream that ended without a cost gets one looked up.
//
// The passthrough surface uses the same lookup for its answers, which are
// not streamed but may still end without a price: on a route whose usage is
// optional or absent, and on any route for an answer the vendor billed but
// this gateway did not hand over (see handlePassthrough for which). Its
// records are published later, so it has its own schedule, and its own pool
// and counters, so a route that looks up every call cannot take the room
// broken-off streams need and a rise in one surface's misses is not read as
// the other's.
//
// The lookup runs after the handler has returned. A streamed response is only
// complete for the client when its handler returns, so waiting here would hold
// every client's stream open for as long as the vendor takes to publish the
// record, which is not immediate: measured on 2026-10-14, the record answers
// 404 for about 11 to 12 seconds after a stream ends (5 to 6 for an image or a
// cache hit), then 200. A stream that ends cleanly carries its cost in its
// last usage on every route, so the lookup is for the streams that did not
// get that far: a client that went away, an event past the relay bound, an
// upstream that broke off. A request the vendor failed keeps answering 404
// for good, which looks the same as not yet published, so the lookup gives up
// at a deadline rather than on any answer.

// generationCostDelays is the wait before each read of the generation record:
// the first just past the measured publishing delay, the rest backing off,
// at 12, 18, 28, 43 and 58 seconds after the stream ended.
var generationCostDelays = []time.Duration{12 * time.Second, 6 * time.Second, 10 * time.Second,
	15 * time.Second, 15 * time.Second}

// generationCostDeadline is how long after the stream a lookup may run,
// reads included; past it the event is written without a cost. It is also
// the most a lookup holds its slot.
const generationCostDeadline = 60 * time.Second

// passthroughCostDelays is the passthrough schedule. The records of calls
// that are not chat completions are published later: measured on
// 2026-10-21, a synthesized voice's and a rerank's answered 404 for 15
// seconds and 200 at 25. So the first read waits 15 seconds and the reads run
// at 15, 25, 35, 50, 70 and 100 seconds.
var passthroughCostDelays = []time.Duration{15 * time.Second, 10 * time.Second, 10 * time.Second,
	15 * time.Second, 20 * time.Second, 30 * time.Second}

// passthroughCostDeadline is the passthrough counterpart of
// generationCostDeadline. It is longer than the drain at shutdown, so a
// lookup still waiting then is written without a cost.
const passthroughCostDeadline = 2 * time.Minute

// generationCostReadTimeout bounds one read of the record.
const generationCostReadTimeout = 5 * time.Second

// generationCostPending bounds the stream lookups waiting at once. Each holds
// one usage event in memory; past the bound an event is written without a
// cost. With a lookup lasting up to the deadline, this is room for about
// eight cost-less stream endings a second, sustained; ordinary streams carry
// their cost and never take a slot.
const generationCostPending = 512

// passthroughCostPending bounds the passthrough lookups, in a pool of their
// own so that a route whose every answer is looked up (a synthesized voice
// carries no usage at all) cannot use up the room broken-off streams need.
// At the two-minute deadline it is room for about two lookups a second
// sustained, and about ten when records appear at the measured 25 seconds.
const passthroughCostPending = 256

// generationCostReadLimit bounds one generation record. A real one is about
// a kilobyte and a half.
const generationCostReadLimit = 64 << 10

// costSchedule is one surface's lookup schedule, its pool of pending slots
// and the counters its outcomes are added to.
type costSchedule struct {
	slots    chan struct{}
	delays   []time.Duration
	deadline time.Duration
	filled   *atomic.Int64
	missing  *atomic.Int64
}

type generationCosts struct {
	wg sync.WaitGroup
	// mu orders admitting a lookup against the start of a drain: a
	// WaitGroup must not gain work while something waits on it, so once
	// draining is set no lookup is admitted.
	mu          sync.Mutex
	draining    bool
	ctx         context.Context
	cancel      context.CancelFunc
	stream      costSchedule
	passthrough costSchedule
}

func newGenerationCosts(m *counters) *generationCosts {
	ctx, cancel := context.WithCancel(context.Background())
	return &generationCosts{
		ctx:    ctx,
		cancel: cancel,
		stream: costSchedule{
			slots:    make(chan struct{}, generationCostPending),
			delays:   generationCostDelays,
			deadline: generationCostDeadline,
			filled:   &m.streamCostFilled,
			missing:  &m.streamCostMissing,
		},
		passthrough: costSchedule{
			slots:    make(chan struct{}, passthroughCostPending),
			delays:   passthroughCostDelays,
			deadline: passthroughCostDeadline,
			filled:   &m.passthroughCostFilled,
			missing:  &m.passthroughCostMissing,
		},
	}
}

// DrainCostLookups waits for pending generation lookups so their events are
// written before the spool closes. Once ctx is done the ones still waiting give
// up and write their events without a cost; it returns when every event is
// written either way.
//
// It is called once the listeners have stopped, so no stream normally ends
// after it begins; one that does is written at once without a cost.
func (s *Server) DrainCostLookups(ctx context.Context) {
	s.costs.mu.Lock()
	s.costs.draining = true
	s.costs.mu.Unlock()
	done := make(chan struct{})
	go func() {
		s.costs.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		s.costs.cancel()
		<-done
	}
}

// withGenerationCost wraps a stream's record so that an event ending without
// a cost has it read from the vendor's generation record first. stopClock fixes
// the request's latency at the end of the stream, so the wait for the record
// is not counted as the request's own time. It returns record unchanged when
// there is nothing to look up with: no generation id, or no credential. sched
// is the surface's schedule, pool and counters.
func (s *Server) withGenerationCost(id string, up config.Upstream, cred string,
	ev *spool.Event, stopClock, record func(), sched *costSchedule) func() {
	if !generationIDPattern.MatchString(id) || cred == "" {
		return record
	}
	return func() {
		if ev.CostUsd != "" {
			record()
			return
		}
		stopClock()
		if !s.costs.admit(sched) {
			sched.missing.Add(1)
			s.log.Warn("generation cost lookup skipped", "keyId", ev.KeyID)
			record()
			return
		}
		go func() {
			defer s.costs.wg.Done()
			defer func() { <-sched.slots }()
			if cost := s.lookupGenerationCost(sched, up, cred, id); cost != "" {
				ev.CostUsd = cost
				sched.filled.Add(1)
			} else {
				sched.missing.Add(1)
				s.log.Warn("generation cost not found", "keyId", ev.KeyID, "generationId", id)
			}
			record()
		}()
	}
}

// admit takes a pending slot in sched and registers the lookup with the
// WaitGroup, or reports that the event has to be written now: the pending
// bound is reached, or the gateway is draining.
func (c *generationCosts) admit(sched *costSchedule) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.draining {
		return false
	}
	select {
	case sched.slots <- struct{}{}:
	default:
		return false
	}
	c.wg.Add(1)
	return true
}

// lookupGenerationCost reads the generation record until it has a usable
// cost, the schedule runs out, or the gateway is shutting down. It returns ""
// when no cost was found.
func (s *Server) lookupGenerationCost(sched *costSchedule, up config.Upstream, cred, id string) json.Number {
	ctx, cancel := context.WithTimeout(s.costs.ctx, sched.deadline)
	defer cancel()
	for _, wait := range sched.delays {
		t := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			t.Stop()
			return ""
		case <-t.C:
		}
		cost, retry := s.readGenerationCost(ctx, up, cred, id)
		if cost != "" || !retry {
			return cost
		}
	}
	return ""
}

// readGenerationCost makes one read. retry says whether a later read could
// still succeed: a record not yet published, a throttle or a server fault can;
// a refused credential or a record without a readable cost cannot.
func (s *Server) readGenerationCost(lookup context.Context, up config.Upstream, cred, id string) (cost json.Number, retry bool) {
	ctx, cancel := context.WithTimeout(lookup, generationCostReadTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		up.BaseURL+"/generation?id="+url.QueryEscape(id), http.NoBody)
	if err != nil {
		return "", false
	}
	req.Header.Set("Authorization", "Bearer "+cred)
	resp, err := s.client.Do(req)
	if err != nil {
		return "", lookup.Err() == nil
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		// Read out so the connection can be reused for the next read.
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, generationCostReadLimit))
	}
	switch {
	case resp.StatusCode == http.StatusOK:
	case resp.StatusCode == http.StatusNotFound, resp.StatusCode == http.StatusTooManyRequests,
		resp.StatusCode >= 500:
		return "", true
	default:
		return "", false
	}
	var rec struct {
		Data struct {
			// total_cost is what the vendor charged. upstream_inference_cost
			// comes as 0 and is not read, and neither are the record's token
			// counts: a cache hit's record carries the original request's
			// tokens at a cost of 0, so the answer's own usage stays.
			TotalCost json.Number `json:"total_cost"`
		} `json:"data"`
	}
	dec := json.NewDecoder(io.LimitReader(resp.Body, generationCostReadLimit))
	dec.UseNumber()
	if dec.Decode(&rec) != nil {
		return "", false
	}
	return spool.CostLiteral(rec.Data.TotalCost), false
}

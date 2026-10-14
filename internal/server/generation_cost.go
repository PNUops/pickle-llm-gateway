package server

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"sync"
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

// generationCostReadTimeout bounds one read of the record.
const generationCostReadTimeout = 5 * time.Second

// generationCostPending bounds the lookups waiting at once. Each holds one
// usage event in memory; past the bound an event is written without a cost.
// With a lookup lasting up to the deadline, this is room for about eight
// cost-less stream endings a second, sustained; ordinary streams carry their
// cost and never take a slot.
const generationCostPending = 512

// generationCostReadLimit bounds one generation record. A real one is about
// a kilobyte and a half.
const generationCostReadLimit = 64 << 10

type generationCosts struct {
	wg sync.WaitGroup
	// mu orders admitting a lookup against the start of a drain: a
	// WaitGroup must not gain work while something waits on it, so once
	// draining is set no lookup is admitted.
	mu       sync.Mutex
	draining bool
	slots    chan struct{}
	ctx      context.Context
	cancel   context.CancelFunc
	delays   []time.Duration
	deadline time.Duration
}

func newGenerationCosts() *generationCosts {
	ctx, cancel := context.WithCancel(context.Background())
	return &generationCosts{
		slots:    make(chan struct{}, generationCostPending),
		ctx:      ctx,
		cancel:   cancel,
		delays:   generationCostDelays,
		deadline: generationCostDeadline,
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
// there is nothing to look up with: no generation id, or no credential.
func (s *Server) withGenerationCost(upstream http.Header, up config.Upstream, cred string,
	ev *spool.Event, stopClock, record func()) func() {
	id := upstream.Get(generationIDHeader)
	if !generationIDPattern.MatchString(id) || cred == "" {
		return record
	}
	return func() {
		if ev.CostUsd != "" {
			record()
			return
		}
		stopClock()
		if !s.costs.admit() {
			s.metrics.streamCostMissing.Add(1)
			s.log.Warn("generation cost lookup skipped", "keyId", ev.KeyID)
			record()
			return
		}
		go func() {
			defer s.costs.wg.Done()
			defer func() { <-s.costs.slots }()
			if cost := s.lookupGenerationCost(up, cred, id); cost != "" {
				ev.CostUsd = cost
				s.metrics.streamCostFilled.Add(1)
			} else {
				s.metrics.streamCostMissing.Add(1)
				s.log.Warn("generation cost not found", "keyId", ev.KeyID, "generationId", id)
			}
			record()
		}()
	}
}

// admit takes a pending slot and registers the lookup with the WaitGroup, or
// reports that the event has to be written now: the pending bound is reached,
// or the gateway is draining.
func (c *generationCosts) admit() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.draining {
		return false
	}
	select {
	case c.slots <- struct{}{}:
	default:
		return false
	}
	c.wg.Add(1)
	return true
}

// lookupGenerationCost reads the generation record until it has a usable
// cost, the schedule runs out, or the gateway is shutting down. It returns ""
// when no cost was found.
func (s *Server) lookupGenerationCost(up config.Upstream, cred, id string) json.Number {
	c := s.costs
	ctx, cancel := context.WithTimeout(c.ctx, c.deadline)
	defer cancel()
	for _, wait := range c.delays {
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

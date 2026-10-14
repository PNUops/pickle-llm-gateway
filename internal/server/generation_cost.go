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
// record, which is not immediate: the first read after a stream ends is
// normally a 404.

// generationCostDelays is the wait before each read of the generation record.
// The record has been seen to appear within a couple of seconds of the end of
// a stream; the tail covers a slow one, and past it the event is written
// without a cost rather than held further.
var generationCostDelays = []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second}

// generationCostPending bounds the lookups waiting at once. Each holds one
// usage event in memory; past the bound an event is written without a cost.
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
}

func newGenerationCosts() *generationCosts {
	ctx, cancel := context.WithCancel(context.Background())
	return &generationCosts{
		slots:  make(chan struct{}, generationCostPending),
		ctx:    ctx,
		cancel: cancel,
		delays: generationCostDelays,
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
	for _, wait := range c.delays {
		t := time.NewTimer(wait)
		select {
		case <-c.ctx.Done():
			t.Stop()
			return ""
		case <-t.C:
		}
		cost, retry := s.readGenerationCost(up, cred, id)
		if cost != "" || !retry {
			return cost
		}
	}
	return ""
}

// readGenerationCost makes one read. retry says whether a later read could
// still succeed: a record not yet published, a throttle or a server fault can;
// a refused credential or a record without a readable cost cannot.
func (s *Server) readGenerationCost(up config.Upstream, cred, id string) (cost json.Number, retry bool) {
	ctx, cancel := context.WithTimeout(s.costs.ctx, 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		up.BaseURL+"/generation?id="+url.QueryEscape(id), http.NoBody)
	if err != nil {
		return "", false
	}
	req.Header.Set("Authorization", "Bearer "+cred)
	resp, err := s.client.Do(req)
	if err != nil {
		return "", s.costs.ctx.Err() == nil
	}
	defer resp.Body.Close()
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

package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pnuops/pickle-llm-gateway/internal/config"
	"github.com/pnuops/pickle-llm-gateway/internal/snapshot"
)

// attemptError carries why every upstream attempt failed. refusal is set when
// the failure is the request's own fault and must be passed through rather
// than retried anywhere.
type attemptError struct {
	err     error
	timeout bool
	// throttled marks an upstream that asked us to slow down (429). It is not
	// retried on the same upstream, and the student sees "busy" rather than a
	// generic upstream failure.
	throttled bool
	refusal   *apiError
}

// upstreamHealth tracks consecutive failures per upstream so a dead one stops
// being tried first. It is advisory: an upstream in cooldown is skipped only
// while something else can serve the request.
type upstreamHealth struct {
	mu        sync.Mutex
	failures  map[string]int
	coolUntil map[string]time.Time
	now       func() time.Time
}

func newUpstreamHealth(now func() time.Time) *upstreamHealth {
	if now == nil {
		now = time.Now
	}
	return &upstreamHealth{
		failures:  map[string]int{},
		coolUntil: map[string]time.Time{},
		now:       now,
	}
}

// coolingFailures is how many consecutive failures put an upstream aside, and
// coolFor is how long. Small numbers on purpose: the point is to stop hammering
// something that is plainly down, not to build a circuit breaker.
const (
	coolingFailures = 3
	coolFor         = 30 * time.Second
)

func (h *upstreamHealth) cooling(ref string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	until, ok := h.coolUntil[ref]
	return ok && h.now().Before(until)
}

func (h *upstreamHealth) recordFailure(ref string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.failures[ref]++
	if h.failures[ref] >= coolingFailures {
		h.coolUntil[ref] = h.now().Add(coolFor)
		h.failures[ref] = 0
		return true
	}
	return false
}

func (h *upstreamHealth) recordSuccess(ref string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.failures, ref)
	delete(h.coolUntil, ref)
}

// callUpstream sends the request to the model's upstream, retrying a transient
// failure and falling back to the model's secondary upstream if it has one.
// This can only be done before anything reaches the client, which is why it
// lives entirely ahead of the response-writing paths.
// The returned count is how many upstream attempts were made, including the
// one that succeeded: 1 is the ordinary path and anything above it means the
// request cost more than it looks like it did.
func (s *Server) callUpstream(ctx context.Context, model *snapshot.Model, key *snapshot.Key,
	params map[string]json.RawMessage, outputCap int) (*http.Response, config.Upstream, int, *attemptError) {
	attempts := 0

	refs := []string{model.UpstreamRef}
	if model.FallbackRef != "" && !strings.EqualFold(model.FallbackRef, model.UpstreamRef) {
		refs = append(refs, model.FallbackRef)
	}
	// Prefer an upstream that is not in cooldown, keeping the original order
	// among equals. With one option left there is nothing to prefer.
	if len(refs) > 1 {
		ordered := make([]string, 0, len(refs))
		for _, ref := range refs {
			if !s.health.cooling(ref) {
				ordered = append(ordered, ref)
			}
		}
		for _, ref := range refs {
			if s.health.cooling(ref) {
				ordered = append(ordered, ref)
			}
		}
		refs = ordered
	}

	var last *attemptError
	for i, ref := range refs {
		up, ok := s.cfg.Upstreams[strings.ToLower(ref)]
		if !ok {
			s.log.Error("model references an unconfigured upstream",
				"model", model.PublicName, "upstreamRef", ref)
			last = &attemptError{err: errUnconfiguredUpstream}
			continue
		}
		// Which bearer this attempt carries. A CREDIT-axis model spends the
		// key's own money budget, so only the key's credential for this
		// upstream will do — the gateway-wide env credential is deliberately
		// never a fallback there (it would spend a shared budget for a key
		// that was never granted one). The primary ref was already checked
		// before anything was forwarded; this guards the fallback ref too.
		cred := up.APIKey
		if model.CreditAxis() {
			cred = key.CredentialFor(up.Ref)
			if cred == "" {
				last = &attemptError{err: errNoKeyCredential}
				continue
			}
		}
		body, err := s.bodyFor(params, up, outputCap)
		if err != nil {
			return nil, up, attempts, &attemptError{err: err}
		}
		for attempt := 0; attempt <= s.cfg.UpstreamRetries; attempt++ {
			attempts++
			resp, ae := s.attempt(ctx, up, body, cred, model.CreditAxis())
			if ae == nil {
				s.health.recordSuccess(ref)
				return resp, up, attempts, nil
			}
			last = ae
			if ae.refusal != nil {
				// The upstream understood and refused: another upstream would
				// refuse the same request, and a retry would only repeat it.
				return nil, up, attempts, ae
			}
			if ctx.Err() != nil {
				return nil, up, attempts, ae
			}
			if ae.throttled {
				// Same upstream, immediately again, is exactly what it asked
				// us not to do. Move on to the fallback if there is one.
				break
			}
			if ae.timeout {
				// Do not send this again to *this* upstream. A completion is
				// not idempotent and it may well be generating right now — it
				// accepted the POST and is simply slow — so repeating it bills
				// a second generation for an answer nobody will read twice.
				//
				// Falling through to the model's fallback is a different
				// matter and is deliberate: from the student's side a timed-out
				// upstream is a down upstream, which is exactly what the
				// fallback exists for. It costs one generation on the second
				// upstream while the first may still be producing one, so the
				// event records which upstream answered and how many attempts
				// it took — see the spool schema.
				break
			}
			if s.health.recordFailure(ref) {
				s.log.Warn("upstream put in cooldown after repeated failures",
					"upstreamRef", ref, "coolFor", coolFor.String())
			}
			s.metrics.upstreamRetries.Add(1)
		}
		// Only when there is somewhere left to fall back to. Counting the last
		// exhausted upstream as a fallback makes the metric and the log both
		// claim a recovery for requests that simply died.
		if i < len(refs)-1 {
			s.metrics.upstreamFallbacks.Add(1)
			s.log.Warn("falling back to the model's secondary upstream",
				"model", model.PublicName, "from", ref)
		}
	}
	if last == nil {
		last = &attemptError{err: errUnconfiguredUpstream}
	}
	return nil, config.Upstream{}, attempts, last
}

// bodyFor renders the request for one upstream, injecting the output cap on
// the field that upstream honors.
func (s *Server) bodyFor(params map[string]json.RawMessage, up config.Upstream, outputCap int) ([]byte, error) {
	if outputCap <= 0 {
		return json.Marshal(params)
	}
	out := make(map[string]json.RawMessage, len(params)+1)
	for k, v := range params {
		out[k] = v
	}
	out[up.CapField] = json.RawMessage(strconv.Itoa(outputCap))
	return json.Marshal(out)
}

// attempt performs one upstream call carrying the given bearer (empty sends
// no auth header). A nil error means resp is a 200 whose body the caller now
// owns.
func (s *Server) attempt(ctx context.Context, up config.Upstream, body []byte, cred string,
	creditAxis bool) (*http.Response, *attemptError) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, up.BaseURL+"/chat/completions", bytes.NewReader(body))
	if err != nil {
		return nil, &attemptError{err: err}
	}
	req.Header.Set("Content-Type", "application/json")
	if cred != "" {
		req.Header.Set("Authorization", "Bearer "+cred)
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return nil, &attemptError{err: err, timeout: isTimeout(err)}
	}
	if resp.StatusCode == http.StatusOK {
		return resp, nil
	}
	detail, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	resp.Body.Close()
	s.log.Warn("upstream refused request", "status", resp.StatusCode,
		"upstreamRef", up.Ref, "detail", upstreamReason(detail))
	switch resp.StatusCode {
	case http.StatusBadRequest, http.StatusUnprocessableEntity:
		// The request itself is wrong; no other upstream will like it better.
		return nil, &attemptError{refusal: &errUpstreamRejected}
	case http.StatusTooManyRequests:
		// The upstream is throttling us. Retrying immediately makes it worse,
		// and the student should be told the service is busy rather than that
		// something broke. A fallback upstream is still worth trying, so this
		// is a throttle, not a refusal.
		return nil, &attemptError{err: errUpstreamThrottled, throttled: true}
	case http.StatusUnauthorized, http.StatusForbidden:
		// Our credential, not the student's problem — and not something a
		// retry fixes. Fall through to another upstream if one exists.
		return nil, &attemptError{err: errUpstreamAuth}
	case http.StatusPaymentRequired:
		if creditAxis {
			// The money ran out on this key's own budget. This is the money
			// axis working, not a fault: retrying spends nothing and fixes
			// nothing, another upstream would answer the same, and cooling
			// the upstream down for it would let one exhausted key reorder
			// everybody else's traffic. Refuse it in the student's own words.
			return nil, &attemptError{refusal: &errCreditExhausted}
		}
		// A TOKEN-axis model runs on the gateway's own account, so a 402
		// there is the platform's bill, not the student's — telling them to
		// request a limit increase would send them to fix something they do
		// not own. Treat it as the upstream fault it is, which also keeps
		// the model's fallback in play: that fallback exists for exactly
		// "the primary cannot serve right now".
		return nil, &attemptError{err: errUpstreamStatus}
	default:
		return nil, &attemptError{err: errUpstreamStatus}
	}
}

// upstreamReason reduces an upstream's error body to the part that is safe to
// keep. The body is the upstream's, not ours, and a provider rejecting a
// request routinely quotes the offending part of it back — which on this
// service is a student's prompt. The service records counters, not text, so
// the free-form message must not land in the journal by way of an error path.
//
// An OpenAI-shaped body has the two fields worth having; anything else is
// reported by its shape alone.
func upstreamReason(detail []byte) string {
	var body struct {
		Error struct {
			Type string `json:"type"`
			Code any    `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(detail, &body); err == nil && (body.Error.Type != "" || body.Error.Code != nil) {
		code, _ := body.Error.Code.(string)
		return strings.TrimSpace(body.Error.Type + " " + code)
	}
	return fmt.Sprintf("unparseable body, %d bytes", len(detail))
}

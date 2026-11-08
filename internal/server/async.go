package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/pnuops/pickle-llm-gateway/internal/config"
	"github.com/pnuops/pickle-llm-gateway/internal/ownership"
	"github.com/pnuops/pickle-llm-gateway/internal/snapshot"
	"github.com/pnuops/pickle-llm-gateway/internal/spool"
)

// The async routes: requests that create a vendor object (a batch) or read,
// list or delete one later. The vendor scopes these objects to a workspace
// that every key of a business account shares, so the vendor keeps one key
// out of another's objects not at all. The api's ownership ledger does
// (internal/ownership): a submit records an intent before the vendor call and
// binds the vendor id after it, and every later request that names an object
// asks the ledger first. A request the ledger cannot decide answers 503;
// nothing outside the async routes waits on it.

// maxObjectIDBytes bounds an object id taken from a path or a query. The
// ledger stores at most this many; a longer one cannot be anybody's.
const maxObjectIDBytes = 255

// asyncCall is one request on an async route, past the checks every such
// request shares: the service switch, authentication, the route's grant, the
// money credential and the gateway-wide slot.
type asyncCall struct {
	s       *Server
	w       http.ResponseWriter
	r       *http.Request
	ev      spool.Event
	doc     snapshot.Document
	key     *snapshot.Key
	up      config.Upstream
	cred    string
	large   *passthroughLargeHold
	latency time.Duration
	start   time.Time
	// done releases the gateway-wide slot and the large-body slot.
	done func()
}

// beginAsync runs the shared checks. On a refusal it has answered and
// recorded, and returns nil; otherwise the caller owns the call and must run
// its done.
func (s *Server) beginAsync(w http.ResponseWriter, r *http.Request, endpoint, capability string) *asyncCall {
	start := s.now()
	noAttempts := 0
	c := &asyncCall{s: s, w: w, r: r, start: start, latency: -1, done: func() {}}
	c.ev = spool.Event{
		EventUUID:   spool.NewEventUUID(),
		RequestedAt: start,
		Endpoint:    endpoint,
		Attempts:    &noAttempts,
	}
	w.Header().Set("X-Request-Id", c.ev.EventUUID)

	doc, keyLookup, _ := s.store.Current()
	c.doc = doc
	c.ev.Generation = doc.Generation
	if !doc.ServiceEnabled {
		c.refuse(errServiceDisabled, spool.StatusAuthRejected)
		return nil
	}
	key, authErr := s.authenticate(r, keyLookup)
	if key != nil {
		c.ev.KeyID = key.KeyID
	}
	if authErr != nil {
		c.refuse(*authErr, spool.StatusAuthRejected)
		return nil
	}
	c.key = key
	if !key.AllowsEndpoint(capability) {
		c.refuse(errEndpointNotAllowed(endpointLabel(capability)), spool.StatusAuthRejected)
		return nil
	}
	if doc.PassthroughRef == "" {
		s.log.Error("async route reached with no passthroughRef in the document",
			"path", r.URL.Path, "keyId", key.KeyID)
		c.refuse(errServiceDisabled, spool.StatusUpstreamErr)
		return nil
	}
	c.ev.BudgetAxis = snapshot.AxisCredit
	ref := strings.ToLower(doc.PassthroughRef)
	c.cred = key.CredentialFor(ref)
	if c.cred == "" {
		if key.CreditPending {
			c.refuse(errCreditPending, spool.StatusAuthRejected)
			return nil
		}
		c.refuse(errCreditUnavailable, spool.StatusAuthRejected)
		return nil
	}
	up, ok := s.cfg.Upstreams[ref]
	if !ok {
		s.log.Error("async upstream is not configured", "upstreamRef", ref)
		c.refuse(errServiceDisabled, spool.StatusUpstreamErr)
		return nil
	}
	c.up = up
	c.ev.UpstreamRef = up.Ref
	// A gateway without the control link cannot ask who owns anything, and
	// guessing would hand one key's objects to another.
	if s.ownership == nil {
		c.refuse(errOwnershipUnavailable, spool.StatusUpstreamErr)
		return nil
	}
	select {
	case s.inFlight <- struct{}{}:
	default:
		c.refuse(errServerBusy, spool.StatusRateLimited)
		return nil
	}
	c.large = &passthroughLargeHold{pool: s.passthroughLarge, keyID: key.KeyID}
	c.done = func() {
		c.large.done()
		<-s.inFlight
	}
	return c
}

func (c *asyncCall) stopClock() {
	if c.latency < 0 {
		c.latency = time.Since(c.start)
	}
}

// record writes the request's own usage event. Like every route, an attempt
// that never resolved to a key is counted but not spooled.
func (c *asyncCall) record() {
	c.stopClock()
	c.ev.LatencyMs = c.latency.Milliseconds()
	c.s.metrics.observe(c.ev)
	if c.ev.KeyID == "" && c.ev.Status == spool.StatusAuthRejected {
		return
	}
	if err := c.s.spool.Write(c.ev); err != nil {
		c.s.metrics.spoolWriteFailures.Add(1)
		c.s.log.Error("usage spool write failed", "error", err)
	}
	c.s.log.Info("async request", "keyId", c.ev.KeyID, "method", c.r.Method,
		"path", c.r.URL.Path, "model", c.ev.PublicModelName, "status", c.ev.Status,
		"errorType", c.ev.ErrorType, "latencyMs", c.ev.LatencyMs)
}

func (c *asyncCall) refuse(e apiError, status string) {
	c.refuseAs(e, status, e.code)
}

func (c *asyncCall) refuseAs(e apiError, status, errorType string) {
	writeAPIError(c.w, e)
	c.ev.Status = status
	c.ev.ErrorType = errorType
	c.record()
}

// owns asks the ledger whether this key owns the object. On anything but a
// yes it has answered and recorded: another key's object and no object get
// the same 404, and a ledger that cannot be asked gets 503.
func (c *asyncCall) owns(kind ownership.Kind, id string) bool {
	owned, err := c.s.ownership.Owned(c.r.Context(), c.key.KeyID, kind, []string{id})
	if err != nil {
		if c.r.Context().Err() != nil {
			c.ev.Status = spool.StatusCanceled
			c.ev.ErrorType = "client_disconnected"
			c.record()
			return false
		}
		c.s.log.Warn("ownership lookup failed", "keyId", c.key.KeyID, "kind", kind, "error", err)
		c.refuse(errOwnershipUnavailable, spool.StatusUpstreamErr)
		return false
	}
	if !owned[id] {
		c.refuse(errObjectNotFound, spool.StatusBadRequest)
		return false
	}
	return true
}

// release tells the ledger an object is gone. It runs after the client has
// its answer and is not retried: a release that does not land is healed the
// next time the owner asks for the object and the vendor says it is gone.
func (c *asyncCall) release(kind ownership.Kind, objectID string, reason ownership.ReleaseReason) {
	ctx := context.WithoutCancel(c.r.Context())
	if _, err := c.s.ownership.Release(ctx, c.key.KeyID, kind, objectID, "", reason); err != nil {
		c.s.log.Warn("ownership release failed", "keyId", c.key.KeyID, "kind", kind,
			"objectId", objectID, "reason", reason, "error", err)
	}
}

// releaseIntent drops an intent whose submit the vendor refused.
func (c *asyncCall) releaseIntent(kind ownership.Kind, intentID string) {
	ctx := context.WithoutCancel(c.r.Context())
	if _, err := c.s.ownership.Release(ctx, c.key.KeyID, kind, "", intentID, ownership.ReleaseNotCreated); err != nil {
		c.s.log.Warn("ownership intent release failed", "keyId", c.key.KeyID, "kind", kind,
			"intentId", intentID, "error", err)
	}
}

// call performs the one upstream attempt. Statuses in pass come back as a
// response; every other failure has been answered and recorded, and the
// second return is false. Like the passthrough surface it never retries: a
// submit the vendor may have accepted must not be sent twice.
func (c *asyncCall) call(ctx context.Context, method, path string, body []byte, pass map[int]bool) (*http.Response, *attemptError) {
	oneAttempt := 1
	c.ev.Attempts = &oneAttempt
	ref := strings.ToLower(c.doc.PassthroughRef)
	c.s.health.recordAttempt(ref)
	resp, ae := c.s.do(ctx, c.up, attemptSpec{
		client:     c.s.passthroughClient,
		method:     method,
		path:       path,
		body:       body,
		cred:       c.cred,
		creditAxis: true,
		headers:    passthroughForwardedHeaders(c.r.Header),
		pass:       pass,
	})
	if ae != nil {
		c.s.health.recordPassiveFailure(ref, ae.kind)
		return nil, ae
	}
	c.s.health.recordSuccess(ref)
	return resp, nil
}

// answerFailure answers an upstream failure the way the passthrough surface
// does.
func (c *asyncCall) answerFailure(ctx context.Context, ae *attemptError) {
	switch {
	case c.r.Context().Err() != nil:
		c.ev.Status = spool.StatusCanceled
		c.ev.ErrorType = "client_disconnected"
		c.record()
	case ctx.Err() != nil || ae.timeout:
		c.refuse(errUpstreamTimeout, spool.StatusTimeout)
	case ae.refusal != nil:
		status := spool.StatusUpstreamErr
		if ae.refusal.code == errCreditExhausted.code {
			status = spool.StatusRateLimited
		}
		c.refuse(*ae.refusal, status)
	case ae.throttled:
		if ae.retryAfter != "" {
			c.w.Header().Set("Retry-After", ae.retryAfter)
		}
		c.refuse(errServerBusy, spool.StatusUpstreamErr)
	default:
		c.s.log.Error("async upstream request failed", "keyId", c.key.KeyID,
			"path", c.r.URL.Path, "error", ae.err)
		if ae.retryAfter != "" {
			c.w.Header().Set("Retry-After", ae.retryAfter)
		}
		c.refuse(errUpstream, spool.StatusUpstreamErr)
	}
}

// readAnswer reads an upstream body under the passthrough surface's bounds:
// the ordinary relay bound without a slot, the response cap with one. On a
// failure it has answered and recorded, and returns false.
func (c *asyncCall) readAnswer(resp *http.Response) ([]byte, bool) {
	limit := c.s.cfg.PassthroughResponseMaxBytes
	ordinary := min(int64(upstreamResponseCapBytes), limit)
	readCap := ordinary
	if c.large.held {
		readCap = limit
	}
	tooLarge := func() {
		c.refuse(errPassthroughResponseTooLarge, spool.StatusUpstreamErr)
	}
	if n := resp.ContentLength; n > limit {
		tooLarge()
		return nil, false
	} else if n > readCap {
		if !c.large.take() {
			tooLarge()
			return nil, false
		}
		readCap = limit
	}
	reader := &io.LimitedReader{R: resp.Body, N: readCap + 1}
	body, err := io.ReadAll(reader)
	if err == nil && int64(len(body)) > readCap && readCap < limit {
		if !c.large.take() {
			tooLarge()
			return nil, false
		}
		reader.N += limit - readCap
		buf := bytes.NewBuffer(body)
		_, err = buf.ReadFrom(reader)
		body = buf.Bytes()
	}
	if err == nil && int64(len(body)) > limit {
		tooLarge()
		return nil, false
	}
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			c.refuse(errUpstreamTimeout, spool.StatusTimeout)
		} else {
			c.refuseAs(errUpstream, spool.StatusUpstreamErr, "upstream_read_failed")
		}
		return nil, false
	}
	return body, true
}

// writeJSON relays a body under the write deadline and records the request.
func (c *asyncCall) writeJSON(status int, body []byte) {
	c.w.Header().Set("Content-Type", "application/json; charset=utf-8")
	c.w.WriteHeader(status)
	var werr error
	c.s.writeUnderDeadline(c.w, len(body), func() { _, werr = c.w.Write(body) })
	if werr != nil {
		c.ev.Status = spool.StatusCanceled
		c.ev.ErrorType = "client_disconnected"
	} else {
		c.ev.Status = spool.StatusOK
	}
	c.record()
}

// settle records the price of a finished object once. The vendor reports it
// on the object only when the work is done, and the owner may read that
// object any number of times on any instance, so the event's id is derived
// from the object: every read after the first produces the same id and the
// api keeps one. The request's own event stays a separate, unpriced call.
func (c *asyncCall) settle(endpoint string, kind ownership.Kind, objectID, model string, u usage) {
	cost := spool.CostLiteral(u.Cost)
	if cost == "" {
		return
	}
	sum := sha256.Sum256([]byte(string(kind) + "\x00" + objectID))
	noAttempts := 0
	ev := spool.Event{
		EventUUID:         "settle-" + hex.EncodeToString(sum[:20]),
		Generation:        c.ev.Generation,
		KeyID:             c.key.KeyID,
		BudgetAxis:        snapshot.AxisCredit,
		UpstreamRef:       c.up.Ref,
		Attempts:          &noAttempts,
		Status:            spool.StatusOK,
		InputTokens:       max(u.PromptTokens, 0),
		OutputTokens:      max(u.CompletionTokens, 0),
		RequestedAt:       c.s.now(),
		Endpoint:          endpoint,
		CostUsd:           cost,
		CachedInputTokens: max(u.PromptTokensDetails.CachedTokens, 0),
		ReasoningTokens:   max(u.CompletionTokensDetails.ReasoningTokens, 0),
	}
	if len(model) <= maxPassthroughNameBytes {
		ev.PublicModelName = model
	}
	c.s.metrics.observe(ev)
	if err := c.s.spool.Write(ev); err != nil {
		c.s.metrics.spoolWriteFailures.Add(1)
		c.s.log.Error("usage spool write failed", "error", err)
	}
}

// validObjectID reports whether a path or query value can name an object:
// non-empty, within the ledger's bound, and free of anything that would
// change the upstream path it is spliced into.
func validObjectID(id string) bool {
	if id == "" || len(id) > maxObjectIDBytes {
		return false
	}
	for i := 0; i < len(id); i++ {
		b := id[i]
		if !(b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z' || b >= '0' && b <= '9' ||
			b == '-' || b == '_' || b == '.') || (b == '.' && (id == "." || id == "..")) {
			return false
		}
	}
	return true
}

// objectIDOf reads the vendor id out of a submit's answer.
func objectIDOf(body []byte) string {
	var head struct {
		ID json.RawMessage `json:"id"`
	}
	if json.Unmarshal(body, &head) != nil {
		return ""
	}
	var id string
	if json.Unmarshal(head.ID, &id) != nil || !validObjectID(id) {
		return ""
	}
	return id
}

// refuseWith answers a fence's refusal, recording its own error type when it
// names one.
func (c *asyncCall) refuseWith(rf *refusal, status string) {
	errorType := rf.errorType
	if errorType == "" {
		errorType = rf.e.code
	}
	c.refuseAs(rf.e, status, errorType)
}

// readRequest reads a request body in the passthrough surface's two tiers: up
// to the ordinary bound without a large-body slot, past it only with one,
// judged on the declared length before anything is read.
func (c *asyncCall) readRequest() ([]byte, bool) {
	cfg := c.s.cfg
	small := min(cfg.RequestBodyMaxBytes, cfg.PassthroughRequestBodyMaxBytes)
	bodyLimit := small
	if c.r.ContentLength > small {
		if c.s.passthroughLarge == nil || c.r.ContentLength > cfg.PassthroughRequestBodyMaxBytes {
			c.refuse(errRequestTooLarge, spool.StatusBadRequest)
			return nil, false
		}
		if !c.large.take() {
			c.refuse(errLargeRequestBusy, spool.StatusRateLimited)
			return nil, false
		}
		bodyLimit = cfg.PassthroughRequestBodyMaxBytes
	}
	c.r.Body = http.MaxBytesReader(c.w, c.r.Body, bodyLimit)
	raw, err := io.ReadAll(c.r.Body)
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			c.refuse(errRequestTooLarge, spool.StatusBadRequest)
		} else {
			c.refuse(errBadJSON, spool.StatusBadRequest)
		}
		return nil, false
	}
	return raw, true
}

// fenceModel runs the passthrough surface's model fence over a submit's
// top-level model and the body fields that can name another one. It returns
// nil after answering a refusal.
func (c *asyncCall) fenceModel(publicModel string, params map[string]json.RawMessage) *snapshot.Model {
	if snapshot.IsPresetModelName(publicModel) {
		c.refuseAs(errPresetNotAllowed, spool.StatusBadRequest, "preset_not_allowed")
		return nil
	}
	model := passthroughModel(&c.doc, publicModel)
	if model == nil {
		c.refuse(errModelNotFound, spool.StatusBadRequest)
		return nil
	}
	c.ev.PublicModelName = model.PublicName
	if !c.key.ServesCreditModel(model) {
		if snapshot.IsRouterModelName(publicModel) {
			c.refuseAs(errRouterModelNotSupported(publicModel), spool.StatusBadRequest,
				"router_model_not_supported")
			return nil
		}
		c.refuseAs(errCreditModelNotAllowed, spool.StatusBadRequest, "credit_model_not_allowed")
		return nil
	}
	if _, present := params[snapshot.PresetField]; present {
		c.refuseAs(errPresetNotAllowed, spool.StatusBadRequest, "preset_not_allowed")
		return nil
	}
	noLookup := func(string) *snapshot.Model { return nil }
	if bad, ok := fenceCandidateModels(&c.doc, noLookup, c.key, params); !ok {
		c.refuseAs(errCandidateModelNotAllowed(bad), spool.StatusBadRequest,
			"credit_model_not_allowed")
		return nil
	}
	if e := c.s.fenceServerTools(&c.doc, noLookup, c.key, params); e != nil {
		c.refuseAs(*e, spool.StatusBadRequest, toolRefusalType(*e))
		return nil
	}
	if e := c.s.fencePlugins(&c.doc, noLookup, c.key, params); e != nil {
		c.refuseAs(*e, spool.StatusBadRequest, toolRefusalType(*e))
		return nil
	}
	return model
}

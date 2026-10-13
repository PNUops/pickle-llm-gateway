package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/pnuops/pickle-llm-gateway/internal/bodies"
	"github.com/pnuops/pickle-llm-gateway/internal/limits"
	"github.com/pnuops/pickle-llm-gateway/internal/snapshot"
	"github.com/pnuops/pickle-llm-gateway/internal/spool"
)

// The shared pipeline every completion surface runs: authenticate, admit
// against limits, resolve the public model name, fence, forward, and meter.
// The gateway parses request and response JSON only to validate parameters,
// swap model names and read token usage; content is never inspected beyond
// size and never stored.
//
// The order of the steps below is the answer a caller gets when a request
// earns more than one refusal, so it is the same on every surface; what a
// surface changes is in its protocol value.
//
// Every surface forwards the parsed body re-serialised, never the bytes it
// received. Forwarding the original would let a duplicate or case-variant
// top-level key mean one thing to the fences, which read the decoded map, and
// another to the vendor, which reads the bytes. Nested values go out as they
// arrived; the fences that read them decode the same way a JSON parser on the
// vendor's side does, the last of a repeated key winning.
func (s *Server) serveProtocol(p *protocol, w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		p.writeError(w, errMethod, "")
		return
	}
	start := s.now()
	// Set before every fence, so a refused request still says which route it
	// came in on. The one shape that never reaches the spool at all is an
	// unauthenticated attempt, which belongs to nobody.
	ev := spool.Event{
		EventUUID:   spool.NewEventUUID(),
		RequestedAt: start,
		Endpoint:    p.endpoint,
	}
	// The request id is the usage event's id, echoed on every response so a
	// student can quote it in a support request (requirements §13) and so it
	// ties their report to the metered event. OpenAI SDKs surface this header.
	w.Header().Set("X-Request-Id", ev.EventUUID)
	record := func() {
		ev.LatencyMs = time.Since(start).Milliseconds()
		s.metrics.observe(ev)
		// A request that never resolved to a key has nobody to account to, and
		// the spool is a small disk with a long retention that anyone on the
		// internet can reach. Writing a line per unauthenticated attempt makes
		// filling it a matter of a loop; the counters still see them, and a
		// rejection for a key that *did* resolve (suspended, revoked, expired)
		// is still spooled, because that one belongs to someone.
		if ev.KeyID == "" && ev.Status == spool.StatusAuthRejected {
			return
		}
		if err := s.spool.Write(ev); err != nil {
			// The spool is the durable accounting record. A failure here is
			// counted as well as logged: otherwise usage simply comes out low
			// and nothing on any surface says why.
			s.metrics.spoolWriteFailures.Add(1)
			s.log.Error("usage spool write failed", "error", err)
		}
		s.log.Info(p.logLine, "keyId", ev.KeyID, "model", ev.PublicModelName,
			"status", ev.Status, "errorType", ev.ErrorType,
			"inputTokens", ev.InputTokens, "outputTokens", ev.OutputTokens,
			"latencyMs", ev.LatencyMs, "ttftMs", ev.TtftMs)
	}
	refuse := func(e apiError, status string) {
		p.writeError(w, e, ev.EventUUID)
		ev.Status = status
		ev.ErrorType = e.code
		record()
	}
	// Same refusal to the caller, a different name in the usage record. Used
	// where two distinct causes deliberately share one public error code and
	// the accounting still has to tell them apart.
	refuseAs := func(e apiError, status, errorType string) {
		p.writeError(w, e, ev.EventUUID)
		ev.Status = status
		ev.ErrorType = errorType
		record()
	}
	refuseWith := func(rf *refusal, status string) {
		if rf.errorType == "" {
			refuse(rf.e, status)
			return
		}
		refuseAs(rf.e, status, rf.errorType)
	}

	// One snapshot view per request: every check below reads this state, so a
	// concurrent reload can never mix generations within a request.
	doc, keyLookup, modelLookup := s.store.Current()
	ev.Generation = doc.Generation
	if !doc.ServiceEnabled {
		refuse(errServiceDisabled, spool.StatusAuthRejected)
		return
	}
	key, authErr := s.authenticate(r, keyLookup)
	// Attribute before refusing: a key that resolved has an owner, and that is
	// the fact worth recording about a rejection.
	if key != nil {
		ev.KeyID = key.KeyID
	}
	if authErr != nil {
		refuse(*authErr, spool.StatusAuthRejected)
		return
	}
	// The surface's own grant. It applies on both axes and does not depend on
	// the model, so it is judged before any limit or the body: a key that was
	// never granted this surface should hear that, not a complaint about a
	// budget or a field.
	if !key.AllowsEndpoint(p.capability) {
		refuse(errEndpointNotAllowed(endpointLabel(p.capability)), spool.StatusAuthRejected)
		return
	}

	// The daily token quota is checked after the model resolves — it governs
	// only TOKEN-axis models, and which axis applies is a fact about the model
	// the request names. But a key that is BOTH quota-exhausted and without
	// any upstream credential cannot pass either axis, so it is refused here,
	// before it spends an in-flight slot and a body parse per attempt.
	if key.QuotaExhausted && len(key.UpstreamCredentials) == 0 {
		refuse(errQuotaExhausted, spool.StatusRateLimited)
		return
	}
	// The gateway-wide cap is checked before any per-key charge: a refusal
	// the student cannot influence must not spend their request budget.
	select {
	case s.inFlight <- struct{}{}:
		defer func() { <-s.inFlight }()
	default:
		refuse(errServerBusy, spool.StatusRateLimited)
		return
	}
	// The per-key limits are applied further down, once the model has told us
	// the budget axis. They all protect self-hosted serving capacity, which a
	// commercial call never touches — see the TOKEN branch below.
	//
	// A body above the ordinary bound needs a large-body slot, judged on the
	// declared length before anything is read: refusing after reading would
	// already have spent the memory the slot exists to bound. A request that
	// declares no length, or a small one, is read through the small cap, so a
	// false declaration cannot reach the large one (see config).
	lim := p.limits(s.cfg)
	bodyLimit := lim.request
	releaseLarge := func() {}
	if r.ContentLength > bodyLimit {
		if s.chatLarge == nil || r.ContentLength > lim.largeRequest {
			refuse(errRequestTooLarge, spool.StatusBadRequest)
			return
		}
		release, ok := s.chatLarge.acquire(key.KeyID)
		if !ok {
			refuse(errLargeRequestBusy, spool.StatusRateLimited)
			return
		}
		releaseLarge = release
		defer release()
		bodyLimit = lim.largeRequest
	}
	r.Body = http.MaxBytesReader(w, r.Body, bodyLimit)
	// Sized from the declared length when there is one, so a 25 MiB body is
	// one allocation rather than a doubling series that peaks near twice it.
	var buf bytes.Buffer
	if r.ContentLength > 0 && r.ContentLength <= bodyLimit {
		buf.Grow(int(r.ContentLength) + bytes.MinRead)
	}
	_, err := buf.ReadFrom(r.Body)
	rawBody := buf.Bytes()
	// The buffer still points at the same bytes; only rawBody may, so that
	// dropping it below actually lets them go.
	buf = bytes.Buffer{}
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			refuse(errRequestTooLarge, spool.StatusBadRequest)
			return
		}
		// Not a malformed body: the upload itself stopped (the connection
		// closed or the read deadline passed). Recording it as invalid_json
		// blamed the caller's JSON for a transport fault, and there is nobody
		// left to answer.
		// The answer is written anyway: when the peer is still there (a
		// malformed chunked body, a read deadline) it would otherwise get an
		// empty 200, and when it is gone the write is harmless.
		p.writeError(w, errRequestBodyUnreadable, ev.EventUUID)
		ev.Status = spool.StatusCanceled
		ev.ErrorType = "request_body_read_failed"
		record()
		return
	}
	var params map[string]json.RawMessage
	if err := json.Unmarshal(rawBody, &params); err != nil {
		refuse(errBadJSON, spool.StatusBadRequest)
		return
	}
	// Stamped here rather than where the stream is set up, because every fence
	// between the two refuses requests that did ask for one. Reading it off a
	// refusal as false would understate streaming by exactly the traffic that
	// never got through, which is the traffic a limit question is about.
	if raw, ok := params["stream"]; ok {
		var streamed bool
		_ = json.Unmarshal(raw, &streamed)
		ev.Streamed = streamed
	}
	req, rf := p.parse(params)
	if rf != nil {
		refuseWith(rf, spool.StatusBadRequest)
		return
	}
	publicModel := req.publicModel
	// Checked before the model resolves, because a preset name resolves to
	// nothing and would otherwise answer "no such model, see GET /v1/models" —
	// advice that sends the caller to a list which cannot contain the answer.
	if snapshot.IsPresetModelName(publicModel) {
		refuseAs(errPresetNotAllowed, spool.StatusBadRequest, "preset_not_allowed")
		return
	}
	model := modelLookup(publicModel)
	if model == nil {
		model = passthroughModel(&doc, publicModel)
	}
	if model == nil {
		refuse(errModelNotFound, spool.StatusBadRequest)
		return
	}
	ev.PublicModelName = publicModel
	// Copy the effective axis out of the request's snapshot. A catalog row
	// written before budgetAxis existed has always meant TOKEN, while a
	// synthesized passthrough model is explicitly CREDIT. Recording the
	// normalized value here keeps later catalog changes from reclassifying the
	// event and leaves requests with no valid route unclassified.
	switch model.BudgetAxis {
	case "", snapshot.AxisToken:
		ev.BudgetAxis = snapshot.AxisToken
	case snapshot.AxisCredit:
		ev.BudgetAxis = snapshot.AxisCredit
	}
	// The parameter allowlist governs the TOKEN axis and nothing else.
	//
	// One machine used to run on both axes, and running on both hid the fact
	// that it was doing two different jobs. On the self-hosted side it is the
	// enforcement point for a service decision — thinking is off by default,
	// with no per-request opt-in (see tokenAxisParams) — and the serving
	// capacity it rations is the platform's own. On the money axis it was our
	// convenience and nothing more: it refused fields the provider defines,
	// on requests the student's own budget pays for, so every field it turned
	// away was one the vendor would have served. Where the vendor accepts a
	// request this gateway has no reason to be what refuses it.
	//
	// It still runs before the permission fences, so a malformed request
	// answers 400 rather than 403, and it runs after the model is read because
	// the axis is a fact about the model the body names.
	if !model.CreditAxis() {
		for name := range params {
			if rf := p.tokenAxisParam(name); rf != nil {
				refuseWith(rf, spool.StatusBadRequest)
				return
			}
		}
	}
	if !key.AllowsModel(model) {
		refuse(errModelNotAllowed, spool.StatusBadRequest)
		return
	}
	// Budget-axis enforcement, now that the model (and so the axis) is known.
	//
	// charge meters this request against the key's tpm bucket. It is a no-op
	// on the CREDIT axis and stays that way for the rest of the handler, so
	// commercial traffic can neither be refused by that bucket nor push it
	// into debt — either one would let a commercial call spend self-hosted
	// serving capacity it never used. Assigning (not declaring) it inside the
	// branch below is deliberate: a `:=` there would shadow this one and leave
	// every TOKEN request unmetered, which is the exact defect this ordering
	// was written to fix.
	charge := func(int) {}
	if model.CreditAxis() {
		// The model lists are checked before the credential, and the order is
		// the answer the caller gets. A key restricted to some models still
		// holds a funded credential, so testing the credential first would
		// answer "no money budget" to somebody who has one — sending them to
		// apply for what they already have. What they actually need is a
		// different model, or an administrator who widens the fence.
		//
		// One call covers both the allow list and the deny list; AllowsCreditModel
		// is the whole fence, so nothing here has to know which half refused.
		// The caller is not told either, deliberately: naming the list would
		// disclose its contents to somebody who is not the approver, and the
		// two refusals ask the same thing of them anyway.
		if !key.AllowsCreditModel(model) {
			// Same public code as the catalogue fence, different advice (see
			// errors.go). The spool gets its own error type so the two stay
			// countable apart — otherwise nobody can tell how many callers hit
			// this one.
			//
			// Re-reading the two predicates here decides only which message to
			// send; the refusal itself was already decided above, and this
			// branch cannot admit anything the fence turned away.
			if key.HasCreditFence() && snapshot.IsRouterModelName(publicModel) {
				refuseAs(errRouterModelNotAllowed(publicModel), spool.StatusBadRequest,
					"router_model_not_allowed")
				return
			}
			refuseAs(errCreditModelNotAllowed, spool.StatusBadRequest,
				"credit_model_not_allowed")
			return
		}
		// The surface's own fences over the body: every other place a request
		// can name a model the vendor may bill, or reach a capability.
		for _, f := range p.fences {
			if rf := f(s, &doc, modelLookup, key, model, params); rf != nil {
				refuseWith(rf, spool.StatusBadRequest)
				return
			}
		}
		// Past the fence, the credential is the whole of the money axis: its
		// issuer holds the limit, so a key granted no money budget simply
		// carries none. No per-key limit of ours applies on this side.
		if key.CredentialFor(model.UpstreamRef) == "" {
			// Same missing credential, two different answers. A budget that
			// was granted and is still being applied ends by itself, so the
			// caller is told to wait and the status says so; anything else
			// needs a person to act, and saying "wait" there would be a
			// promise nothing keeps.
			if key.CreditPending {
				refuse(errCreditPending, spool.StatusAuthRejected)
				return
			}
			refuse(errCreditUnavailable, spool.StatusAuthRejected)
			return
		}
	} else {
		// Every per-key limit below guards self-hosted serving capacity, so
		// all of them wait until the axis is known. Admission comes before the
		// daily quota, as it always has: a key that exhausted its allowance is
		// the one most likely to be sitting in a retry loop, and leaving it
		// unmetered by rpm would remove the only brake on that loop.
		rpm, tpm, conc := s.keyLimits(key)
		adm := s.limiter.Acquire(key.KeyID, rpm, tpm, conc)
		if adm.Reason != limits.OK {
			// Tell the client when to come back rather than leaving it to
			// guess; SDK retry helpers read this header.
			w.Header().Set("Retry-After", strconv.Itoa(int(math.Ceil(adm.RetryAfter.Seconds()))))
			switch adm.Reason {
			case limits.Rpm:
				refuse(errRateRequests, spool.StatusRateLimited)
			case limits.Tpm:
				refuse(errRateTokens, spool.StatusRateLimited)
			default:
				refuse(errRateConcurrency, spool.StatusRateLimited)
			}
			return
		}
		defer adm.Release()
		w.Header().Set("X-RateLimit-Limit-Requests", strconv.Itoa(rpm))
		w.Header().Set("X-RateLimit-Remaining-Requests", strconv.Itoa(adm.RemainingRequests))
		if key.QuotaExhausted {
			refuse(errQuotaExhausted, spool.StatusRateLimited)
			return
		}
		keyID := key.KeyID
		charge = func(tokens int) { s.limiter.ChargeTokens(keyID, tpm, tokens) }
	}
	// Output length. A JSON null is what SDKs send for "unset" and is treated
	// as absent; an explicit value above the model cap is refused. Whatever the
	// student sent (on any field the surface reads) is normalized onto one
	// field and capped at the model maximum, so the limit lands on the field
	// the upstream actually honors — forwarding the student's field verbatim
	// would let a legacy `max_tokens`-only server ignore a
	// `max_completion_tokens` request and blow past the cap.
	// The normalization runs whether or not the model declares a maximum. A
	// model with none is a valid document (the field is optional, and the
	// control plane may simply not set it), and leaving the student's field
	// untouched in that case puts it back exactly where it does not work: a
	// legacy `max_tokens`-only upstream ignores `max_completion_tokens`, and
	// the request the student thought they had bounded generates without a
	// limit, billed.
	outputCap := 0
	asked := 0
	for _, f := range p.capReadFields {
		raw, ok := params[f]
		if !ok {
			continue
		}
		delete(params, f) // re-added on the write field when the body is built
		if string(bytes.TrimSpace(raw)) == "null" {
			continue
		}
		var n int
		if json.Unmarshal(raw, &n) != nil || n <= 0 {
			refuse(errInvalidParamValue(f), spool.StatusBadRequest)
			return
		}
		if model.MaxOutputTokens > 0 && n > model.MaxOutputTokens {
			refuse(errOutputTooLong, spool.StatusBadRequest)
			return
		}
		if asked == 0 || n < asked {
			asked = n
		}
	}
	switch {
	case asked > 0:
		outputCap = asked
	case model.MaxOutputTokens > 0:
		outputCap = model.MaxOutputTokens
	}
	// Input length: token counting needs the model's tokenizer, which the
	// gateway does not have. This guard only refuses what cannot possibly
	// fit (bytes far beyond the token budget); exact enforcement is the
	// upstream's context-window error.
	inputBytes := len(req.input)
	if model.MaxInputTokens > 0 && len(req.input) > model.MaxInputTokens*6 {
		refuse(errInputTooLong, spool.StatusBadRequest)
		return
	}

	// Already on the event, stamped before the fences above.
	streaming := ev.Streamed
	p.shape(params, req, model, streaming)

	upCtx, cancel := context.WithTimeout(r.Context(), s.cfg.RequestMaxDuration)
	defer cancel()

	// Try the model's upstream, then its fallback if it has one. Nothing has
	// been written to the client yet, so switching upstreams here is invisible
	// to the student; once the response starts, it is not.
	resp, up, attempts, attemptErr := s.callUpstream(upCtx, p, model, key, params, outputCap, r.Header)
	ev.TtftMs = time.Since(start).Milliseconds()
	// Which upstream was reached, and how many tries it took. The response's
	// own model field is rewritten to the public name before the student sees
	// it, so without this the accounting cannot tell a free local model from a
	// paid fallback — and the two are billed to different people. Recorded
	// before the failure switch below, because a request that died on the way
	// still spent whatever the upstream had already done for it; both fields
	// stay empty only when nothing was contacted at all.
	ev.UpstreamRef = up.Ref
	ev.Attempts = attempts
	if attemptErr != nil {
		switch {
		case r.Context().Err() != nil:
			// The student went away; nothing can be written back.
			ev.Status = spool.StatusCanceled
			ev.ErrorType = "client_disconnected"
			record()
		case upCtx.Err() != nil || attemptErr.timeout:
			refuse(errUpstreamTimeout, spool.StatusTimeout)
		case attemptErr.refusal != nil:
			// A budget refusal is accounting, not a fault: it belongs with the
			// other limit refusals so "how often did this key hit a wall"
			// counts it and "is the upstream broken" does not.
			status := spool.StatusUpstreamErr
			if attemptErr.refusal.code == errCreditExhausted.code {
				status = spool.StatusRateLimited
			}
			refuse(*attemptErr.refusal, status)
		case attemptErr.throttled:
			// Every upstream is throttling us: the service really is busy.
			relayRetryAfter(w, model, attemptErr)
			refuse(errServerBusy, spool.StatusUpstreamErr)
		default:
			s.log.Error("upstream request failed", "keyId", key.KeyID,
				"model", publicModel, "error", attemptErr.err)
			relayRetryAfter(w, model, attemptErr)
			refuse(errUpstream, spool.StatusUpstreamErr)
		}
		return
	}
	defer resp.Body.Close()

	// Body capture: only for a key that opted in, and only when the delivery
	// channel exists — captured text is never written to this host's disk, so
	// with no channel there is nothing to capture into.
	var capture *bodies.Record
	if s.bodies.Enabled() && key.RecordBodies {
		capture = &bodies.Record{
			EventUUID:   ev.EventUUID,
			KeyID:       key.KeyID,
			RequestedAt: start,
		}
		capture.Request, capture.RequestTruncated = p.capture(req.input)
	}

	// The upstream is answering, so the request body has done its job. Every
	// copy of it is let go here rather than when the handler returns: the
	// read bytes, the decoded members, the parsed input that points into
	// them, and the re-serialised body the transport would otherwise keep
	// reachable for the whole response. That is what lets the large-body slot
	// go now, so a long stream does not hold one.
	releaseUpstreamBody(resp)
	rawBody, params, req.input = nil, nil, nil
	releaseLarge()

	if !streaming {
		s.finishNonStream(p, w, resp, model.PublicName, model.UpstreamModel, model.CreditAxis(), charge, &ev, record, inputBytes, capture)
		return
	}
	// A stream commits its 200 before the body is read, so the id goes out
	// with it; whatever happens later is reported inside the stream.
	if model.CreditAxis() {
		relayGenerationID(w, resp.Header)
	}
	a := streamArgs{
		capture:      capture,
		publicName:   model.PublicName,
		sentModel:    model.UpstreamModel,
		keyID:        key.KeyID,
		charge:       charge,
		inputBytes:   inputBytes,
		forwardUsage: req.wantsUsage,
		clientCtx:    r.Context(),
		upCtx:        upCtx,
	}
	s.relayStream(w, resp, p.newStream(s, a, &ev), a, &ev, record)
}

// upstreamResponseCapBytes bounds what one upstream response may cost this
// process without a large-body slot: a whole non-streamed response, or one
// event of a stream (a stream holds one event at a time, so its total length
// is left to the duration cap). The non-stream path holds the raw bytes, the
// decoded map and the re-marshalled output at once, so the real cost is a
// multiple of this, times the in-flight cap. A non-streamed response past it
// is read further only with a slot, up to the surface's large response bound.
const upstreamResponseCapBytes = 8 << 20

func (s *Server) finishNonStream(p *protocol, w http.ResponseWriter, resp *http.Response, publicName, sentModel string, creditAxis bool, charge func(int), ev *spool.Event, record func(), inputBytes int, capture *bodies.Record) {
	largeResponseMax := p.limits(s.cfg).largeResponse
	// releaseLargeResponse frees a large-body slot taken below. It runs once
	// the output is marshalled, before the write: the slot bounds what parsing
	// costs, and a client that reads slowly must not hold it while it reads
	// (the edge does not buffer responses).
	releaseLargeResponse := func() {}
	body, err := io.ReadAll(io.LimitReader(resp.Body, upstreamResponseCapBytes+1))
	if err == nil && len(body) > upstreamResponseCapBytes {
		// Past the ordinary bound. The rest is read only with a large-body
		// slot, because a response is the same size-times-count problem as a
		// request. The vendor has already billed it either way, which is why
		// the refusal points at streaming rather than at a retry.
		release, ok := s.chatLarge.acquire(ev.KeyID)
		if !ok || largeResponseMax <= upstreamResponseCapBytes {
			release()
			s.refuseResponseTooLarge(p, w, ev, charge, record, inputBytes)
			return
		}
		defer release()
		releaseLargeResponse = release
		limit := largeResponseMax - int64(len(body))
		buf := bytes.NewBuffer(body)
		_, err = buf.ReadFrom(io.LimitReader(resp.Body, limit+1))
		body = buf.Bytes()
		if err == nil && int64(len(body)) > largeResponseMax {
			s.refuseResponseTooLarge(p, w, ev, charge, record, inputBytes)
			return
		}
	}
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			p.writeError(w, errUpstreamTimeout, ev.EventUUID)
			ev.Status = spool.StatusTimeout
			ev.ErrorType = errUpstreamTimeout.code
		} else {
			p.writeError(w, errUpstream, ev.EventUUID)
			ev.Status = spool.StatusUpstreamErr
			ev.ErrorType = "upstream_read_failed"
		}
		// The upstream may have generated tokens before the read failed; charge
		// an input-side estimate so a client looping large failing requests is
		// still rate-limited rather than metered as free.
		s.settleUsage(ev, usage{}, false, inputBytes, 0)
		charge(ev.InputTokens + ev.OutputTokens)
		record()
		return
	}
	// A response that does not parse as JSON is not something the gateway can
	// vouch for, and forwarding it verbatim would expose the upstream's model
	// identifiers — answer an upstream error instead.
	var m map[string]any
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	if dec.Decode(&m) != nil {
		p.writeError(w, errUpstream, ev.EventUUID)
		ev.Status = spool.StatusUpstreamErr
		ev.ErrorType = "upstream_invalid_response"
		// Charge the minute bucket but leave the event's token counts at zero.
		// The bucket is in-memory and minute-scale, so charging it is only
		// rate limiting: without it a client looping against an upstream that
		// answers garbage pays nothing and can repeat without bound. The event
		// is the accounting record, and the api's daily-allowance query counts
		// tokens without looking at status — writing an estimate there would
		// spend a student's daily quota on an upstream fault they cannot
		// influence, which is the same principle that puts the gateway-wide
		// cap ahead of every per-key charge.
		in, _ := estimateTokens(inputBytes, 0)
		charge(in)
		record()
		return
	}
	res := p.readResponse(m, publicName)
	s.noteServedModel(ev, sentModel, res.servedModel)
	contentChars := 0
	if !res.haveUsage {
		contentChars = len(res.answer)
	}
	if capture != nil {
		capture.Response, capture.ResponseTruncated = capString(res.answer)
	}
	out, err := json.Marshal(m)
	if err != nil {
		p.writeError(w, errUpstream, ev.EventUUID)
		ev.Status = spool.StatusUpstreamErr
		ev.ErrorType = "upstream_invalid_response"
		// Defensive only, and no test covers it: everything a UseNumber decode
		// produces marshals again, so nothing a valid decode above can yield
		// reaches here. Kept for the same reason as the decode failure, and
		// charged the same way — bucket yes, accounting record no.
		if res.haveUsage {
			charge(res.usage.PromptTokens + res.usage.CompletionTokens)
		} else {
			in, outTok := estimateTokens(inputBytes, contentChars)
			charge(in + outTok)
		}
		record()
		return
	}
	body, m = nil, nil
	releaseLargeResponse()
	// Only on the answer actually relayed: a refusal above is the gateway's
	// own error, not the vendor's generation.
	if creditAxis {
		relayGenerationID(w, resp.Header)
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	if _, err := w.Write(out); err != nil {
		ev.Status = spool.StatusCanceled
		ev.ErrorType = "client_disconnected"
	} else {
		ev.Status = spool.StatusOK
	}
	s.settleUsage(ev, res.usage, res.haveUsage, inputBytes, contentChars)
	charge(ev.InputTokens + ev.OutputTokens)
	s.bodies.Offer(capture)
	if capture != nil {
		s.metrics.bodiesCaptured.Add(1)
	}
	record()
}

// refuseResponseTooLarge answers a non-streamed response this service will
// not relay. Its usage cannot be read, so the input side is estimated the way
// a failed read is: a client looping on oversized answers is still metered.
func (s *Server) refuseResponseTooLarge(p *protocol, w http.ResponseWriter, ev *spool.Event, charge func(int), record func(), inputBytes int) {
	p.writeError(w, errChatResponseTooLarge, ev.EventUUID)
	ev.Status = spool.StatusUpstreamErr
	ev.ErrorType = errChatResponseTooLarge.code
	s.settleUsage(ev, usage{}, false, inputBytes, 0)
	charge(ev.InputTokens + ev.OutputTokens)
	record()
}

type usage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
	// Cost is what the vendor charged for this request. It reaches the client
	// verbatim either way; reading it here is what puts a dollar figure in the
	// accounting, which until now had only the vendor's own cumulative meters.
	// json.Number so the vendor's precision survives — it reports down to
	// about 1e-7 and a float64 round trip is not something to spend on a
	// number that is added up across a month.
	Cost json.Number `json:"cost"`
	// The two details are subsets of the counts above, not additions.
	PromptTokensDetails struct {
		CachedTokens int `json:"cached_tokens"`
	} `json:"prompt_tokens_details"`
	CompletionTokensDetails struct {
		ReasoningTokens int `json:"reasoning_tokens"`
	} `json:"completion_tokens_details"`
}

// servedModelMismatch records that the vendor answered with a model other than
// the one requested. The vendor falls back on its own and prices the request
// using whatever it ultimately used, so this is a real event; the response's
// model field is rewritten to the public name a line later, which makes this
// the only place the fact exists at all.
//
// It is recorded only when the answer differs from what was asked for, and
// that is a correction of the reasoning this function first shipped with.
//
// The argument for recording it always was that an absent value otherwise
// means both "the vendor reported nothing" and "the same model we asked for".
// True, and it cost more than it was worth: the two names it compares live in
// different namespaces. What goes on the wire is the upstream model id, while
// the event's public model name is the name this service issues, and for every
// catalogue model those differ by design -- hiding the upstream name is what a
// public name is for. Anything downstream comparing the two therefore reported
// a fallback on every ordinary self-hosted request.
//
// Comparing here is the only place the right pair is in hand. The distinction
// that was lost is one nobody had a use for; the count that was wrong is read
// by an aggregate.
//
// The name comes from an upstream response body, so it is bounded here rather
// than left to the control plane's own limit: an oversized one would otherwise
// reach the spool on a small disk with a 90-day retention.
func (s *Server) noteServedModel(ev *spool.Event, requested, served string) {
	if served == "" || requested == "" || strings.EqualFold(requested, served) {
		return
	}
	ev.ServedModelName = boundServedModel(served)
	s.log.Warn("upstream served a different model than requested",
		"keyId", ev.KeyID, "publicModel", ev.PublicModelName,
		"requested", requested, "served", served, "eventUuid", ev.EventUUID)
}

// servedModelMax is shorter than the control plane's own text bound on
// purpose. Nothing legitimate approaches it, and the cost of a long one is
// paid on this side first.
const servedModelMax = 128

func boundServedModel(served string) string {
	if len(served) > servedModelMax {
		return served[:servedModelMax]
	}
	return served
}

// capString bounds one captured answer. A cut record says so rather than
// looking like a short answer.
func capString(s string) (string, bool) {
	return capAt(s, bodies.ResponseCapBytes)
}

func capAt(s string, limit int) (string, bool) {
	if len(s) <= limit {
		return s, false
	}
	// Cut on a rune boundary so the stored text stays valid UTF-8.
	cut := limit
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut], true
}

// settleUsage fills the event's token counts: exact when the upstream
// reported usage, a byte-based estimate flagged as such otherwise.
func (s *Server) settleUsage(ev *spool.Event, u usage, haveUsage bool, inputBytes, contentChars int) {
	if haveUsage {
		ev.InputTokens = u.PromptTokens
		ev.OutputTokens = u.CompletionTokens
		ev.CostUsd = spool.CostLiteral(u.Cost)
		ev.CachedInputTokens = nonNegative(u.PromptTokensDetails.CachedTokens)
		ev.ReasoningTokens = nonNegative(u.CompletionTokensDetails.ReasoningTokens)
		return
	}
	// The estimate path writes no price and no breakdown. An estimate has
	// neither, and leaving a value from somewhere else would be a number
	// claiming to be exact next to a flag saying the tokens are not. Every
	// caller reaches this function, which is why the rule lives here rather
	// than at each of the four call sites.
	ev.InputTokens, ev.OutputTokens = estimateTokens(inputBytes, contentChars)
	ev.Estimated = true
}

func nonNegative(v int) int {
	if v < 0 {
		return 0
	}
	return v
}

// estimateTokens is the byte-based fallback when no upstream usage arrived.
// It is separate from settleUsage because the rate limiter sometimes needs the
// estimate without it reaching the accounting record — see the response paths
// that charge the bucket but leave the event's token counts at zero.
func estimateTokens(inputBytes, contentChars int) (in, out int) {
	return inputBytes / 4, contentChars / 3
}

func isTimeout(err error) bool {
	var ne interface{ Timeout() bool }
	if errors.As(err, &ne) {
		return ne.Timeout()
	}
	return errors.Is(err, context.DeadlineExceeded)
}

// The passthrough surface: the client-facing routes that exist only to hand a
// request to the commercial provider and hand its answer back. Everything here
// is on the money axis by construction — self-serving answers on
// /v1/chat/completions alone — so nothing in this file parses a body to decide
// an axis, and nothing in it can move what chat does.
//
// Two properties are load-bearing and are the reason the file is separate.
// The fences run in a fixed order (authenticate, endpoint, credential, model),
// and every bound this surface answers to is its own: its own body cap, its
// own response cap, its own header wait and its own pool of in-flight slots.
package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/pnuops/pickle-llm-gateway/internal/snapshot"
	"github.com/pnuops/pickle-llm-gateway/internal/spool"
)

// The request body is parsed whole, and the body forwarded upstream is the one
// this file re-serialized from that parse.
//
// A bounded prefix read was tried first and is unsound, because JSON objects
// may repeat a member and every parser takes the last one. A fence that judged
// `model` from the front of a body forwarded verbatim can be handed a second
// `model` further in — inside the prefix, or past it where no prefix read can
// ever look — and the name that was checked is then not the name that gets
// billed. The reserved self-serve prefix guard falls to the same trick.
//
// Re-serializing is what closes it: the upstream cannot see a member this file
// did not resolve, so "the name the fence judged" and "the name the vendor
// serves" are the same string by construction rather than by inspection. It
// costs a copy of a request body that is already bounded well below the
// response cap, which is the cheaper half of this surface.

// passthroughRoute is one client-facing path this surface opens.
type passthroughRoute struct {
	// capability is the snapshot token that grants the route. Several routes
	// may share one: a capability is a thing a key may do, not a URL, so the
	// image catalogue read travels with image generation rather than needing a
	// grant of its own.
	capability string
	method     string
	// upstreamPath is appended to the upstream's base URL. The client-facing
	// path is the same string under /v1, which is the whole point of a
	// passthrough surface — there is no mapping to keep in step.
	upstreamPath string
	// readsBody marks the routes that carry a JSON body naming a model. A route
	// without one names no model, so the model fence has nothing to judge and
	// does not run — stated here rather than inferred from the method, so that
	// adding a route makes the question unavoidable.
	readsBody bool
}

var (
	routeImages = passthroughRoute{
		capability:   snapshot.EndpointImages,
		method:       http.MethodPost,
		upstreamPath: "/images",
		readsBody:    true,
	}
	// The image catalogue is served unfiltered, unlike GET /v1/models. That
	// list is the platform's own and says what we serve; this one is the
	// vendor's public catalogue, which anybody can read from the vendor. The
	// money fence still refuses at call time, so nothing here grants a model.
	routeImageModels = passthroughRoute{
		capability:   snapshot.EndpointImages,
		method:       http.MethodGet,
		upstreamPath: "/images/models",
	}
	routeEmbeddings = passthroughRoute{
		capability:   snapshot.EndpointEmbeddings,
		method:       http.MethodPost,
		upstreamPath: "/embeddings",
		readsBody:    true,
	}
)

// endpointNames are the words a refusal uses for a capability. The token is
// carried alongside the Korean name because the token is what the contract
// enumerates and what the approval screen lists, so a student reading a
// refusal and the person granting it are naming the same thing.
var endpointNames = map[string]string{
	snapshot.EndpointImages:     "이미지",
	snapshot.EndpointEmbeddings: "임베딩",
}

// endpointLabel renders one capability for a message. A token with no name
// here still reads sensibly, which matters because the vocabulary belongs to
// the control plane and may gain an entry before this map does.
func endpointLabel(capability string) string {
	name, ok := endpointNames[capability]
	if !ok {
		return capability
	}
	return name + "(" + capability + ")"
}

// handlePassthrough builds the handler for one route. The fence order it runs
// is the order the caller needs the answers in, and it is not the same as
// chat's: the endpoint fence comes first because a key that was never granted
// the capability should hear that, not a complaint about a money budget it may
// well have.
func (s *Server) handlePassthrough(route passthroughRoute) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != route.method {
			writeAPIError(w, errMethod)
			return
		}
		start := s.now()
		ev := spool.Event{EventUUID: spool.NewEventUUID(), RequestedAt: start}
		w.Header().Set("X-Request-Id", ev.EventUUID)
		record := func() {
			ev.LatencyMs = time.Since(start).Milliseconds()
			s.metrics.observe(ev)
			// Same rule as chat: an attempt that never resolved to a key has
			// nobody to account to, and the spool is a small disk anyone on
			// the internet can write to by looping.
			if ev.KeyID == "" && ev.Status == spool.StatusAuthRejected {
				return
			}
			if err := s.spool.Write(ev); err != nil {
				s.metrics.spoolWriteFailures.Add(1)
				s.log.Error("usage spool write failed", "error", err)
			}
			s.log.Info("passthrough request", "keyId", ev.KeyID, "path", r.URL.Path,
				"model", ev.PublicModelName, "status", ev.Status, "errorType", ev.ErrorType,
				"inputTokens", ev.InputTokens, "outputTokens", ev.OutputTokens,
				"latencyMs", ev.LatencyMs)
		}
		refuse := func(e apiError, status string) {
			writeAPIError(w, e)
			ev.Status = status
			ev.ErrorType = e.code
			record()
		}
		// Same refusal to the caller, a different name in the usage record, for
		// the codes two distinct causes deliberately share.
		refuseAs := func(e apiError, status, errorType string) {
			writeAPIError(w, e)
			ev.Status = status
			ev.ErrorType = errorType
			record()
		}

		doc, keyLookup, _ := s.store.Current()
		ev.Generation = doc.Generation
		if !doc.ServiceEnabled {
			refuse(errServiceDisabled, spool.StatusAuthRejected)
			return
		}
		key, authErr := s.authenticate(r, keyLookup)
		if key != nil {
			ev.KeyID = key.KeyID
		}
		if authErr != nil {
			refuse(*authErr, spool.StatusAuthRejected)
			return
		}

		// The endpoint fence. Empty grants nothing, so this is the check that
		// keeps the surface shut for every key that exists today.
		if !key.AllowsEndpoint(route.capability) {
			refuse(errEndpointNotAllowed(endpointLabel(route.capability)),
				spool.StatusAuthRejected)
			return
		}
		// Granted, but the document names nobody to serve it. That is a
		// deployment fault and not the caller's key, so it must not come back
		// as a budget refusal telling them to apply for something they have.
		if doc.PassthroughRef == "" {
			s.log.Error("passthrough route reached with no passthroughRef in the document",
				"path", r.URL.Path, "keyId", key.KeyID)
			refuse(errServiceDisabled, spool.StatusUpstreamErr)
			return
		}
		ev.BudgetAxis = snapshot.AxisCredit
		// The credential is the whole of the money axis: its issuer holds the
		// limit, so a key granted no money budget simply carries none.
		if key.CredentialFor(doc.PassthroughRef) == "" {
			if key.CreditPending {
				refuse(errCreditPending, spool.StatusAuthRejected)
				return
			}
			refuse(errCreditUnavailable, spool.StatusAuthRejected)
			return
		}

		// This surface's own slot pool, taken after the cheap refusals so a
		// key that cannot pass a fence never occupies one, and before the body
		// read, which is the first thing that costs memory. Separate from the
		// gateway-wide pool on purpose: it is what bounds this surface's heap
		// to a number that can be written down, and it stops a two-minute
		// image generation from holding a slot chat needs.
		select {
		case s.passthroughInFlight <- struct{}{}:
			defer func() { <-s.passthroughInFlight }()
		default:
			refuse(errServerBusy, spool.StatusRateLimited)
			return
		}

		var body []byte
		inputBytes := 0
		if route.readsBody {
			r.Body = http.MaxBytesReader(w, r.Body, s.cfg.PassthroughRequestBodyMaxBytes)
			raw, err := io.ReadAll(r.Body)
			if err != nil {
				var tooLarge *http.MaxBytesError
				if errors.As(err, &tooLarge) {
					refuse(errRequestTooLarge, spool.StatusBadRequest)
				} else {
					refuse(errBadJSON, spool.StatusBadRequest)
				}
				return
			}
			inputBytes = len(raw)

			// Decoding into a map of raw values resolves repeated members the
			// same way the upstream would, and leaves every value's bytes
			// untouched so nothing is reinterpreted on the way through.
			var params map[string]json.RawMessage
			if json.Unmarshal(raw, &params) != nil {
				refuse(errBadJSON, spool.StatusBadRequest)
				return
			}
			var publicModel string
			if v, ok := params["model"]; !ok || json.Unmarshal(v, &publicModel) != nil || publicModel == "" {
				refuse(errMissingParam("model"), spool.StatusBadRequest)
				return
			}
			if refused := s.checkPassthroughN(params, refuse); refused {
				return
			}

			// The model fence, unchanged and not rebuilt. passthroughModel
			// synthesizes the CREDIT-axis model this name would resolve to and
			// AllowsCreditModel compares lower-cased strings without consulting
			// the catalogue, so a name no catalogue lists is fenced exactly as
			// it is on chat.
			model := passthroughModel(&doc, publicModel)
			if model == nil {
				// A reserved self-serve prefix, or a name too long to be one.
				// Either way it is not a passthrough name and never leaves.
				refuse(errModelNotFound, spool.StatusBadRequest)
				return
			}
			// Recorded only once the length guard above has accepted it. The
			// name is client input on its way to the spool and the journal,
			// and the guard is what bounds it — assigning any earlier would
			// let a refused request write kilobytes of junk to both.
			ev.PublicModelName = model.PublicName
			if !key.AllowsCreditModel(model) {
				refuseAs(errCreditModelNotAllowed, spool.StatusBadRequest,
					"credit_model_not_allowed")
				return
			}
			forward, err := json.Marshal(params)
			if err != nil {
				refuse(errBadJSON, spool.StatusBadRequest)
				return
			}
			body = forward
		}

		upCtx, cancel := context.WithTimeout(r.Context(), s.cfg.RequestMaxDuration)
		defer cancel()

		// Lowered at the lookup like every other upstream-ref comparison, so
		// the case a document happens to use cannot decide whether the surface
		// works.
		ref := strings.ToLower(doc.PassthroughRef)
		up, ok := s.cfg.Upstreams[ref]
		if !ok {
			// The loader refuses a passthroughRef that names no configured
			// upstream, so reaching this means the document changed under the
			// request. It is still a deployment fault, not the caller's.
			s.log.Error("passthrough upstream is not configured", "upstreamRef", ref)
			refuse(errServiceDisabled, spool.StatusUpstreamErr)
			return
		}
		ev.UpstreamRef = up.Ref
		ev.Attempts = 1
		s.health.recordAttempt(ref)
		// One attempt, no retry and no fallback, unlike chat. A passthrough
		// model carries no fallback ref to try, and an image generation is
		// expensive and not idempotent: an upstream that accepted the POST may
		// be generating right now, so repeating it bills a second generation
		// for an answer nobody reads twice.
		resp, ae := s.do(upCtx, up, attemptSpec{
			client:     s.passthroughClient,
			method:     route.method,
			path:       route.upstreamPath,
			body:       body,
			cred:       key.CredentialFor(ref),
			creditAxis: true,
			headers:    passthroughForwardedHeaders(r.Header),
		})
		if ae != nil {
			s.health.recordPassiveFailure(ref, ae.kind)
			switch {
			case r.Context().Err() != nil:
				ev.Status = spool.StatusCanceled
				ev.ErrorType = "client_disconnected"
				record()
			case upCtx.Err() != nil || ae.timeout:
				refuse(errUpstreamTimeout, spool.StatusTimeout)
			case ae.refusal != nil:
				status := spool.StatusUpstreamErr
				if ae.refusal.code == errCreditExhausted.code {
					status = spool.StatusRateLimited
				}
				refuse(*ae.refusal, status)
			case ae.throttled:
				refuse(errServerBusy, spool.StatusUpstreamErr)
			default:
				s.log.Error("passthrough upstream request failed", "keyId", key.KeyID,
					"path", r.URL.Path, "error", ae.err)
				refuse(errUpstream, spool.StatusUpstreamErr)
			}
			return
		}
		defer resp.Body.Close()
		s.health.recordSuccess(ref)

		// Body capture is deliberately absent on this surface, not forgotten.
		// The extractors are chat-shaped — a `messages` array in and
		// `choices[0].message.content` out — and neither has a counterpart in
		// an image or an embedding response. Widening them is a body-record
		// design question, not a routing one.
		s.finishPassthrough(w, resp, route, inputBytes, &ev, record)
	}
}

// finishPassthrough reads the upstream answer, meters it and forwards it
// verbatim.
//
// Verbatim is the point: chat re-marshals its response because it has to
// rewrite the model name, but a passthrough model's public name IS its
// upstream name, so there is nothing to rewrite and re-serialising a
// multi-megabyte base64 payload would put a second copy of it on the heap for
// no gain. The usage object is lifted out with a decode into a two-field
// envelope, which walks the body without materialising it.
func (s *Server) finishPassthrough(w http.ResponseWriter, resp *http.Response, route passthroughRoute,
	inputBytes int, ev *spool.Event, record func()) {
	// One byte past the cap, so hitting it is detectable. io.LimitReader stops
	// silently, and a body cut mid-JSON is indistinguishable from an upstream
	// sending garbage — which is the wrong thing to tell someone whose request
	// was merely too big to relay.
	limit := s.cfg.PassthroughResponseMaxBytes
	body, err := io.ReadAll(io.LimitReader(resp.Body, limit+1))
	if err == nil && int64(len(body)) > limit {
		writeAPIError(w, errPassthroughResponseTooLarge)
		ev.Status = spool.StatusUpstreamErr
		ev.ErrorType = errPassthroughResponseTooLarge.code
		record()
		return
	}
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			writeAPIError(w, errUpstreamTimeout)
			ev.Status = spool.StatusTimeout
			ev.ErrorType = errUpstreamTimeout.code
		} else {
			writeAPIError(w, errUpstream)
			ev.Status = spool.StatusUpstreamErr
			ev.ErrorType = "upstream_read_failed"
		}
		record()
		return
	}
	// A response that does not parse as JSON is not something the gateway can
	// vouch for, and forwarding it verbatim could hand back whatever the
	// upstream put in it. Same answer chat gives.
	u, haveUsage, ok := passthroughUsage(body)
	if !ok {
		writeAPIError(w, errUpstream)
		ev.Status = spool.StatusUpstreamErr
		ev.ErrorType = "upstream_invalid_response"
		record()
		return
	}

	// Bound the write to the client. The slot this request holds is released
	// only when the handler returns, and nothing else bounds a caller that
	// reads its answer a byte at a time: upCtx covers the upstream call, and
	// the server sets no write timeout. With a pool this small, two slow
	// readers would otherwise be the whole surface.
	_ = http.NewResponseController(w).SetWriteDeadline(time.Now().Add(s.cfg.RequestMaxDuration))
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	if _, err := w.Write(body); err != nil {
		ev.Status = spool.StatusCanceled
		ev.ErrorType = "client_disconnected"
	} else {
		ev.Status = spool.StatusOK
	}
	if !route.readsBody {
		// A catalogue read spends no tokens and the vendor reports none, so
		// there is nothing to estimate. Leaving Estimated false here is the
		// honest record: a zero that is known, not a zero that was guessed.
		record()
		return
	}
	// Every route this surface opens returns usage, so the estimate below is
	// the degraded path and its `estimated` flag is the signal that the
	// vendor stopped reporting — see settleUsage.
	s.settleUsage(ev, u, haveUsage, inputBytes, 0)
	record()
}

// checkPassthroughN enforces the bound on `n`, answering true when it already
// refused the request.
//
// The comparison is made in float space and before any conversion to int.
// Go leaves an out-of-range float-to-int conversion implementation-defined,
// and the two architectures in play disagree: `n: 1e19` saturates to a huge
// positive on arm64 and wraps to a huge negative on amd64. Converting first
// would therefore refuse on the development machine and forward on the
// deployed host, which is the worst possible place for a bound to differ.
//
// A value below one is refused as well. It cannot produce an oversized
// response, but it is not a request the vendor can serve either, and spending
// an upstream call to be told so helps nobody.
func (s *Server) checkPassthroughN(params map[string]json.RawMessage,
	refuse func(apiError, string)) bool {
	raw, ok := params["n"]
	if !ok || string(bytes.TrimSpace(raw)) == "null" {
		// SDKs send null for "unset", which is the same as absent.
		return false
	}
	var num json.Number
	if json.Unmarshal(raw, &num) != nil {
		refuse(errInvalidParamValue("n"), spool.StatusBadRequest)
		return true
	}
	f, err := num.Float64()
	if err != nil || f < 1 {
		refuse(errInvalidParamValue("n"), spool.StatusBadRequest)
		return true
	}
	if f > float64(s.cfg.PassthroughMaxN) {
		refuse(errPassthroughTooManyItems(s.cfg.PassthroughMaxN), spool.StatusBadRequest)
		return true
	}
	return false
}

// passthroughUsage lifts the token counts out of a response. ok=false means
// the body is not JSON at all.
//
// It reads both field namings because this surface does not control which one
// the vendor uses per route, and a struct that quietly fills with zeros would
// record a metered request as costing nothing while leaving `estimated` false
// — a wrong number that says it is exact. A usage object that yields nothing
// on either naming is therefore reported as absent, which sends the event down
// the estimate path where the flag says so.
func passthroughUsage(body []byte) (usage, bool, bool) {
	var envelope struct {
		Usage *struct {
			PromptTokens     int `json:"prompt_tokens"`
			CompletionTokens int `json:"completion_tokens"`
			TotalTokens      int `json:"total_tokens"`
			InputTokens      int `json:"input_tokens"`
			OutputTokens     int `json:"output_tokens"`
		} `json:"usage"`
	}
	if json.Unmarshal(body, &envelope) != nil {
		return usage{}, false, false
	}
	if envelope.Usage == nil {
		return usage{}, false, true
	}
	u := usage{
		PromptTokens:     envelope.Usage.PromptTokens,
		CompletionTokens: envelope.Usage.CompletionTokens,
		TotalTokens:      envelope.Usage.TotalTokens,
	}
	if u.PromptTokens == 0 && u.CompletionTokens == 0 {
		u.PromptTokens = envelope.Usage.InputTokens
		u.CompletionTokens = envelope.Usage.OutputTokens
	}
	if u.PromptTokens == 0 && u.CompletionTokens == 0 {
		if u.TotalTokens == 0 {
			return usage{}, false, true
		}
		// A total with no split. The event has no field for a total, and
		// recording the split as two zeros would report a metered request as
		// costing nothing while leaving `estimated` false — a wrong number
		// wearing the flag that says it is exact. Carrying it on the input
		// side keeps the sum right, which is what every consumer adds up; the
		// split is simply not knowable from what the vendor sent.
		u.PromptTokens = u.TotalTokens
	}
	return u, true, true
}

// passthroughHeaderAllowlist is which client headers reach the upstream. It is
// empty, and that is a decision rather than an omission — the mechanism is
// here so that opening one is a single entry.
//
// Nothing survived the list. `Accept` is the one a client legitimately sets,
// and forwarding it is how metering breaks: a caller asking for image/png gets
// a body with no `usage` in it and the event silently drops to an estimate.
// `Accept-Encoding` would hand back a body this surface cannot read.
// `User-Agent` describes the student's machine to a third party for no benefit.
// Vendor attribution headers name the vendor in code that is deliberately
// vendor-neutral, and would attribute a student's call to our account anyway.
// `Content-Type` and `Authorization` are set by the gateway itself and are not
// the client's to choose.
var passthroughHeaderAllowlist = []string{}

// passthroughForwardedHeaders copies the allowlisted client headers, dropping
// everything else. Values are copied whole; nothing here interprets them.
func passthroughForwardedHeaders(h http.Header) http.Header {
	var out http.Header
	for _, name := range passthroughHeaderAllowlist {
		v := h.Values(name)
		if len(v) == 0 {
			continue
		}
		if out == nil {
			out = http.Header{}
		}
		out[http.CanonicalHeaderKey(name)] = append([]string(nil), v...)
	}
	return out
}

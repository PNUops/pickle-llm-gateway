// The chat-completions path: authenticate, admit against limits, translate
// the public model name, forward, and meter. The gateway parses request and
// response JSON only to validate parameters, swap model names and read token
// usage; content is never inspected beyond size and never stored.
package server

import (
	"bufio"
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

// maxPassthroughNameBytes bounds a passthrough model name. Catalog names are
// short by construction; an uncatalogued name is client input headed for logs,
// the usage spool and an upstream URL-less request body, so junk is cut off
// here rather than carried through all three.
const maxPassthroughNameBytes = 256

// passthroughModel synthesizes a model entry for a public name the catalog
// does not list, when the current document names a passthrough upstream. The
// synthesized model is always on the CREDIT axis: the commercial provider
// prices it, so the per-key credential (and the money limit behind it) is the
// only budget that can govern it. Names under the self-serve prefix never
// pass through — a typo in a curated name must stay a 404 rather than become
// a billable request to the commercial provider.
func passthroughModel(doc *snapshot.Document, publicName string) *snapshot.Model {
	// The prefix guard is case-insensitive even though catalog lookup is not:
	// "PICKLE-general" must fail like "pickle-generall" does, not slip past
	// the guard into a billable request. Retired prefixes stay guarded too.
	if doc.PassthroughRef == "" ||
		snapshot.IsReservedModelName(publicName) ||
		// A preset stands in for the model and its fallbacks, so a name
		// carrying one is not a name this fence can judge. Guarded here rather
		// than at the call sites because this is the one door every
		// uncatalogued name goes through.
		snapshot.IsPresetModelName(publicName) ||
		len(publicName) > maxPassthroughNameBytes {
		return nil
	}
	return &snapshot.Model{
		PublicName:    publicName,
		UpstreamRef:   doc.PassthroughRef,
		UpstreamModel: publicName,
		BudgetAxis:    snapshot.AxisCredit,
	}
}

// `route` is deliberately not fenced, and this is inference rather than a
// verified fact. The vendor's current fallback and routing documents do not
// mention the field at all — not in the examples, not in the settings tables —
// so it reads as a retired one. The reasoning for leaving it open is that it
// selects AMONG candidates and `models` is the only candidate list, so fencing
// every entry of that list bounds whatever `route` can reach, and `route` with
// no `models` has nothing to select from.
//
// Recorded as inference on purpose. A note that claimed this was checked would
// be read as settled by the next person, and an unverified vendor behaviour
// treated as settled is exactly what defeated this fence once already.
//
// candidateModelsField is the request field that names models the vendor may
// serve INSTEAD of the one in `model`. The vendor's own documentation is
// explicit that any error can trigger a fallback to the next entry and that
// "requests are priced using the model that was ultimately used", so an entry
// here is a model this key can be billed for.
//
// That makes it the money fence's business rather than the parameter surface's.
// The fence is built on model identity — AllowsCreditModel judges one public
// name — so a field that changes which model answers walks around it while
// every record, the spool included, still shows the name that was judged.
const candidateModelsField = "models"

// fenceCandidateModels applies the money fence to every entry of that list,
// returning the first entry that fails.
//
// Judging beats refusing here. Refusing the field would take back a capability
// the vendor offers and this gateway has no reason to withhold; judging it
// leaves fallback working between models the key may already use, which is
// what the field is for. The body is parsed whole for `model` already, so the
// extra cost is walking a short array.
//
// An unreadable list fails closed. A list this build cannot parse is one it
// cannot fence, and forwarding it would hand the vendor candidates nobody
// judged.
func fenceCandidateModels(doc *snapshot.Document, modelLookup func(string) *snapshot.Model,
	key *snapshot.Key, params map[string]json.RawMessage) (string, bool) {
	raw, present := params[candidateModelsField]
	if !present || string(bytes.TrimSpace(raw)) == "null" {
		return "", true
	}
	var names []string
	if json.Unmarshal(raw, &names) != nil {
		return "", false
	}
	for _, name := range names {
		if !allowsCandidateModel(doc, modelLookup, key, name) {
			return name, false
		}
	}
	return "", true
}

// allowsCandidateModel resolves one candidate exactly the way the request's own
// `model` is resolved and applies the same two fences. Resolving it the same
// way is the point: it is what keeps a reserved self-serve prefix out of the
// list, so a typo in a curated name cannot leave for the vendor as a fallback
// after being refused as a primary.
func allowsCandidateModel(doc *snapshot.Document, modelLookup func(string) *snapshot.Model,
	key *snapshot.Key, name string) bool {
	// An empty candidate is refused rather than ignored. The primary `model`
	// already refuses an empty string, and a list that quietly tolerates one is
	// the asymmetry a fail-closed claim cannot afford — `[null]` decodes to
	// exactly this.
	if name == "" {
		return false
	}
	// A preset in a candidate is the same refusal as a preset in `model`.
	if snapshot.IsPresetModelName(name) {
		return false
	}
	// A reserved self-serve name is never a candidate, catalogued or not. The
	// list goes to the vendor, so such a name there is either a typo or our own
	// naming leaving the platform; the catalogue lookup below would otherwise
	// accept a TOKEN-axis row and hand it over, which is exactly the leak the
	// primary path's prefix guard exists to stop.
	if snapshot.IsReservedModelName(name) {
		return false
	}
	m := modelLookup(name)
	if m == nil {
		m = passthroughModel(doc, name)
	}
	if m == nil {
		return false
	}
	// The axis has to be checked, not inferred. AllowsCreditModel answers true
	// for anything off the money axis — that is what makes it a money fence —
	// so a candidate that resolves to a TOKEN-axis catalogue row would sail
	// through every check here. Requiring the money axis closes that
	// structurally rather than by naming the shapes that could reach it, and
	// costs nothing: a self-hosted model is not something the vendor can serve,
	// so it is meaningless as a fallback anyway.
	if !m.CreditAxis() {
		return false
	}
	return key.AllowsModel(m) && key.AllowsCreditModel(m)
}

// The vendor's server tools are the sixth channel, and they arrive inside
// `tools` rather than beside it. A tool runs on the vendor's side during the
// completion, and of the twelve it offers, four take a model of their own:
// advisor consults a stronger model mid-generation, subagent delegates to a
// smaller worker, fusion runs a panel plus an analyst, and image_generation
// produces an image. The advisor's documentation says its model may be any
// model on the platform and gives a tilde floating alias as the first example —
// the shape closed everywhere else this round.
//
// image_generation is the one that matters most, because it runs on the chat
// path and so reaches past the capability fence that governs the image routes.
// Note that the vendor's overview table lists its second model as "None" while
// its own detail page documents parameters.model with a default; the detail
// page is the specific one and is what this follows. Somebody reading only the
// overview will think this tool takes no model.
//
// These are judged rather than blocked, and judged by the same function the
// fallback candidates go through — a tool that names a model this key may
// already use is not a problem, and this way there is one rule with one more
// place that applies it rather than a second rule to keep in step. A tool that
// names no model passes untouched.
//
// WHAT THIS DELIBERATELY DOES NOT CATCH, so that it does not read as an
// oversight: a tool of the vendor's that this build does not know, naming its
// model through a parameter this build does not know, is forwarded unjudged.
// Refusing unknown tools was considered and declined (operator, 2026-09-06).
// The money is still bounded either way — the key's own credit limit is what
// caps it, so nobody can spend past what was approved — and what leaks is only
// which model that approved amount is spent on. While the deny list is a cost
// control, that is the caller's own balance draining faster, and a caller who
// gets this far is doing it on purpose.
//
// THE CONDITION THAT REVERSES IT: the moment a deny list carries policy rather
// than cost — a vendor nobody may use, a model excluded for a reason that is
// not price — the "their own balance" argument stops holding, because the harm
// is then no longer the caller's to absorb. Revisit this the first time a deny
// list is used that way.
const toolsField = "tools"

// serverToolPrefix marks the vendor's own tools. A caller's function tool
// carries its schema under `function`, so its own "parameters" is a different
// thing entirely and is deliberately not read here.
const serverToolPrefix = "openrouter:"

// imageGenerationTool produces an image from the chat path. It answers to the
// image capability as well: a grant that is required to call the image route
// and not required to reach the same work through a tool is not a grant.
const imageGenerationTool = "openrouter:image_generation"

// toolModelParams are the parameters through which a server tool names the
// model it will call. A tool naming one some other way is forwarded unjudged —
// see the decision recorded above; that is a choice, not a gap nobody noticed.
var toolModelParams = []string{"model", "analysis_models"}

// fenceServerTools judges every model a server tool names, and applies the
// image capability to the tool that generates images. It returns the refusal to
// send, or nil.
func (s *Server) fenceServerTools(doc *snapshot.Document, modelLookup func(string) *snapshot.Model,
	key *snapshot.Key, params map[string]json.RawMessage) *apiError {
	raw, present := params[toolsField]
	if !present || string(bytes.TrimSpace(raw)) == "null" {
		return nil
	}
	var tools []json.RawMessage
	if json.Unmarshal(raw, &tools) != nil {
		// Unreadable is unfenceable, and this field can carry a model.
		e := errInvalidParamValue(toolsField)
		return &e
	}
	for _, rawTool := range tools {
		var tool struct {
			Type       string                     `json:"type"`
			Parameters map[string]json.RawMessage `json:"parameters"`
		}
		if json.Unmarshal(rawTool, &tool) != nil {
			e := errInvalidParamValue(toolsField)
			return &e
		}
		if !strings.HasPrefix(strings.ToLower(tool.Type), serverToolPrefix) {
			continue
		}
		if strings.EqualFold(tool.Type, imageGenerationTool) &&
			!key.AllowsEndpoint(snapshot.EndpointImages) {
			e := errEndpointNotAllowed(endpointLabel(snapshot.EndpointImages))
			return &e
		}
		for _, name := range toolModelParams {
			v, ok := tool.Parameters[name]
			if !ok {
				continue
			}
			// The vendor spells these both ways: one model, or a list.
			var one string
			if json.Unmarshal(v, &one) == nil {
				if !allowsCandidateModel(doc, modelLookup, key, one) {
					e := errToolModelNotAllowed(one)
					return &e
				}
				continue
			}
			var many []string
			if json.Unmarshal(v, &many) != nil {
				e := errInvalidParamValue(toolsField)
				return &e
			}
			for _, candidate := range many {
				if !allowsCandidateModel(doc, modelLookup, key, candidate) {
					e := errToolModelNotAllowed(candidate)
					return &e
				}
			}
		}
	}
	return nil
}

// tokenAxisParams are the top-level request fields the gateway forwards for a
// self-hosted model. An unknown field is refused rather than silently
// forwarded, so replacing the upstream can never silently change what student
// code is allowed to send.
//
// `reasoning_effort` is absent from this list on purpose, and so are
// `chat_template_kwargs` and `thinking_token_budget`: all three are the
// enforcement point for one service decision, that thinking is off by default
// on the self-hosted side with no per-request opt-in. The serving process does
// not refuse them — measured, same request with and only without the field:
//
//	reasoning_effort "low" → 200, content null, reasoning "Here's a thinking process:..."
//	field absent           → 200, content "Hello! How can I help you today", reasoning null
//
// so the field works, moves the answer out of `content`, and this list is the
// only thing standing between a caller and thinking. Opening it is a service
// decision, not a consistency fix: completion tokens go up by orders of
// magnitude when thinking is on, and that lands on the shared token allowance.
// (The often-quoted ratio from the benchmark round is an estimate, not a
// measurement — its thinking-off side was converted from a different
// checkpoint. Do not restate it as measured.)
var tokenAxisParams = map[string]bool{
	"model":                 true,
	"messages":              true,
	"stream":                true,
	"stream_options":        true,
	"max_tokens":            true,
	"max_completion_tokens": true,
	"temperature":           true,
	"top_p":                 true,
	"stop":                  true,
	"presence_penalty":      true,
	"frequency_penalty":     true,
	"seed":                  true,
	"user":                  true,
	"response_format":       true,
	"tools":                 true,
	"tool_choice":           true,
	"parallel_tool_calls":   true,
}

// creditOnlyParams are fields that are ordinary on the paid side and closed on
// the self-hosted one. The set permits nothing: the money axis has no allowlist
// at all, so no request is checked against a set there. It exists only to
// classify a refusal on the self-hosted side.
//
// That classification is worth keeping on its own. Without it a caller sending
// `reasoning_effort` to a self-hosted model hears "unknown field", which is
// false and dead-ends them; with it they hear that the field works on a paid
// model, which is the only clue they get about what to do next.
//
// `reasoning_effort` and `verbosity` are documented Chat Completions fields on
// the commercial side, and agent tools set them on their own from model
// metadata. They stay closed on the self-hosted side for three reasons, none
// of which is that the serving process would fail on them. Thinking there is
// disabled as a server default and re-opening it is a service decision, not a
// per-request one; a field that changes nothing would still read to the caller
// as if it had; and a model whose fallback points at a commercial upstream
// would have that upstream honour the field, putting reasoning tokens on the
// self-hosted allowance. The axis is a property of the model, not of the
// upstream that happens to answer.
var creditOnlyParams = map[string]bool{
	"reasoning_effort": true,
	"verbosity":        true,
}

type usage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

func (s *Server) handleChat(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeAPIError(w, errMethod)
		return
	}
	start := s.now()
	ev := spool.Event{EventUUID: spool.NewEventUUID(), RequestedAt: start}
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
		s.log.Info("chat request", "keyId", ev.KeyID, "model", ev.PublicModelName,
			"status", ev.Status, "errorType", ev.ErrorType,
			"inputTokens", ev.InputTokens, "outputTokens", ev.OutputTokens,
			"latencyMs", ev.LatencyMs, "ttftMs", ev.TtftMs)
	}
	refuse := func(e apiError, status string) {
		writeAPIError(w, e)
		ev.Status = status
		ev.ErrorType = e.code
		record()
	}
	// Same refusal to the caller, a different name in the usage record. Used
	// where two distinct causes deliberately share one public error code and
	// the accounting still has to tell them apart.
	refuseAs := func(e apiError, status, errorType string) {
		writeAPIError(w, e)
		ev.Status = status
		ev.ErrorType = errorType
		record()
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
	r.Body = http.MaxBytesReader(w, r.Body, s.cfg.RequestBodyMaxBytes)
	rawBody, err := io.ReadAll(r.Body)
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			refuse(errRequestTooLarge, spool.StatusBadRequest)
		} else {
			refuse(errBadJSON, spool.StatusBadRequest)
		}
		return
	}
	var params map[string]json.RawMessage
	if err := json.Unmarshal(rawBody, &params); err != nil {
		refuse(errBadJSON, spool.StatusBadRequest)
		return
	}
	var publicModel string
	if raw, ok := params["model"]; !ok || json.Unmarshal(raw, &publicModel) != nil || publicModel == "" {
		refuse(errMissingParam("model"), spool.StatusBadRequest)
		return
	}
	messagesRaw, ok := params["messages"]
	if !ok {
		refuse(errMissingParam("messages"), spool.StatusBadRequest)
		return
	}
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
			if tokenAxisParams[name] {
				continue
			}
			if creditOnlyParams[name] {
				// Known field, wrong axis. Same public code as any other
				// rejected field — one more code is one more thing every SDK
				// has to learn for a fact it already handles — but different
				// advice, and its own spool type so the two stay countable
				// apart.
				refuseAs(errParamNeedsCreditModel(name), spool.StatusBadRequest,
					"credit_only_parameter")
				return
			}
			refuse(errUnsupportedParam(name), spool.StatusBadRequest)
			return
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
		// A preset carries its own model and fallback list, so it reaches past
		// every check here from outside the fields they read. Refused rather
		// than judged: presets live on the platform's vendor account, which
		// every student key is issued under, so one created there is reachable
		// by all of them.
		if _, present := params[snapshot.PresetField]; present {
			refuseAs(errPresetNotAllowed, spool.StatusBadRequest, "preset_not_allowed")
			return
		}
		// The same fence over the fallback candidates. Without this the field
		// is a way around everything above it: the vendor may serve and bill
		// any entry, while the fence, the spool and the student's own screen
		// all keep showing the name that was judged.
		if bad, ok := fenceCandidateModels(&doc, modelLookup, key, params); !ok {
			refuseAs(errCandidateModelNotAllowed(bad), spool.StatusBadRequest,
				"credit_model_not_allowed")
			return
		}
		// And over the models the vendor's own server tools name, which reach
		// past everything above from inside `tools`.
		if e := s.fenceServerTools(&doc, modelLookup, key, params); e != nil {
			refuseAs(*e, spool.StatusBadRequest, toolRefusalType(*e))
			return
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
	// student sent (on either OpenAI field) is normalized onto the upstream's
	// configured cap field and capped at the model maximum, so the limit lands
	// on the field the upstream actually honors — forwarding the student's
	// field verbatim would let a legacy `max_tokens`-only server ignore a
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
	for _, f := range []string{"max_completion_tokens", "max_tokens"} {
		raw, ok := params[f]
		if !ok {
			continue
		}
		delete(params, f) // re-added below on up.CapField
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
	if model.MaxInputTokens > 0 && len(messagesRaw) > model.MaxInputTokens*6 {
		refuse(errInputTooLong, spool.StatusBadRequest)
		return
	}

	streaming := false
	if raw, ok := params["stream"]; ok {
		_ = json.Unmarshal(raw, &streaming)
	}
	// Usage must always come back from the upstream for metering, but the
	// usage chunk is only forwarded when the student asked for it — clients
	// that never opted in would break on a chunk with an empty choices array.
	studentWantsUsage := false
	if raw, ok := params["stream_options"]; ok {
		var opts struct {
			IncludeUsage bool `json:"include_usage"`
		}
		_ = json.Unmarshal(raw, &opts)
		studentWantsUsage = opts.IncludeUsage
	}
	params["model"] = json.RawMessage(strconv.Quote(model.UpstreamModel))
	if streaming {
		params["stream_options"] = withIncludeUsage(params["stream_options"])
	}

	upCtx, cancel := context.WithTimeout(r.Context(), s.cfg.RequestMaxDuration)
	defer cancel()

	// Try the model's upstream, then its fallback if it has one. Nothing has
	// been written to the client yet, so switching upstreams here is invisible
	// to the student; once the response starts, it is not.
	resp, up, attempts, attemptErr := s.callUpstream(upCtx, model, key, params, outputCap)
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
			refuse(errServerBusy, spool.StatusUpstreamErr)
		default:
			s.log.Error("upstream request failed", "keyId", key.KeyID,
				"model", publicModel, "error", attemptErr.err)
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
		capture.Request, capture.RequestTruncated = capRequest(messagesRaw)
	}

	if !streaming {
		s.finishNonStream(w, resp, model.PublicName, model.UpstreamModel, charge, &ev, record, len(messagesRaw), capture)
		return
	}
	s.finishStream(w, resp, streamArgs{
		capture:      capture,
		publicName:   model.PublicName,
		sentModel:    model.UpstreamModel,
		keyID:        key.KeyID,
		charge:       charge,
		inputBytes:   len(messagesRaw),
		forwardUsage: studentWantsUsage,
		clientCtx:    r.Context(),
		upCtx:        upCtx,
	}, &ev, record)
}

// withIncludeUsage forces stream_options.include_usage on so streamed
// responses carry token counts, merging any options the student sent.
func withIncludeUsage(raw json.RawMessage) json.RawMessage {
	opts := map[string]json.RawMessage{}
	if len(raw) > 0 {
		_ = json.Unmarshal(raw, &opts)
	}
	opts["include_usage"] = json.RawMessage("true")
	out, err := json.Marshal(opts)
	if err != nil {
		return json.RawMessage(`{"include_usage":true}`)
	}
	return out
}

// upstreamResponseCapBytes bounds what one upstream response may cost this
// process. A chat completion is text; the previous 32 MiB allowed for was
// several times what any model produces, and the non-stream path holds the raw
// bytes, the decoded map and the re-marshalled output at once — so the real
// cost is a multiple of this, times the in-flight cap.
const upstreamResponseCapBytes = 8 << 20

// servedModelMismatch records that the vendor answered with a model other than
// the one requested. The vendor falls back on its own and prices the request
// using whatever it ultimately used, so this is a real event; the response's
// model field is rewritten to the public name a line later, which makes this
// the only place the fact exists at all.
//
// It is logged rather than spooled. The usage event has no field for a served
// model, and adding one is a change the control plane has to accept first —
// the same ordering that governed budgetAxis — so the log is what closes the
// gap today.
func (s *Server) noteServedModel(ev *spool.Event, requested, served string) {
	if requested == "" || served == "" || strings.EqualFold(requested, served) {
		return
	}
	s.log.Warn("upstream served a different model than requested",
		"keyId", ev.KeyID, "publicModel", ev.PublicModelName,
		"requested", requested, "served", served, "eventUuid", ev.EventUUID)
}

func (s *Server) finishNonStream(w http.ResponseWriter, resp *http.Response, publicName, sentModel string, charge func(int), ev *spool.Event, record func(), inputBytes int, capture *bodies.Record) {
	body, err := io.ReadAll(io.LimitReader(resp.Body, upstreamResponseCapBytes))
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
		writeAPIError(w, errUpstream)
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
	if raw, has := m["model"]; has {
		served, _ := raw.(string)
		s.noteServedModel(ev, sentModel, served)
		m["model"] = publicName
	}
	var u usage
	haveUsage := false
	if uraw, has := m["usage"]; has && uraw != nil {
		if b, err := json.Marshal(uraw); err == nil && json.Unmarshal(b, &u) == nil {
			haveUsage = true
		}
	}
	// The assistant text is needed for the no-usage size estimate, and again
	// for capture when the key opted in.
	answer := ""
	if choices, _ := m["choices"].([]any); len(choices) > 0 {
		if first, _ := choices[0].(map[string]any); first != nil {
			if msg, _ := first["message"].(map[string]any); msg != nil {
				answer, _ = msg["content"].(string)
			}
		}
	}
	contentChars := 0
	if !haveUsage {
		contentChars = len(answer)
	}
	if capture != nil {
		capture.Response, capture.ResponseTruncated = capString(answer)
	}
	out, err := json.Marshal(m)
	if err != nil {
		writeAPIError(w, errUpstream)
		ev.Status = spool.StatusUpstreamErr
		ev.ErrorType = "upstream_invalid_response"
		// Defensive only, and no test covers it: everything a UseNumber decode
		// produces marshals again, so nothing a valid decode above can yield
		// reaches here. Kept for the same reason as the decode failure, and
		// charged the same way — bucket yes, accounting record no.
		if haveUsage {
			charge(u.PromptTokens + u.CompletionTokens)
		} else {
			in, outTok := estimateTokens(inputBytes, contentChars)
			charge(in + outTok)
		}
		record()
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	if _, err := w.Write(out); err != nil {
		ev.Status = spool.StatusCanceled
		ev.ErrorType = "client_disconnected"
	} else {
		ev.Status = spool.StatusOK
	}
	s.settleUsage(ev, u, haveUsage, inputBytes, contentChars)
	charge(ev.InputTokens + ev.OutputTokens)
	s.bodies.Offer(capture)
	if capture != nil {
		s.metrics.bodiesCaptured.Add(1)
	}
	record()
}

type streamArgs struct {
	publicName string
	// sentModel is the upstream model name this request asked for, so a
	// vendor-side fallback can be noticed before the name is rewritten.
	sentModel string
	// keyID is for logging only; metering goes through charge, which already
	// knows the key and whether this request's axis is metered at all.
	keyID        string
	charge       func(int)
	inputBytes   int
	forwardUsage bool
	capture      *bodies.Record // nil unless the key opted into body capture
	clientCtx    context.Context
	upCtx        context.Context
}

func (s *Server) finishStream(w http.ResponseWriter, resp *http.Response, a streamArgs, ev *spool.Event, record func()) {
	fl, _ := w.(http.Flusher)
	w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	if fl != nil {
		fl.Flush()
	}
	writeFailed := false
	writeRaw := func(b []byte) bool {
		if writeFailed {
			return false
		}
		if _, err := w.Write(b); err != nil {
			writeFailed = true
			return false
		}
		if fl != nil {
			fl.Flush()
		}
		return true
	}

	var u usage
	haveUsage := false
	contentChars := 0
	dropped := 0
	// Assembled assistant text, only when the key opted into capture.
	var answer strings.Builder

	// SSE events are framed by blank lines and one event's data may span
	// several `data:` lines, so payloads are assembled per event before
	// parsing. An assembled payload that still does not parse is dropped, not
	// forwarded: a verbatim chunk would leak the upstream model identifier.
	var dataLines [][]byte
	notedServed := false
	flushEvent := func() bool {
		if len(dataLines) == 0 {
			return true
		}
		payload := bytes.Join(dataLines, []byte("\n"))
		dataLines = nil
		if bytes.Equal(payload, []byte("[DONE]")) {
			return writeRaw([]byte("data: [DONE]\n\n"))
		}
		c, ok := rewriteChunk(payload, a.publicName)
		if !ok {
			// Not valid JSON. A payload that does not even open an object is a
			// heartbeat/keepalive (`ping`) with nothing structured to leak —
			// forward it verbatim. One that looks like a truncated object is
			// dropped rather than risk leaking a partial upstream identifier,
			// and the answer is now incomplete, so the request is marked
			// degraded below.
			if len(payload) == 0 || payload[0] != '{' {
				return writeRaw(append(append([]byte("data: "), payload...), '\n', '\n'))
			}
			dropped++
			return true
		}
		if !notedServed && c.servedModel != "" {
			notedServed = true
			s.noteServedModel(ev, a.sentModel, c.servedModel)
		}
		if c.usage != nil {
			u = *c.usage
			haveUsage = true
			if !a.forwardUsage {
				if c.choicesEmpty {
					// The gateway-requested usage chunk; the student did not
					// opt in, so it is consumed for metering and not sent.
					return true
				}
				c.out = c.stripUsage()
			}
		}
		contentChars += c.contentChars
		if a.capture != nil && c.content != "" && answer.Len() < bodies.ResponseCapBytes {
			answer.WriteString(c.content)
		}
		return writeRaw(append(append([]byte("data: "), c.out...), '\n', '\n'))
	}

	// Bounded like the non-stream read. ReadBytes accumulates into one growing
	// allocation until it finds a newline, so an upstream that emits a long
	// line without one grows it without limit — times the in-flight cap, on a
	// host with half a gigabyte.
	br := bufio.NewReaderSize(io.LimitReader(resp.Body, upstreamResponseCapBytes), 64<<10)
	var readErr error
	for {
		line, err := br.ReadBytes('\n')
		trimmed := bytes.TrimRight(line, "\r\n")
		switch {
		case len(trimmed) == 0 && len(line) > 0:
			flushEvent()
		case len(trimmed) == 0:
		case trimmed[0] == ':':
			// Comment lines are keepalives; forward as-is.
			writeRaw(append(trimmed, '\n', '\n'))
		default:
			if after, ok := bytes.CutPrefix(trimmed, []byte("data:")); ok {
				dataLines = append(dataLines, bytes.TrimSpace(after))
			}
			// Other SSE fields (event:, id:, retry:) are not part of the
			// surface and are dropped.
		}
		if writeFailed {
			break
		}
		if err != nil {
			if !errors.Is(err, io.EOF) {
				readErr = err
			} else {
				flushEvent()
			}
			break
		}
	}
	if dropped > 0 {
		s.log.Warn("dropped unparseable stream chunks", "keyId", a.keyID, "count", dropped)
	}

	// A stream that ends without the upstream's own [DONE] (deadline or an
	// interrupted read) gets a terminal error event plus [DONE] so the client
	// can tell a truncated answer from a complete one, rather than guessing
	// from a missing terminator.
	emitStreamError := func(code, msg string) {
		writeRaw([]byte(`data: {"error":{"message":"` + msg + `","type":"server_error","code":"` + code + `"}}` + "\n\n"))
		writeRaw([]byte("data: [DONE]\n\n"))
	}
	switch {
	case writeFailed || a.clientCtx.Err() != nil:
		ev.Status = spool.StatusCanceled
		ev.ErrorType = "client_disconnected"
	case errors.Is(readErr, context.DeadlineExceeded) || errors.Is(a.upCtx.Err(), context.DeadlineExceeded):
		// The gateway's own duration cap, not an upstream fault.
		ev.Status = spool.StatusTimeout
		ev.ErrorType = "request_deadline_exceeded"
		emitStreamError("request_deadline_exceeded", "요청 전체 시간 상한을 초과해 스트림을 종료했습니다.")
	case readErr != nil:
		ev.Status = spool.StatusUpstreamErr
		ev.ErrorType = "upstream_stream_interrupted"
		emitStreamError("upstream_stream_interrupted", "모델 서버 응답이 중간에 끊겼습니다. 응답이 불완전할 수 있습니다.")
	case dropped > 0:
		// The stream reached its end, but some chunks could not be forwarded,
		// so the answer the student received is incomplete. The upstream's
		// [DONE] already went out, so this cannot be signalled inline anymore;
		// record it as degraded rather than let it read as a clean success.
		ev.Status = spool.StatusUpstreamErr
		ev.ErrorType = "upstream_chunk_unreadable"
	default:
		ev.Status = spool.StatusOK
	}
	s.settleUsage(ev, u, haveUsage, a.inputBytes, contentChars)
	a.charge(ev.InputTokens + ev.OutputTokens)
	if a.capture != nil {
		a.capture.Response, a.capture.ResponseTruncated = capString(answer.String())
		s.bodies.Offer(a.capture)
		s.metrics.bodiesCaptured.Add(1)
	}
	record()
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

// capRequest bounds the captured prompt. Under the cap it stays the messages
// array the student sent. Over it, the array cannot be cut and still parse, so
// the record carries a JSON string holding the prefix instead and says so —
// losing the structure is better than losing the whole prompt, and far better
// than carrying two megabytes per record through a queue.
func capRequest(raw json.RawMessage) (json.RawMessage, bool) {
	if len(raw) <= bodies.RequestCapBytes {
		return append(json.RawMessage(nil), raw...), false
	}
	prefix, _ := capAt(string(raw), bodies.RequestCapBytes)
	encoded, err := json.Marshal(prefix)
	if err != nil {
		return nil, true
	}
	return encoded, true
}

// chunk is one parsed and rewritten SSE payload.
type chunk struct {
	out          []byte
	parsed       map[string]any
	servedModel  string // the upstream's own model name, before it is rewritten
	usage        *usage
	content      string // this chunk's assistant text, for capture
	contentChars int
	choicesEmpty bool
}

// stripUsage re-marshals the chunk without its usage field, for the case
// where a content-bearing chunk carries usage the student did not ask for.
func (c *chunk) stripUsage() []byte {
	delete(c.parsed, "usage")
	b, err := json.Marshal(c.parsed)
	if err != nil {
		return c.out
	}
	return b
}

// rewriteChunk swaps the model name in one SSE data payload and pulls token
// usage out of the chunk that carries it. ok=false means the payload did not
// parse as JSON.
func rewriteChunk(payload []byte, publicName string) (chunk, bool) {
	var m map[string]any
	dec := json.NewDecoder(bytes.NewReader(payload))
	dec.UseNumber()
	if dec.Decode(&m) != nil {
		return chunk{}, false
	}
	c := chunk{parsed: m, choicesEmpty: true}
	if raw, has := m["model"]; has {
		c.servedModel, _ = raw.(string)
		m["model"] = publicName
	}
	if uraw, has := m["usage"]; has && uraw != nil {
		if b, err := json.Marshal(uraw); err == nil {
			var parsed usage
			if json.Unmarshal(b, &parsed) == nil {
				c.usage = &parsed
			}
		}
	}
	if choices, _ := m["choices"].([]any); len(choices) > 0 {
		c.choicesEmpty = false
		if first, _ := choices[0].(map[string]any); first != nil {
			if delta, _ := first["delta"].(map[string]any); delta != nil {
				if content, _ := delta["content"].(string); content != "" {
					c.content = content
					c.contentChars = len(content)
				}
			}
		}
	}
	b, err := json.Marshal(m)
	if err != nil {
		return chunk{}, false
	}
	c.out = b
	return c, true
}

// settleUsage fills the event's token counts: exact when the upstream
// reported usage, a byte-based estimate flagged as such otherwise.
func (s *Server) settleUsage(ev *spool.Event, u usage, haveUsage bool, inputBytes, contentChars int) {
	if haveUsage {
		ev.InputTokens = u.PromptTokens
		ev.OutputTokens = u.CompletionTokens
		return
	}
	ev.InputTokens, ev.OutputTokens = estimateTokens(inputBytes, contentChars)
	ev.Estimated = true
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

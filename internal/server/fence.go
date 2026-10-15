// The money fence over the request body: every place a request can name a
// model the vendor may bill other than `model` itself, and the capabilities a
// body can reach without its own route.
package server

import (
	"bytes"
	"encoding/json"
	"regexp"
	"strings"

	"github.com/pnuops/pickle-llm-gateway/internal/snapshot"
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
		PublicName:  publicName,
		UpstreamRef: doc.PassthroughRef,
		// The two names are deliberately the same string, and something now
		// depends on it: the passthrough surface has no upstream name in scope
		// where it records a served model, so it passes the public one. That is
		// correct only while this identity holds. Mapping a name here without
		// threading the upstream one down there turns that comparison into the
		// public-versus-upstream one that was wrong everywhere else.
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
// the shape every other fence here closes.
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
//
// The same reasoning does not cover a tool that runs in a container, because
// what leaks there is not money but another key's files: the vendor shares
// containers across every key of an account. The two container tools known
// today are refused below; a container tool the vendor adds later would pass
// through this gap until it is added to that list.
const toolsField = "tools"

// serverToolPrefix marks the vendor's own tools in their canonical spelling. A
// caller's function tool carries its schema under `function`, so its own
// "parameters" is a different thing entirely and is deliberately not read here.
const serverToolPrefix = "openrouter:"

// imageGenerationTool produces an image from the chat path. It answers to the
// image capability as well: a grant that is required to call the image route
// and not required to reach the same work through a tool is not a grant.
const imageGenerationTool = "openrouter:image_generation"

// webSearchTool searches the web during the completion, billed per search. It
// answers to the web search capability wherever it appears, nested tool lists
// included, for the same reason the image tool answers to images.
const webSearchTool = "openrouter:web_search"

// shellTool and bashTool run commands in a sandbox container on the vendor's
// side. A container is scoped to the vendor account, not to a key, and the id
// that selects one comes from the request: an id the caller names, or one
// derived from session_id, the X-Session-Id header or prompt_cache_key. Two
// keys under one account that arrive at the same id reach the same files, and
// a common session value is enough for that to happen by accident. So both
// answer to the containers capability, which is not grantable yet; the inputs
// that derive the id are left alone, since without a container they only pin
// a provider for prompt caching.
const (
	shellTool = "openrouter:shell"
	bashTool  = "openrouter:bash"
)

// containerTools are the server tools that create or reach a container. The
// vendor's container guide names these two and no others. apply_patch is not
// one: the vendor documents it as a patch proposal that the caller's own
// application applies, with no backend beyond the inference request.
var containerTools = map[string]bool{shellTool: true, bashTool: true}

// serverToolFamilies is the one list of the vendor's other spellings for its
// server tools, by the base name a spelling reduces to. The vendor accepts
// each tool under several types (its tool catalogue lists them as aliases):
// web_search, web_search_preview and web_search_preview_2025_03_11 all run
// the paid search on the chat route, web_search_20250305 does on the Messages
// route, and advisor_20260301 there takes a model of its own. A fence that
// reads only the "openrouter:" spelling is walked around by every other one,
// so every spelling is reduced to its canonical tool before it is judged.
//
// A base name is matched as a family: anything that begins with one of these
// is that tool, which also covers a dated spelling the vendor adds later. The
// families are the tools a fence here acts on, plus the ones it knowingly lets
// through so that the list says so: web_fetch passes (operator, 2026-10-14),
// and tool_search and apply_patch take neither a model nor a grant. shell
// needs the containers grant. code_execution is absent: the vendor refuses it
// as an unknown shorthand.
//
// bash is not a family, because its dated spellings are a different tool. The
// vendor's bash runs in its sandbox only under its own engine, and documents
// Anthropic's built-in bash_20250124 as a tool that returns the command to the
// caller's application, which runs it. So only the bare word "bash" is read as
// the vendor's tool (see canonicalServerTool), and bash_<date> passes as the
// client tool it is. A live call (2026-10-16) bore that out: bash_20250124 came
// back as a tool_use for the caller with no container, while openrouter:shell,
// and openrouter:bash under the vendor's engine, ran in one and returned its id.
var serverToolFamilies = []struct{ base, tool string }{
	{"web_search", webSearchTool},
	{"image_generation", imageGenerationTool},
	{"advisor", "openrouter:advisor"},
	{"fusion", "openrouter:fusion"},
	{"subagent", "openrouter:subagent"},
	{"web_fetch", "openrouter:web_fetch"},
	{"tool_search", "openrouter:tool_search"},
	{"shell", shellTool},
	{"apply_patch", "openrouter:apply_patch"},
}

// toolVersionSuffix is the date a spelling carries after its base name, in
// either form the vendor uses: _20250305 or _2025_03_11.
var toolVersionSuffix = regexp.MustCompile(`_(\d{8}|\d{4}_\d{2}_\d{2})$`)

// canonicalServerTool reduces a tool type to the vendor tool it runs, or ""
// when it is not one of the vendor's server tools (a caller's function tool, a
// client-side tool). The "openrouter:" spelling is canonical already and kept
// as written, so a vendor tool this build has no family for is still judged
// by the model fields it names.
func canonicalServerTool(rawType string) string {
	t := strings.ToLower(strings.TrimSpace(rawType))
	if strings.HasPrefix(t, serverToolPrefix) {
		// The vendor's own namespace: a dated or spaced spelling there is
		// still that tool, so it reduces like the bare one. Under this
		// prefix even a dated bash is the vendor's bash, not a client tool.
		name := strings.TrimSpace(strings.TrimPrefix(t, serverToolPrefix))
		base := toolVersionSuffix.ReplaceAllString(name, "")
		if base == "bash" {
			return bashTool
		}
		for _, f := range serverToolFamilies {
			if strings.HasPrefix(base, f.base) {
				return f.tool
			}
		}
		return serverToolPrefix + name
	}
	if t == "bash" {
		return bashTool
	}
	base := toolVersionSuffix.ReplaceAllString(t, "")
	for _, f := range serverToolFamilies {
		if strings.HasPrefix(base, f.base) {
			return f.tool
		}
	}
	return ""
}

// toolModelParams are the parameters through which a server tool names the
// model it will call. A tool naming one some other way is forwarded unjudged —
// see the decision recorded above; that is a choice, not a gap nobody noticed.
var toolModelParams = []string{"model", "analysis_models"}

// toolModelsRequiredOnFencedKey are the parameters a server tool must name on a
// key that carries a model fence, because leaving them out does not mean "no
// model" — it means the vendor picks one this fence never sees. The advisor's
// documented order is the tool definition's model, then whatever the executing
// model passes in its own tool call, then the outer model; the middle step is
// decided mid-generation and never appears in the request. Fusion defaults its
// panel to the vendor's quality preset (floating aliases), and its two
// documents disagree on the analyst's default. The subagent is absent because
// its documented fallback is the outer model, which is already judged.
var toolModelsRequiredOnFencedKey = map[string][]string{
	"openrouter:advisor": {"model"},
	"openrouter:fusion":  {"model", "analysis_models"},
}

// nestedToolsParam carries server tools for a tool's own inner calls: the
// subagent's worker and the fusion panel each take one. Every entry there runs
// on the same key, so it is fenced exactly like a top-level tool.
const nestedToolsParam = "tools"

// maxNestedToolDepth bounds the recursion through nestedToolsParam. The vendor
// documents one level (the subagent refuses to nest itself); a few more are
// tolerated so a shape the vendor adds later is fenced rather than refused, and
// anything deeper fails closed rather than being forwarded unread.
const maxNestedToolDepth = 3

// fenceServerTools judges every model a server tool names, and applies the
// image, web search and containers capabilities to the tools that need them. It
// returns the refusal to send, or nil.
func (s *Server) fenceServerTools(doc *snapshot.Document, modelLookup func(string) *snapshot.Model,
	key *snapshot.Key, params map[string]json.RawMessage) *apiError {
	raw, present := params[toolsField]
	if !present || string(bytes.TrimSpace(raw)) == "null" {
		return nil
	}
	return fenceToolList(doc, modelLookup, key, raw, 0)
}

func fenceToolList(doc *snapshot.Document, modelLookup func(string) *snapshot.Model,
	key *snapshot.Key, raw json.RawMessage, depth int) *apiError {
	var tools []json.RawMessage
	if json.Unmarshal(raw, &tools) != nil {
		// Unreadable is unfenceable, and this field can carry a model.
		e := errInvalidParamValue(toolsField)
		return &e
	}
	for _, rawTool := range tools {
		// Decoded into maps, not a tagged struct. Go matches struct tags
		// case-insensitively with the last duplicate winning, so a tool carrying
		// both "type" and "Type" would be judged by one key while the vendor,
		// which receives these bytes verbatim, reads the other. A map reads the
		// exact key, as the vendor does.
		var tool map[string]json.RawMessage
		if json.Unmarshal(rawTool, &tool) != nil {
			e := errInvalidParamValue(toolsField)
			return &e
		}
		var rawType string
		if v, ok := tool["type"]; ok && json.Unmarshal(v, &rawType) != nil {
			e := errInvalidParamValue(toolsField)
			return &e
		}
		var parameters map[string]json.RawMessage
		if v, ok := tool["parameters"]; ok && json.Unmarshal(v, &parameters) != nil {
			e := errInvalidParamValue(toolsField)
			return &e
		}
		toolType := canonicalServerTool(rawType)
		if toolType == "" {
			continue
		}
		if toolType == imageGenerationTool &&
			!key.AllowsEndpoint(snapshot.EndpointImages) {
			e := errEndpointNotAllowed(endpointLabel(snapshot.EndpointImages))
			return &e
		}
		if toolType == webSearchTool &&
			!key.AllowsEndpoint(snapshot.EndpointWebSearch) {
			e := errEndpointNotAllowed(endpointLabel(snapshot.EndpointWebSearch))
			return &e
		}
		if containerTools[toolType] &&
			!key.AllowsEndpoint(snapshot.EndpointContainers) {
			e := errContainerToolNotAllowed(endpointLabel(snapshot.EndpointContainers))
			return &e
		}
		// A tool names its model in its parameters on the chat route and at
		// its own top level on the Messages route (the advisor's model sits
		// beside its type there). Both places are read, and a name in either
		// is judged.
		for _, place := range []map[string]json.RawMessage{tool, parameters} {
			for _, name := range toolModelParams {
				v, ok := place[name]
				if !ok {
					continue
				}
				bad, refused, unreadable := judgeModelValue(doc, modelLookup, key, v)
				if unreadable {
					e := errInvalidParamValue(toolsField)
					return &e
				}
				if refused {
					e := errToolModelNotAllowed(bad)
					return &e
				}
				if e := onlineNeedsWebSearch(key, v); e != nil {
					return e
				}
			}
		}
		if key.HasCreditFence() {
			for _, name := range toolModelsRequiredOnFencedKey[toolType] {
				// An empty value is as good as none: the vendor falls back to
				// its default either way, and an empty list judged nothing.
				if !namesModel(tool, name) && !namesModel(parameters, name) {
					e := errToolModelRequired(strings.TrimSpace(rawType), name)
					return &e
				}
			}
		}
		if nested, ok := parameters[nestedToolsParam]; ok && string(bytes.TrimSpace(nested)) != "null" {
			if depth >= maxNestedToolDepth {
				e := errInvalidParamValue(toolsField)
				return &e
			}
			if e := fenceToolList(doc, modelLookup, key, nested, depth+1); e != nil {
				return e
			}
		}
	}
	return nil
}

// namesModel reports whether a tool names a model through one field.
func namesModel(place map[string]json.RawMessage, name string) bool {
	v, ok := place[name]
	return ok && !emptyModelValue(v)
}

// judgeModelValue applies the candidate fence to a field the vendor spells both
// ways, one model or a list. It reports the first refused name, or that the
// value could not be read at all — which the caller refuses too, since a value
// this build cannot read is one it cannot fence.
func judgeModelValue(doc *snapshot.Document, modelLookup func(string) *snapshot.Model,
	key *snapshot.Key, v json.RawMessage) (bad string, refused, unreadable bool) {
	// null is the field left out, not a model named "". Whether leaving it out
	// is acceptable is the caller's question (see toolModelsRequiredOnFencedKey),
	// and answering it here would refuse a key that carries no list at all.
	if string(bytes.TrimSpace(v)) == "null" {
		return "", false, false
	}
	var one string
	if json.Unmarshal(v, &one) == nil {
		if !allowsCandidateModel(doc, modelLookup, key, one) {
			return one, true, false
		}
		return "", false, false
	}
	var many []string
	if json.Unmarshal(v, &many) != nil {
		return "", false, true
	}
	for _, candidate := range many {
		if !allowsCandidateModel(doc, modelLookup, key, candidate) {
			return candidate, true, false
		}
	}
	return "", false, false
}

// The `plugins` field is the seventh channel. Its entries configure work the
// vendor does around the completion, and two kinds choose models:
//
//   - The routers (snapshot.RouterPluginID) steer which model a router picks.
//     They are refused on a fenced key, judged by nothing: their candidates are
//     wildcard patterns or a score, which the fence cannot judge, and the
//     vendor documents each as read only by its own router model, which a
//     fenced key cannot call anyway. Refusing costs such a key nothing.
//   - fusion names its analyst and its panel outright, and configures a fusion
//     run however that run was started — including by the server tool, which a
//     fenced key can call. Its names are judged like a tool's. Its `preset`
//     expands into a panel on the vendor's side, so on a fenced key it is
//     refused; it is not the account preset refused for every key elsewhere.
//
// context-compression is deliberately not fenced. Its documentation says it
// may move a request to a model with a longer context but not from which set,
// and it switches itself on for small-context endpoints without the field, so
// refusing the entry would not close it. It waits on a live call.
//
// A plugin this build does not know is forwarded, for the same reason and on
// the same reversal condition as an unknown server tool (see above).
const pluginsField = "plugins"

const fusionPluginID = "fusion"

// emptyModelValue reports a model field that names nothing: null, "", or [].
func emptyModelValue(v json.RawMessage) bool {
	var one string
	if json.Unmarshal(v, &one) == nil {
		return one == ""
	}
	var many []json.RawMessage
	return json.Unmarshal(v, &many) == nil && len(many) == 0
}

// fusionPluginPreset is the fusion plugin's own panel selector.
const fusionPluginPreset = "preset"

// fencePlugins judges the models plugin entries name and refuses router
// configuration on a fenced key. It returns the refusal to send, or nil.
func (s *Server) fencePlugins(doc *snapshot.Document, modelLookup func(string) *snapshot.Model,
	key *snapshot.Key, params map[string]json.RawMessage) *apiError {
	raw, present := params[pluginsField]
	if !present || string(bytes.TrimSpace(raw)) == "null" {
		return nil
	}
	var plugins []map[string]json.RawMessage
	if json.Unmarshal(raw, &plugins) != nil {
		e := errInvalidParamValue(pluginsField)
		return &e
	}
	for _, plugin := range plugins {
		var id string
		if json.Unmarshal(plugin["id"], &id) != nil {
			// A missing id decodes from nil and fails here too: an entry the
			// vendor will dispatch somewhere is one this has to be able to name.
			// A null id decodes to "" without error and is forwarded like any
			// unknown plugin; it names nothing the vendor can dispatch.
			e := errInvalidParamValue(pluginsField)
			return &e
		}
		if router := snapshot.RouterPluginID(id); router != "" {
			if key.HasCreditFence() {
				e := errRouterPluginNotAllowed(router)
				return &e
			}
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(id), fusionPluginID) {
			continue
		}
		for _, name := range toolModelParams {
			v, ok := plugin[name]
			if !ok {
				continue
			}
			bad, refused, unreadable := judgeModelValue(doc, modelLookup, key, v)
			if unreadable {
				e := errInvalidParamValue(pluginsField)
				return &e
			}
			if refused {
				e := errPluginModelNotAllowed(bad)
				return &e
			}
			if e := onlineNeedsWebSearch(key, v); e != nil {
				return e
			}
		}
		if key.HasCreditFence() {
			// The preset expands on the vendor's side, and an empty panel may
			// fall back to the vendor's default one; neither is judged.
			if v, ok := plugin[fusionPluginPreset]; ok && string(bytes.TrimSpace(v)) != "null" {
				e := errFusionPluginNeedsModels
				return &e
			}
			if v, ok := plugin["analysis_models"]; ok && string(bytes.TrimSpace(v)) != "null" && emptyModelValue(v) {
				e := errFusionPluginNeedsModels
				return &e
			}
		}
		if nested, ok := plugin[nestedToolsParam]; ok && string(bytes.TrimSpace(nested)) != "null" {
			if e := fenceToolList(doc, modelLookup, key, nested, 1); e != nil {
				return e
			}
		}
	}
	return nil
}

// The fences below adapt the checks above to the pipeline's fence list, each
// with the refusal and the usage-record name the chat handler always gave it.

// fencePresetField refuses a preset. A preset carries its own model and
// fallback list, so it reaches past every check here from outside the fields
// they read. Refused rather than judged: presets live on the platform's vendor
// account, which every student key is issued under, so one created there is
// reachable by all of them.
func fencePresetField(_ *Server, _ *snapshot.Document, _ func(string) *snapshot.Model,
	_ *snapshot.Key, _ *snapshot.Model, params map[string]json.RawMessage) *refusal {
	if _, present := params[snapshot.PresetField]; present {
		return &refusal{e: errPresetNotAllowed, errorType: "preset_not_allowed"}
	}
	return nil
}

// fenceCandidates applies the model fence to the fallback candidates. Without
// it the field is a way around everything before it: the vendor may serve and
// bill any entry, while the fence, the spool and the student's own screen all
// keep showing the name that was judged.
func fenceCandidates(_ *Server, doc *snapshot.Document, lookup func(string) *snapshot.Model,
	key *snapshot.Key, _ *snapshot.Model, params map[string]json.RawMessage) *refusal {
	if bad, ok := fenceCandidateModels(doc, lookup, key, params); !ok {
		return &refusal{e: errCandidateModelNotAllowed(bad), errorType: "credit_model_not_allowed"}
	}
	return nil
}

// fenceServerToolsStep covers the models the vendor's own server tools name,
// which reach past the model fence from inside `tools`.
func fenceServerToolsStep(s *Server, doc *snapshot.Document, lookup func(string) *snapshot.Model,
	key *snapshot.Key, _ *snapshot.Model, params map[string]json.RawMessage) *refusal {
	if e := s.fenceServerTools(doc, lookup, key, params); e != nil {
		return &refusal{e: *e, errorType: toolRefusalType(*e)}
	}
	return nil
}

// fencePluginsStep covers the models `plugins` entries name or steer.
func fencePluginsStep(s *Server, doc *snapshot.Document, lookup func(string) *snapshot.Model,
	key *snapshot.Key, _ *snapshot.Model, params map[string]json.RawMessage) *refusal {
	if e := s.fencePlugins(doc, lookup, key, params); e != nil {
		return &refusal{e: *e, errorType: toolRefusalType(*e)}
	}
	return nil
}

// The capabilities a chat body reaches without a route of its own. Each is
// judged by the fields that ask for that feature and nothing else, so a
// request that merely could have used it is never refused for it; and a
// refusal is a refusal, never a quiet rewrite of the request into one the key
// may send. The server tools among them are judged in fenceToolList, where
// nesting is already followed.
//
// What is deliberately not judged, so it does not read as a gap: the
// OpenAI-style web_search_options field (whether the vendor acts on it is
// unconfirmed), a model that searches on its own without being asked, and a
// PDF sent with no engine to a model that cannot read PDFs, which the vendor
// answers with its paid engine. The last needs to know which models read PDFs
// themselves, and this document does not carry that yet.

// onlineSuffix asks the vendor to add web results to a model's answer.
const onlineSuffix = ":online"

// webPluginID is the vendor's web search plugin.
const webPluginID = "web"

// fileParserPluginID configures how files in the messages are read, and
// paidPdfEngine is the engine that is billed per page.
const (
	fileParserPluginID = "file-parser"
	paidPdfEngine      = "mistral-ocr"
)

// modalitiesField names the kinds of output asked for; "image" there makes a
// chat completion an image generation.
const modalitiesField = "modalities"

func fenceChatCapabilities(_ *Server, _ *snapshot.Document, _ func(string) *snapshot.Model,
	key *snapshot.Key, model *snapshot.Model, params map[string]json.RawMessage) *refusal {
	needs := func(capability string) *refusal {
		if key.AllowsEndpoint(capability) {
			return nil
		}
		return &refusal{e: errEndpointNotAllowed(endpointLabel(capability)), errorType: "endpoint_not_allowed"}
	}
	// :online on the model or on any fallback candidate: either may be the
	// model that answers, and the search is billed whichever does.
	online := isOnlineName(model.PublicName)
	if raw, ok := params[candidateModelsField]; ok && string(bytes.TrimSpace(raw)) != "null" {
		var names []string
		// Already parsed whole by the candidate fence, so this cannot fail.
		_ = json.Unmarshal(raw, &names)
		for _, name := range names {
			online = online || isOnlineName(name)
		}
	}
	if online {
		if rf := needs(snapshot.EndpointWebSearch); rf != nil {
			return rf
		}
	}
	if raw, ok := params[pluginsField]; ok && string(bytes.TrimSpace(raw)) != "null" {
		var plugins []map[string]json.RawMessage
		// Already parsed by the plugins fence, which refuses what it cannot read.
		_ = json.Unmarshal(raw, &plugins)
		for _, plugin := range plugins {
			var id string
			_ = json.Unmarshal(plugin["id"], &id)
			id = strings.ToLower(strings.TrimSpace(id))
			switch id {
			case webPluginID:
				if rf := needs(snapshot.EndpointWebSearch); rf != nil {
					return rf
				}
			case fileParserPluginID:
				paid, readable := asksPaidPdfEngine(plugin)
				if !readable {
					return &refusal{e: errInvalidParamValue(pluginsField)}
				}
				if paid {
					if rf := needs(snapshot.EndpointPdfOcr); rf != nil {
						return rf
					}
				}
			}
		}
	}
	if raw, ok := params[modalitiesField]; ok && string(bytes.TrimSpace(raw)) != "null" {
		var kinds []string
		if json.Unmarshal(raw, &kinds) != nil {
			// A list this build cannot read is one it cannot judge.
			return &refusal{e: errInvalidParamValue(modalitiesField)}
		}
		for _, kind := range kinds {
			if strings.EqualFold(strings.TrimSpace(kind), "image") {
				if rf := needs(snapshot.EndpointImages); rf != nil {
					return rf
				}
			}
		}
	}
	return nil
}

// onlineNeedsWebSearch applies the web search grant to a model a tool or a
// plugin names. A model that answers inside a tool searches the web on this
// key's money just as the request's own model would. The value was already
// read by judgeModelValue, so a shape it accepted decodes here.
func onlineNeedsWebSearch(key *snapshot.Key, v json.RawMessage) *apiError {
	if key.AllowsEndpoint(snapshot.EndpointWebSearch) {
		return nil
	}
	var names []string
	var one string
	if json.Unmarshal(v, &one) == nil {
		names = []string{one}
	} else {
		_ = json.Unmarshal(v, &names)
	}
	for _, name := range names {
		if isOnlineName(name) {
			e := errEndpointNotAllowed(endpointLabel(snapshot.EndpointWebSearch))
			return &e
		}
	}
	return nil
}

func isOnlineName(name string) bool {
	return strings.HasSuffix(strings.ToLower(strings.TrimSpace(name)), onlineSuffix)
}

// asksPaidPdfEngine reports whether a file-parser plugin entry names the paid
// PDF engine. readable is false when the pdf settings are there but cannot be
// read, which the caller refuses: an engine it cannot read is one it cannot
// judge.
func asksPaidPdfEngine(plugin map[string]json.RawMessage) (paid, readable bool) {
	raw, ok := plugin["pdf"]
	if !ok || string(bytes.TrimSpace(raw)) == "null" {
		return false, true
	}
	var pdf map[string]json.RawMessage
	if json.Unmarshal(raw, &pdf) != nil {
		return false, false
	}
	engineRaw, ok := pdf["engine"]
	if !ok || string(bytes.TrimSpace(engineRaw)) == "null" {
		return false, true
	}
	var engine string
	if json.Unmarshal(engineRaw, &engine) != nil {
		return false, false
	}
	return strings.EqualFold(strings.TrimSpace(engine), paidPdfEngine), true
}

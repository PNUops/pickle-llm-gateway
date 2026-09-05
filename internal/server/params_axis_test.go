package server

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/pnuops/pickle-llm-gateway/internal/snapshot"
)

// The parameter surface is decided by the model's budget axis, and the two
// axes are deliberately not symmetric. The self-hosted side has an allowlist
// that enforces a service decision; the money side has none at all, because
// there the provider defines the fields and the student's own budget pays for
// them.
//
// The self-hosted half is the one that rots dangerously. Thinking is off there
// by default with no per-request opt-in, and this allowlist is the only thing
// enforcing it — a field that slips in turns it on silently and bills the
// shared token allowance for reasoning nobody asked for.

const (
	reasoningBody = `{"model":"vendor-model","messages":[{"role":"user","content":"hi"}],"reasoning_effort":"low"}`
	verbosityBody = `{"model":"vendor-model","messages":[{"role":"user","content":"hi"}],"verbosity":"low"}`
)

func TestCreditOnlyParamsReachThePaidUpstream(t *testing.T) {
	h := newHarness(t, creditDoc, nil)

	status, body := h.chat(t, testToken, reasoningBody)
	if status != 200 {
		t.Fatalf("reasoning_effort on a paid model: got %d %s", status, body)
	}
	params, _ := h.mock.last()
	// Forwarded verbatim: the provider defines the field, so the gateway has
	// no business normalising a value it does not interpret.
	if got := string(params["reasoning_effort"]); got != `"low"` {
		t.Fatalf("reasoning_effort reached the upstream as %q, want %q", got, `"low"`)
	}

	if status, body = h.chat(t, testToken, verbosityBody); status != 200 {
		t.Fatalf("verbosity on a paid model: got %d %s", status, body)
	}
	params, _ = h.mock.last()
	if got := string(params["verbosity"]); got != `"low"` {
		t.Fatalf("verbosity reached the upstream as %q, want %q", got, `"low"`)
	}
}

func TestCreditOnlyParamsRefusedOnSelfHostedModel(t *testing.T) {
	for _, tc := range []struct{ name, field, body string }{
		{"reasoning_effort", "reasoning_effort", `{"model":"pickle-general","messages":[{"role":"user","content":"hi"}],"reasoning_effort":"low"}`},
		{"verbosity", "verbosity", `{"model":"pickle-general","messages":[{"role":"user","content":"hi"}],"verbosity":"low"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, creditDoc, nil)
			status, body := h.chat(t, testToken, tc.body)
			if status != 400 || errCode(t, body) != "unsupported_parameter" {
				t.Fatalf("self-hosted model carrying %s: got %d %s", tc.field, status, body)
			}
			// The advice has to name the way out, or the caller is left to
			// guess that the field is fine somewhere else.
			if msg := errMessage(t, body); !strings.Contains(msg, tc.field) || !strings.Contains(msg, "유료 모델") {
				t.Fatalf("message does not point at the paid axis: %q", msg)
			}
			// Refused before the request costs anything.
			if h.mock.callCount() != 0 {
				t.Fatalf("the upstream was called %d times for a refused request", h.mock.callCount())
			}
			evs := h.spoolEvents(t)
			if len(evs) != 1 || evs[0].ErrorType != "credit_only_parameter" {
				t.Fatalf("spooled %+v, want one credit_only_parameter event", evs)
			}
		})
	}
}

// Thinking on the self-hosted side is a server default, not a per-request
// option, and these are the fields that would override it. Nothing about
// opening the money axis may reach them: a field that quietly switches
// thinking on is the most expensive failure this surface has, because the
// completion tokens land on the shared allowance and the request still looks
// ordinary.
func TestThinkingFieldsRefusedOnSelfHostedModel(t *testing.T) {
	for _, field := range []string{
		`"chat_template_kwargs":{"enable_thinking":true}`,
		`"thinking_token_budget":100`,
		`"reasoning_effort":"high"`,
	} {
		h := newHarness(t, creditDoc, nil)
		body := `{"model":"pickle-general","messages":[{"role":"user","content":"hi"}],` + field + `}`
		status, resp := h.chat(t, testToken, body)
		if status != 400 || errCode(t, resp) != "unsupported_parameter" {
			t.Fatalf("self-hosted model carrying %s: got %d %s", field, status, resp)
		}
		if h.mock.callCount() != 0 {
			t.Fatalf("self-hosted model carrying %s reached the upstream", field)
		}
	}
}

// The money axis carries no allowlist, so the provider's own fields reach it
// verbatim — routing, plugins, caching, sampling extensions and anything the
// vendor adds later without this gateway being changed. Refusing them was our
// convenience rather than a control: the credential's own limit is what bounds
// this axis, and every field turned away here was one the vendor would have
// served on a request the student's budget pays for.
func TestProviderFieldsReachThePaidUpstream(t *testing.T) {
	for _, tc := range []struct{ field, key, want string }{
		{`"provider":{"order":["openai"]}`, "provider", `{"order":["openai"]}`},
		{`"route":"fallback"`, "route", `"fallback"`},
		{`"plugins":[{"id":"web"}]`, "plugins", `[{"id":"web"}]`},
		{`"transforms":["middle-out"]`, "transforms", `["middle-out"]`},
		{`"web_search_options":{"search_context_size":"high"}`, "web_search_options", `{"search_context_size":"high"}`},
		{`"reasoning":{"effort":"high"}`, "reasoning", `{"effort":"high"}`},
		{`"prediction":{"type":"content"}`, "prediction", `{"type":"content"}`},
		{`"chat_template_kwargs":{"enable_thinking":true}`, "chat_template_kwargs", `{"enable_thinking":true}`},
		// A field this build has never heard of travels too. That is the point
		// of removing the list rather than lengthening it.
		{`"some_field_added_next_month":42`, "some_field_added_next_month", `42`},
	} {
		h := newHarness(t, creditDoc, nil)
		body := `{"model":"vendor-model","messages":[{"role":"user","content":"hi"}],` + tc.field + `}`
		status, resp := h.chat(t, testToken, body)
		if status != 200 {
			t.Fatalf("paid model carrying %s: got %d %s", tc.field, status, resp)
		}
		params, _ := h.mock.last()
		if got := string(params[tc.key]); got != tc.want {
			t.Fatalf("%s reached the upstream as %q, want %q", tc.key, got, tc.want)
		}
	}
}

// Opening the axis must not open the four things that are not the allowlist's
// to decide. Each is what it is for a reason the list never carried.
func TestPaidAxisKeepsItsNonAllowlistRules(t *testing.T) {
	h := newHarness(t, creditDoc, nil)

	// The public model name is translated to the upstream's own, so a caller
	// sending arbitrary fields still cannot learn which server answers.
	if status, resp := h.chat(t, testToken, creditChatBody); status != 200 {
		t.Fatalf("paid chat: %d %s", status, resp)
	}
	params, _ := h.mock.last()
	if got := string(params["model"]); got != `"`+upstreamModel+`"` {
		t.Fatalf("model reached the upstream as %s", got)
	}

	// Usage is forced on even when the caller sent stream_options themselves
	// and asked for the opposite. Metering depends on it.
	body := `{"model":"vendor-model","messages":[{"role":"user","content":"hi"}],` +
		`"stream":true,"stream_options":{"include_usage":false}}`
	if status, resp := h.chat(t, testToken, body); status != 200 {
		t.Fatalf("paid stream: %d %s", status, resp)
	}
	params, _ = h.mock.last()
	var opts struct {
		IncludeUsage bool `json:"include_usage"`
	}
	if err := json.Unmarshal(params["stream_options"], &opts); err != nil {
		t.Fatal(err)
	}
	if !opts.IncludeUsage {
		t.Fatalf("include_usage was not forced on: %s", params["stream_options"])
	}

	// A reserved self-serve prefix is still never a passthrough model, whatever
	// else the body carries.
	status, resp := h.chat(t, testToken,
		`{"model":"pickle-nosuch","messages":[],"provider":{"order":["openai"]}}`)
	if status != 404 || errCode(t, resp) != "model_not_found" {
		t.Fatalf("reserved prefix: %d %s", status, resp)
	}
}

// Moving the parameter check behind model resolution changed exactly one
// answer: a request that is wrong in both ways now hears about the model
// first. Nothing is disclosed and nothing is spent, but the order is a fact
// callers can depend on, so it is pinned rather than left to drift.
func TestUnknownModelAnswersBeforeUnknownParameter(t *testing.T) {
	h := newHarness(t, creditDoc, nil)
	body := `{"model":"pickle-nosuch","messages":[{"role":"user","content":"hi"}],"nonsense_field":1}`
	status, resp := h.chat(t, testToken, body)
	if status != 404 || errCode(t, resp) != "model_not_found" {
		t.Fatalf("unknown model with an unknown field: got %d %s", status, resp)
	}
	if h.mock.callCount() != 0 {
		t.Fatalf("the upstream was called for an unknown model")
	}
}

// creditPassthroughDoc is creditDoc with a passthrough upstream, so a vendor
// model name that no catalogue lists still resolves — which is what a fallback
// candidate normally is.
func creditPassthroughDoc(d *snapshot.Document) {
	creditDoc(d)
	d.PassthroughRef = "mock"
}

// models[] names models the vendor may serve and bill INSTEAD of the one in
// `model`, so the money fence has to judge every entry. Judging rather than
// refusing keeps fallback working between models the key may already use.
func TestCandidateModelsAreFenced(t *testing.T) {
	// Allowed candidates travel, and reach the upstream untouched.
	h := newHarness(t, creditPassthroughDoc, nil)
	body := `{"model":"vendor-model","messages":[{"role":"user","content":"hi"}],` +
		`"models":["openai/gpt-5","openai/gpt-5-mini"]}`
	if status, resp := h.chat(t, testToken, body); status != 200 {
		t.Fatalf("allowed candidates: %d %s", status, resp)
	}
	params, _ := h.mock.last()
	if got := string(params["models"]); got != `["openai/gpt-5","openai/gpt-5-mini"]` {
		t.Fatalf("candidate list reached the upstream as %s", got)
	}
}

// The bypass this fence closes: a key whose deny list forbids a model puts it
// in models[] instead, the vendor falls back to it on any error, and prices the
// request using the model it actually used. Every record here would still show
// the name that was judged.
func TestDeniedCandidateModelIsRefused(t *testing.T) {
	h := newHarness(t, func(d *snapshot.Document) {
		creditPassthroughDoc(d)
		d.Keys[0].CreditDeniedModels = []string{"openai/*-pro"}
	}, nil)
	body := `{"model":"vendor-model","messages":[{"role":"user","content":"hi"}],` +
		`"models":["openai/gpt-5","openai/o1-pro"]}`
	status, resp := h.chat(t, testToken, body)
	if status != http.StatusForbidden || errCode(t, resp) != "model_not_allowed" {
		t.Fatalf("denied candidate: %d %s", status, resp)
	}
	// The caller sent a list and cannot otherwise tell which entry is the
	// problem.
	if msg := errMessage(t, resp); !strings.Contains(msg, "openai/o1-pro") {
		t.Fatalf("the refusal has to name the entry: %s", msg)
	}
	if h.mock.callCount() != 0 {
		t.Fatalf("a fenced candidate reached the upstream")
	}
	evs := h.spoolEvents(t)
	if len(evs) != 1 || evs[0].ErrorType != "credit_model_not_allowed" {
		t.Fatalf("spooled %+v", evs)
	}
}

// An allow list bounds candidates the same way it bounds the primary.
func TestCandidateOutsideAllowListIsRefused(t *testing.T) {
	h := newHarness(t, func(d *snapshot.Document) {
		creditPassthroughDoc(d)
		d.Keys[0].CreditAllowedModels = []string{"openai/*"}
	}, nil)
	body := `{"model":"vendor-model","messages":[{"role":"user","content":"hi"}],` +
		`"models":["openai/gpt-5","google/gemini-3-pro"]}`
	status, resp := h.chat(t, testToken, body)
	if status != http.StatusForbidden || errCode(t, resp) != "model_not_allowed" {
		t.Fatalf("%d %s", status, resp)
	}
}

// A reserved self-serve name must not reach the vendor as a fallback, and the
// catalogue lookup would otherwise accept one: pickle-general is a real row.
func TestReservedNameIsNeverACandidate(t *testing.T) {
	h := newHarness(t, creditPassthroughDoc, nil)
	for _, list := range []string{`["pickle-general"]`, `["pickle-nosuch"]`, `["~pickle-general"]`} {
		body := `{"model":"vendor-model","messages":[{"role":"user","content":"hi"}],"models":` + list + `}`
		status, resp := h.chat(t, testToken, body)
		if status != http.StatusForbidden || errCode(t, resp) != "model_not_allowed" {
			t.Fatalf("%s: %d %s", list, status, resp)
		}
		if h.mock.callCount() != 0 {
			t.Fatalf("%s reached the upstream", list)
		}
	}
}

// A list this build cannot read is a list it cannot fence.
func TestUnreadableCandidateListIsRefused(t *testing.T) {
	h := newHarness(t, creditPassthroughDoc, nil)
	for _, list := range []string{`"openai/gpt-5"`, `[1,2]`, `{"a":1}`} {
		body := `{"model":"vendor-model","messages":[{"role":"user","content":"hi"}],"models":` + list + `}`
		status, resp := h.chat(t, testToken, body)
		if status != http.StatusForbidden || errCode(t, resp) != "model_not_allowed" {
			t.Fatalf("%s: %d %s", list, status, resp)
		}
	}
	// null and absent both mean the caller sent no candidates.
	body := `{"model":"vendor-model","messages":[{"role":"user","content":"hi"}],"models":null}`
	if status, resp := h.chat(t, testToken, body); status != 200 {
		t.Fatalf("null candidates: %d %s", status, resp)
	}
}

// The candidate fence is a money-axis rule and needs to be nowhere else: a
// self-hosted request never forwards `models` at all, because the field is not
// on that axis's allowlist. Pinned so that widening the allowlist later cannot
// quietly open a fallback path with no fence behind it.
func TestSelfHostedRefusesCandidateModels(t *testing.T) {
	h := newHarness(t, creditPassthroughDoc, nil)
	body := `{"model":"pickle-general","messages":[{"role":"user","content":"hi"}],` +
		`"models":["openai/gpt-5"]}`
	status, resp := h.chat(t, testToken, body)
	if status != 400 || errCode(t, resp) != "unsupported_parameter" {
		t.Fatalf("%d %s", status, resp)
	}
	if h.mock.callCount() != 0 {
		t.Fatal("reached the upstream")
	}
}

// A router name is not a model: the vendor resolves it at request time and
// bills whatever it picked, so no list here can bound it. A key that carries a
// fence has had a decision made about which models it may reach, and this name
// walks around that decision.
//
// The reachable shape is the one the plan calls the representative use — allow
// open, deny the expensive names — because an empty allow list admits the
// router and the deny list never matches it.
func TestRouterModelRefusedOnFencedKey(t *testing.T) {
	for _, tc := range []struct {
		name  string
		fence func(*snapshot.Key)
	}{
		{"deny list only", func(k *snapshot.Key) { k.CreditDeniedModels = []string{"openai/*-pro"} }},
		{"allow list only", func(k *snapshot.Key) { k.CreditAllowedModels = []string{"openrouter/*"} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, func(d *snapshot.Document) {
				creditPassthroughDoc(d)
				tc.fence(&d.Keys[0])
			}, nil)
			body := `{"model":"openrouter/auto","messages":[{"role":"user","content":"hi"}]}`
			status, resp := h.chat(t, testToken, body)
			if status != http.StatusForbidden || errCode(t, resp) != "model_not_allowed" {
				t.Fatalf("%d %s", status, resp)
			}
			// The advice differs from the ordinary fence refusal: the remedy is
			// to name a model, not to go and read a list.
			if msg := errMessage(t, resp); !strings.Contains(msg, "직접 지정") {
				t.Fatalf("message does not say what to do: %s", msg)
			}
			if h.mock.callCount() != 0 {
				t.Fatal("a router name reached the upstream on a fenced key")
			}
			evs := h.spoolEvents(t)
			if len(evs) != 1 || evs[0].ErrorType != "router_model_not_allowed" {
				t.Fatalf("spooled %+v", evs)
			}
		})
	}
}

// A key with no fence at all is bounded only by the money it was granted, so
// the router takes nothing away from it and stays available.
func TestRouterModelAllowedOnUnfencedKey(t *testing.T) {
	h := newHarness(t, creditPassthroughDoc, nil)
	body := `{"model":"openrouter/auto","messages":[{"role":"user","content":"hi"}]}`
	if status, resp := h.chat(t, testToken, body); status != 200 {
		t.Fatalf("%d %s", status, resp)
	}
}

// The router cannot ride in as a fallback candidate either, on a fenced key.
func TestRouterModelRefusedAsCandidate(t *testing.T) {
	h := newHarness(t, func(d *snapshot.Document) {
		creditPassthroughDoc(d)
		d.Keys[0].CreditDeniedModels = []string{"openai/*-pro"}
	}, nil)
	body := `{"model":"vendor-model","messages":[{"role":"user","content":"hi"}],` +
		`"models":["openrouter/auto"]}`
	status, resp := h.chat(t, testToken, body)
	if status != http.StatusForbidden || errCode(t, resp) != "model_not_allowed" {
		t.Fatalf("%d %s", status, resp)
	}
	if h.mock.callCount() != 0 {
		t.Fatal("a router candidate reached the upstream")
	}
}

// The tilde form the vendor uses for floating aliases must not walk past it.
func TestRouterNameVariantsAreCaught(t *testing.T) {
	for _, name := range []string{"openrouter/auto", "OpenRouter/Auto", "~openrouter/auto"} {
		if !snapshot.IsRouterModelName(name) {
			t.Fatalf("%q was not recognised as a router name", name)
		}
	}
	// The whole vendor namespace is a router namespace, so every one of these
	// is caught without being named. A list held one of them and missed four.
	for _, name := range []string{
		"openrouter/auto-beta", "openrouter/free", "openrouter/fusion",
		"openrouter/pareto-code", "openrouter/bodybuilder",
		"openrouter/whatever-ships-next",
		// Normalization: a prefix test is defeated by one leading byte.
		" openrouter/auto", "~openrouter/auto", "openrouter/auto:nitro",
	} {
		if !snapshot.IsRouterModelName(name) {
			t.Fatalf("%q was not recognised as a router name", name)
		}
	}
	for _, name := range []string{"openai/gpt-5", "openrouterx/auto", ""} {
		if snapshot.IsRouterModelName(name) {
			t.Fatalf("%q was wrongly treated as a router name", name)
		}
	}
}

// A preset stands in for the model and its fallback list, so it reaches past
// the fence from outside the fields the fence reads. All three shapes the
// vendor accepts are refused, for every key: presets live on the platform's own
// vendor account, which every student key is issued under.
func TestPresetIsRefusedInEveryShape(t *testing.T) {
	for _, tc := range []struct{ name, body string }{
		{"top-level field", `{"model":"vendor-model","messages":[],"preset":"my-slug"}`},
		{"model position", `{"model":"@preset/my-slug","messages":[]}`},
		{"appended to a model", `{"model":"openai/gpt-5@preset/my-slug","messages":[]}`},
		{"in a candidate", `{"model":"vendor-model","messages":[],"models":["openai/gpt-5@preset/s"]}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, creditPassthroughDoc, nil)
			status, resp := h.chat(t, testToken, tc.body)
			if status == 200 {
				t.Fatalf("a preset was served: %s", resp)
			}
			if h.mock.callCount() != 0 {
				t.Fatalf("a preset reached the upstream")
			}
		})
	}
	// An unfenced key is refused too — the reachability is the point, not the
	// money.
	h := newHarness(t, creditPassthroughDoc, nil)
	status, resp := h.chat(t, testToken, `{"model":"vendor-model","messages":[],"preset":"s"}`)
	if status != 400 || errCode(t, resp) != "preset_not_supported" {
		t.Fatalf("%d %s", status, resp)
	}
	if msg := errMessage(t, resp); !strings.Contains(msg, "직접 지정") {
		t.Fatalf("message does not say what to do: %s", msg)
	}
}

// A deny list has to match wider than an allow list, because it only ever takes
// away. Reading the tilde strictly on the deny side left a key that denied a
// vendor still able to reach its newest model through a floating alias.
func TestDenyListCatchesFloatingAliases(t *testing.T) {
	h := newHarness(t, func(d *snapshot.Document) {
		creditPassthroughDoc(d)
		d.Keys[0].CreditDeniedModels = []string{"anthropic/*"}
	}, nil)
	for _, name := range []string{"anthropic/claude-opus-4", "~anthropic/claude-opus-latest"} {
		body := `{"model":"` + name + `","messages":[{"role":"user","content":"hi"}]}`
		status, resp := h.chat(t, testToken, body)
		if status != http.StatusForbidden || errCode(t, resp) != "model_not_allowed" {
			t.Fatalf("%s: %d %s", name, status, resp)
		}
	}
	// The allow list keeps the strict reading: opening a vendor does not open
	// its moving targets.
	h2 := newHarness(t, func(d *snapshot.Document) {
		creditPassthroughDoc(d)
		d.Keys[0].CreditAllowedModels = []string{"anthropic/*"}
	}, nil)
	status, resp := h2.chat(t, testToken,
		`{"model":"~anthropic/claude-opus-latest","messages":[{"role":"user","content":"hi"}]}`)
	if status != http.StatusForbidden {
		t.Fatalf("an allow list must not pick up a floating alias: %d %s", status, resp)
	}
}

// A variant suffix is the same model at another rate, so an exact deny entry
// has to reach it. One function was keeping two rules: the leading-star branch
// stripped variants and the exact branch did not.
func TestExactDenyCatchesVariantSuffixes(t *testing.T) {
	h := newHarness(t, func(d *snapshot.Document) {
		creditPassthroughDoc(d)
		d.Keys[0].CreditDeniedModels = []string{"openai/gpt-5-pro"}
	}, nil)
	for _, name := range []string{"openai/gpt-5-pro", "openai/gpt-5-pro:nitro", "openai/gpt-5-pro:free"} {
		body := `{"model":"` + name + `","messages":[{"role":"user","content":"hi"}]}`
		status, resp := h.chat(t, testToken, body)
		if status != http.StatusForbidden || errCode(t, resp) != "model_not_allowed" {
			t.Fatalf("%s: %d %s", name, status, resp)
		}
	}
	// A different model that merely shares the prefix is untouched.
	if status, resp := h.chat(t, testToken,
		`{"model":"openai/gpt-5-pro-max","messages":[{"role":"user","content":"hi"}]}`); status != 200 {
		t.Fatalf("a neighbouring model was caught: %d %s", status, resp)
	}
}

// A candidate that resolves to a self-hosted row would sail through the money
// fence, because AllowsCreditModel answers true for anything off that axis.
func TestCandidateMustBeOnTheMoneyAxis(t *testing.T) {
	h := newHarness(t, creditPassthroughDoc, nil)
	// pickle-general is a real catalogue row on the TOKEN axis.
	body := `{"model":"vendor-model","messages":[],"models":["pickle-general"]}`
	status, resp := h.chat(t, testToken, body)
	if status != http.StatusForbidden || errCode(t, resp) != "model_not_allowed" {
		t.Fatalf("%d %s", status, resp)
	}
	// An empty candidate is refused rather than ignored; [null] decodes to it.
	for _, list := range []string{`[""]`, `[null]`, `["openai/gpt-5",""]`} {
		body := `{"model":"vendor-model","messages":[],"models":` + list + `}`
		status, resp := h.chat(t, testToken, body)
		if status != http.StatusForbidden {
			t.Fatalf("%s: %d %s", list, status, resp)
		}
	}
}

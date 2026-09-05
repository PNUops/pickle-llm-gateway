package server

import (
	"encoding/json"
	"strings"
	"testing"
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
		{`"models":["a","b"]`, "models", `["a","b"]`},
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

package server

import (
	"strings"
	"testing"
)

// The parameter surface is decided by the model's budget axis, so these tests
// pin both halves: what a paid model may carry that a self-hosted one may not,
// and what neither may carry. The second half is the one that rots quietly —
// a field opened for the paid side reaches the commercial provider verbatim,
// and the fields below are the ones that would spend money or change routing
// if they ever slipped in.

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
// option, and these are the fields that would override it. They stay refused
// on both axes: opening them is a service decision that has not been made.
func TestThinkingFieldsRefusedOnBothAxes(t *testing.T) {
	for _, model := range []string{"pickle-general", "vendor-model"} {
		for _, field := range []string{
			`"chat_template_kwargs":{"enable_thinking":true}`,
			`"thinking_token_budget":100`,
		} {
			h := newHarness(t, creditDoc, nil)
			body := `{"model":"` + model + `","messages":[{"role":"user","content":"hi"}],` + field + `}`
			status, resp := h.chat(t, testToken, body)
			if status != 400 || errCode(t, resp) != "unsupported_parameter" {
				t.Fatalf("%s carrying %s: got %d %s", model, field, status, resp)
			}
			if h.mock.callCount() != 0 {
				t.Fatalf("%s carrying %s reached the upstream", model, field)
			}
		}
	}
}

// Routing and billing fields the commercial provider accepts. Passthrough
// forwards a body largely as it arrives, so admitting these would leave the
// money axis with no control of ours at all — the credential's own limit would
// be the whole of it. They are refused on the paid axis on purpose.
func TestProviderRoutingFieldsStayRefusedOnPaidModel(t *testing.T) {
	for _, field := range []string{
		`"models":["a","b"]`,
		`"provider":{"order":["openai"]}`,
		`"route":"fallback"`,
		`"plugins":[{"id":"web"}]`,
		`"transforms":["middle-out"]`,
		`"web_search_options":{"search_context_size":"high"}`,
	} {
		h := newHarness(t, creditDoc, nil)
		body := `{"model":"vendor-model","messages":[{"role":"user","content":"hi"}],` + field + `}`
		status, resp := h.chat(t, testToken, body)
		if status != 400 || errCode(t, resp) != "unsupported_parameter" {
			t.Fatalf("paid model carrying %s: got %d %s", field, status, resp)
		}
		if h.mock.callCount() != 0 {
			t.Fatalf("paid model carrying %s reached the upstream", field)
		}
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

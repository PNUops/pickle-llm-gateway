package server

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/pnuops/pickle-llm-gateway/internal/config"
	"github.com/pnuops/pickle-llm-gateway/internal/snapshot"
)

// Reasoning on a self-hosted model is opened per upstream, because the capacity
// it spends is the upstream's. These tests pin the three settings and the one
// place the request is admitted (the primary upstream) against the place it is
// rewritten (each attempt).

func reasoningOn(setting string) func(*config.Config) {
	return func(c *config.Config) {
		up := c.Upstreams["mock"]
		up.Reasoning = setting
		c.Upstreams["mock"] = up
	}
}

func selfHostedBody(extra string) string {
	return `{"model":"pickle-general","messages":[{"role":"user","content":"hi"}]` + extra + `}`
}

func TestReasoningEffortForwardedByEffortUpstream(t *testing.T) {
	h := newHarness(t, nil, reasoningOn(config.ReasoningEffort))
	for _, v := range []string{"none", "minimal", "low", "medium", "high", "xhigh"} {
		status, body := h.chat(t, testToken, selfHostedBody(`,"reasoning_effort":"`+v+`"`))
		if status != 200 {
			t.Fatalf("reasoning_effort %s: got %d %s", v, status, body)
		}
		params, _ := h.mock.last()
		if got := string(params["reasoning_effort"]); got != `"`+v+`"` {
			t.Fatalf("reasoning_effort reached the upstream as %s, want %q", got, v)
		}
		if _, ok := params["chat_template_kwargs"]; ok {
			t.Fatalf("an effort upstream was sent the template switch: %v", params)
		}
	}
}

func TestReasoningEffortTranslatedForTemplateUpstream(t *testing.T) {
	h := newHarness(t, nil, reasoningOn(config.ReasoningTemplate))
	for _, tc := range []struct{ effort, want string }{
		{"none", `{"enable_thinking":false}`},
		{"low", `{"enable_thinking":true}`},
		{"high", `{"enable_thinking":true}`},
	} {
		status, body := h.chat(t, testToken, selfHostedBody(`,"reasoning_effort":"`+tc.effort+`"`))
		if status != 200 {
			t.Fatalf("reasoning_effort %s: got %d %s", tc.effort, status, body)
		}
		params, _ := h.mock.last()
		if _, ok := params["reasoning_effort"]; ok {
			// A template server reads the effort as a reasoning switch of its
			// own; the template switch has to be the only one it sees.
			t.Fatalf("reasoning_effort was forwarded to a template upstream: %v", params)
		}
		if got := string(params["chat_template_kwargs"]); got != tc.want {
			t.Fatalf("effort %s became %s, want %s", tc.effort, got, tc.want)
		}
	}
	// Absent means the server's own default: nothing is injected.
	if status, body := h.chat(t, testToken, selfHostedBody("")); status != 200 {
		t.Fatalf("no field: got %d %s", status, body)
	}
	params, _ := h.mock.last()
	if _, ok := params["chat_template_kwargs"]; ok {
		t.Fatalf("a request without reasoning_effort had the template switch injected: %v", params)
	}
}

func TestReasoningEffortStillRefusedOnClosedUpstream(t *testing.T) {
	// The default setting is closed, and an unset upstream behaves as it did
	// before the setting existed.
	h := newHarness(t, nil, nil)
	status, body := h.chat(t, testToken, selfHostedBody(`,"reasoning_effort":"low"`))
	if status != 400 || errCode(t, body) != "unsupported_parameter" {
		t.Fatalf("closed upstream: got %d %s", status, body)
	}
	if h.mock.callCount() != 0 {
		t.Fatalf("the upstream was called for a refused request")
	}
	evs := h.spoolEvents(t)
	if len(evs) != 1 || evs[0].ErrorType != "credit_only_parameter" {
		t.Fatalf("spooled %+v, want one credit_only_parameter event", evs)
	}
}

func TestReasoningEffortValueChecked(t *testing.T) {
	for _, setting := range []string{config.ReasoningEffort, config.ReasoningTemplate} {
		h := newHarness(t, nil, reasoningOn(setting))
		for _, bad := range []string{`"maximum"`, `"LOW"`, `1`, `true`, `{"effort":"low"}`} {
			status, body := h.chat(t, testToken, selfHostedBody(`,"reasoning_effort":`+bad))
			if status != 400 || errCode(t, body) != "invalid_parameter_value" {
				t.Fatalf("%s upstream, reasoning_effort %s: got %d %s", setting, bad, status, body)
			}
		}
		if h.mock.callCount() != 0 {
			t.Fatalf("%s upstream was called for an invalid value", setting)
		}
	}
}

func TestReasoningEffortNullIsAbsent(t *testing.T) {
	for _, setting := range []string{config.ReasoningEffort, config.ReasoningTemplate} {
		h := newHarness(t, nil, reasoningOn(setting))
		status, body := h.chat(t, testToken, selfHostedBody(`,"reasoning_effort":null`))
		if status != 200 {
			t.Fatalf("%s upstream, null: got %d %s", setting, status, body)
		}
		params, _ := h.mock.last()
		if _, ok := params["reasoning_effort"]; ok {
			t.Fatalf("%s upstream was sent a null reasoning_effort", setting)
		}
		if _, ok := params["chat_template_kwargs"]; ok {
			t.Fatalf("%s upstream was sent a template switch for null", setting)
		}
	}
}

// Opening reasoning opens the standard field and nothing else. The serving
// process's own switches stay closed, so a caller cannot reach a setting the
// translation above does not produce.
func TestVendorReasoningFieldsStayClosedWhenOpen(t *testing.T) {
	for _, setting := range []string{config.ReasoningEffort, config.ReasoningTemplate} {
		h := newHarness(t, nil, reasoningOn(setting))
		for _, extra := range []string{
			`,"chat_template_kwargs":{"enable_thinking":true}`,
			`,"thinking_token_budget":1024`,
			`,"reasoning":{"effort":"high"}`,
			`,"reasoning_effort":"low","chat_template_kwargs":{"enable_thinking":true}`,
		} {
			status, body := h.chat(t, testToken, selfHostedBody(extra))
			if status != 400 || errCode(t, body) != "unsupported_parameter" {
				t.Fatalf("%s upstream, %s: got %d %s", setting, extra, status, body)
			}
		}
		if h.mock.callCount() != 0 {
			t.Fatalf("%s upstream was called for a refused field", setting)
		}
	}
}

// A paid model forwards its reasoning fields verbatim whatever its upstream's
// setting: the setting is about self-hosted capacity only, and the money axis
// shares the same upstream reference.
func TestReasoningSettingDoesNotTouchPaidModels(t *testing.T) {
	bodies := map[string]string{
		"reasoning_effort": `"low"`,
		"reasoning":        `{"effort":"high","max_tokens":2000}`,
	}
	for _, setting := range []string{config.ReasoningClosed, config.ReasoningEffort, config.ReasoningTemplate} {
		h := newHarness(t, creditDoc, reasoningOn(setting))
		for field, value := range bodies {
			body := `{"model":"vendor-model","messages":[{"role":"user","content":"hi"}],"` + field + `":` + value + `}`
			status, resp := h.chat(t, testToken, body)
			if status != 200 {
				t.Fatalf("%s upstream, paid model with %s: got %d %s", setting, field, status, resp)
			}
			params, _ := h.mock.last()
			if got := string(params[field]); got != value {
				t.Fatalf("%s upstream: paid model's %s became %s, want %s", setting, field, got, value)
			}
			if _, ok := params["chat_template_kwargs"]; ok {
				t.Fatalf("%s upstream: paid model was rewritten: %v", setting, params)
			}
		}
	}
}

// Admission follows the primary upstream; each attempt is rewritten for the
// upstream it goes to. A closed fallback serves the request without reasoning
// rather than turning an admitted request into a refusal.
func TestReasoningRewrittenForTheFallbackThatAnswers(t *testing.T) {
	for _, tc := range []struct {
		name, fallback string
		check          func(t *testing.T, got map[string]json.RawMessage)
	}{
		{"closed fallback drops the field", config.ReasoningClosed, func(t *testing.T, got map[string]json.RawMessage) {
			if _, ok := got["reasoning_effort"]; ok {
				t.Fatalf("closed fallback was sent reasoning_effort: %v", got)
			}
			if _, ok := got["chat_template_kwargs"]; ok {
				t.Fatalf("closed fallback was sent the template switch: %v", got)
			}
		}},
		{"template fallback gets the switch", config.ReasoningTemplate, func(t *testing.T, got map[string]json.RawMessage) {
			if _, ok := got["reasoning_effort"]; ok {
				t.Fatalf("template fallback was sent reasoning_effort: %v", got)
			}
			if string(got["chat_template_kwargs"]) != `{"enable_thinking":true}` {
				t.Fatalf("template fallback got %s", got["chat_template_kwargs"])
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var mu sync.Mutex
			var seen map[string]json.RawMessage
			fb := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				raw, _ := io.ReadAll(r.Body)
				mu.Lock()
				_ = json.Unmarshal(raw, &seen)
				mu.Unlock()
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, `{"id":"fb","object":"chat.completion","model":"other","choices":[{"index":0,"message":{"role":"assistant","content":"fallback"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`)
			}))
			defer fb.Close()
			h := newHarness(t, func(d *snapshot.Document) { d.Models[0].FallbackRef = "backup" },
				func(c *config.Config) {
					reasoningOn(config.ReasoningEffort)(c)
					c.UpstreamRetries = 0
					c.Upstreams["backup"] = config.Upstream{Ref: "backup", BaseURL: fb.URL,
						CapField: "max_completion_tokens", Reasoning: tc.fallback}
				})
			h.mock.set(func(u *mockOpts) { u.status = http.StatusBadGateway })
			status, body := h.chat(t, testToken, selfHostedBody(`,"reasoning_effort":"high"`))
			if status != 200 {
				t.Fatalf("got %d %s", status, body)
			}
			// The primary was tried first and carried the field as written.
			params, _ := h.mock.last()
			if string(params["reasoning_effort"]) != `"high"` {
				t.Fatalf("primary got reasoning_effort %s", params["reasoning_effort"])
			}
			mu.Lock()
			got := seen
			mu.Unlock()
			if got == nil {
				t.Fatal("the fallback was never called")
			}
			tc.check(t, got)
		})
	}
}

// A closed primary refuses even when the fallback would accept: the decision
// belongs to the upstream the model is declared on.
func TestReasoningJudgedOnPrimaryUpstream(t *testing.T) {
	h := newHarness(t, func(d *snapshot.Document) { d.Models[0].FallbackRef = "backup" },
		func(c *config.Config) {
			c.Upstreams["backup"] = config.Upstream{Ref: "backup", BaseURL: "http://127.0.0.1:1",
				CapField: "max_completion_tokens", Reasoning: config.ReasoningEffort}
		})
	status, body := h.chat(t, testToken, selfHostedBody(`,"reasoning_effort":"low"`))
	if status != 400 || errCode(t, body) != "unsupported_parameter" {
		t.Fatalf("closed primary with an open fallback: got %d %s", status, body)
	}
}

// A stream is shaped twice before it leaves: the usage request merged into
// stream_options, then the reasoning field for the attempt. Both have to reach
// the upstream.
func TestReasoningEffortOnStream(t *testing.T) {
	h := newHarness(t, nil, reasoningOn(config.ReasoningTemplate))
	status, body := h.chat(t, testToken, selfHostedBody(`,"stream":true,"reasoning_effort":"low"`))
	if status != 200 {
		t.Fatalf("got %d %s", status, body)
	}
	params, _ := h.mock.last()
	if string(params["chat_template_kwargs"]) != `{"enable_thinking":true}` {
		t.Fatalf("stream lost the template switch: %v", params)
	}
	if _, ok := params["reasoning_effort"]; ok {
		t.Fatalf("stream forwarded reasoning_effort to a template upstream: %v", params)
	}
	if string(params["stream_options"]) != `{"include_usage":true}` {
		t.Fatalf("stream lost its usage request: %s", params["stream_options"])
	}
}

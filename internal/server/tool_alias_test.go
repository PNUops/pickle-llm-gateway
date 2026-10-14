package server

import (
	"strings"
	"testing"

	"github.com/pnuops/pickle-llm-gateway/internal/snapshot"
)

// The vendor runs each of its server tools under several types. Every
// spelling below was taken from its tool catalogue or run on a live call, and
// each is judged as the tool it runs rather than passed for lacking the
// "openrouter:" prefix.
var (
	webSearchSpellings = []string{
		"openrouter:web_search", "web_search", "WEB_SEARCH", "web_search_preview",
		"web_search_preview_2025_03_11", "web_search_2025_08_26",
		"web_search_20250305", "web_search_20260209", "web_search_20260318",
	}
	imageGenerationSpellings = []string{"openrouter:image_generation", "image_generation"}
	advisorSpellings         = []string{"openrouter:advisor", "advisor_20260301"}
	// Passed without a grant: web_fetch by decision, code_execution because
	// the vendor refuses it itself, and the client-side tools are not the
	// vendor's at all.
	passingSpellings = []string{
		"openrouter:web_fetch", "web_fetch", "web_fetch_20250910", "code_execution_20250825",
		"bash_20250124", "text_editor_20250728", "function", "custom",
	}
)

func toolsBody(model, tool string) string {
	return `{"model":"` + model + `","messages":[{"role":"user","content":"hi"}],"tools":[` + tool + `]}`
}

func TestCanonicalServerTool(t *testing.T) {
	for _, s := range webSearchSpellings {
		if got := canonicalServerTool(s); got != webSearchTool {
			t.Errorf("%s -> %q", s, got)
		}
	}
	for _, s := range imageGenerationSpellings {
		if got := canonicalServerTool(s); got != imageGenerationTool {
			t.Errorf("%s -> %q", s, got)
		}
	}
	for _, s := range advisorSpellings {
		if got := canonicalServerTool(s); got != "openrouter:advisor" {
			t.Errorf("%s -> %q", s, got)
		}
	}
	for _, s := range []string{"function", "custom", "", "bash_20250124", "code_execution_20250825"} {
		if got := canonicalServerTool(s); got != "" {
			t.Errorf("%s -> %q, want not a vendor tool", s, got)
		}
	}
}

// Chat route: every search and image spelling needs its grant, as the
// "openrouter:" spelling already did.
func TestChatToolSpellingsNeedTheirGrant(t *testing.T) {
	const model = "anthropic/claude-sonnet-4.5"
	h := newHarness(t, creditPassthroughDoc, nil)
	granted := newHarness(t, func(d *snapshot.Document) {
		creditPassthroughDoc(d)
		d.Keys[0].PassthroughEndpoints = []string{snapshot.EndpointChat,
			snapshot.EndpointWebSearch, snapshot.EndpointImages}
	}, nil)
	check := func(spellings []string, grant string) {
		for _, s := range spellings {
			body := toolsBody(model, `{"type":"`+s+`"}`)
			status, out := h.chat(t, testToken, body)
			if status != 403 || errCode(t, out) != "endpoint_not_allowed" || !strings.Contains(errMessage(t, out), grant) {
				t.Errorf("%s without %s: %d %s", s, grant, status, out)
			}
			if status, out := granted.chat(t, testToken, body); status != 200 {
				t.Errorf("%s with %s: %d %s", s, grant, status, out)
			}
		}
	}
	check(webSearchSpellings, "web_search")
	check(imageGenerationSpellings, "images")
	// Nested inside another tool's own list as well.
	nested := toolsBody(model, `{"type":"openrouter:subagent","parameters":{"tools":[{"type":"web_search_preview"}]}}`)
	if status, out := h.chat(t, testToken, nested); status != 403 || errCode(t, out) != "endpoint_not_allowed" {
		t.Errorf("nested spelling: %d %s", status, out)
	}
	for _, s := range passingSpellings {
		if status, out := h.chat(t, testToken, toolsBody(model, `{"type":"`+s+`"}`)); status != 200 {
			t.Errorf("%s: %d %s", s, status, out)
		}
	}
}

// Messages route: the bare search spelling, the image tool and the advisor,
// in tools and in deferred_tools.
func TestMessagesToolSpellingsNeedTheirGrant(t *testing.T) {
	const model = "anthropic/claude-sonnet-4.5"
	h, v := messagesHarness(t, nil)
	for _, field := range []string{"tools", "deferred_tools"} {
		for _, c := range []struct{ spelling, grant string }{
			{"web_search", "web_search"}, {"web_search_preview", "web_search"},
			{"web_search_20260209", "web_search"}, {"image_generation", "images"},
		} {
			body := messagesWith(model, field, `[{"type":"`+c.spelling+`","name":"x"}]`)
			status, out, _ := sendMessages(t, h, nil, body)
			e := expectAnthropicRefusal(t, status, out, 403, "permission_error", "endpoint_not_allowed")
			if !strings.Contains(e.Error.Message, c.grant) {
				t.Errorf("%s %s: %s", field, c.spelling, e.Error.Message)
			}
		}
	}
	if v.callCount() != 0 {
		t.Fatal("a refused request reached the vendor")
	}
	for _, s := range passingSpellings {
		if status, out, _ := sendMessages(t, h, nil, messagesWith(model, "tools", `[{"type":"`+s+`","name":"x"}]`)); status != 200 {
			t.Errorf("%s: %d %s", s, status, out)
		}
	}
}

// The advisor names its model at the tool's top level on the Messages route
// and in parameters on the chat route; a model outside the key's list is
// refused in either place, and a fenced key has to name one.
func TestAdvisorModelIsJudgedWhereverItIsNamed(t *testing.T) {
	const model = "anthropic/claude-sonnet-4.5"
	fenced := func(d *snapshot.Document) {
		creditPassthroughDoc(d)
		d.Keys[0].CreditAllowedModels = []string{"anthropic/claude-sonnet-*"}
	}
	h, v := messagesHarness(t, func(d *snapshot.Document) { d.Keys[0].CreditAllowedModels = []string{"anthropic/claude-sonnet-*"} })
	for _, tool := range []string{
		`{"type":"advisor_20260301","name":"advisor","model":"anthropic/claude-opus-5.5"}`,
		`{"type":"advisor_20260301","name":"advisor","parameters":{"model":"anthropic/claude-opus-5.5"}}`,
		`{"type":"openrouter:advisor","model":"anthropic/claude-opus-5.5"}`,
	} {
		status, out, _ := sendMessages(t, h, nil, messagesWith(model, "tools", `[`+tool+`]`))
		e := expectAnthropicRefusal(t, status, out, 403, "permission_error", "model_not_allowed")
		if !strings.Contains(e.Error.Message, "anthropic/claude-opus-5.5") {
			t.Errorf("%s: %s", tool, e.Error.Message)
		}
	}
	// Named nowhere: the vendor would pick, so a fenced key is refused.
	status, out, _ := sendMessages(t, h, nil, messagesWith(model, "tools", `[{"type":"advisor_20260301","name":"advisor"}]`))
	expectAnthropicRefusal(t, status, out, 403, "permission_error", "model_not_allowed")
	if v.callCount() != 0 {
		t.Fatal("a refused request reached the vendor")
	}
	// A model inside the list passes from either place.
	for _, tool := range []string{
		`{"type":"advisor_20260301","name":"advisor","model":"anthropic/claude-sonnet-4.5"}`,
		`{"type":"advisor_20260301","name":"advisor","parameters":{"model":"anthropic/claude-sonnet-4.5"}}`,
	} {
		if status, out, _ := sendMessages(t, h, nil, messagesWith(model, "tools", `[`+tool+`]`)); status != 200 {
			t.Errorf("%s: %d %s", tool, status, out)
		}
	}

	c := newHarness(t, fenced, nil)
	body := toolsBody(model, `{"type":"openrouter:advisor","model":"anthropic/claude-opus-5.5"}`)
	if status, out := c.chat(t, testToken, body); status != 403 || errCode(t, out) != "model_not_allowed" {
		t.Errorf("chat top-level advisor model: %d %s", status, out)
	}
}

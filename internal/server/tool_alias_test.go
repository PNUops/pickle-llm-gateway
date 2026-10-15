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
	// the vendor refuses it itself, tool_search and apply_patch because they
	// run no container, and the client-side tools are not the vendor's at all.
	// bash_20250124 is Anthropic's client bash: the vendor returns its command
	// to the caller rather than running it.
	passingSpellings = []string{
		"openrouter:web_fetch", "web_fetch", "web_fetch_20250910", "code_execution_20250825",
		"bash_20250124", "text_editor_20250728", "computer_20250124", "function", "custom",
		"openrouter:tool_search", "tool_search_tool_regex_20251119",
		"openrouter:apply_patch", "apply_patch",
	}
	// Run in a sandbox container shared across the account's keys, so they
	// need the containers grant, which nobody holds yet.
	containerSpellings = []string{
		"openrouter:shell", "shell", "shell_20250124", "shell_2026_01_01", "SHELL",
		" openrouter:Shell ", "openrouter:bash", " openrouter:bash ", "bash", "BASH",
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
	for _, s := range []string{"openrouter:shell", "shell", "shell_20250124", "SHELL", " openrouter:Shell "} {
		if got := canonicalServerTool(s); got != shellTool {
			t.Errorf("%s -> %q", s, got)
		}
	}
	// Only the bare word is the vendor's bash; a dated spelling is
	// Anthropic's client tool and must not be pulled into the family.
	for _, s := range []string{"openrouter:bash", "bash", "BASH", " bash "} {
		if got := canonicalServerTool(s); got != bashTool {
			t.Errorf("%s -> %q", s, got)
		}
	}
	for _, s := range []string{"function", "custom", "", "bash_20250124", "bash_20241022",
		"text_editor_20250728", "computer_20250124", "code_execution_20250825"} {
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

// containerRefusal checks one refusal message: it names the capability, and
// it does not send the student to ask for a grant the console cannot give.
func containerRefusal(t *testing.T, where, msg string) {
	t.Helper()
	if !strings.Contains(msg, "코드 실행(containers)") || !strings.Contains(msg, "아직 신청할 수 없는") ||
		strings.Contains(msg, "콘솔에서 이 기능을 신청하면") {
		t.Errorf("%s: %s", where, msg)
	}
}

// The sandbox tools reach a container that every key of the vendor account
// shares, so a key holding only chat is refused them on both routes, in every
// place a tool list can sit, and nothing reaches the vendor.
func TestContainerToolsNeedTheContainersGrant(t *testing.T) {
	const model = "anthropic/claude-sonnet-4.5"
	h := newHarness(t, creditPassthroughDoc, nil)
	for _, s := range containerSpellings {
		status, out := h.chat(t, testToken, toolsBody(model, `{"type":"`+s+`"}`))
		if status != 403 || errCode(t, out) != "endpoint_not_allowed" {
			t.Errorf("chat %q: %d %s", s, status, out)
			continue
		}
		containerRefusal(t, "chat "+s, errMessage(t, out))
	}
	nested := toolsBody(model, `{"type":"openrouter:subagent","parameters":{"tools":[{"type":"openrouter:shell"}]}}`)
	if status, out := h.chat(t, testToken, nested); status != 403 || errCode(t, out) != "endpoint_not_allowed" {
		t.Errorf("chat nested: %d %s", status, out)
	}
	if h.mock.callCount() != 0 {
		t.Fatal("a refused chat request reached the vendor")
	}

	m, v := messagesHarness(t, nil)
	for _, field := range []string{"tools", "deferred_tools"} {
		for _, s := range containerSpellings {
			body := messagesWith(model, field, `[{"type":"`+s+`","name":"x"}]`)
			status, out, _ := sendMessages(t, m, nil, body)
			e := expectAnthropicRefusal(t, status, out, 403, "permission_error", "endpoint_not_allowed")
			containerRefusal(t, field+" "+s, e.Error.Message)
		}
	}
	status, out, _ := sendMessages(t, m, nil, messagesWith(model, "tools",
		`[{"type":"openrouter:subagent","parameters":{"tools":[{"type":"bash","name":"bash"}]}}]`))
	expectAnthropicRefusal(t, status, out, 403, "permission_error", "endpoint_not_allowed")
	if v.callCount() != 0 {
		t.Fatal("a refused Messages request reached the vendor")
	}
}

// What a client sends for its own tools keeps passing: a function or custom
// tool named bash, a Messages tool with only a name (every tool Claude Code
// declares), and Anthropic's dated client tools.
func TestClientToolsNamedBashStillPass(t *testing.T) {
	const model = "anthropic/claude-sonnet-4.5"
	h := newHarness(t, creditPassthroughDoc, nil)
	for _, tool := range []string{
		`{"type":"function","function":{"name":"bash","parameters":{"type":"object"}}}`,
		`{"type":"function","function":{"name":"shell","parameters":{"type":"object"}}}`,
		`{"type":"bash_20250124","name":"bash"}`,
	} {
		if status, out := h.chat(t, testToken, toolsBody(model, tool)); status != 200 {
			t.Errorf("chat %s: %d %s", tool, status, out)
		}
	}
	m, _ := messagesHarness(t, nil)
	for _, tool := range []string{
		`{"name":"bash","description":"d","input_schema":{"type":"object"}}`,
		`{"name":"Bash","input_schema":{"type":"object"}}`,
		`{"type":"custom","name":"bash","input_schema":{"type":"object"}}`,
		`{"type":"bash_20250124","name":"bash"}`,
		`{"type":"text_editor_20250728","name":"str_replace_based_edit_tool"}`,
	} {
		if status, out, _ := sendMessages(t, m, nil, messagesWith(model, "tools", `[`+tool+`]`)); status != 200 {
			t.Errorf("messages %s: %d %s", tool, status, out)
		}
	}
}

// The grant is what opens the door: the same requests pass on a key that
// holds it, so granting it later is the only change needed here.
func TestContainersGrantOpensContainerTools(t *testing.T) {
	const model = "anthropic/claude-sonnet-4.5"
	grant := func(d *snapshot.Document) {
		d.Keys[0].PassthroughEndpoints = []string{snapshot.EndpointChat, snapshot.EndpointContainers}
	}
	h := newHarness(t, func(d *snapshot.Document) { creditPassthroughDoc(d); grant(d) }, nil)
	m, _ := messagesHarness(t, grant)
	for _, s := range containerSpellings {
		if status, out := h.chat(t, testToken, toolsBody(model, `{"type":"`+s+`"}`)); status != 200 {
			t.Errorf("chat %q: %d %s", s, status, out)
		}
		for _, field := range []string{"tools", "deferred_tools"} {
			body := messagesWith(model, field, `[{"type":"`+s+`","name":"x"}]`)
			if status, out, _ := sendMessages(t, m, nil, body); status != 200 {
				t.Errorf("messages %s %q: %d %s", field, s, status, out)
			}
		}
	}
}

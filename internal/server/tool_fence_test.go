package server

import (
	"strings"
	"testing"

	"github.com/pnuops/pickle-llm-gateway/internal/snapshot"
)

// The standard API's own container tools reduce to a container tool too:
// local_shell is routed to the vendor's sandbox on models it does not host,
// and code_interpreter runs in a hosted container.
func TestStandardContainerToolSpellings(t *testing.T) {
	for s, want := range map[string]string{
		"local_shell":                 shellTool,
		"LOCAL_SHELL":                 shellTool,
		"openrouter:local_shell":      shellTool,
		"code_interpreter":            codeInterpreterTool,
		"code_interpreter_20250825":   codeInterpreterTool,
		"openrouter:code_interpreter": codeInterpreterTool,
	} {
		if got := canonicalServerTool(s); got != want {
			t.Errorf("%s -> %q, want %q", s, got, want)
		}
	}

	const model = "anthropic/claude-sonnet-4.5"
	h := newHarness(t, creditPassthroughDoc, nil)
	m, v := messagesHarness(t, nil)
	for _, s := range []string{"local_shell", "code_interpreter"} {
		tool := `{"type":"` + s + `","container":{"type":"auto"}}`
		status, out := h.chat(t, testToken, toolsBody(model, tool))
		if status != 403 || errCode(t, out) != "endpoint_not_allowed" {
			t.Errorf("chat %s: %d %s", s, status, out)
			continue
		}
		containerRefusal(t, "chat "+s, errMessage(t, out))
		status, out, _ = sendMessages(t, m, nil, messagesWith(model, "tools", `[`+tool+`]`))
		expectAnthropicRefusal(t, status, out, 403, "permission_error", "endpoint_not_allowed")
	}
	if h.mock.callCount() != 0 || v.callCount() != 0 {
		t.Fatal("a refused request reached the vendor")
	}

	granted := newHarness(t, func(d *snapshot.Document) {
		creditPassthroughDoc(d)
		d.Keys[0].PassthroughEndpoints = []string{snapshot.EndpointChat, snapshot.EndpointContainers}
	}, nil)
	for _, s := range []string{"local_shell", "code_interpreter"} {
		if status, out := granted.chat(t, testToken, toolsBody(model, `{"type":"`+s+`"}`)); status != 200 {
			t.Errorf("granted %s: %d %s", s, status, out)
		}
	}
}

// A tool in the vendor's own namespace that this build does not know is
// refused, files included; the known ones and the standard tools pass.
func TestUnknownVendorToolsAreRefused(t *testing.T) {
	const model = "anthropic/claude-sonnet-4.5"
	h := newHarness(t, creditPassthroughDoc, nil)
	for _, tool := range []string{
		`{"type":"openrouter:files"}`,
		`{"type":"openrouter:something_new"}`,
		`{"type":" OpenRouter:Files "}`,
		`{"type":"openrouter:subagent","parameters":{"tools":[{"type":"openrouter:files"}]}}`,
		`{"type":"namespace","name":"n","tools":[{"type":"openrouter:something_new"}]}`,
	} {
		status, out := h.chat(t, testToken, toolsBody(model, tool))
		if status != 400 || errCode(t, out) != "unsupported_parameter" ||
			!strings.Contains(errMessage(t, out), "지원하지 않는 서버 도구") {
			t.Errorf("%s: %d %s", tool, status, out)
		}
	}
	if h.mock.callCount() != 0 {
		t.Fatal("a refused request reached the vendor")
	}
	evs := h.spoolEvents(t)
	if len(evs) == 0 || evs[0].ErrorType != "unsupported_parameter" {
		t.Fatalf("events: %+v", evs)
	}
	// A long unknown name is not echoed back.
	long := `{"type":"openrouter:` + strings.Repeat("x", 300) + `"}`
	if _, out := h.chat(t, testToken, toolsBody(model, long)); strings.Contains(errMessage(t, out), "xxxx") {
		t.Errorf("long name echoed: %s", out)
	}

	for _, tool := range []string{
		`{"type":"openrouter:datetime"}`,
		`{"type":"openrouter:experimental__search_models"}`,
		`{"type":"openrouter:web_fetch"}`,
		`{"type":"openrouter:tool_search"}`,
		`{"type":"openrouter:apply_patch"}`,
		`{"type":"mcp","server_label":"s","server_url":"https://example.com/mcp"}`,
		`{"type":"file_search","vector_store_ids":["vs_1"]}`,
		`{"type":"computer_use_preview","display_width":1024,"display_height":768,"environment":"browser"}`,
		`{"type":"tool_search"}`,
		`{"type":"apply_patch"}`,
	} {
		if status, out := h.chat(t, testToken, toolsBody(model, tool)); status != 200 {
			t.Errorf("%s: %d %s", tool, status, out)
		}
	}

	m, _ := messagesHarness(t, nil)
	status, out, _ := sendMessages(t, m, nil, messagesWith(model, "tools", `[{"type":"openrouter:files","name":"f"}]`))
	expectAnthropicRefusal(t, status, out, 400, "invalid_request_error", "unsupported_parameter")
}

// A namespace's members sit in its own list and are fenced like any tool.
func TestNamespaceMembersAreFenced(t *testing.T) {
	const model = "anthropic/claude-sonnet-4.5"
	h := newHarness(t, creditPassthroughDoc, nil)
	for _, tool := range []string{
		`{"type":"namespace","name":"n","tools":[{"type":"shell"}]}`,
		`{"type":"NAMESPACE","name":"n","tools":[{"type":"web_search"}]}`,
		`{"type":"namespace","name":"n","tools":[{"type":"namespace","name":"m","tools":[{"type":"local_shell"}]}]}`,
	} {
		status, out := h.chat(t, testToken, toolsBody(model, tool))
		if status != 403 || errCode(t, out) != "endpoint_not_allowed" {
			t.Errorf("%s: %d %s", tool, status, out)
		}
	}
	for _, tool := range []string{
		`{"type":"namespace","name":"n","tools":"x"}`,
		`{"type":"namespace","name":"n","tools":[{"type":"namespace","tools":[{"type":"namespace","tools":[{"type":"namespace","tools":[{"type":"namespace","tools":[]}]}]}]}]}`,
	} {
		status, out := h.chat(t, testToken, toolsBody(model, tool))
		if status != 400 || errCode(t, out) != "invalid_parameter_value" {
			t.Errorf("%s: %d %s", tool, status, out)
		}
	}
	ok := `{"type":"namespace","name":"functions","description":"d","tools":[` +
		`{"type":"function","name":"wait","parameters":{"type":"object"}},` +
		`{"type":"custom","name":"exec","format":{"type":"grammar","syntax":"lark","definition":"start: /.+/"}}]}`
	if status, out := h.chat(t, testToken, toolsBody(model, ok)); status != 200 {
		t.Errorf("client namespace: %d %s", status, out)
	}
}

// tool_choice can name a hosted tool by type without declaring it, so the
// type rules apply there too.
func TestToolChoiceIsFenced(t *testing.T) {
	const model = "anthropic/claude-sonnet-4.5"
	h := newHarness(t, creditPassthroughDoc, nil)
	body := func(choice string) string {
		return `{"model":"` + model + `","messages":[{"role":"user","content":"hi"}],"tool_choice":` + choice + `}`
	}
	for _, tc := range []struct{ choice, code string }{
		{`{"type":"web_search"}`, "endpoint_not_allowed"},
		{`{"type":"web_search_preview"}`, "endpoint_not_allowed"},
		{`{"type":"image_generation"}`, "endpoint_not_allowed"},
		{`{"type":"shell"}`, "endpoint_not_allowed"},
		{`{"type":"local_shell"}`, "endpoint_not_allowed"},
		{`{"type":"code_interpreter"}`, "endpoint_not_allowed"},
		{`{"type":"openrouter:files"}`, "unsupported_parameter"},
		{`{"type":"allowed_tools","mode":"auto","tools":[{"type":"function","name":"f"},{"type":"code_interpreter"}]}`, "endpoint_not_allowed"},
		{`{"type":"allowed_tools","mode":"auto","tools":"x"}`, "invalid_parameter_value"},
		{`{"type":"allowed_tools","allowed_tools":{"mode":"auto","tools":[{"type":"code_interpreter"}]}}`, "endpoint_not_allowed"},
		{`{"type":"allowed_tools","allowed_tools":{"mode":"auto","tools":[{"type":"web_search"}]}}`, "endpoint_not_allowed"},
		{`{"type":"allowed_tools","allowed_tools":[1]}`, "invalid_parameter_value"},
		{`{"type":5}`, "invalid_parameter_value"},
		{`[1]`, "invalid_parameter_value"},
	} {
		status, out := h.chat(t, testToken, body(tc.choice))
		if (status != 403 && status != 400) || errCode(t, out) != tc.code {
			t.Errorf("%s: %d %s", tc.choice, status, out)
		}
		if tc.code == "invalid_parameter_value" && !strings.Contains(errMessage(t, out), "tool_choice") {
			t.Errorf("%s: refusal does not name the field: %s", tc.choice, out)
		}
	}
	if h.mock.callCount() != 0 {
		t.Fatal("a refused request reached the vendor")
	}
	for _, choice := range []string{
		`"auto"`, `"none"`, `"required"`, `null`,
		`{"type":"function","function":{"name":"web_search"}}`,
		`{"type":"function","name":"f"}`,
		`{"type":"mcp","server_label":"s"}`,
		`{"type":"file_search"}`,
		`{"type":"allowed_tools","mode":"required","tools":[{"type":"function","name":"f"}]}`,
		`{"type":"allowed_tools","allowed_tools":{"mode":"required","tools":[{"type":"function","function":{"name":"f"}}]}}`,
	} {
		if status, out := h.chat(t, testToken, body(choice)); status != 200 {
			t.Errorf("%s: %d %s", choice, status, out)
		}
	}
	granted := newHarness(t, func(d *snapshot.Document) {
		creditPassthroughDoc(d)
		d.Keys[0].PassthroughEndpoints = []string{snapshot.EndpointChat, snapshot.EndpointWebSearch}
	}, nil)
	if status, out := granted.chat(t, testToken, body(`{"type":"web_search"}`)); status != 200 {
		t.Errorf("granted web_search choice: %d %s", status, out)
	}

	// Anthropic's forms name tools declared in tools, which are fenced there.
	m, _ := messagesHarness(t, nil)
	for _, choice := range []string{`{"type":"auto"}`, `{"type":"any"}`, `{"type":"tool","name":"web_search"}`} {
		if status, out, _ := sendMessages(t, m, nil, messagesWith(model, "tool_choice", choice)); status != 200 {
			t.Errorf("messages %s: %d %s", choice, status, out)
		}
	}
	status, out, _ := sendMessages(t, m, nil, messagesWith(model, "tool_choice", `{"type":"web_search_20250305"}`))
	expectAnthropicRefusal(t, status, out, 403, "permission_error", "endpoint_not_allowed")
}

// web_search_options is an object; a scalar there is a malformed value, not a
// request for a search, and null is the field left out.
func TestWebSearchOptionsShape(t *testing.T) {
	h := newHarness(t, creditPassthroughDoc, nil)
	body := func(v string) string {
		return `{"model":"vendor-model","messages":[{"role":"user","content":"hi"}],"web_search_options":` + v + `}`
	}
	for _, v := range []string{"false", "0", `"low"`, "[]"} {
		if status, out := h.chat(t, testToken, body(v)); status != 400 || errCode(t, out) != "invalid_parameter_value" {
			t.Errorf("%s: %d %s", v, status, out)
		}
	}
	if status, out := h.chat(t, testToken, body("null")); status != 200 {
		t.Errorf("null: %d %s", status, out)
	}
	if status, out := h.chat(t, testToken, body("{}")); status != 403 || errCode(t, out) != "endpoint_not_allowed" {
		t.Errorf("{}: %d %s", status, out)
	}
}

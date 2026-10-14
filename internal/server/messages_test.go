package server

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/pnuops/pickle-llm-gateway/internal/snapshot"
	"github.com/pnuops/pickle-llm-gateway/internal/spool"
)

// vendorMessages stands in for the vendor's Messages endpoint: it records the
// last request and answers with whatever the test set.
type vendorMessages struct {
	mu      sync.Mutex
	calls   int
	path    string
	auth    string
	headers http.Header
	body    map[string]json.RawMessage

	status int
	answer string
	stream string
}

func (v *vendorMessages) install(h *harness) {
	h.srv.client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		raw, _ := io.ReadAll(r.Body)
		v.mu.Lock()
		defer v.mu.Unlock()
		v.calls++
		v.path = r.URL.Path
		v.auth = r.Header.Get("Authorization")
		v.headers = r.Header.Clone()
		v.body = map[string]json.RawMessage{}
		_ = json.Unmarshal(raw, &v.body)
		status, out := v.status, v.answer
		if status == 0 {
			status = http.StatusOK
		}
		var streamed bool
		_ = json.Unmarshal(v.body["stream"], &streamed)
		hdr := make(http.Header)
		if streamed && status == http.StatusOK {
			out = v.stream
			hdr.Set("Content-Type", "text/event-stream")
		} else {
			hdr.Set("Content-Type", "application/json")
		}
		return &http.Response{StatusCode: status, Header: hdr, Request: r,
			Body: io.NopCloser(strings.NewReader(out))}, nil
	})}
}

func (v *vendorMessages) callCount() int {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.calls
}

const vendorMessageAnswer = `{"id":"msg_1","type":"message","role":"assistant","model":"` + upstreamModel + `",` +
	`"content":[{"type":"text","text":"안녕"},{"type":"text","text":"하세요"}],"stop_reason":"end_turn",` +
	`"usage":{"input_tokens":10,"output_tokens":4,"cache_read_input_tokens":20,"cache_creation_input_tokens":5,"cost":0.0012}}`

// messagesDoc is a document with a catalogued paid model, a passthrough
// upstream for uncatalogued vendor names, and a self-hosted model.
func messagesDoc(d *snapshot.Document) {
	creditPassthroughDoc(d)
}

func messagesHarness(t *testing.T, mutate func(*snapshot.Document)) (*harness, *vendorMessages) {
	t.Helper()
	h := newHarness(t, func(d *snapshot.Document) {
		messagesDoc(d)
		if mutate != nil {
			mutate(d)
		}
	}, nil)
	v := &vendorMessages{answer: vendorMessageAnswer}
	v.install(h)
	return h, v
}

// sendMessages posts to /v1/messages. hdr replaces the default bearer: a nil
// map sends the bearer, and a header set to "" is removed.
func sendMessages(t *testing.T, h *harness, hdr map[string]string, body string) (int, []byte, http.Header) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, h.gw.URL+"/v1/messages?beta=true", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+testToken)
	for k, v := range hdr {
		if v == "" {
			req.Header.Del(k)
			continue
		}
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, out, resp.Header
}

func messagesFor(model string) string {
	return `{"model":"` + model + `","max_tokens":100,"messages":[{"role":"user","content":"hi"}]}`
}

func messagesWith(model, field, value string) string {
	return `{"model":"` + model + `","` + field + `":` + value + `,"messages":[{"role":"user","content":"hi"}]}`
}

// anthropicError decodes the Anthropic envelope and fails on anything else,
// the OpenAI one included.
type anthropicEnvelope struct {
	Type  string `json:"type"`
	Error struct {
		Type    string `json:"type"`
		Message string `json:"message"`
		Code    string `json:"code"`
	} `json:"error"`
	RequestID string `json:"request_id"`
}

func anthropicError(t *testing.T, body []byte) anthropicEnvelope {
	t.Helper()
	var e anthropicEnvelope
	if err := json.Unmarshal(body, &e); err != nil || e.Type != "error" || e.Error.Type == "" || e.Error.Code == "" {
		t.Fatalf("not an Anthropic error envelope: %s", body)
	}
	return e
}

func expectAnthropicRefusal(t *testing.T, status int, body []byte, wantStatus int, wantType, wantCode string) anthropicEnvelope {
	t.Helper()
	e := anthropicError(t, body)
	if status != wantStatus || e.Error.Type != wantType || e.Error.Code != wantCode {
		t.Fatalf("got %d %s/%s, want %d %s/%s: %s", status, e.Error.Type, e.Error.Code,
			wantStatus, wantType, wantCode, body)
	}
	return e
}

func TestMessagesNonStreamRoundTrip(t *testing.T) {
	h, v := messagesHarness(t, nil)
	status, body, hdr := sendMessages(t, h, map[string]string{
		"Anthropic-Version": "2023-06-01",
		"Anthropic-Beta":    "interleaved-thinking-2025-05-14,context-1m-2025-08-07",
		"X-Unlisted":        "dropped",
	}, messagesFor("vendor-model"))
	if status != 200 {
		t.Fatalf("status %d: %s", status, body)
	}
	if v.path != "/messages" || v.auth != "Bearer "+keyCred {
		t.Fatalf("upstream got path %q auth %q", v.path, v.auth)
	}
	if string(v.body["model"]) != `"`+upstreamModel+`"` || string(v.body["max_tokens"]) != "100" {
		t.Fatalf("upstream body: %s / %s", v.body["model"], v.body["max_tokens"])
	}
	if v.headers.Get("Anthropic-Version") != "2023-06-01" ||
		v.headers.Get("Anthropic-Beta") != "interleaved-thinking-2025-05-14,context-1m-2025-08-07" ||
		v.headers.Get("X-Unlisted") != "" {
		t.Fatalf("forwarded headers: %v", v.headers)
	}
	var out map[string]any
	_ = json.Unmarshal(body, &out)
	if out["model"] != "vendor-model" || strings.Contains(string(body), upstreamModel) {
		t.Fatalf("answer model not rewritten: %s", body)
	}
	if hdr.Get("X-Request-Id") == "" {
		t.Fatal("no request id")
	}
	ev := lastEvent(t, h)
	if ev.Endpoint != "messages" || ev.Status != spool.StatusOK || ev.Estimated ||
		ev.InputTokens != 35 || ev.CachedInputTokens != 20 || ev.OutputTokens != 4 ||
		ev.CostUsd != "0.0012" || ev.BudgetAxis != snapshot.AxisCredit {
		t.Fatalf("record: %+v", ev)
	}
}

// The model is never filled in for the caller on a passthrough name, and the
// self-hosted fields the chat surface forwards are not invented either.
func TestMessagesSendsNoInventedOutputCap(t *testing.T) {
	h, v := messagesHarness(t, nil)
	body := `{"model":"anthropic/claude-sonnet-4.5","messages":[{"role":"user","content":"hi"}]}`
	if status, out, _ := sendMessages(t, h, nil, body); status != 200 {
		t.Fatalf("status %d: %s", status, out)
	}
	for _, f := range []string{"max_tokens", "max_completion_tokens", "stream_options"} {
		if _, ok := v.body[f]; ok {
			t.Fatalf("gateway added %s", f)
		}
	}
}

func TestMessagesOutputCapOverModelMaximum(t *testing.T) {
	h, v := messagesHarness(t, func(d *snapshot.Document) {
		for i := range d.Models {
			if d.Models[i].PublicName == "vendor-model" {
				d.Models[i].MaxOutputTokens = 50
			}
		}
	})
	status, body, _ := sendMessages(t, h, nil, messagesFor("vendor-model"))
	e := expectAnthropicRefusal(t, status, body, 400, "invalid_request_error", "output_limit_exceeded")
	if !strings.Contains(e.Error.Message, "max_tokens 값을 50 이하로") {
		t.Fatalf("message without the cap: %s", e.Error.Message)
	}
	if v.callCount() != 0 {
		t.Fatal("refused request reached the vendor")
	}
}

func TestMessagesKeyHeaders(t *testing.T) {
	h, _ := messagesHarness(t, nil)
	cases := []struct {
		name   string
		hdr    map[string]string
		status int
		code   string
	}{
		{"x-api-key alone", map[string]string{"Authorization": "", "X-Api-Key": testToken}, 200, ""},
		{"bearer wins over a bad x-api-key", map[string]string{"X-Api-Key": "pickle-nope"}, 200, ""},
		{"bad bearer is not rescued by x-api-key",
			map[string]string{"Authorization": "Bearer pickle-nope", "X-Api-Key": testToken}, 401, "invalid_api_key"},
		{"no key", map[string]string{"Authorization": ""}, 401, "missing_api_key"},
	}
	for _, c := range cases {
		status, body, _ := sendMessages(t, h, c.hdr, messagesFor("vendor-model"))
		if c.status == 200 {
			if status != 200 {
				t.Fatalf("%s: %d %s", c.name, status, body)
			}
			continue
		}
		e := expectAnthropicRefusal(t, status, body, c.status, "authentication_error", c.code)
		if c.code == "missing_api_key" && !strings.Contains(e.Error.Message, "x-api-key") {
			t.Fatalf("%s: message does not name x-api-key: %s", c.name, e.Error.Message)
		}
	}
}

func TestMessagesNeedsChatGrant(t *testing.T) {
	h, v := messagesHarness(t, func(d *snapshot.Document) { d.Keys[0].PassthroughEndpoints = nil })
	status, body, _ := sendMessages(t, h, nil, messagesFor("vendor-model"))
	e := expectAnthropicRefusal(t, status, body, 403, "permission_error", "endpoint_not_allowed")
	if !strings.Contains(e.Error.Message, "Chat(chat)") {
		t.Fatalf("message: %s", e.Error.Message)
	}
	if v.callCount() != 0 {
		t.Fatal("refused request reached the vendor")
	}
}

func TestMessagesRefusesSelfHostedModel(t *testing.T) {
	h, v := messagesHarness(t, nil)
	for _, model := range []string{"pickle-general", "campus-x"} {
		status, body, _ := sendMessages(t, h, nil, messagesFor(model))
		e := expectAnthropicRefusal(t, status, body, 400, "invalid_request_error", "model_not_available_on_route")
		if !strings.Contains(e.Error.Message, "/v1/chat/completions") {
			t.Fatalf("%s: message %s", model, e.Error.Message)
		}
	}
	if v.callCount() != 0 {
		t.Fatal("self-hosted request reached an upstream")
	}
	ev := lastEvent(t, h)
	if ev.Status != spool.StatusBadRequest || ev.ErrorType != "model_not_available_on_route" || ev.Endpoint != "messages" {
		t.Fatalf("record: %+v", ev)
	}
}

// Model names follow the chat rules: an uncatalogued name is forwarded as
// written whether or not it carries a provider prefix, and only the key's own
// lists decide whether it may go.
func TestMessagesModelNamesFollowChatRules(t *testing.T) {
	h, v := messagesHarness(t, nil)
	for _, model := range []string{"claude-opus-5-5", "anthropic/claude-sonnet-4.5", "~anthropic/claude-opus-latest", "vendor-model"} {
		if status, body, _ := sendMessages(t, h, nil, messagesFor(model)); status != 200 {
			t.Fatalf("%s: %d %s", model, status, body)
		}
	}
	// The bare name went out as the caller wrote it.
	h, v = messagesHarness(t, nil)
	if status, body, _ := sendMessages(t, h, nil, messagesFor("claude-opus-5-5")); status != 200 {
		t.Fatalf("%d %s", status, body)
	}
	var sent string
	_ = json.Unmarshal(v.body["model"], &sent)
	if sent != "claude-opus-5-5" {
		t.Fatalf("forwarded model %q", sent)
	}

	// A key whose lists do not admit the name refuses it as on chat.
	h, v = messagesHarness(t, func(d *snapshot.Document) { d.Keys[0].CreditAllowedModels = []string{"anthropic/*"} })
	status, body, _ := sendMessages(t, h, nil, messagesFor("claude-opus-5-5"))
	expectAnthropicRefusal(t, status, body, 403, "permission_error", "model_not_allowed")
	if v.callCount() != 0 {
		t.Fatal("a name outside the allow list reached the vendor")
	}
	// A pattern with a vendor segment never reaches a bare name; an exact entry
	// does.
	h, v = messagesHarness(t, func(d *snapshot.Document) { d.Keys[0].CreditDeniedModels = []string{"claude-opus-5-5"} })
	status, body, _ = sendMessages(t, h, nil, messagesFor("claude-opus-5-5"))
	expectAnthropicRefusal(t, status, body, 403, "permission_error", "model_not_allowed")
	if v.callCount() != 0 {
		t.Fatal("a denied name reached the vendor")
	}
}

func TestMessagesModelFences(t *testing.T) {
	fenced := func(d *snapshot.Document) { d.Keys[0].CreditAllowedModels = []string{"anthropic/*"} }
	cases := []struct {
		name, body, code, where string
		status                  int
	}{
		{"primary outside the list", messagesFor("openai/gpt-5"), "model_not_allowed", "", 403},
		{"fallback outside the list",
			messagesWith("anthropic/claude-sonnet-4.5", "fallbacks", `[{"model":"openai/gpt-5"}]`),
			"model_not_allowed", "fallbacks", 403},
		{"fallback unreadable",
			messagesWith("anthropic/claude-sonnet-4.5", "fallbacks", `[{"model":7}]`),
			"invalid_parameter_value", "fallbacks", 400},
		{"models candidate outside the list",
			messagesWith("anthropic/claude-sonnet-4.5", "models", `["openai/gpt-5"]`),
			"model_not_allowed", "models", 403},
		{"server tool naming a model outside the list",
			messagesWith("anthropic/claude-sonnet-4.5", "tools",
				`[{"type":"openrouter:advisor","parameters":{"model":"openai/gpt-5"}}]`),
			"model_not_allowed", "tools", 403},
		{"bash server tool naming a model outside the list",
			messagesWith("anthropic/claude-sonnet-4.5", "tools",
				`[{"type":"openrouter:bash","parameters":{"model":"openai/gpt-5"}}]`),
			"model_not_allowed", "tools", 403},
		{"deferred server tool naming a model outside the list",
			messagesWith("anthropic/claude-sonnet-4.5", "deferred_tools",
				`[{"type":"openrouter:subagent","parameters":{"model":"openai/gpt-5"}}]`),
			"model_not_allowed", "tools", 403},
		{"deferred tools unreadable",
			messagesWith("anthropic/claude-sonnet-4.5", "deferred_tools", `{"x":1}`),
			"invalid_parameter_value", "deferred_tools", 400},
		{"preset", messagesWith("anthropic/claude-sonnet-4.5", "preset", `"p"`),
			"preset_not_supported", "", 400},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			h, v := messagesHarness(t, fenced)
			status, body, _ := sendMessages(t, h, nil, c.body)
			wantType := "permission_error"
			if c.status == 400 {
				wantType = "invalid_request_error"
			}
			e := expectAnthropicRefusal(t, status, body, c.status, wantType, c.code)
			if c.where != "" && !strings.Contains(e.Error.Message, c.where) {
				t.Fatalf("message does not say where: %s", e.Error.Message)
			}
			if v.callCount() != 0 {
				t.Fatal("refused request reached the vendor")
			}
		})
	}
	// The same shapes inside the list pass, a bare server tool included.
	h, _ := messagesHarness(t, fenced)
	for _, body := range []string{
		messagesWith("anthropic/claude-sonnet-4.5", "fallbacks", `[{"model":"anthropic/claude-haiku-4.5"}]`),
		messagesWith("anthropic/claude-sonnet-4.5", "tools",
			`[{"name":"read_file","description":"d","input_schema":{"type":"object"}},{"type":"openrouter:bash"}]`),
	} {
		if status, out, _ := sendMessages(t, h, nil, body); status != 200 {
			t.Fatalf("%s: %d %s", body, status, out)
		}
	}
}

func TestMessagesWebSearchNeedsGrant(t *testing.T) {
	model := "anthropic/claude-sonnet-4.5"
	asks := map[string]string{
		"online model":     messagesFor(model + ":online"),
		"online fallback":  messagesWith(model, "fallbacks", `[{"model":"anthropic/claude-haiku-4.5:online"}]`),
		"online in models": messagesWith(model, "models", `["anthropic/claude-haiku-4.5:online"]`),
		"web plugin":       messagesWith(model, "plugins", `[{"id":"web"}]`),
		"web search tool":  messagesWith(model, "tools", `[{"type":"openrouter:web_search"}]`),
		"native web search tool": messagesWith(model, "tools",
			`[{"type":"web_search_20250305","name":"web_search","max_uses":5}]`),
		"deferred native web search tool": messagesWith(model, "deferred_tools",
			`[{"type":"web_search_20260209","name":"web_search"}]`),
	}
	h, v := messagesHarness(t, nil)
	for name, body := range asks {
		status, out, _ := sendMessages(t, h, nil, body)
		e := expectAnthropicRefusal(t, status, out, 403, "permission_error", "endpoint_not_allowed")
		if !strings.Contains(e.Error.Message, "web_search") {
			t.Fatalf("%s: %s", name, e.Error.Message)
		}
	}
	if v.callCount() != 0 {
		t.Fatal("refused request reached the vendor")
	}
	granted, _ := messagesHarness(t, func(d *snapshot.Document) {
		d.Keys[0].PassthroughEndpoints = []string{snapshot.EndpointChat, snapshot.EndpointWebSearch}
	})
	for name, body := range asks {
		if status, out, _ := sendMessages(t, granted, nil, body); status != 200 {
			t.Fatalf("%s with the grant: %d %s", name, status, out)
		}
	}
}

// The other vendor server tools and remote MCP servers are not web search, so
// a key without the web_search grant sends them through as written.
func TestMessagesOtherServerToolsNeedNoWebSearchGrant(t *testing.T) {
	model := "anthropic/claude-sonnet-4.5"
	asks := map[string]string{
		"web fetch tool": messagesWith(model, "tools",
			`[{"type":"web_fetch_20250910","name":"web_fetch","max_uses":3}]`),
		"code execution tool": messagesWith(model, "tools",
			`[{"type":"code_execution_20250825","name":"code_execution"}]`),
		"mcp servers": messagesWith(model, "mcp_servers",
			`[{"type":"url","url":"https://mcp.example.com/sse","name":"example"}]`),
	}
	h, v := messagesHarness(t, nil)
	for name, body := range asks {
		status, out, _ := sendMessages(t, h, nil, body)
		if status != 200 {
			t.Fatalf("%s: %d %s", name, status, out)
		}
		var sent map[string]json.RawMessage
		_ = json.Unmarshal([]byte(body), &sent)
		for _, field := range []string{"tools", "mcp_servers"} {
			if want, ok := sent[field]; ok && string(v.body[field]) != string(want) {
				t.Fatalf("%s: upstream %s = %s, want %s", name, field, v.body[field], want)
			}
		}
	}
}

// Every refusal on the surface is in the Anthropic envelope, the ones written
// after the upstream answered included.
func TestMessagesRefusalsUseAnthropicEnvelope(t *testing.T) {
	h, v := messagesHarness(t, nil)

	req, _ := http.NewRequest(http.MethodGet, h.gw.URL+"/v1/messages", nil)
	req.Header.Set("Authorization", "Bearer "+testToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	expectAnthropicRefusal(t, resp.StatusCode, raw, 405, "invalid_request_error", "method_not_allowed")

	status, body, _ := sendMessages(t, h, nil, `{"model":`)
	expectAnthropicRefusal(t, status, body, 400, "invalid_request_error", "invalid_json")
	status, body, _ = sendMessages(t, h, nil, `{"messages":[]}`)
	expectAnthropicRefusal(t, status, body, 400, "invalid_request_error", "missing_parameter")
	status, body, _ = sendMessages(t, h, nil, `{"model":"anthropic/x"}`)
	expectAnthropicRefusal(t, status, body, 400, "invalid_request_error", "missing_parameter")

	// The vendor's own rejection, relayed with its reason.
	v.status, v.answer = 400, `{"type":"error","error":{"type":"invalid_request_error","message":"output_config.effort: Extra inputs are not permitted"}}`
	status, body, _ = sendMessages(t, h, nil, messagesFor("anthropic/claude-sonnet-4.5"))
	e := expectAnthropicRefusal(t, status, body, 400, "invalid_request_error", "upstream_rejected")
	if !strings.Contains(e.Error.Message, "Extra inputs are not permitted") || e.RequestID == "" {
		t.Fatalf("relayed rejection: %s", body)
	}

	v.status, v.answer = 429, `{}`
	status, body, _ = sendMessages(t, h, nil, messagesFor("anthropic/claude-sonnet-4.5"))
	expectAnthropicRefusal(t, status, body, 503, "overloaded_error", "server_busy")

	v.status, v.answer = 402, `{}`
	status, body, _ = sendMessages(t, h, nil, messagesFor("anthropic/claude-sonnet-4.5"))
	expectAnthropicRefusal(t, status, body, 429, "rate_limit_error", "credit_exhausted")

	// A 200 the vendor did not meter is relayed as Chat relays one, and
	// recorded failed without counts.
	v.status, v.answer = 0, `{"type":"message","model":"x","content":[{"type":"text","text":"hi"}]}`
	status, body, _ = sendMessages(t, h, nil, messagesFor("anthropic/claude-sonnet-4.5"))
	if status != 200 || !strings.Contains(string(body), `"hi"`) {
		t.Fatalf("unmetered answer: %d %s", status, body)
	}
	if ev := lastEvent(t, h); ev.Status != spool.StatusUpstreamErr || ev.ErrorType != "upstream_usage_missing" ||
		ev.InputTokens != 0 || ev.OutputTokens != 0 || ev.Estimated {
		t.Fatalf("record: %+v", ev)
	}

	// A 200 that does not parse.
	v.answer = `not json`
	status, body, _ = sendMessages(t, h, nil, messagesFor("anthropic/claude-sonnet-4.5"))
	expectAnthropicRefusal(t, status, body, 502, "api_error", "upstream_error")
}

func TestMessagesUnreadableCostKeepsCounts(t *testing.T) {
	h, v := messagesHarness(t, nil)
	v.answer = strings.Replace(vendorMessageAnswer, `"cost":0.0012`, `"cost":"n/a"`, 1)
	if status, body, _ := sendMessages(t, h, nil, messagesFor("vendor-model")); status != 200 {
		t.Fatalf("%d %s", status, body)
	}
	if ev := lastEvent(t, h); ev.Status != spool.StatusOK || ev.InputTokens != 35 || ev.OutputTokens != 4 || ev.CostUsd != "" {
		t.Fatalf("record: %+v", ev)
	}
}

func TestMessagesCountTokensIsUnknown(t *testing.T) {
	h, v := messagesHarness(t, nil)
	for _, c := range []struct{ path, token string }{
		{"/v1/messages/count_tokens?beta=true", testToken},
		{"/v1/messages/count_tokens", ""},
		{"/v1/messages/batches", testToken},
		{"/v1/messages/", testToken},
	} {
		token := c.token
		req, _ := http.NewRequest(http.MethodPost, h.gw.URL+c.path,
			strings.NewReader(messagesFor("anthropic/claude-sonnet-4.5")))
		if token != "" {
			req.Header.Set("X-Api-Key", token)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		expectAnthropicRefusal(t, resp.StatusCode, raw, 404, "not_found_error", "unknown_endpoint")
		if resp.Header.Get("Retry-After") != "" {
			t.Fatal("a 404 asks for no retry")
		}
	}
	if v.callCount() != 0 {
		t.Fatal("count_tokens reached the vendor")
	}
	if evs := h.spoolEvents(t); len(evs) != 0 {
		t.Fatalf("count_tokens was recorded: %+v", evs)
	}
}

const (
	vendorStreamStart = `{"type":"message_start","message":{"id":"msg_1","type":"message","role":"assistant","model":"` +
		upstreamModel + `","content":[],"usage":{"input_tokens":10,"cache_read_input_tokens":20,"cache_creation_input_tokens":5,"output_tokens":1}}}`
	vendorStreamBlock = `{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`
	vendorStreamText1 = `{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"안녕"}}`
	vendorStreamText2 = `{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"하세요"}}`
	vendorStreamBStop = `{"type":"content_block_stop","index":0}`
	vendorStreamDelta = `{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"input_tokens":null,"cache_read_input_tokens":null,"cache_creation_input_tokens":null,"output_tokens":7}}`
	vendorStreamStop  = `{"type":"message_stop"}`
)

func vendorEvent(name, data string) string { return "event: " + name + "\ndata: " + data + "\n\n" }

func TestMessagesStreamRelaysNamedEvents(t *testing.T) {
	h, v := messagesHarness(t, nil)
	v.stream = ": keepalive\n\n" +
		vendorEvent("message_start", vendorStreamStart) +
		vendorEvent("ping", `{"type": "ping"}`) +
		vendorEvent("content_block_start", vendorStreamBlock) +
		"event: content_block_delta\nid: 3\nretry: 10\ndata: " + vendorStreamText1 + "\n\n" +
		vendorEvent("content_block_delta", vendorStreamText2) +
		vendorEvent("content_block_stop", vendorStreamBStop) +
		vendorEvent("message_delta", `{"type":"message_delta","delta":{},"usage":{"output_tokens":3}}`) +
		vendorEvent("message_delta", vendorStreamDelta) +
		vendorEvent("message_stop", vendorStreamStop)

	status, body, hdr := sendMessages(t, h, nil,
		`{"model":"vendor-model","stream":true,"max_tokens":100,"messages":[{"role":"user","content":"hi"}]}`)
	if status != 200 || !strings.HasPrefix(hdr.Get("Content-Type"), "text/event-stream") {
		t.Fatalf("status %d %q: %s", status, hdr.Get("Content-Type"), body)
	}
	start := strings.Replace(vendorStreamStart, `"model":"`+upstreamModel+`"`, `"model":"vendor-model"`, 1)
	var startMap map[string]any
	_ = json.Unmarshal([]byte(start), &startMap)
	startOut, _ := json.Marshal(startMap)
	want := ": keepalive\n\n" +
		vendorEvent("message_start", string(startOut)) +
		vendorEvent("ping", `{"type": "ping"}`) +
		vendorEvent("content_block_start", vendorStreamBlock) +
		vendorEvent("content_block_delta", vendorStreamText1) +
		vendorEvent("content_block_delta", vendorStreamText2) +
		vendorEvent("content_block_stop", vendorStreamBStop) +
		vendorEvent("message_delta", `{"type":"message_delta","delta":{},"usage":{"output_tokens":3}}`) +
		vendorEvent("message_delta", vendorStreamDelta) +
		vendorEvent("message_stop", vendorStreamStop)
	if string(body) != want {
		t.Fatalf("relayed stream differs:\n got %q\nwant %q", body, want)
	}
	if strings.Contains(string(body), upstreamModel) {
		t.Fatal("upstream model name leaked")
	}
	ev := lastEvent(t, h)
	if ev.Status != spool.StatusOK || ev.Estimated || !ev.Streamed ||
		ev.InputTokens != 35 || ev.CachedInputTokens != 20 || ev.OutputTokens != 7 || ev.CostUsd != "" {
		t.Fatalf("record: %+v", ev)
	}
}

// A revised input count on message_delta replaces the one from message_start.
func TestMessagesStreamDeltaInputRevision(t *testing.T) {
	h, v := messagesHarness(t, nil)
	v.stream = vendorEvent("message_start", vendorStreamStart) +
		vendorEvent("message_delta", `{"type":"message_delta","delta":{},"usage":{"input_tokens":12,"cache_read_input_tokens":0,"output_tokens":2}}`) +
		vendorEvent("message_stop", vendorStreamStop)
	if status, body, _ := sendMessages(t, h, nil, messagesWith("vendor-model", "stream", "true")); status != 200 {
		t.Fatalf("%d %s", status, body)
	}
	ev := lastEvent(t, h)
	if ev.InputTokens != 17 || ev.CachedInputTokens != 0 || ev.OutputTokens != 2 {
		t.Fatalf("record: %+v", ev)
	}
}

func TestMessagesStreamEndings(t *testing.T) {
	cases := []struct {
		name, stream, errorType, tail string
	}{
		{"no usage", vendorEvent("message_start", `{"type":"message_start","message":{"model":"x"}}`) +
			vendorEvent("message_stop", vendorStreamStop), "upstream_usage_missing", "event: message_stop\n"},
		{"vendor error", vendorEvent("message_start", vendorStreamStart) +
			vendorEvent("error", `{"type":"error","error":{"type":"overloaded_error","message":"Overloaded"}}`),
			"upstream_error", "event: error\n"},
		{"cut short", vendorEvent("message_start", vendorStreamStart),
			"upstream_stream_unterminated", "event: error\n"},
		{"unparseable event", vendorEvent("message_start", vendorStreamStart) +
			vendorEvent("content_block_delta", `{"type":"content_bl`) +
			vendorEvent("message_delta", vendorStreamDelta) + vendorEvent("message_stop", vendorStreamStop),
			"upstream_chunk_unreadable", "event: message_stop\n"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			h, v := messagesHarness(t, nil)
			v.stream = c.stream
			status, body, _ := sendMessages(t, h, nil, messagesWith("vendor-model", "stream", "true"))
			if status != 200 {
				t.Fatalf("%d %s", status, body)
			}
			events := strings.Split(strings.TrimSpace(string(body)), "\n\n")
			last := events[len(events)-1] + "\n"
			if !strings.HasPrefix(last, c.tail) {
				t.Fatalf("stream ends with %q", last)
			}
			if c.tail == "event: error\n" {
				data := strings.TrimPrefix(strings.SplitN(last, "\n", 2)[1], "data: ")
				var e map[string]any
				if json.Unmarshal([]byte(strings.TrimSpace(data)), &e) != nil || e["type"] != "error" {
					t.Fatalf("error event is not the Anthropic shape: %q", last)
				}
			}
			if strings.Contains(string(body), "[DONE]") {
				t.Fatal("chat terminator on a Messages stream")
			}
			if ev := lastEvent(t, h); ev.Status != spool.StatusUpstreamErr || ev.ErrorType != c.errorType {
				t.Fatalf("record: %+v", ev)
			}
		})
	}
}

// The new refusals keep the catalogue's rules: an error status, a type from
// the gateway's set, a code no other error answers with unless the sharing is
// deliberate.
func TestMessagesErrorsAreWellFormed(t *testing.T) {
	added := map[string]apiError{
		"errMissingKeyMessages":         errMissingKeyMessages,
		"errModelNotOnMessages":         errModelNotOnMessages,
		"errFallbackModelNotAllowed(x)": errFallbackModelNotAllowed("x"),
	}
	okTypes := map[string]bool{
		"invalid_request_error": true, "authentication_error": true,
		"permission_error": true, "rate_limit_error": true, "server_error": true,
	}
	shared := map[string]bool{"missing_api_key": true, "model_not_allowed": true, "invalid_parameter_value": true}
	codes := map[string]bool{}
	for _, e := range allAPIErrors() {
		codes[e.code] = true
	}
	for name, e := range added {
		if !okTypes[e.typ] || e.status < 400 || e.status > 599 || e.code == "" || e.message == "" {
			t.Errorf("%s malformed: %+v", name, e)
		}
		if codes[e.code] && !shared[e.code] {
			t.Errorf("%s reuses code %q", name, e.code)
		}
	}
}

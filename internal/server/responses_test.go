package server

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/pnuops/pickle-llm-gateway/internal/snapshot"
	"github.com/pnuops/pickle-llm-gateway/internal/spool"
)

const vendorResponseAnswer = `{"id":"resp_1","object":"response","status":"completed","error":null,` +
	`"model":"` + upstreamModel + `","output":[` +
	`{"type":"reasoning","id":"rs_1","summary":[]},` +
	`{"type":"message","id":"msg_1","role":"assistant","content":[{"type":"output_text","text":"안녕"},{"type":"output_text","text":"하세요"}]}],` +
	`"usage":{"input_tokens":30,"input_tokens_details":{"cached_tokens":20},"output_tokens":9,` +
	`"output_tokens_details":{"reasoning_tokens":4},"total_tokens":39,"cost":0.0021}}`

// responsesHarness reuses the Messages vendor stand-in: it records the last
// request and answers whatever the test set, on any path.
func responsesHarness(t *testing.T, mutate func(*snapshot.Document)) (*harness, *vendorMessages) {
	t.Helper()
	h, v := messagesHarness(t, mutate)
	v.answer = vendorResponseAnswer
	return h, v
}

func sendResponses(t *testing.T, h *harness, hdr map[string]string, body string) (int, []byte, http.Header) {
	t.Helper()
	return sendTo(t, h, "/v1/responses", hdr, body)
}

func sendTo(t *testing.T, h *harness, path string, hdr map[string]string, body string) (int, []byte, http.Header) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, h.gw.URL+path, strings.NewReader(body))
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

func responsesFor(model string) string {
	return `{"model":"` + model + `","input":"hi"}`
}

func responsesWith(model, field, value string) string {
	return `{"model":"` + model + `","` + field + `":` + value + `,"input":"hi"}`
}

func TestResponsesNonStreamRoundTrip(t *testing.T) {
	h, v := responsesHarness(t, nil)
	status, body, hdr := sendResponses(t, h, map[string]string{
		"X-Session-Id":      "s1",
		"Session-Id":        "codex-session",
		"X-Codex-Window-Id": "w1",
	}, `{"model":"vendor-model","input":[{"role":"user","content":"hi"}],"store":false,"max_output_tokens":100,`+
		`"include":["reasoning.encrypted_content"],"prompt_cache_key":"k","reasoning":{"effort":"low"}}`)
	if status != 200 {
		t.Fatalf("status %d: %s", status, body)
	}
	if v.path != "/responses" || v.auth != "Bearer "+keyCred {
		t.Fatalf("upstream got path %q auth %q", v.path, v.auth)
	}
	if string(v.body["model"]) != `"`+upstreamModel+`"` || string(v.body["max_output_tokens"]) != "100" ||
		string(v.body["store"]) != "false" || string(v.body["prompt_cache_key"]) != `"k"` {
		t.Fatalf("upstream body: %v", v.body)
	}
	if v.headers.Get("X-Session-Id") != "s1" || v.headers.Get("Session-Id") != "" || v.headers.Get("X-Codex-Window-Id") != "" {
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
	if ev.Endpoint != "responses" || ev.Status != spool.StatusOK || ev.Estimated ||
		ev.InputTokens != 30 || ev.CachedInputTokens != 20 || ev.OutputTokens != 9 || ev.ReasoningTokens != 4 ||
		ev.CostUsd != "0.0021" || ev.BudgetAxis != snapshot.AxisCredit {
		t.Fatalf("record: %+v", ev)
	}
}

func TestResponsesSendsNoInventedFields(t *testing.T) {
	h, v := responsesHarness(t, nil)
	if status, out, _ := sendResponses(t, h, nil, responsesFor("anthropic/claude-sonnet-4.5")); status != 200 {
		t.Fatalf("status %d: %s", status, out)
	}
	for _, f := range []string{"max_tokens", "max_output_tokens", "stream_options", "store"} {
		if _, ok := v.body[f]; ok {
			t.Fatalf("gateway added %s", f)
		}
	}
}

func TestResponsesOutputCapOverModelMaximum(t *testing.T) {
	h, v := responsesHarness(t, func(d *snapshot.Document) {
		for i := range d.Models {
			if d.Models[i].PublicName == "vendor-model" {
				d.Models[i].MaxOutputTokens = 50
			}
		}
	})
	status, body, _ := sendResponses(t, h, nil, responsesWith("vendor-model", "max_output_tokens", "100"))
	if status != 400 || errCode(t, body) != "output_limit_exceeded" ||
		!strings.Contains(errMessage(t, body), "max_output_tokens 값을 50 이하로") {
		t.Fatalf("%d %s", status, body)
	}
	if v.callCount() != 0 {
		t.Fatal("refused request reached the vendor")
	}
	// No cap asked for: the model's own goes out on the surface's field.
	if status, body, _ := sendResponses(t, h, nil, responsesFor("vendor-model")); status != 200 ||
		string(v.body["max_output_tokens"]) != "50" {
		t.Fatalf("%d %s / %s", status, body, v.body["max_output_tokens"])
	}
}

func TestResponsesRequiredFields(t *testing.T) {
	h, v := responsesHarness(t, nil)
	for _, c := range []struct{ body, field string }{
		{`{"input":"hi"}`, "model"},
		{`{"model":"","input":"hi"}`, "model"},
		{`{"model":"vendor-model"}`, "input"},
		{`{"model":"vendor-model","input":null}`, "input"},
	} {
		status, body, _ := sendResponses(t, h, nil, c.body)
		if status != 400 || errCode(t, body) != "missing_parameter" || !strings.Contains(errMessage(t, body), c.field) {
			t.Errorf("%s: %d %s", c.body, status, body)
		}
	}
	if v.callCount() != 0 {
		t.Fatal("refused request reached the vendor")
	}
}

// Asking for a stored response, or naming one, is refused with the reason;
// the fields left off or set to their stateless values pass.
func TestResponsesRefusesStoredState(t *testing.T) {
	h, v := responsesHarness(t, nil)
	for _, c := range []struct{ field, value, advice string }{
		{"store", "true", "input에 모두 담아"},
		{"previous_response_id", `"resp_123"`, "이전 응답을 이어 쓸 수 없습니다"},
		{"previous_response_id", `""`, "이전 응답을 이어 쓸 수 없습니다"},
		{"conversation", `"conv_1"`, "대화를 서버에 저장하지 않습니다"},
		{"conversation", `{"id":"conv_1"}`, "대화를 서버에 저장하지 않습니다"},
		{"background", "true", "background를 빼거나"},
	} {
		status, body, _ := sendResponses(t, h, nil, responsesWith("vendor-model", c.field, c.value))
		msg := errMessage(t, body)
		if status != 400 || errCode(t, body) != "unsupported_parameter" ||
			!strings.Contains(msg, c.field) || !strings.Contains(msg, c.advice) {
			t.Errorf("%s=%s: %d %s", c.field, c.value, status, body)
		}
	}
	for _, c := range []struct{ field, value string }{{"store", `"yes"`}, {"background", "1"}} {
		status, body, _ := sendResponses(t, h, nil, responsesWith("vendor-model", c.field, c.value))
		if status != 400 || errCode(t, body) != "invalid_parameter_value" {
			t.Errorf("%s=%s: %d %s", c.field, c.value, status, body)
		}
	}
	if v.callCount() != 0 {
		t.Fatal("refused request reached the vendor")
	}
	if ev := lastEvent(t, h); ev.Status != spool.StatusBadRequest || ev.Endpoint != "responses" {
		t.Fatalf("record: %+v", ev)
	}
	for _, c := range []struct{ field, value string }{
		{"store", "false"}, {"store", "null"}, {"previous_response_id", "null"},
		{"conversation", "null"}, {"background", "false"},
	} {
		if status, body, _ := sendResponses(t, h, nil, responsesWith("vendor-model", c.field, c.value)); status != 200 {
			t.Errorf("%s=%s: %d %s", c.field, c.value, status, body)
		}
	}
}

func TestResponsesNeedsChatGrant(t *testing.T) {
	h, v := responsesHarness(t, func(d *snapshot.Document) { d.Keys[0].PassthroughEndpoints = nil })
	status, body, _ := sendResponses(t, h, nil, responsesFor("vendor-model"))
	if status != 403 || errCode(t, body) != "endpoint_not_allowed" || !strings.Contains(errMessage(t, body), "Chat(chat)") {
		t.Fatalf("%d %s", status, body)
	}
	if v.callCount() != 0 {
		t.Fatal("refused request reached the vendor")
	}
}

func TestResponsesRefusesSelfHostedModel(t *testing.T) {
	h, v := responsesHarness(t, nil)
	for _, model := range []string{"pickle-general", "campus-x"} {
		status, body, _ := sendResponses(t, h, nil, responsesFor(model))
		if status != 400 || errCode(t, body) != "model_not_available_on_route" ||
			!strings.Contains(errMessage(t, body), "/v1/responses") {
			t.Fatalf("%s: %d %s", model, status, body)
		}
	}
	if v.callCount() != 0 {
		t.Fatal("self-hosted request reached an upstream")
	}
}

// Tools declared inside the input are fenced like the top-level list: Codex
// sends its tools as one input item, and a client-run tool search answers
// with the tools it loaded.
func TestResponsesInputToolsAreFenced(t *testing.T) {
	h, v := responsesHarness(t, nil)
	codexItem := `{"type":"additional_tools","role":"developer","tools":[` +
		`{"type":"namespace","name":"functions","tools":[` +
		`{"type":"custom","name":"exec","format":{"type":"grammar","syntax":"lark","definition":"start: /.+/"}},` +
		`{"type":"function","name":"wait","parameters":{"type":"object"}}]}]}`
	ok := `{"model":"vendor-model","input":[` + codexItem + `,{"role":"user","content":"hi"}]}`
	if status, body, _ := sendResponses(t, h, nil, ok); status != 200 {
		t.Fatalf("Codex tool item: %d %s", status, body)
	}
	for _, c := range []struct{ item, code string }{
		{`{"type":"additional_tools","tools":[{"type":"web_search","external_web_access":false}]}`, "endpoint_not_allowed"},
		{`{"type":"additional_tools","tools":[{"type":"namespace","name":"n","tools":[{"type":"local_shell"}]}]}`, "endpoint_not_allowed"},
		{`{"type":"tool_search_output","call_id":"c","tools":[{"type":"code_interpreter","container":{"type":"auto"}}]}`, "endpoint_not_allowed"},
		{`{"type":"additional_tools","tools":[{"type":"openrouter:files"}]}`, "unsupported_parameter"},
		{`{"type":"additional_tools","tools":{"type":"shell"}}`, "invalid_parameter_value"},
	} {
		body := `{"model":"vendor-model","input":[` + c.item + `]}`
		status, out, _ := sendResponses(t, h, nil, body)
		if status/100 != 4 || errCode(t, out) != c.code {
			t.Errorf("%s: %d %s", c.item, status, out)
		}
		if c.code == "invalid_parameter_value" && !strings.Contains(errMessage(t, out), "input") {
			t.Errorf("refusal does not name input: %s", out)
		}
	}
	if v.callCount() != 1 {
		t.Fatalf("refused requests reached the vendor: %d calls", v.callCount())
	}
}

// The standard spelling Codex sends for its default web search needs the web
// search grant, in tools and as a tool_choice; the other surfaces' fences run
// here too.
func TestResponsesToolFences(t *testing.T) {
	h, v := responsesHarness(t, nil)
	for _, c := range []struct{ field, value, code string }{
		{"tools", `[{"type":"web_search","external_web_access":false}]`, "endpoint_not_allowed"},
		{"tools", `[{"type":"code_interpreter","container":{"type":"auto"}}]`, "endpoint_not_allowed"},
		{"tools", `[{"type":"local_shell"}]`, "endpoint_not_allowed"},
		{"tools", `[{"type":"shell","environment":{"type":"local"}}]`, "endpoint_not_allowed"},
		{"tools", `[{"type":"image_generation"}]`, "endpoint_not_allowed"},
		{"tools", `[{"type":"openrouter:files"}]`, "unsupported_parameter"},
		{"tool_choice", `{"type":"web_search_preview"}`, "endpoint_not_allowed"},
		{"deferred_tools", `[{"type":"openrouter:shell"}]`, "endpoint_not_allowed"},
		{"models", `["vendor-model","other/model:online"]`, "endpoint_not_allowed"},
		{"plugins", `[{"id":"web"}]`, "endpoint_not_allowed"},
		{"preset", `"@preset/x"`, "preset_not_supported"},
	} {
		status, out, _ := sendResponses(t, h, nil, responsesWith("vendor-model", c.field, c.value))
		if status/100 != 4 || errCode(t, out) != c.code {
			t.Errorf("%s=%s: %d %s", c.field, c.value, status, out)
		}
	}
	if v.callCount() != 0 {
		t.Fatal("refused request reached the vendor")
	}
	for _, c := range []struct{ field, value string }{
		{"tools", `[{"type":"function","name":"f","parameters":{"type":"object"}},{"type":"apply_patch"},` +
			`{"type":"mcp","server_label":"s","server_url":"https://example.com/mcp"},{"type":"file_search","vector_store_ids":["v"]}]`},
		{"tool_choice", `"auto"`},
		{"include", `["reasoning.encrypted_content"]`},
		{"reasoning", `{"effort":"low","context":"all_turns"}`},
	} {
		if status, out, _ := sendResponses(t, h, nil, responsesWith("vendor-model", c.field, c.value)); status != 200 {
			t.Errorf("%s=%s: %d %s", c.field, c.value, status, out)
		}
	}
}

func TestResponsesSubpathsAreUnknown(t *testing.T) {
	h, v := responsesHarness(t, nil)
	for _, c := range []struct{ method, path string }{
		{http.MethodGet, "/v1/responses/resp_1"},
		{http.MethodPost, "/v1/responses/resp_1/cancel"},
		{http.MethodGet, "/v1/responses/resp_1/input_items"},
		{http.MethodPost, "/v1/responses/compact"},
		{http.MethodPost, "/v1/responses/"},
	} {
		req, _ := http.NewRequest(c.method, h.gw.URL+c.path, strings.NewReader(responsesFor("vendor-model")))
		req.Header.Set("Authorization", "Bearer "+testToken)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 404 || errCode(t, raw) != "unknown_endpoint" {
			t.Errorf("%s %s: %d %s", c.method, c.path, resp.StatusCode, raw)
		}
	}
	if v.callCount() != 0 {
		t.Fatal("a subpath reached the vendor")
	}
	if evs := h.spoolEvents(t); len(evs) != 0 {
		t.Fatalf("a subpath was recorded: %+v", evs)
	}
	req, _ := http.NewRequest(http.MethodGet, h.gw.URL+"/v1/responses", nil)
	req.Header.Set("Authorization", "Bearer "+testToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("GET /v1/responses: %d", resp.StatusCode)
	}
}

// A failed response inside a 200 is relayed and recorded as the vendor's
// error; an incomplete one with usage is an ordinary ending.
func TestResponsesNonStreamStatuses(t *testing.T) {
	for _, c := range []struct {
		name, answer, status, errorType string
	}{
		{"failed", `{"id":"r","status":"failed","error":{"code":"server_error","message":"x"},"model":"` + upstreamModel +
			`","output":[],"usage":{"input_tokens":3,"output_tokens":0}}`, spool.StatusUpstreamErr, "upstream_error"},
		{"incomplete", `{"id":"r","status":"incomplete","incomplete_details":{"reason":"max_output_tokens"},"error":null,"model":"` +
			upstreamModel + `","output":[],"usage":{"input_tokens":3,"output_tokens":5,"cost":0.0001}}`, spool.StatusOK, ""},
		{"no usage", `{"id":"r","status":"completed","error":null,"model":"` + upstreamModel + `","output":[]}`,
			spool.StatusUpstreamErr, "upstream_usage_missing"},
	} {
		t.Run(c.name, func(t *testing.T) {
			h, v := responsesHarness(t, nil)
			v.answer = c.answer
			status, body, _ := sendResponses(t, h, nil, responsesFor("vendor-model"))
			if status != 200 || strings.Contains(string(body), upstreamModel) {
				t.Fatalf("%d %s", status, body)
			}
			ev := lastEvent(t, h)
			if ev.Status != c.status || ev.ErrorType != c.errorType || ev.Estimated {
				t.Fatalf("record: %+v", ev)
			}
		})
	}
}

const (
	respCreated = `{"type":"response.created","sequence_number":0,"response":{"id":"resp_1","object":"response","status":"in_progress","model":"` +
		upstreamModel + `","output":[],"usage":null}}`
	respInProgress = `{"type":"response.in_progress","sequence_number":1,"response":{"id":"resp_1","status":"in_progress","model":"` +
		upstreamModel + `","usage":null}}`
	respItemAdded = `{"type":"response.output_item.added","sequence_number":2,"output_index":0,"item":{"type":"message","id":"msg_1","role":"assistant","content":[]}}`
	respDelta1    = `{"type":"response.output_text.delta","sequence_number":3,"item_id":"msg_1","output_index":0,"content_index":0,"delta":"안녕"}`
	respDelta2    = `{"type":"response.output_text.delta","sequence_number":4,"item_id":"msg_1","output_index":0,"content_index":0,"delta":"하세요"}`
	respCompleted = `{"type":"response.completed","sequence_number":5,"response":{"id":"resp_1","status":"completed","error":null,"model":"` +
		upstreamModel + `","usage":{"input_tokens":30,"input_tokens_details":{"cached_tokens":20},"output_tokens":9,` +
		`"output_tokens_details":{"reasoning_tokens":4},"total_tokens":39,"cost":0.0021}}}`
)

func dataEvent(data string) string { return "data: " + data + "\n\n" }

func renamed(t *testing.T, data string) string {
	t.Helper()
	var m map[string]any
	dec := json.NewDecoder(strings.NewReader(strings.Replace(data, `"model":"`+upstreamModel+`"`, `"model":"vendor-model"`, 1)))
	dec.UseNumber()
	if err := dec.Decode(&m); err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(m)
	return string(b)
}

// The vendor's stream shape: data lines only, ended by [DONE]. It is relayed
// event for event with the model renamed, and metered from the ending.
func TestResponsesStreamRelaysEvents(t *testing.T) {
	h, v := responsesHarness(t, nil)
	v.stream = ": OPENROUTER PROCESSING\n\n" +
		dataEvent(respCreated) + dataEvent(respInProgress) + dataEvent(respItemAdded) +
		dataEvent(respDelta1) + dataEvent(respDelta2) + dataEvent(respCompleted) + "data: [DONE]\n\n"
	status, body, hdr := sendResponses(t, h, nil, `{"model":"vendor-model","stream":true,"input":"hi","store":false}`)
	if status != 200 || !strings.HasPrefix(hdr.Get("Content-Type"), "text/event-stream") {
		t.Fatalf("status %d %q: %s", status, hdr.Get("Content-Type"), body)
	}
	want := ": OPENROUTER PROCESSING\n\n" +
		dataEvent(renamed(t, respCreated)) + dataEvent(renamed(t, respInProgress)) + dataEvent(respItemAdded) +
		dataEvent(respDelta1) + dataEvent(respDelta2) + dataEvent(renamed(t, respCompleted)) + "data: [DONE]\n\n"
	if string(body) != want {
		t.Fatalf("relayed stream differs:\n got %q\nwant %q", body, want)
	}
	if strings.Contains(string(body), upstreamModel) {
		t.Fatal("upstream model name leaked")
	}
	if _, ok := v.body["stream_options"]; ok {
		t.Fatal("gateway added stream_options")
	}
	ev := lastEvent(t, h)
	if ev.Status != spool.StatusOK || ev.Estimated || !ev.Streamed || ev.Endpoint != "responses" ||
		ev.InputTokens != 30 || ev.CachedInputTokens != 20 || ev.OutputTokens != 9 || ev.ReasoningTokens != 4 ||
		ev.CostUsd != "0.0021" {
		t.Fatalf("record: %+v", ev)
	}
}

// OpenAI's own servers, and the self-hosted server, name each event on an
// event: line and send no [DONE]. Read by the data's type all the same, and
// closed with [DONE] so a client sees one ending shape.
func TestResponsesStreamNamedEventsWithoutDone(t *testing.T) {
	h, v := responsesHarness(t, nil)
	v.stream = "event: response.created\n" + dataEvent(respCreated) +
		"event: response.output_text.delta\n" + dataEvent(respDelta1) +
		"event: response.completed\n" + dataEvent(respCompleted)
	status, body, _ := sendResponses(t, h, nil, `{"model":"vendor-model","stream":true,"input":"hi"}`)
	if status != 200 {
		t.Fatalf("%d %s", status, body)
	}
	want := dataEvent(renamed(t, respCreated)) + dataEvent(respDelta1) + dataEvent(renamed(t, respCompleted)) + "data: [DONE]\n\n"
	if string(body) != want {
		t.Fatalf("relayed stream differs:\n got %q\nwant %q", body, want)
	}
	if ev := lastEvent(t, h); ev.Status != spool.StatusOK || ev.OutputTokens != 9 {
		t.Fatalf("record: %+v", ev)
	}
}

func TestResponsesStreamEndings(t *testing.T) {
	incomplete := `{"type":"response.incomplete","response":{"id":"r","status":"incomplete","incomplete_details":{"reason":"max_output_tokens"},` +
		`"model":"` + upstreamModel + `","usage":{"input_tokens":3,"output_tokens":16,"cost":0.0001}}}`
	failed := `{"type":"response.failed","response":{"id":"r","status":"failed","error":{"code":"server_error","message":"boom"},` +
		`"model":"` + upstreamModel + `","usage":{"input_tokens":3,"output_tokens":1}}}`
	streamError := `{"type":"error","code":"rate_limit_exceeded","message":"slow down","param":null}`
	cases := []struct {
		name, stream, status, errorType string
		tailType                        string
	}{
		{"incomplete with usage", dataEvent(respCreated) + dataEvent(incomplete) + "data: [DONE]\n\n",
			spool.StatusOK, "", "response.incomplete"},
		{"failed", dataEvent(respCreated) + dataEvent(failed) + "data: [DONE]\n\n",
			spool.StatusUpstreamErr, "upstream_error", "response.failed"},
		{"stream error", dataEvent(respCreated) + dataEvent(streamError) + "data: [DONE]\n\n",
			spool.StatusUpstreamErr, "upstream_error", "error"},
		{"no usage", dataEvent(respCreated) +
			dataEvent(`{"type":"response.completed","response":{"id":"r","status":"completed","model":"x"}}`),
			spool.StatusUpstreamErr, "upstream_usage_missing", "response.completed"},
		{"cut short", dataEvent(respCreated) + dataEvent(respDelta1),
			spool.StatusUpstreamErr, "upstream_stream_unterminated", "error"},
		{"done without an ending", dataEvent(respCreated) + dataEvent(respDelta1) + "data: [DONE]\n\n",
			spool.StatusUpstreamErr, "upstream_stream_unterminated", "error"},
		{"unparseable event", dataEvent(respCreated) + dataEvent(`{"type":"response.out`) + dataEvent(respCompleted),
			spool.StatusUpstreamErr, "upstream_chunk_unreadable", "response.completed"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			h, v := responsesHarness(t, nil)
			v.stream = c.stream
			status, body, _ := sendResponses(t, h, nil, responsesWith("vendor-model", "stream", "true"))
			if status != 200 {
				t.Fatalf("%d %s", status, body)
			}
			if strings.Contains(string(body), upstreamModel) {
				t.Fatalf("upstream model leaked: %s", body)
			}
			events := strings.Split(strings.TrimSpace(string(body)), "\n\n")
			if events[len(events)-1] != "data: [DONE]" || strings.Count(string(body), "[DONE]") != 1 {
				t.Fatalf("stream does not end with one [DONE]: %q", body)
			}
			var last map[string]any
			if json.Unmarshal([]byte(strings.TrimPrefix(events[len(events)-2], "data: ")), &last) != nil ||
				last["type"] != c.tailType {
				t.Fatalf("last event before [DONE]: %q", events[len(events)-2])
			}
			ev := lastEvent(t, h)
			if ev.Status != c.status || ev.ErrorType != c.errorType || ev.Estimated {
				t.Fatalf("record: %+v", ev)
			}
			if c.name == "incomplete with usage" && (ev.OutputTokens != 16 || ev.CostUsd != "0.0001") {
				t.Fatalf("incomplete usage: %+v", ev)
			}
			if c.name == "failed" && ev.InputTokens != 3 {
				t.Fatalf("failed usage: %+v", ev)
			}
		})
	}
}

// The gateway's own stream error is the standard error event, then [DONE].
func TestResponsesStreamErrorEventShape(t *testing.T) {
	x := &responsesStream{}
	out := string(x.errorEvent("upstream_stream_interrupted", "끊겼습니다"))
	parts := strings.Split(strings.TrimSpace(out), "\n\n")
	if len(parts) != 2 || parts[1] != "data: [DONE]" {
		t.Fatalf("%q", out)
	}
	var e map[string]any
	if json.Unmarshal([]byte(strings.TrimPrefix(parts[0], "data: ")), &e) != nil ||
		e["type"] != "error" || e["code"] != "upstream_stream_interrupted" || e["message"] != "끊겼습니다" {
		t.Fatalf("%q", out)
	}
}

// A model named at the top of an event this relay does not know is renamed
// too, rather than carried out verbatim.
func TestResponsesStreamRenamesAnyModel(t *testing.T) {
	h, v := responsesHarness(t, nil)
	v.stream = dataEvent(respCreated) +
		dataEvent(`{"type":"response.vendor_extension","model":"`+upstreamModel+`"}`) +
		dataEvent(respCompleted)
	status, body, _ := sendResponses(t, h, nil, responsesWith("vendor-model", "stream", "true"))
	if status != 200 || strings.Contains(string(body), upstreamModel) {
		t.Fatalf("%d %s", status, body)
	}
}

// The generation id goes out on a paid answer, streamed or not.
func TestResponsesRelaysGenerationID(t *testing.T) {
	h, v := responsesHarness(t, nil)
	inner := h.srv.client.Transport
	h.srv.client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		resp, err := inner.RoundTrip(r)
		if err == nil {
			resp.Header.Set(generationIDHeader, testGenerationID)
		}
		return resp, err
	})}
	if _, _, hdr := sendResponses(t, h, nil, responsesFor("vendor-model")); hdr.Get(generationIDHeader) != testGenerationID {
		t.Fatalf("non-stream: %v", hdr)
	}
	v.stream = dataEvent(respCreated) + dataEvent(respCompleted)
	if _, _, hdr := sendResponses(t, h, nil, responsesWith("vendor-model", "stream", "true")); hdr.Get(generationIDHeader) != testGenerationID {
		t.Fatalf("stream: %v", hdr)
	}
}

func TestResponsesErrorsAreWellFormed(t *testing.T) {
	if e := errModelNotOnResponses; e.status != 400 || e.typ != "invalid_request_error" ||
		e.code != errModelNotOnMessages.code || !strings.Contains(e.message, "/v1/responses") {
		t.Fatalf("%+v", e)
	}
	if !strings.Contains(errNotFound.message, "POST /v1/responses") {
		t.Fatalf("unknown route message does not list the route: %s", errNotFound.message)
	}
}

package server

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/pnuops/pickle-llm-gateway/internal/snapshot"
	"github.com/pnuops/pickle-llm-gateway/internal/spool"
)

// The vendor's legacy completion: Chat's usage, with each choice's text in
// `text`.
const vendorCompletionAnswer = `{"id":"gen-1","object":"text_completion","created":1,"model":"` + upstreamModel + `",` +
	`"provider":"P","choices":[{"index":0,"text":" four five","finish_reason":"length","logprobs":null}],` +
	`"usage":{"prompt_tokens":3,"completion_tokens":2,"total_tokens":5,"cost":2.2e-7,` +
	`"completion_tokens_details":{"reasoning_tokens":1}}}`

// The vendor's stream, as measured: data lines, a usage chunk that still
// carries its choice (the finish reason a second time), then [DONE].
var (
	completionChunk1 = `{"id":"gen-1","object":"text_completion.chunk","created":1,"model":"` + upstreamModel + `",` +
		`"choices":[{"index":0,"finish_reason":null,"text":" four five"}]}`
	completionChunk2 = `{"id":"gen-1","object":"text_completion.chunk","created":1,"model":"` + upstreamModel + `",` +
		`"choices":[{"index":0,"finish_reason":"length","text":""}]}`
	completionUsage = `{"id":"gen-1","object":"text_completion.chunk","created":1,"model":"` + upstreamModel + `",` +
		`"choices":[{"index":0,"finish_reason":"length","text":""}],` +
		`"usage":{"prompt_tokens":3,"completion_tokens":4,"total_tokens":7,"cost":2.2e-7}}`
)

func completionsHarness(t *testing.T, mutate func(*snapshot.Document)) (*harness, *vendorMessages) {
	t.Helper()
	h, v := messagesHarness(t, mutate)
	v.answer = vendorCompletionAnswer
	v.stream = ": OPENROUTER PROCESSING\n\n" + dataEvent(completionChunk1) + dataEvent(completionChunk2) +
		dataEvent(completionUsage) + "data: [DONE]\n\n"
	return h, v
}

func completionsFor(model string) string {
	return `{"model":"` + model + `","prompt":"one two three"}`
}

func TestCompletionsNonStreamRoundTrip(t *testing.T) {
	h, v := completionsHarness(t, nil)
	status, body, hdr := sendTo(t, h, "/v1/completions", nil,
		`{"model":"vendor-model","prompt":"one two three","max_tokens":5,"temperature":0}`)
	if status != 200 {
		t.Fatalf("status %d: %s", status, body)
	}
	if v.path != "/completions" || v.auth != "Bearer "+keyCred {
		t.Fatalf("upstream got path %q auth %q", v.path, v.auth)
	}
	if string(v.body["model"]) != `"`+upstreamModel+`"` || string(v.body["prompt"]) != `"one two three"` ||
		string(v.body["max_tokens"]) != "5" || string(v.body["temperature"]) != "0" {
		t.Fatalf("upstream body: %v", v.body)
	}
	var out map[string]any
	_ = json.Unmarshal(body, &out)
	if out["model"] != "vendor-model" || strings.Contains(string(body), upstreamModel) {
		t.Fatalf("answer model not rewritten: %s", body)
	}
	if !strings.Contains(string(body), `"text":" four five"`) || hdr.Get("X-Request-Id") == "" {
		t.Fatalf("answer: %s", body)
	}
	ev := lastEvent(t, h)
	if ev.Endpoint != spool.EndpointCompletions || ev.Status != spool.StatusOK || ev.Estimated || ev.Streamed ||
		ev.InputTokens != 3 || ev.OutputTokens != 2 || ev.ReasoningTokens != 1 || ev.CostUsd != "2.2e-7" ||
		ev.BudgetAxis != snapshot.AxisCredit {
		t.Fatalf("record: %+v", ev)
	}
}

func TestCompletionsRequiredFields(t *testing.T) {
	h, v := completionsHarness(t, nil)
	for _, tc := range []struct{ body, param string }{
		{`{"prompt":"hi"}`, "model"},
		{`{"model":"","prompt":"hi"}`, "model"},
		{`{"models":["vendor-model"],"prompt":"hi"}`, "model"},
		{`{"model":"vendor-model"}`, "prompt"},
		{`{"model":"vendor-model","prompt":null}`, "prompt"},
	} {
		status, body, _ := sendTo(t, h, "/v1/completions", nil, tc.body)
		if status != 400 || errCode(t, body) != "missing_parameter" || !strings.Contains(errMessage(t, body), tc.param) {
			t.Fatalf("%s: %d %s", tc.body, status, body)
		}
	}
	if v.callCount() != 0 {
		t.Fatal("refused request reached the vendor")
	}
}

func TestCompletionsRefusesSelfHostedModel(t *testing.T) {
	h, v := completionsHarness(t, nil)
	for _, model := range []string{"pickle-general", "campus-x"} {
		status, body, _ := sendTo(t, h, "/v1/completions", nil, completionsFor(model))
		if status != 400 || errCode(t, body) != "model_not_available_on_route" ||
			!strings.Contains(errMessage(t, body), "/v1/completions") {
			t.Fatalf("%s: %d %s", model, status, body)
		}
	}
	if v.callCount() != 0 {
		t.Fatal("self-hosted request reached an upstream")
	}
}

func TestCompletionsNeedsChatGrant(t *testing.T) {
	h, v := completionsHarness(t, func(d *snapshot.Document) { d.Keys[0].PassthroughEndpoints = nil })
	status, body, _ := sendTo(t, h, "/v1/completions", nil, completionsFor("vendor-model"))
	if status != 403 || errCode(t, body) != "endpoint_not_allowed" || !strings.Contains(errMessage(t, body), "Chat(chat)") {
		t.Fatalf("%d %s", status, body)
	}
	if v.callCount() != 0 {
		t.Fatal("refused request reached the vendor")
	}
	if ev := lastEvent(t, h); ev.Endpoint != spool.EndpointCompletions || ev.Status != spool.StatusAuthRejected {
		t.Fatalf("record: %+v", ev)
	}
}

// Chat's fences run here: the key's money fence and the per-use grants read
// the same fields.
func TestCompletionsFences(t *testing.T) {
	h, v := completionsHarness(t, func(d *snapshot.Document) {
		d.Keys[0].CreditAllowedModels = []string{"vendor-model", "openai/*"}
	})
	for _, tc := range []struct {
		name, body string
		status     int
		code       string
	}{
		{"model outside the list", completionsFor("anthropic/claude-haiku-5.5"), 403, "model_not_allowed"},
		{"candidate outside the list", `{"model":"vendor-model","models":["anthropic/claude-haiku-5.5"],"prompt":"x"}`,
			403, "model_not_allowed"},
		{"web plugin without the grant", `{"model":"vendor-model","plugins":[{"id":"web"}],"prompt":"x"}`,
			403, "endpoint_not_allowed"},
		{"online suffix without the grant", completionsFor("openai/gpt-6-luna:online"), 403, "endpoint_not_allowed"},
		{"preset", `{"model":"vendor-model","preset":"p","prompt":"x"}`, 400, "preset_not_supported"},
	} {
		status, body, _ := sendTo(t, h, "/v1/completions", nil, tc.body)
		if status != tc.status || errCode(t, body) != tc.code {
			t.Fatalf("%s: %d %s", tc.name, status, body)
		}
	}
	if v.callCount() != 0 {
		t.Fatal("refused request reached the vendor")
	}
}

func TestCompletionsOutputCap(t *testing.T) {
	h, v := completionsHarness(t, func(d *snapshot.Document) {
		for i := range d.Models {
			if d.Models[i].PublicName == "vendor-model" {
				d.Models[i].MaxOutputTokens = 50
			}
		}
	})
	status, body, _ := sendTo(t, h, "/v1/completions", nil, `{"model":"vendor-model","prompt":"x","max_tokens":100}`)
	if status != 400 || errCode(t, body) != "output_limit_exceeded" ||
		!strings.Contains(errMessage(t, body), "max_tokens 값을 50 이하로") {
		t.Fatalf("%d %s", status, body)
	}
	if status, body, _ := sendTo(t, h, "/v1/completions", nil, completionsFor("vendor-model")); status != 200 ||
		string(v.body["max_tokens"]) != "50" {
		t.Fatalf("%d %s / %s", status, body, v.body["max_tokens"])
	}
	// The other output field is read too, refused past the cap, and folded
	// into max_tokens rather than forwarded beside it.
	status, body, _ = sendTo(t, h, "/v1/completions", nil, `{"model":"vendor-model","prompt":"x","max_completion_tokens":100}`)
	if status != 400 || errCode(t, body) != "output_limit_exceeded" {
		t.Fatalf("%d %s", status, body)
	}
	status, body, _ = sendTo(t, h, "/v1/completions", nil,
		`{"model":"vendor-model","prompt":"x","max_tokens":10,"max_completion_tokens":20}`)
	if _, forwarded := v.body["max_completion_tokens"]; status != 200 || forwarded || string(v.body["max_tokens"]) != "10" {
		t.Fatalf("%d %s / %v", status, body, v.body)
	}
}

// A stream is relayed chunk by chunk with the model renamed. The vendor's
// usage chunk still carries a choice, so a caller that did not ask for usage
// gets that chunk without its usage rather than not at all; one that asked
// gets it whole.
func TestCompletionsStream(t *testing.T) {
	for _, wantsUsage := range []bool{false, true} {
		h, v := completionsHarness(t, nil)
		body := `{"model":"vendor-model","prompt":"one two three","stream":true}`
		if wantsUsage {
			body = `{"model":"vendor-model","prompt":"one two three","stream":true,"stream_options":{"include_usage":true}}`
		}
		status, out, _ := sendTo(t, h, "/v1/completions", nil, body)
		if status != 200 {
			t.Fatalf("usage=%v: %d %s", wantsUsage, status, out)
		}
		if string(v.body["stream_options"]) != `{"include_usage":true}` {
			t.Fatalf("usage=%v: stream_options %s", wantsUsage, v.body["stream_options"])
		}
		s := string(out)
		if strings.Contains(s, upstreamModel) || !strings.HasSuffix(s, "data: [DONE]\n\n") ||
			strings.Count(s, `"model":"vendor-model"`) != 3 || !strings.Contains(s, `"text":" four five"`) {
			t.Fatalf("usage=%v: relayed %q", wantsUsage, s)
		}
		if strings.Contains(s, `"usage"`) != wantsUsage {
			t.Fatalf("usage=%v: relayed %q", wantsUsage, s)
		}
		ev := lastEvent(t, h)
		if ev.Status != spool.StatusOK || !ev.Streamed || ev.Estimated || ev.Endpoint != spool.EndpointCompletions ||
			ev.InputTokens != 3 || ev.OutputTokens != 4 || ev.CostUsd != "2.2e-7" {
			t.Fatalf("usage=%v: record %+v", wantsUsage, ev)
		}
	}
}

// Without usage the answer is relayed as it came and the record says usage
// was missing, with no estimate; the vendor's error inside a 200 is recorded
// as an upstream error.
func TestCompletionsNonStreamStatuses(t *testing.T) {
	h, v := completionsHarness(t, nil)
	v.answer = `{"id":"gen-1","object":"text_completion","model":"` + upstreamModel + `","choices":[{"index":0,"text":"x"}]}`
	if status, body, _ := sendTo(t, h, "/v1/completions", nil, completionsFor("vendor-model")); status != 200 {
		t.Fatalf("%d %s", status, body)
	}
	if ev := lastEvent(t, h); ev.Status != spool.StatusUpstreamErr || ev.ErrorType != "upstream_usage_missing" ||
		ev.InputTokens != 0 || ev.Estimated {
		t.Fatalf("no usage: %+v", ev)
	}
	v.answer = `{"id":"gen-1","object":"text_completion","model":"` + upstreamModel + `",` +
		`"choices":[{"index":0,"text":"","finish_reason":"error"}],"usage":{"prompt_tokens":3,"completion_tokens":0,"cost":1e-7}}`
	if status, body, _ := sendTo(t, h, "/v1/completions", nil, completionsFor("vendor-model")); status != 200 {
		t.Fatalf("%d %s", status, body)
	}
	if ev := lastEvent(t, h); ev.Status != spool.StatusUpstreamErr || ev.ErrorType != "upstream_error" ||
		ev.InputTokens != 3 || ev.CostUsd != "1e-7" {
		t.Fatalf("error in a 200: %+v", ev)
	}
}

// The text of a legacy chunk is read from `text`. It feeds the rate-limit
// estimate when a stream ends without usage, so reading the chat delta here
// would charge an unmetered stream nothing for its output.
func TestCompletionsChunkText(t *testing.T) {
	ck, ok := rewriteChunk([]byte(completionChunk1), "vendor-model", true)
	if !ok || ck.content != " four five" || ck.contentChars != len(" four five") {
		t.Fatalf("legacy chunk: %+v", ck)
	}
	if ck, _ := rewriteChunk([]byte(completionChunk1), "vendor-model", false); ck.contentChars != 0 {
		t.Fatalf("a chat reading found text in a legacy chunk: %+v", ck)
	}
	if x, _ := protoCompletions.newStream(nil, streamArgs{}, nil).(*chatStream); x == nil || !x.legacyText {
		t.Fatal("the completions stream reads chunks as chat deltas")
	}
	res := readCompletionsResponse(map[string]any{"choices": []any{map[string]any{"text": "abc"}}}, "vendor-model")
	if res.answer != "abc" {
		t.Fatalf("answer %q", res.answer)
	}
}

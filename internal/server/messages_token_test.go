package server

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pnuops/pickle-llm-gateway/internal/config"
	"github.com/pnuops/pickle-llm-gateway/internal/snapshot"
	"github.com/pnuops/pickle-llm-gateway/internal/spool"
)

// selfHostedMessages is a serving process's Messages endpoint and its token
// counter. hold, when set, keeps every Messages answer back until it is
// closed, so a test can fill the turn bound.
type selfHostedMessages struct {
	mu          sync.Mutex
	paths       []string
	bodies      []map[string]json.RawMessage
	count       int
	countStatus int
	answer      string
	stream      string
	hold        chan struct{}
}

func (u *selfHostedMessages) install(h *harness) {
	h.srv.client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		raw, _ := io.ReadAll(r.Body)
		body := map[string]json.RawMessage{}
		_ = json.Unmarshal(raw, &body)
		u.mu.Lock()
		u.paths = append(u.paths, r.URL.Path)
		u.bodies = append(u.bodies, body)
		count, countStatus, answer, stream, hold := u.count, u.countStatus, u.answer, u.stream, u.hold
		u.mu.Unlock()
		hdr := http.Header{"Content-Type": {"application/json"}}
		if r.URL.Path == "/tokenize" {
			// The serving process's chat tokenizer, beside the versioned API.
			return &http.Response{StatusCode: http.StatusOK, Header: hdr, Request: r,
				Body: io.NopCloser(strings.NewReader(`{"count":` + itoa(count) + `,"max_model_len":131072,"tokens":[1,2,3]}`))}, nil
		}
		if strings.HasSuffix(r.URL.Path, "/count_tokens") {
			if countStatus == 0 {
				countStatus = http.StatusOK
			}
			out := `{"input_tokens":` + itoa(count) + `}`
			if countStatus != http.StatusOK {
				out = `{"error":{"type":"internal_error","message":"tokenizer down"}}`
			}
			return &http.Response{StatusCode: countStatus, Header: hdr, Request: r,
				Body: io.NopCloser(strings.NewReader(out))}, nil
		}
		if hold != nil {
			select {
			case <-hold:
			case <-r.Context().Done():
				return nil, r.Context().Err()
			}
		}
		var streamed bool
		_ = json.Unmarshal(body["stream"], &streamed)
		out := answer
		if streamed {
			out = stream
			hdr.Set("Content-Type", "text/event-stream")
		}
		return &http.Response{StatusCode: http.StatusOK, Header: hdr, Request: r,
			Body: io.NopCloser(strings.NewReader(out))}, nil
	})}
}

func itoa(n int) string { b, _ := json.Marshal(n); return string(b) }

func (u *selfHostedMessages) calls() ([]string, []map[string]json.RawMessage) {
	u.mu.Lock()
	defer u.mu.Unlock()
	return append([]string(nil), u.paths...), append([]map[string]json.RawMessage(nil), u.bodies...)
}

// The serving process with prefix-cache details on: input_tokens is the part
// not read from the cache, and the three together are the whole input.
const selfHostedAnswer = `{"id":"msg_1","type":"message","role":"assistant","model":"` + upstreamModel + `",` +
	`"content":[{"type":"text","text":"안녕하세요"}],"stop_reason":"end_turn",` +
	`"usage":{"input_tokens":1850,"output_tokens":7,"cache_read_input_tokens":4192,"cache_creation_input_tokens":0}}`

const selfHostedStream = "event: message_start\n" +
	`data: {"type":"message_start","message":{"id":"msg_1","type":"message","role":"assistant","model":"` + upstreamModel + `","content":[],"usage":{"input_tokens":1850,"output_tokens":0,"cache_read_input_tokens":4192}}}` + "\n\n" +
	"event: content_block_start\n" +
	`data: {"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"t1","name":"write_file","input":{}}}` + "\n\n" +
	"event: content_block_delta\n" +
	`data: {"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"path\":\"a\"}"}}` + "\n\n" +
	"event: message_delta\n" +
	`data: {"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":9}}` + "\n\n" +
	"event: message_stop\n" +
	`data: {"type":"message_stop"}` + "\n\n"

// selfHostedHarness opens the mock upstream to Messages with the given bounds.
func selfHostedHarness(t *testing.T, maxInput, maxConcurrent int, wait time.Duration, mutate func(*snapshot.Document)) (*harness, *selfHostedMessages) {
	t.Helper()
	h := newHarness(t, mutate, func(c *config.Config) {
		up := c.Upstreams["mock"]
		up.Messages = config.MessagesOpen
		up.MessagesMaxInputTokens = maxInput
		up.MessagesMaxConcurrent = maxConcurrent
		up.MessagesQueueWait = wait
		c.Upstreams["mock"] = up
	})
	u := &selfHostedMessages{answer: selfHostedAnswer, stream: selfHostedStream, count: 100}
	u.install(h)
	return h, u
}

const agentTurnBody = `{"model":"pickle-general","max_tokens":32000,"system":[{"type":"text","text":"sys","cache_control":{"type":"ephemeral"}}],` +
	`"messages":[{"role":"user","content":"hi"}],` +
	`"tools":[{"name":"mcp__fs__write_file","description":"w","input_schema":{"type":"object"},"cache_control":{"type":"ephemeral"}}],` +
	`"tool_choice":{"type":"auto"},` +
	`"thinking":{"type":"enabled","budget_tokens":1024},"output_config":{"effort":"high"},` +
	`"metadata":{"user_id":"u"},"context_management":{"edits":[]}}`

func TestSelfHostedMessagesClosedByDefault(t *testing.T) {
	h := newHarness(t, nil, nil)
	u := &selfHostedMessages{answer: selfHostedAnswer}
	u.install(h)
	code, raw, _ := sendMessages(t, h, nil, messagesFor("pickle-general"))
	expectAnthropicRefusal(t, code, raw, 400, "invalid_request_error", "model_not_available_on_route")
	if paths, _ := u.calls(); len(paths) != 0 {
		t.Fatalf("a closed upstream was called: %v", paths)
	}
}

func TestSelfHostedMessagesServedAndMeteredOnTotalInput(t *testing.T) {
	h, u := selfHostedHarness(t, 0, 0, 0, nil)
	code, raw, _ := sendMessages(t, h, nil, agentTurnBody)
	if code != 200 || !strings.Contains(string(raw), `"model":"pickle-general"`) {
		t.Fatalf("%d %s", code, raw)
	}
	paths, bodies := u.calls()
	if len(paths) != 1 || !strings.HasSuffix(paths[0], "/messages") {
		t.Fatalf("upstream calls: %v", paths)
	}
	sent := bodies[0]
	for _, gone := range []string{"thinking", "output_config", "metadata", "context_management"} {
		if _, ok := sent[gone]; ok {
			t.Fatalf("%s reached the self-hosted upstream: %s", gone, sent[gone])
		}
	}
	// The agent's large fixed output request is lowered to the model maximum.
	if string(sent["max_tokens"]) != "4096" || string(sent["model"]) != `"`+upstreamModel+`"` {
		t.Fatalf("shaped body: max_tokens %s model %s", sent["max_tokens"], sent["model"])
	}
	ev := lastEvent(t, h)
	// Total input is the uncached part plus what was read and written in the
	// cache, as chat's prompt_tokens is; the daily allowance sums this field.
	if ev.Status != spool.StatusOK || ev.BudgetAxis != snapshot.AxisToken || ev.Endpoint != spool.EndpointMessages ||
		ev.InputTokens != 1850+4192 || ev.CachedInputTokens != 4192 || ev.OutputTokens != 7 || ev.Estimated {
		t.Fatalf("record: %+v", ev)
	}
}

func TestSelfHostedMessagesStreamMeteredOnTotalInput(t *testing.T) {
	h, _ := selfHostedHarness(t, 0, 0, 0, nil)
	body := strings.Replace(agentTurnBody, `"max_tokens":32000`, `"max_tokens":100,"stream":true`, 1)
	code, raw, _ := sendMessages(t, h, nil, body)
	if code != 200 || !strings.Contains(string(raw), "message_stop") {
		t.Fatalf("%d %s", code, raw)
	}
	ev := lastEvent(t, h)
	if ev.Status != spool.StatusOK || ev.InputTokens != 1850+4192 || ev.CachedInputTokens != 4192 || ev.OutputTokens != 9 {
		t.Fatalf("record: %+v", ev)
	}
}

func TestSelfHostedMessagesFieldRules(t *testing.T) {
	h, u := selfHostedHarness(t, 0, 0, 0, nil)
	for _, c := range []struct {
		name, body, code string
	}{
		{"template switch", `{"model":"pickle-general","messages":[],"chat_template_kwargs":{"enable_thinking":true}}`, "unsupported_parameter"},
		{"thinking budget", `{"model":"pickle-general","messages":[],"thinking_token_budget":10}`, "unsupported_parameter"},
		{"server tool", `{"model":"pickle-general","messages":[],"tools":[{"type":"web_search_20250305","name":"web_search"}]}`, "invalid_parameter_value"},
		{"chat-shaped tool", `{"model":"pickle-general","messages":[],"tools":[{"type":"function","function":{"name":"f"}}]}`, "invalid_parameter_value"},
		{"tool without schema", `{"model":"pickle-general","messages":[],"tools":[{"name":"f"}]}`, "invalid_parameter_value"},
		{"choice of undeclared tool", `{"model":"pickle-general","messages":[],"tools":[{"name":"f","input_schema":{}}],"tool_choice":{"type":"tool","name":"g"}}`, "invalid_parameter_value"},
		{"string choice", `{"model":"pickle-general","messages":[],"tool_choice":"auto"}`, "invalid_parameter_value"},
	} {
		code, raw, _ := sendMessages(t, h, nil, c.body)
		if code != 400 || !strings.Contains(string(raw), `"code":"`+c.code+`"`) {
			t.Fatalf("%s: %d %s", c.name, code, raw)
		}
	}
	if paths, _ := u.calls(); len(paths) != 0 {
		t.Fatalf("a refused request reached the upstream: %v", paths)
	}
	ok := `{"model":"pickle-general","max_tokens":10,"messages":[],"tools":[{"type":"custom","name":"f","input_schema":{},"strict":true}],` +
		`"tool_choice":{"type":"tool","name":"f","disable_parallel_tool_use":true},"top_k":5,"stop_sequences":["x"]}`
	if code, raw, _ := sendMessages(t, h, nil, ok); code != 200 {
		t.Fatalf("allowed fields refused: %d %s", code, raw)
	}

	// A field off the list is removed and the turn goes on: agent clients
	// add fields release by release and their users cannot stop them.
	unknown := `{"model":"pickle-general","max_tokens":10,"messages":[],"mcp_servers":[],"betas_hint":{"x":1}}`
	if code, raw, _ := sendMessages(t, h, nil, unknown); code != 200 {
		t.Fatalf("unknown fields refused: %d %s", code, raw)
	}
	_, bodies := u.calls()
	sent := bodies[len(bodies)-1]
	if _, has := sent["mcp_servers"]; has {
		t.Fatalf("unknown field forwarded: %v", sent)
	}
	if _, has := sent["betas_hint"]; has {
		t.Fatalf("unknown field forwarded: %v", sent)
	}
	if got := h.srv.metrics.messagesFieldsStripped.Load(); got != 2 {
		t.Fatalf("stripped fields counted %d, want 2", got)
	}
}

func TestSelfHostedMessagesInputCap(t *testing.T) {
	h, u := selfHostedHarness(t, 64, 0, 0, nil)
	// A body under the cap in bytes is certainly under it in tokens and is
	// not counted. Here the cap is tiny, so every body is counted.
	u.count = 65
	code, raw, _ := sendMessages(t, h, nil, agentTurnBody)
	expectAnthropicRefusal(t, code, raw, 400, "invalid_request_error", "input_too_long")
	var envelope struct {
		Error struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	_ = json.Unmarshal(raw, &envelope)
	if !strings.HasPrefix(envelope.Error.Message, "prompt is too long: 65 tokens > 64 maximum") ||
		!strings.Contains(envelope.Error.Message, "/compact") {
		t.Fatalf("message: %s", raw)
	}
	paths, bodies := u.calls()
	if len(paths) != 1 || !strings.HasSuffix(paths[0], "/messages/count_tokens") {
		t.Fatalf("upstream calls: %v", paths)
	}
	// The counter reads the input and nothing that steers generation.
	for name := range bodies[0] {
		switch name {
		case "model", "messages", "system", "tools", "tool_choice":
		default:
			t.Fatalf("counter got %s", name)
		}
	}
	if ev := lastEvent(t, h); ev.Status != spool.StatusBadRequest || ev.ErrorType != "input_too_long" {
		t.Fatalf("record: %+v", ev)
	}
	if h.srv.metrics.messagesInputRefused.Load() != 1 {
		t.Fatal("refusal not counted")
	}

	u.mu.Lock()
	u.count = 64
	u.mu.Unlock()
	if code, raw, _ := sendMessages(t, h, nil, agentTurnBody); code != 200 {
		t.Fatalf("input at the cap refused: %d %s", code, raw)
	}

	// A counter that fails admits the request; the upstream's own context
	// limit still stands behind it.
	u.mu.Lock()
	u.countStatus = http.StatusInternalServerError
	u.mu.Unlock()
	if code, raw, _ := sendMessages(t, h, nil, agentTurnBody); code != 200 {
		t.Fatalf("failed count refused the request: %d %s", code, raw)
	}
	if h.srv.metrics.messagesCountFailures.Load() == 0 {
		t.Fatal("count failure not counted")
	}
}

func TestSelfHostedMessagesSmallBodySkipsCounting(t *testing.T) {
	h, u := selfHostedHarness(t, 65536, 0, 0, nil)
	if code, raw, _ := sendMessages(t, h, nil, agentTurnBody); code != 200 {
		t.Fatalf("%d %s", code, raw)
	}
	if paths, _ := u.calls(); len(paths) != 1 || strings.HasSuffix(paths[0], "/count_tokens") {
		t.Fatalf("small body was counted: %v", paths)
	}
}

func TestSelfHostedMessagesTurnBound(t *testing.T) {
	h, u := selfHostedHarness(t, 0, 1, 50*time.Millisecond, nil)
	hold := make(chan struct{})
	u.hold = hold
	first := make(chan int, 1)
	go func() {
		code, _, _ := sendMessages(t, h, nil, agentTurnBody)
		first <- code
	}()
	deadline := time.Now().Add(5 * time.Second)
	for h.srv.agents.snapshot()["mock"]["inUse"] != 1 {
		if time.Now().After(deadline) {
			t.Fatal("first turn never took its place")
		}
		time.Sleep(5 * time.Millisecond)
	}
	start := time.Now()
	code, raw, hdr := sendMessages(t, h, nil, agentTurnBody)
	expectAnthropicRefusal(t, code, raw, 429, "rate_limit_error", "agent_busy")
	if hdr.Get("Retry-After") != "10" {
		t.Fatalf("Retry-After %q", hdr.Get("Retry-After"))
	}
	if time.Since(start) < 50*time.Millisecond {
		t.Fatal("the second turn did not wait before it was refused")
	}
	if ev := lastEvent(t, h); ev.Status != spool.StatusRateLimited || ev.ErrorType != "agent_concurrency" {
		t.Fatalf("record: %+v", ev)
	}
	close(hold)
	if c := <-first; c != 200 {
		t.Fatalf("first turn: %d", c)
	}
	if h.srv.agents.snapshot()["mock"]["inUse"] != 0 {
		t.Fatal("place not released")
	}
	// Chat is not bounded by the agent places.
	if rec := serveVariant(t, h, protoChat, nil, chatBody); rec.Code != 200 {
		t.Fatalf("chat: %d", rec.Code)
	}
}

func TestSelfHostedMessagesQueueAdmitsWhenFreed(t *testing.T) {
	h, u := selfHostedHarness(t, 0, 1, 5*time.Second, nil)
	hold := make(chan struct{})
	u.hold = hold
	first := make(chan int, 1)
	go func() {
		code, _, _ := sendMessages(t, h, nil, agentTurnBody)
		first <- code
	}()
	for h.srv.agents.snapshot()["mock"]["inUse"] != 1 {
		time.Sleep(5 * time.Millisecond)
	}
	second := make(chan int, 1)
	go func() {
		code, _, _ := sendMessages(t, h, nil, agentTurnBody)
		second <- code
	}()
	for h.srv.agents.snapshot()["mock"]["waiting"] != 1 {
		time.Sleep(5 * time.Millisecond)
	}
	close(hold)
	if a, b := <-first, <-second; a != 200 || b != 200 {
		t.Fatalf("turns: %d %d", a, b)
	}
}

func TestSelfHostedMessagesNeverFallsBack(t *testing.T) {
	h := newHarness(t, func(d *snapshot.Document) { d.Models[0].FallbackRef = "hosted" }, func(c *config.Config) {
		up := c.Upstreams["mock"]
		up.Messages = config.MessagesOpen
		c.Upstreams["mock"] = up
		// Open as well: the turn was counted and placed on the primary, so
		// no fallback takes it, open or not.
		c.Upstreams["hosted"] = config.Upstream{Ref: "hosted", BaseURL: "http://hosted.invalid/v1", CapField: "max_completion_tokens",
			Messages: config.MessagesOpen}
	})
	var mu sync.Mutex
	var hosts []string
	h.srv.client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		mu.Lock()
		hosts = append(hosts, r.URL.Host)
		mu.Unlock()
		return &http.Response{StatusCode: http.StatusBadGateway, Header: http.Header{}, Request: r,
			Body: io.NopCloser(strings.NewReader(`{}`))}, nil
	})}
	code, _, _ := sendMessages(t, h, nil, messagesFor("pickle-general"))
	if code < 500 {
		t.Fatalf("code %d", code)
	}
	mu.Lock()
	defer mu.Unlock()
	for _, host := range hosts {
		if host == "hosted.invalid" {
			t.Fatal("the fallback was attempted")
		}
	}
	if len(hosts) == 0 {
		t.Fatal("primary never attempted")
	}
}

func TestSelfHostedCountTokens(t *testing.T) {
	h, u := selfHostedHarness(t, 0, 0, 0, nil)
	u.count = 366
	post := func(body string) (int, []byte) {
		req, _ := http.NewRequest(http.MethodPost, h.gw.URL+"/v1/messages/count_tokens?beta=true", strings.NewReader(body))
		req.Header.Set("X-Api-Key", testToken)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		raw, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, raw
	}
	code, raw := post(agentTurnBody)
	if code != 200 || string(raw) != `{"input_tokens":366}`+"\n" && string(raw) != `{"input_tokens":366}` {
		t.Fatalf("%d %s", code, raw)
	}
	paths, bodies := u.calls()
	if len(paths) != 1 || !strings.HasSuffix(paths[0], "/messages/count_tokens") ||
		string(bodies[0]["model"]) != `"`+upstreamModel+`"` {
		t.Fatalf("upstream: %v %v", paths, bodies)
	}
	// Counting is not metered: no usage record.
	if events := h.spoolEvents(t); len(events) != 0 {
		t.Fatalf("count_tokens was recorded: %+v", events)
	}
	// An unknown model gets the route's 404, as a paid one does.
	code, raw = post(messagesFor("no-such-model"))
	expectAnthropicRefusal(t, code, raw, 404, "not_found_error", "unknown_endpoint")
}

// An agent turn cut off before its usage arrives is still metered, by
// estimate and flagged as one, on the self-hosted axis: the upstream already
// ran the prefill, and the record is what the daily allowance sums. The
// counted input stands in for the byte estimate when the body was counted.
func TestSelfHostedMessagesCutStreamKeepsEstimate(t *testing.T) {
	h, u := selfHostedHarness(t, 64, 0, 0, nil)
	u.count = 60
	u.stream = strings.Split(selfHostedStream, "event: message_delta")[0]
	body := strings.Replace(agentTurnBody, `"max_tokens":32000`, `"max_tokens":100,"stream":true`, 1)
	if code, raw, _ := sendMessages(t, h, nil, body); code != 200 {
		t.Fatalf("%d %s", code, raw)
	}
	ev := lastEvent(t, h)
	if ev.Status == spool.StatusOK || !ev.Estimated || ev.InputTokens != 60 || ev.OutputTokens == 0 {
		t.Fatalf("record: %+v", ev)
	}
}

func TestSelfHostedCountTokensRefusals(t *testing.T) {
	h, u := selfHostedHarness(t, 0, 0, 0, nil)
	u.countStatus = http.StatusBadRequest
	post := func(method, body string) (int, []byte) {
		req, _ := http.NewRequest(method, h.gw.URL+"/v1/messages/count_tokens", strings.NewReader(body))
		req.Header.Set("X-Api-Key", testToken)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		raw, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, raw
	}
	// A request that cannot name its model hears the route's 404, so a
	// client falls back to its own estimate as it did before.
	for _, c := range []struct{ method, body string }{
		{http.MethodGet, ""},
		{http.MethodPost, "not json"},
		{http.MethodPost, `{"messages":[]}`},
	} {
		code, raw := post(c.method, c.body)
		expectAnthropicRefusal(t, code, raw, 404, "not_found_error", "unknown_endpoint")
	}
	// The counter's own rejection names the model the way the caller does.
	u.mu.Lock()
	u.countStatus = 0
	u.mu.Unlock()
	h.srv.client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusBadRequest, Header: http.Header{"Content-Type": {"application/json"}}, Request: r,
			Body: io.NopCloser(strings.NewReader(`{"error":{"type":"BadRequestError","message":"model ` + upstreamModel + ` rejected this input"}}`))}, nil
	})}
	code, raw := post(http.MethodPost, agentTurnBody)
	if code != 400 || strings.Contains(string(raw), upstreamModel) {
		t.Fatalf("counter rejection leaked the served name: %d %s", code, raw)
	}
	// A tool the turn would refuse is refused here in the same words.
	code, raw = post(http.MethodPost, `{"model":"pickle-general","messages":[],"tools":[{"type":"web_search_20250305","name":"web_search"}]}`)
	expectAnthropicRefusal(t, code, raw, 400, "invalid_request_error", "invalid_parameter_value")
}

// chatLongHarness opens the mock upstream to agents with one place and a
// chat threshold of 64 tokens, behind a base URL ending in /v1 as a serving
// process's does.
func chatLongHarness(t *testing.T) (*harness, *selfHostedMessages) {
	t.Helper()
	h := newHarness(t, nil, func(c *config.Config) {
		up := c.Upstreams["mock"]
		up.BaseURL += "/v1"
		up.Messages = config.MessagesOpen
		up.MessagesMaxConcurrent = 1
		up.MessagesQueueWait = 50 * time.Millisecond
		up.MessagesChatInputTokens = 64
		c.Upstreams["mock"] = up
	})
	u := &selfHostedMessages{answer: selfHostedAnswer, stream: selfHostedStream, count: 64}
	u.install(h)
	return h, u
}

// A chat prompt at the threshold takes one of the agent places, so a long
// chat prefill and an agent turn are bounded together on the same box.
func TestSelfHostedChatLongInputTakesTurnPlace(t *testing.T) {
	h, u := chatLongHarness(t)
	hold := make(chan struct{})
	u.hold = hold
	first := make(chan int, 1)
	go func() { first <- serveVariant(t, h, protoChat, nil, chatBody).Code }()
	deadline := time.Now().Add(5 * time.Second)
	for h.srv.agents.snapshot()["mock"]["inUse"] != 1 {
		if time.Now().After(deadline) {
			t.Fatal("long chat never took a place")
		}
		time.Sleep(5 * time.Millisecond)
	}
	code, raw, _ := sendMessages(t, h, nil, agentTurnBody)
	expectAnthropicRefusal(t, code, raw, 429, "rate_limit_error", "agent_busy")
	close(hold)
	if c := <-first; c != 200 {
		t.Fatalf("long chat: %d", c)
	}
	paths, bodies := u.calls()
	if len(paths) == 0 || paths[0] != "/tokenize" {
		t.Fatalf("chat was not counted at the tokenizer beside /v1: %v", paths)
	}
	// The tokenizer reads what renders the prompt and nothing else.
	for name := range bodies[0] {
		switch name {
		case "model", "messages", "tools":
		default:
			t.Fatalf("tokenizer got %s", name)
		}
	}
	if string(bodies[0]["model"]) != `"`+upstreamModel+`"` {
		t.Fatalf("tokenizer model %s", bodies[0]["model"])
	}
	if h.srv.metrics.chatLongInputPlaces.Load() != 1 {
		t.Fatal("long chat not counted")
	}
	if h.srv.agents.snapshot()["mock"]["inUse"] != 0 {
		t.Fatal("place not released")
	}
}

// Over the bound, a long chat is refused the way an agent turn is.
func TestSelfHostedChatLongInputRefusedWhenFull(t *testing.T) {
	h, u := chatLongHarness(t)
	hold := make(chan struct{})
	u.hold = hold
	first := make(chan int, 1)
	go func() {
		code, _, _ := sendMessages(t, h, nil, agentTurnBody)
		first <- code
	}()
	for h.srv.agents.snapshot()["mock"]["inUse"] != 1 {
		time.Sleep(5 * time.Millisecond)
	}
	rec := serveVariant(t, h, protoChat, nil, chatBody)
	if rec.Code != 429 || !strings.Contains(rec.Body.String(), `"agent_busy"`) || rec.Header().Get("Retry-After") != "10" {
		t.Fatalf("long chat over the bound: %d %s %v", rec.Code, rec.Body, rec.Header())
	}
	if ev := lastEvent(t, h); ev.Status != spool.StatusRateLimited || ev.ErrorType != "agent_concurrency" {
		t.Fatalf("record: %+v", ev)
	}
	close(hold)
	<-first
}

// Below the threshold, or when the count fails, chat stays outside the bound.
func TestSelfHostedChatShortInputOutsideBound(t *testing.T) {
	h, u := chatLongHarness(t)
	u.count = 63
	if rec := serveVariant(t, h, protoChat, nil, chatBody); rec.Code != 200 {
		t.Fatalf("short chat: %d %s", rec.Code, rec.Body)
	}
	if h.srv.metrics.chatLongInputPlaces.Load() != 0 {
		t.Fatal("short chat took a place")
	}
	// A body too short in bytes to reach the threshold is not counted.
	h2, u2 := chatLongHarness(t)
	up := h2.srv.cfg.Upstreams["mock"]
	up.MessagesChatInputTokens = 32768
	h2.srv.cfg.Upstreams["mock"] = up
	if rec := serveVariant(t, h2, protoChat, nil, chatBody); rec.Code != 200 {
		t.Fatalf("small chat: %d", rec.Code)
	}
	if paths, _ := u2.calls(); len(paths) != 1 || paths[0] == "/tokenize" {
		t.Fatalf("small chat was counted: %v", paths)
	}
}

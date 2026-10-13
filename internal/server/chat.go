// The chat-completions surface: what chat changes about the shared pipeline
// (see pipeline.go). The model, its fallback list and the messages are read
// here; every check and the metering run in the pipeline.
package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"github.com/pnuops/pickle-llm-gateway/internal/bodies"
	"github.com/pnuops/pickle-llm-gateway/internal/config"
	"github.com/pnuops/pickle-llm-gateway/internal/snapshot"
	"github.com/pnuops/pickle-llm-gateway/internal/spool"
)

// firstCandidateModel reads the first entry of the fallback list for a request
// that has no `model`. present is false when there is no usable list at all
// (absent, null or empty), which is the same missing `model` as before; valid
// is false when the list is there but its first entry cannot be a model name.
func firstCandidateModel(params map[string]json.RawMessage) (first string, present, valid bool) {
	raw, ok := params[candidateModelsField]
	if !ok || string(bytes.TrimSpace(raw)) == "null" {
		return "", false, false
	}
	var names []string
	if json.Unmarshal(raw, &names) != nil {
		return "", true, false
	}
	if len(names) == 0 {
		return "", false, false
	}
	return names[0], true, names[0] != ""
}

// renameFirstCandidate puts the primary's upstream name in place of the name
// the caller used, which is what `model` gets on every other request. Only the
// first entry: the others go out as written, exactly as they do beside an
// explicit `model`. The list was already parsed and fenced whole, so the
// decode cannot fail here.
func renameFirstCandidate(params map[string]json.RawMessage, upstream string) {
	var names []string
	if json.Unmarshal(params[candidateModelsField], &names) != nil || len(names) == 0 {
		return
	}
	names[0] = upstream
	if out, err := json.Marshal(names); err == nil {
		params[candidateModelsField] = out
	}
}

// tokenAxisParams are the top-level request fields the gateway forwards for a
// self-hosted model. An unknown field is refused rather than silently
// forwarded, so replacing the upstream can never silently change what student
// code is allowed to send.
//
// `reasoning_effort` is absent from this list on purpose, and so are
// `chat_template_kwargs` and `thinking_token_budget`: all three are the
// enforcement point for one service decision, that thinking is off by default
// on the self-hosted side with no per-request opt-in. The serving process does
// not refuse them — measured, same request with and only without the field:
//
//	reasoning_effort "low" → 200, content null, reasoning "Here's a thinking process:..."
//	field absent           → 200, content "Hello! How can I help you today", reasoning null
//
// so the field works, moves the answer out of `content`, and this list is the
// only thing standing between a caller and thinking. Opening it is a service
// decision, not a consistency fix: completion tokens go up by orders of
// magnitude when thinking is on, and that lands on the shared token allowance.
// (The often-quoted ratio from the benchmark round is an estimate, not a
// measurement — its thinking-off side was converted from a different
// checkpoint. Do not restate it as measured.)
var tokenAxisParams = map[string]bool{
	"model":                 true,
	"messages":              true,
	"stream":                true,
	"stream_options":        true,
	"max_tokens":            true,
	"max_completion_tokens": true,
	"temperature":           true,
	"top_p":                 true,
	"stop":                  true,
	"presence_penalty":      true,
	"frequency_penalty":     true,
	"seed":                  true,
	"user":                  true,
	"response_format":       true,
	"tools":                 true,
	"tool_choice":           true,
	"parallel_tool_calls":   true,
}

// creditOnlyParams are fields that are ordinary on the paid side and closed on
// the self-hosted one. The set permits nothing: the money axis has no allowlist
// at all, so no request is checked against a set there. It exists only to
// classify a refusal on the self-hosted side.
//
// That classification is worth keeping on its own. Without it a caller sending
// `reasoning_effort` to a self-hosted model hears "unknown field", which is
// false and dead-ends them; with it they hear that the field works on a paid
// model, which is the only clue they get about what to do next.
//
// `reasoning_effort` and `verbosity` are documented Chat Completions fields on
// the commercial side, and agent tools set them on their own from model
// metadata. They stay closed on the self-hosted side for three reasons, none
// of which is that the serving process would fail on them. Thinking there is
// disabled as a server default and re-opening it is a service decision, not a
// per-request one; a field that changes nothing would still read to the caller
// as if it had; and a model whose fallback points at a commercial upstream
// would have that upstream honour the field, putting reasoning tokens on the
// self-hosted allowance. The axis is a property of the model, not of the
// upstream that happens to answer.
var creditOnlyParams = map[string]bool{
	"reasoning_effort": true,
	"verbosity":        true,
}

// protoChat is the chat-completions surface.
var protoChat = &protocol{
	endpoint:     spool.EndpointChat,
	capability:   snapshot.EndpointChat,
	upstreamPath: "/chat/completions",
	logLine:      "chat request",
	limits: func(c *config.Config) bodyLimits {
		return bodyLimits{
			request:       c.RequestBodyMaxBytes,
			largeRequest:  c.ChatLargeBodyMaxBytes,
			largeResponse: c.ChatLargeResponseMaxBytes,
		}
	},
	parse:          parseChat,
	tokenAxisParam: chatTokenAxisParam,
	fences: []fence{
		fencePresetField,
		fenceCandidates,
		fenceServerToolsStep,
		fencePluginsStep,
		fenceChatCapabilities,
	},
	requestHeaders: chatForwardedHeaders,
	// Chat has two output fields, and which one an upstream honours is a fact
	// about the upstream (a legacy server reads max_tokens only), so the cap
	// goes on the upstream's configured field.
	capReadFields: []string{"max_completion_tokens", "max_tokens"},
	capWriteField: "",
	shape:         shapeChat,
	capture:       capRequest,
	readResponse:  readChatResponse,
	newStream: func(s *Server, a streamArgs, ev *spool.Event) streamExtractor {
		return &chatStream{s: s, a: a, ev: ev}
	},
	writeError: func(w http.ResponseWriter, e apiError, _ string) { writeAPIError(w, e) },
}

func (s *Server) handleChat(w http.ResponseWriter, r *http.Request) {
	s.serveProtocol(protoChat, w, r)
}

// parseChat reads the model name and the messages.
func parseChat(params map[string]json.RawMessage) (parsedRequest, *refusal) {
	var req parsedRequest
	// fromCandidates marks a request that named its models only through the
	// fallback list. The vendor documents that shape (`model` is optional when
	// `models` is given, and the first entry is then the primary), so it is
	// served rather than refused: the first entry stands in for `model` in
	// every check, the axis and the naming rules included, and the candidate
	// fence still judges every entry, that first one too.
	if raw, ok := params["model"]; ok && json.Unmarshal(raw, &req.publicModel) != nil {
		return req, &refusal{e: errMissingParam("model")}
	}
	if req.publicModel == "" {
		first, present, valid := firstCandidateModel(params)
		switch {
		case !present:
			return req, &refusal{e: errMissingParam("model")}
		case !valid:
			return req, &refusal{e: errInvalidParamValue(candidateModelsField)}
		}
		req.publicModel, req.fromCandidates = first, true
	}
	messages, ok := params["messages"]
	if !ok {
		return req, &refusal{e: errMissingParam("messages")}
	}
	req.input = messages
	// Usage must always come back from the upstream for metering, but the
	// usage chunk is only forwarded when the student asked for it — clients
	// that never opted in would break on a chunk with an empty choices array.
	if raw, ok := params["stream_options"]; ok {
		var opts struct {
			IncludeUsage bool `json:"include_usage"`
		}
		_ = json.Unmarshal(raw, &opts)
		req.wantsUsage = opts.IncludeUsage
	}
	return req, nil
}

func chatTokenAxisParam(name string) *refusal {
	if tokenAxisParams[name] {
		return nil
	}
	if creditOnlyParams[name] {
		// Known field, wrong axis. Same public code as any other rejected
		// field — one more code is one more thing every SDK has to learn for a
		// fact it already handles — but different advice, and its own spool
		// type so the two stay countable apart.
		return &refusal{e: errParamNeedsCreditModel(name), errorType: "credit_only_parameter"}
	}
	return &refusal{e: errUnsupportedParam(name)}
}

// shapeChat puts the upstream name in place of the public one and asks a
// stream for its usage.
func shapeChat(params map[string]json.RawMessage, req parsedRequest, model *snapshot.Model, streaming bool) {
	if req.fromCandidates {
		// Forwarded in the shape the caller sent, the one the vendor documents:
		// no `model`, and the list with its first entry in the upstream name.
		// Moving that entry into `model` instead would depend on how the vendor
		// orders `model` against the list, which only its prose describes.
		delete(params, "model")
		renameFirstCandidate(params, model.UpstreamModel)
	} else {
		params["model"] = json.RawMessage(strconv.Quote(model.UpstreamModel))
	}
	if streaming {
		params["stream_options"] = withIncludeUsage(params["stream_options"])
	}
}

// chatPayloadError reports whether a chat payload, one stream chunk or a whole
// non-streamed answer, carries the vendor's own error: an error member, or a
// choice that finished with "error". The vendor sends both under a 200 when a
// generation fails part way, so the status alone says nothing.
func chatPayloadError(m map[string]any) bool {
	if m["error"] != nil {
		return true
	}
	choices, _ := m["choices"].([]any)
	for _, choice := range choices {
		if item, _ := choice.(map[string]any); item != nil && item["finish_reason"] == "error" {
			return true
		}
	}
	return false
}

// readChatResponse reads a non-streamed chat completion.
func readChatResponse(m map[string]any, publicName string) responseResult {
	res := responseResult{upstreamError: chatPayloadError(m)}
	if raw, has := m["model"]; has {
		res.servedModel, _ = raw.(string)
		m["model"] = publicName
	}
	if uraw, has := m["usage"]; has && uraw != nil {
		if b, err := json.Marshal(uraw); err == nil && json.Unmarshal(b, &res.usage) == nil {
			res.haveUsage = true
		}
	}
	// The assistant text is needed for the no-usage size estimate, and again
	// for capture when the key opted in.
	if choices, _ := m["choices"].([]any); len(choices) > 0 {
		if first, _ := choices[0].(map[string]any); first != nil {
			if msg, _ := first["message"].(map[string]any); msg != nil {
				res.answer, _ = msg["content"].(string)
			}
		}
	}
	return res
}

// withIncludeUsage forces stream_options.include_usage on so streamed
// responses carry token counts, merging any options the student sent.
func withIncludeUsage(raw json.RawMessage) json.RawMessage {
	opts := map[string]json.RawMessage{}
	if len(raw) > 0 {
		_ = json.Unmarshal(raw, &opts)
	}
	opts["include_usage"] = json.RawMessage("true")
	out, err := json.Marshal(opts)
	if err != nil {
		return json.RawMessage(`{"include_usage":true}`)
	}
	return out
}

// chatStream reads a chat-completions stream: data lines only, ended by
// `data: [DONE]`. The event:, id: and retry: fields are not part of this
// surface and are dropped.
type chatStream struct {
	s  *Server
	a  streamArgs
	ev *spool.Event

	res streamResult
	// Assembled assistant text, only when the key opted into capture.
	answer      strings.Builder
	notedServed bool
	// sawDone is the upstream's own terminator. A stream that ends without it
	// ended early, however cleanly the connection closed.
	sawDone bool
}

// An assembled payload that does not parse is dropped, not forwarded: a
// verbatim chunk would leak the upstream model identifier.
func (c *chatStream) event(e sseEvent) []byte {
	payload := e.data
	if bytes.Equal(payload, []byte("[DONE]")) {
		c.sawDone = true
		return []byte("data: [DONE]\n\n")
	}
	ck, ok := rewriteChunk(payload, c.a.publicName)
	if !ok {
		// Not valid JSON. A payload that does not even open an object is a
		// heartbeat/keepalive (`ping`) with nothing structured to leak —
		// forward it verbatim. One that looks like a truncated object is
		// dropped rather than risk leaking a partial upstream identifier,
		// and the answer is now incomplete, so the request is marked
		// degraded.
		if len(payload) == 0 || payload[0] != '{' {
			return append(append([]byte("data: "), payload...), '\n', '\n')
		}
		c.res.dropped++
		return nil
	}
	if !c.notedServed && ck.servedModel != "" {
		c.notedServed = true
		c.s.noteServedModel(c.ev, c.a.sentModel, ck.servedModel)
	}
	c.res.upstreamError = c.res.upstreamError || ck.upstreamError
	if ck.usage != nil {
		c.res.usage = *ck.usage
		c.res.haveUsage = true
		if !c.a.forwardUsage {
			if ck.choicesEmpty && !ck.upstreamError {
				// The gateway-requested usage chunk; the student did not
				// opt in, so it is consumed for metering and not sent.
				return nil
			}
			ck.out = ck.stripUsage()
		}
	}
	c.res.contentChars += ck.contentChars
	if c.a.capture != nil && ck.content != "" && c.answer.Len() < bodies.ResponseCapBytes {
		c.answer.WriteString(ck.content)
	}
	return append(append([]byte("data: "), ck.out...), '\n', '\n')
}

func (c *chatStream) done() bool { return c.sawDone }

func (c *chatStream) result() streamResult {
	r := c.res
	r.answer = c.answer.String()
	return r
}

// errorEvent is an error chunk in the vendor's own shape, then [DONE].
func (c *chatStream) errorEvent(code, msg string) []byte {
	return []byte(`data: {"error":{"message":"` + msg + `","type":"server_error","code":"` + code + `"}}` + "\n\n" +
		"data: [DONE]\n\n")
}

func (c *chatStream) terminator() []byte { return []byte("data: [DONE]\n\n") }

// capRequest bounds the captured prompt. Under the cap it stays the messages
// array the student sent. Over it, the array cannot be cut and still parse, so
// the record carries a JSON string holding the prefix instead and says so —
// losing the structure is better than losing the whole prompt, and far better
// than carrying two megabytes per record through a queue.
func capRequest(raw json.RawMessage) (json.RawMessage, bool) {
	if len(raw) <= bodies.RequestCapBytes {
		return append(json.RawMessage(nil), raw...), false
	}
	prefix, _ := capAt(string(raw), bodies.RequestCapBytes)
	encoded, err := json.Marshal(prefix)
	if err != nil {
		return nil, true
	}
	return encoded, true
}

// chunk is one parsed and rewritten SSE payload.
type chunk struct {
	out           []byte
	parsed        map[string]any
	servedModel   string // the upstream's own model name, before it is rewritten
	usage         *usage
	content       string // this chunk's assistant text, for capture
	contentChars  int
	choicesEmpty  bool
	upstreamError bool
}

// stripUsage re-marshals the chunk without its usage field, for the case
// where a content-bearing chunk carries usage the student did not ask for.
func (c *chunk) stripUsage() []byte {
	delete(c.parsed, "usage")
	b, err := json.Marshal(c.parsed)
	if err != nil {
		return c.out
	}
	return b
}

// rewriteChunk swaps the model name in one SSE data payload and pulls token
// usage out of the chunk that carries it. ok=false means the payload did not
// parse as JSON.
func rewriteChunk(payload []byte, publicName string) (chunk, bool) {
	var m map[string]any
	dec := json.NewDecoder(bytes.NewReader(payload))
	dec.UseNumber()
	if dec.Decode(&m) != nil {
		return chunk{}, false
	}
	c := chunk{parsed: m, choicesEmpty: true, upstreamError: chatPayloadError(m)}
	if raw, has := m["model"]; has {
		c.servedModel, _ = raw.(string)
		m["model"] = publicName
	}
	if uraw, has := m["usage"]; has && uraw != nil {
		if b, err := json.Marshal(uraw); err == nil {
			var parsed usage
			if json.Unmarshal(b, &parsed) == nil {
				c.usage = &parsed
			}
		}
	}
	if choices, _ := m["choices"].([]any); len(choices) > 0 {
		c.choicesEmpty = false
		if first, _ := choices[0].(map[string]any); first != nil {
			if delta, _ := first["delta"].(map[string]any); delta != nil {
				if content, _ := delta["content"].(string); content != "" {
					c.content = content
					c.contentChars = len(content)
				}
			}
		}
	}
	b, err := json.Marshal(m)
	if err != nil {
		return chunk{}, false
	}
	c.out = b
	return c, true
}

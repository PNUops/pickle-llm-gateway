// The OpenAI Responses surface: POST /v1/responses, the API OpenAI's newer SDK
// helpers and agent clients such as Codex call. Like the Messages surface it
// is a passthrough to the vendor's own route, not a translation: the body goes
// out in the shape it came in, with the model renamed, and the answer comes
// back the same way.
//
// It serves paid models only, for the reason the Messages surface gives: the
// self-hosted side has its own parameter rules on this surface that have not
// been written, and its metering there is unmeasured.
//
// The surface is stateless. The vendor stores no response, so a request that
// asks for one to be stored, or refers to one stored earlier, is refused with
// the reason rather than forwarded or quietly rewritten: dropping the field
// would leave the caller believing the response was kept, and the next request
// naming it would fail somewhere far less clear.
package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"github.com/pnuops/pickle-llm-gateway/internal/config"
	"github.com/pnuops/pickle-llm-gateway/internal/snapshot"
	"github.com/pnuops/pickle-llm-gateway/internal/spool"
)

var protoResponses = &protocol{
	endpoint:     spool.EndpointResponses,
	capability:   snapshot.EndpointChat,
	upstreamPath: "/responses",
	logLine:      "responses request",
	// The chat bounds and the same large-body slot pool, as on the Messages
	// surface: an agent request carries its whole tool list and context.
	limits: func(c *config.Config) bodyLimits {
		return bodyLimits{
			request:       c.RequestBodyMaxBytes,
			largeRequest:  c.ChatLargeBodyMaxBytes,
			largeResponse: c.ChatLargeResponseMaxBytes,
		}
	},
	parse:      parseResponses,
	creditOnly: &errModelNotOnResponses,
	// Unreachable while the surface is closed to the self-hosted axis; if that
	// is lifted without writing this surface's own list, every field is
	// refused rather than forwarded unread.
	tokenAxisParam: func(name string) *refusal { return &refusal{e: errUnsupportedParam(name)} },
	fences: []fence{
		fencePresetField,
		fenceCandidates,
		fenceServerToolsStep,
		fenceDeferredTools,
		fenceResponsesInput,
		fenceToolChoice,
		fencePluginsStep,
		fenceChatCapabilities,
	},
	// The chat headers. An agent client's own headers (its session and turn
	// ids) describe the client, not a vendor feature, and are not forwarded.
	requestHeaders: chatForwardedHeaders,
	capReadFields:  []string{"max_output_tokens"},
	capWriteField:  "max_output_tokens",
	shape:          shapeResponses,
	// No body capture: its record format is not designed for this surface.
	capture:         nil,
	noUsageEstimate: true,
	readResponse:    readResponsesResponse,
	newStream: func(s *Server, a streamArgs, ev *spool.Event) streamExtractor {
		return &responsesStream{s: s, a: a, ev: ev}
	},
	writeError: func(w http.ResponseWriter, e apiError, _ string) { writeAPIError(w, e) },
}

func (s *Server) handleResponses(w http.ResponseWriter, r *http.Request) {
	s.serveProtocol(protoResponses, w, r)
}

// handleResponsesUnknown answers every route under the Responses path —
// retrieving, cancelling or listing a stored response, and compaction — with a
// fast 404. Each of them reads a stored response, and the vendor stores none.
// It is answered before authentication and records nothing, like any other
// route this service does not serve.
func (s *Server) handleResponsesUnknown(w http.ResponseWriter, _ *http.Request) {
	writeAPIError(w, errNotFound)
}

// statefulFields are the request fields that store a response or refer to a
// stored one, each with what the caller should do instead. A flag counts only
// when it is true; a reference counts whenever it is there and not null.
var statefulFields = []struct {
	name   string
	isFlag bool
	advice string
}{
	{"store", true, "이 서비스는 응답을 서버에 저장하지 않습니다. store를 빼거나 false로 보내고, 이전 대화는 input에 모두 담아 보내주세요."},
	{"previous_response_id", false, "이 서비스는 응답을 서버에 저장하지 않아 이전 응답을 이어 쓸 수 없습니다. 이전 대화를 input에 모두 담아 보내주세요."},
	{"conversation", false, "이 서비스는 대화를 서버에 저장하지 않습니다. 이전 대화를 input에 모두 담아 보내주세요."},
	{"background", true, "백그라운드 실행은 응답을 서버에 저장해야 해서 지원하지 않습니다. background를 빼거나 false로 보내주세요."},
}

// parseResponses reads the model and the input, the two fields the vendor
// requires, and refuses a request that relies on stored state.
func parseResponses(params map[string]json.RawMessage) (parsedRequest, *refusal) {
	var req parsedRequest
	raw, ok := params["model"]
	if !ok || json.Unmarshal(raw, &req.publicModel) != nil || req.publicModel == "" {
		return req, &refusal{e: errMissingParam("model")}
	}
	input, ok := params["input"]
	if !ok || string(bytes.TrimSpace(input)) == "null" {
		return req, &refusal{e: errMissingParam("input")}
	}
	for _, f := range statefulFields {
		v, ok := params[f.name]
		if !ok || string(bytes.TrimSpace(v)) == "null" {
			continue
		}
		if f.isFlag {
			var on bool
			if json.Unmarshal(v, &on) != nil {
				return req, &refusal{e: errInvalidParamValue(f.name)}
			}
			if !on {
				continue
			}
		}
		return req, &refusal{e: errUnsupportedValue(f.name, f.advice)}
	}
	req.input = input
	return req, nil
}

// shapeResponses puts the upstream name in place of the public one. Usage
// arrives on every Responses answer, streamed or not, so nothing is added.
func shapeResponses(params map[string]json.RawMessage, _ parsedRequest, model *snapshot.Model, _ bool) {
	params["model"] = json.RawMessage(strconv.Quote(model.UpstreamModel))
}

// fenceResponsesInput fences tool definitions carried inside the input. A
// Responses input item can declare tools to the model: Codex sends its whole
// tool list as one such item rather than in tools, and a client-run tool
// search answers with the tools it loaded. Whatever the item's type, a tools
// list there is declared to the model on this key, so every one is fenced
// like the top-level list. Items without one are not read.
func fenceResponsesInput(_ *Server, doc *snapshot.Document, lookup func(string) *snapshot.Model,
	key *snapshot.Key, _ *snapshot.Model, params map[string]json.RawMessage) *refusal {
	raw := params["input"]
	var items []json.RawMessage
	if json.Unmarshal(raw, &items) != nil {
		// A string input carries no items.
		return nil
	}
	for _, rawItem := range items {
		var item map[string]json.RawMessage
		if json.Unmarshal(rawItem, &item) != nil {
			continue
		}
		list, ok := item[toolsField]
		if !ok || string(bytes.TrimSpace(list)) == "null" {
			continue
		}
		if e := fenceToolList(doc, lookup, key, list, 0); e != nil {
			if e.code == "invalid_parameter_value" {
				return &refusal{e: errInvalidParamValue("input")}
			}
			return &refusal{e: *e, errorType: toolRefusalType(*e)}
		}
	}
	return nil
}

// responsesUsage is the vendor's usage object on this surface. Unlike the
// Messages surface, the input count includes the cached part, so it maps onto
// the pipeline's terms directly.
type responsesUsage struct {
	InputTokens        *int `json:"input_tokens"`
	OutputTokens       *int `json:"output_tokens"`
	InputTokensDetails struct {
		CachedTokens *int `json:"cached_tokens"`
	} `json:"input_tokens_details"`
	OutputTokensDetails struct {
		ReasoningTokens *int `json:"reasoning_tokens"`
	} `json:"output_tokens_details"`
	// Read raw and kept only when it is a number, as on the Messages surface.
	Cost json.RawMessage `json:"cost"`
}

// normalized renders the usage in the pipeline's terms. It requires both
// counts: an answer that reports one of them is not one this service can
// meter.
func (r responsesUsage) normalized() (usage, bool) {
	var u usage
	if r.InputTokens == nil || r.OutputTokens == nil {
		return u, false
	}
	u.PromptTokens = nonNegative(*r.InputTokens)
	u.CompletionTokens = nonNegative(*r.OutputTokens)
	u.TotalTokens = u.PromptTokens + u.CompletionTokens
	u.Cost = messagesUsage{Cost: r.Cost}.cost()
	u.PromptTokensDetails.CachedTokens = nonNegative(intOf(r.InputTokensDetails.CachedTokens))
	u.CompletionTokensDetails.ReasoningTokens = nonNegative(intOf(r.OutputTokensDetails.ReasoningTokens))
	return u, true
}

func decodeResponsesUsage(raw any) (usage, bool) {
	if raw == nil {
		return usage{}, false
	}
	var r responsesUsage
	b, err := json.Marshal(raw)
	if err != nil || json.Unmarshal(b, &r) != nil {
		return usage{}, false
	}
	return r.normalized()
}

// responseFailed reports whether a response object says it failed: the
// status the vendor sets, or an error member that is not null (a successful
// response carries "error": null).
func responseFailed(resp map[string]any) bool {
	return resp["status"] == "failed" || resp["error"] != nil
}

// readResponsesResponse reads a non-streamed response object. The answer
// text is the output_text parts of its message items.
func readResponsesResponse(m map[string]any, publicName string) responseResult {
	res := responseResult{upstreamError: responseFailed(m)}
	if raw, has := m["model"]; has {
		res.servedModel, _ = raw.(string)
		m["model"] = publicName
	}
	res.usage, res.haveUsage = decodeResponsesUsage(m["usage"])
	res.answer = responsesOutputText(m["output"])
	return res
}

func responsesOutputText(output any) string {
	items, _ := output.([]any)
	var text strings.Builder
	for _, item := range items {
		it, _ := item.(map[string]any)
		if it == nil || it["type"] != "message" {
			continue
		}
		parts, _ := it["content"].([]any)
		for _, part := range parts {
			if p, _ := part.(map[string]any); p != nil && p["type"] == "output_text" {
				t, _ := p["text"].(string)
				text.WriteString(t)
			}
		}
	}
	return text.String()
}

// responsesEnds are the events that close a response. The vendor sends
// [DONE] after one of them, and OpenAI's own servers end the stream without
// it, so the stream is read as complete on the event rather than on [DONE].
// incomplete is an ordinary ending, the way a chat completion that stopped at
// its length limit is: the answer is cut where the caller asked, and the usage
// it carries is what was billed. failed is the vendor's own error.
var responsesEnds = map[string]bool{
	"response.completed":  true,
	"response.incomplete": true,
	"response.failed":     true,
}

// responsesStream reads a Responses stream. The kind of each event is the
// type member of its data, which is what clients dispatch on; the vendor sends
// no event: line, and one that arrives is not forwarded, like chat. Every
// event that carries the response object (created, in_progress and the
// endings) has its model renamed; the rest go out as the upstream wrote them.
// The stream is always closed with data: [DONE] after its ending, whether or
// not the upstream sent one, so a client sees one ending shape.
type responsesStream struct {
	s  *Server
	a  streamArgs
	ev *spool.Event

	res         streamResult
	ended       bool
	notedServed bool
}

const responsesDone = "data: [DONE]\n\n"

func (x *responsesStream) event(e sseEvent) []byte {
	if bytes.Equal(e.data, []byte("[DONE]")) {
		// The upstream's own terminator. One after an ending is never read
		// (the relay stops at the ending); one before it ends nothing, and the
		// stream is classified by what it never sent.
		return nil
	}
	var payload map[string]any
	dec := json.NewDecoder(bytes.NewReader(e.data))
	dec.UseNumber()
	if dec.Decode(&payload) != nil {
		// Not an object: nothing this relay can vouch for.
		x.res.dropped++
		return nil
	}
	typ, _ := payload["type"].(string)
	out := e.data
	renamed := false
	// The model is renamed wherever it sits: in the response object, and at
	// the top of an event this relay does not know, which would otherwise
	// carry the upstream's id verbatim.
	for _, place := range []map[string]any{payload, mapOf(payload["response"])} {
		raw, has := place["model"]
		if !has {
			continue
		}
		served, _ := raw.(string)
		if !x.notedServed && served != "" {
			x.notedServed = true
			x.s.noteServedModel(x.ev, x.a.sentModel, served)
		}
		place["model"] = x.a.publicName
		renamed = true
	}
	if renamed {
		b, err := json.Marshal(payload)
		if err != nil {
			x.res.dropped++
			return nil
		}
		out = b
	}
	if resp := mapOf(payload["response"]); resp != nil && responsesEnds[typ] {
		if u, ok := decodeResponsesUsage(resp["usage"]); ok {
			x.res.usage, x.res.haveUsage = u, true
		}
		if typ == "response.failed" || responseFailed(resp) {
			x.res.upstreamError = true
		}
	}
	switch {
	case typ == "error":
		x.res.upstreamError = true
	case typ == "response.output_text.delta":
		d, _ := payload["delta"].(string)
		x.res.contentChars += len(d)
	}
	frame := append(append([]byte("data: "), out...), '\n', '\n')
	if responsesEnds[typ] || typ == "error" {
		x.ended = true
		frame = append(frame, responsesDone...)
	}
	return frame
}

// mapOf is v as an object, or nil.
func mapOf(v any) map[string]any {
	m, _ := v.(map[string]any)
	return m
}

// done is true after an ending or the upstream's own error. Nothing the
// upstream sends after its error is part of an answer.
func (x *responsesStream) done() bool { return x.ended }

func (x *responsesStream) result() streamResult { return x.res }

// errorEvent is the standard error event, then [DONE].
func (x *responsesStream) errorEvent(code, msg string) []byte {
	b, _ := json.Marshal(map[string]any{
		"type":    "error",
		"code":    code,
		"message": msg,
		"param":   nil,
	})
	return append(append(append([]byte("data: "), b...), '\n', '\n'), responsesDone...)
}

// terminator is [DONE]. It is reached only when an error arrived without an
// ending, which cannot happen here (an error is an ending), so it is the safe
// choice rather than a used one.
func (x *responsesStream) terminator() []byte { return []byte(responsesDone) }

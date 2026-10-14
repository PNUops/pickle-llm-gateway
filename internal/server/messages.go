// The Anthropic Messages surface: POST /v1/messages, the API Anthropic's SDKs
// and agent clients such as Claude Code call. It is a passthrough to the
// vendor's own Messages endpoint, not a translation: the body goes out in the
// shape it came in, with the model renamed, and the answer comes back the
// same way. What this file changes about the shared pipeline is how the key is
// read, which fields name a model, how usage is read, and the error envelope.
//
// It serves paid models only. A self-hosted model is refused with a reason
// rather than forwarded, because the self-hosted side has its own parameter
// rules on this surface that have not been written.
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

var protoMessages = &protocol{
	endpoint:     spool.EndpointMessages,
	capability:   snapshot.EndpointChat,
	upstreamPath: "/messages",
	logLine:      "messages request",
	token:        messagesToken,
	// The chat bounds and the same large-body slot pool. An agent request
	// carries its whole tool list and context, so it is the one most likely
	// to need a slot; giving it a pool of its own would be a second set of
	// numbers to measure for traffic nobody has measured yet.
	limits: func(c *config.Config) bodyLimits {
		return bodyLimits{
			request:       c.RequestBodyMaxBytes,
			largeRequest:  c.ChatLargeBodyMaxBytes,
			largeResponse: c.ChatLargeResponseMaxBytes,
		}
	},
	parse:      parseMessages,
	creditOnly: &errModelNotOnMessages,
	// Unreachable while the surface is closed to the self-hosted axis; if
	// that is ever lifted without writing this surface's own list, every
	// field is refused rather than forwarded unread.
	tokenAxisParam: func(name string) *refusal { return &refusal{e: errUnsupportedParam(name)} },
	// Model names follow the chat rules: a name the catalogue does not list is
	// forwarded to the vendor under the key's allow and deny lists, whether or
	// not it carries a provider prefix.
	fences: []fence{
		fencePresetField,
		fenceCandidates,
		fenceFallbacks,
		fenceServerToolsStep,
		fenceDeferredTools,
		fencePluginsStep,
		fenceChatCapabilities,
	},
	requestHeaders: messagesForwardedHeaders,
	capReadFields:  []string{"max_tokens"},
	capWriteField:  "max_tokens",
	shape:          shapeMessages,
	// No body capture: its record format is not designed for this surface.
	capture:         nil,
	noUsageEstimate: true,
	readResponse:    readMessagesResponse,
	newStream:       newMessagesStream,
	writeError:      writeAnthropicError,
}

func (s *Server) handleMessages(w http.ResponseWriter, r *http.Request) {
	s.serveProtocol(protoMessages, w, r)
}

// handleMessagesUnknown answers the token-counting route, and anything else
// under the Messages path, with a fast 404 in the Anthropic envelope. The vendor has no such route to forward to, and a client
// that gets a 404 here falls back to its own estimate without retrying. It is
// answered before authentication, like any other route this service does not
// serve, and it records nothing: nothing was served.
func (s *Server) handleMessagesUnknown(w http.ResponseWriter, _ *http.Request) {
	writeAnthropicError(w, errNotFound, "")
}

// messagesToken reads the key the way an Anthropic client sends it: as a
// bearer, or in x-api-key. A bearer wins when both are present, the same order
// every other route reads, so one request cannot authenticate as two keys
// depending on which header a reader looks at first.
func messagesToken(r *http.Request) (string, *apiError) {
	if token := bearerValue(r); token != "" {
		return token, nil
	}
	if token := strings.TrimSpace(r.Header.Get("X-Api-Key")); token != "" {
		return token, nil
	}
	return "", &errMissingKeyMessages
}

// parseMessages reads the model and the messages, the two fields the vendor
// requires. max_tokens is optional at the vendor and is not required here.
func parseMessages(params map[string]json.RawMessage) (parsedRequest, *refusal) {
	var req parsedRequest
	raw, ok := params["model"]
	if !ok || json.Unmarshal(raw, &req.publicModel) != nil || req.publicModel == "" {
		return req, &refusal{e: errMissingParam("model")}
	}
	messages, ok := params["messages"]
	if !ok {
		return req, &refusal{e: errMissingParam("messages")}
	}
	// The size guard reads the messages alone. The system prompt is left out
	// rather than joined on, which would be one more copy of the body.
	req.input = messages
	return req, nil
}

// shapeMessages puts the upstream name in place of the public one. Usage
// arrives on every Messages answer, streamed or not, so nothing is added.
func shapeMessages(params map[string]json.RawMessage, _ parsedRequest, model *snapshot.Model, _ bool) {
	params["model"] = json.RawMessage(strconv.Quote(model.UpstreamModel))
}

// messagesHeaderAllowlist adds the two headers an Anthropic client sends on
// every request to the ones chat forwards. anthropic-version selects the
// request shape; anthropic-beta carries the client's beta opt-ins, which are
// relayed as sent, since a list kept here would refuse every opt-in released
// after it was written.
var messagesHeaderAllowlist = []string{"Anthropic-Version", "Anthropic-Beta"}

func messagesForwardedHeaders(h http.Header) http.Header {
	out := chatForwardedHeaders(h)
	for _, name := range messagesHeaderAllowlist {
		v := h.Get(name)
		if v == "" || len(v) > forwardedHeaderValueMax {
			continue
		}
		if out == nil {
			out = http.Header{}
		}
		out.Set(name, v)
	}
	return out
}

// fallbacksField is the Messages surface's own fallback list: up to a few
// {model} entries the vendor may serve and bill instead of `model`. It is the
// same channel as `models` in another shape, so it gets the same fence.
const fallbacksField = "fallbacks"

func fenceFallbacks(_ *Server, doc *snapshot.Document, lookup func(string) *snapshot.Model,
	key *snapshot.Key, _ *snapshot.Model, params map[string]json.RawMessage) *refusal {
	raw, present := params[fallbacksField]
	if !present || string(bytes.TrimSpace(raw)) == "null" {
		return nil
	}
	// Maps, not a tagged struct, for the reason the tools fence gives: the
	// vendor reads the exact key, and Go's struct decoding does not.
	var entries []map[string]json.RawMessage
	if json.Unmarshal(raw, &entries) != nil {
		return &refusal{e: errInvalidParamValue(fallbacksField)}
	}
	for _, entry := range entries {
		var name string
		// An entry without a readable model is refused: the vendor would
		// dispatch it somewhere this fence cannot see.
		if json.Unmarshal(entry["model"], &name) != nil {
			return &refusal{e: errInvalidParamValue(fallbacksField)}
		}
		if !allowsCandidateModel(doc, lookup, key, name) {
			return &refusal{e: errFallbackModelNotAllowed(name), errorType: "credit_model_not_allowed"}
		}
		if isOnlineName(name) && !key.AllowsEndpoint(snapshot.EndpointWebSearch) {
			return &refusal{e: errEndpointNotAllowed(endpointLabel(snapshot.EndpointWebSearch)),
				errorType: "endpoint_not_allowed"}
		}
	}
	return nil
}

// deferredToolsField carries tool definitions the model loads on demand. A
// server tool there runs on this key exactly as one in `tools` does, so it is
// fenced the same way, and an unreadable list fails closed. Anthropic's own
// spellings of the vendor's tools (web_search_20250305, advisor_20260301) are
// reduced to the tool they run by the same fence, in either list.
const deferredToolsField = "deferred_tools"

func fenceDeferredTools(_ *Server, doc *snapshot.Document, lookup func(string) *snapshot.Model,
	key *snapshot.Key, _ *snapshot.Model, params map[string]json.RawMessage) *refusal {
	raw, present := params[deferredToolsField]
	if !present || string(bytes.TrimSpace(raw)) == "null" {
		return nil
	}
	if e := fenceToolList(doc, lookup, key, raw, 0); e != nil {
		if e.code == "invalid_parameter_value" {
			return &refusal{e: errInvalidParamValue(deferredToolsField)}
		}
		return &refusal{e: *e, errorType: toolRefusalType(*e)}
	}
	return nil
}

// messagesUsage is the vendor's usage object on this surface. Its input count
// excludes the cached part, read and written, so the three are added to get
// what chat calls prompt tokens.
type messagesUsage struct {
	InputTokens              *int `json:"input_tokens"`
	OutputTokens             *int `json:"output_tokens"`
	CacheReadInputTokens     *int `json:"cache_read_input_tokens"`
	CacheCreationInputTokens *int `json:"cache_creation_input_tokens"`
	// Cost is read raw and kept only when it is a number: a malformed one
	// must not take the token counts down with it.
	Cost                json.RawMessage `json:"cost"`
	OutputTokensDetails struct {
		ReasoningTokens int `json:"reasoning_tokens"`
	} `json:"output_tokens_details"`
}

// cost is the vendor's price when it reported one as a number, else "".
func (m messagesUsage) cost() json.Number {
	var n json.Number
	dec := json.NewDecoder(bytes.NewReader(m.Cost))
	dec.UseNumber()
	var v any
	if len(m.Cost) == 0 || dec.Decode(&v) != nil {
		return ""
	}
	n, _ = v.(json.Number)
	return n
}

func intOf(p *int) int {
	if p == nil {
		return 0
	}
	return *p
}

// decodeMessagesUsage reads a usage object from a decoded answer or event.
func decodeMessagesUsage(raw any) (messagesUsage, bool) {
	var u messagesUsage
	if raw == nil {
		return u, false
	}
	b, err := json.Marshal(raw)
	if err != nil || json.Unmarshal(b, &u) != nil {
		return u, false
	}
	return u, true
}

// normalized renders a whole answer's usage in the pipeline's terms. It
// requires both counts: an answer that reports one of them is not one this
// service can meter.
func (m messagesUsage) normalized() (usage, bool) {
	var u usage
	if m.InputTokens == nil || m.OutputTokens == nil {
		return u, false
	}
	read := nonNegative(intOf(m.CacheReadInputTokens))
	u.PromptTokens = nonNegative(*m.InputTokens) + read + nonNegative(intOf(m.CacheCreationInputTokens))
	u.CompletionTokens = nonNegative(*m.OutputTokens)
	u.TotalTokens = u.PromptTokens + u.CompletionTokens
	u.Cost = m.cost()
	u.PromptTokensDetails.CachedTokens = read
	u.CompletionTokensDetails.ReasoningTokens = m.OutputTokensDetails.ReasoningTokens
	return u, true
}

// readMessagesResponse reads a non-streamed Messages answer.
func readMessagesResponse(m map[string]any, publicName string) responseResult {
	res := responseResult{upstreamError: m["type"] == "error"}
	if raw, has := m["model"]; has {
		res.servedModel, _ = raw.(string)
		m["model"] = publicName
	}
	if mu, ok := decodeMessagesUsage(m["usage"]); ok {
		res.usage, res.haveUsage = mu.normalized()
	}
	if blocks, _ := m["content"].([]any); len(blocks) > 0 {
		var text strings.Builder
		for _, block := range blocks {
			if b, _ := block.(map[string]any); b != nil && b["type"] == "text" {
				t, _ := b["text"].(string)
				text.WriteString(t)
			}
		}
		res.answer = text.String()
	}
	return res
}

// messagesStream meters a Messages stream. The input side arrives on
// message_start and the output count on message_delta, which the vendor's
// schema documents as the running total, so the last one is kept. Either
// event may carry the cache counts, and message_delta's input fields are null
// unless the vendor revised them, in which case the revision wins.
type messagesStream struct {
	s  *Server
	a  streamArgs
	ev *spool.Event

	input, cacheRead, cacheCreation int
	sawStart, sawDelta              bool
	notedServed                     bool
}

func newMessagesStream(s *Server, a streamArgs, ev *spool.Event) streamExtractor {
	m := &messagesStream{s: s, a: a, ev: ev}
	return &namedStream{
		ends:      map[string]bool{"message_stop": true},
		errorName: "error",
		read:      m.read,
		errorData: anthropicStreamErrorData(ev.EventUUID),
	}
}

func (m *messagesStream) read(e sseEvent, res *streamResult) ([]byte, bool) {
	var payload map[string]any
	dec := json.NewDecoder(bytes.NewReader(e.data))
	dec.UseNumber()
	if dec.Decode(&payload) != nil {
		// Not an object: nothing this relay can vouch for.
		return nil, false
	}
	switch e.name {
	case "message_start":
		msg, _ := payload["message"].(map[string]any)
		if msg == nil {
			return nil, false
		}
		if raw, has := msg["model"]; has {
			served, _ := raw.(string)
			if !m.notedServed && served != "" {
				m.notedServed = true
				m.s.noteServedModel(m.ev, m.a.sentModel, served)
			}
			msg["model"] = m.a.publicName
		}
		if mu, ok := decodeMessagesUsage(msg["usage"]); ok && mu.InputTokens != nil {
			m.sawStart = true
			m.input = nonNegative(*mu.InputTokens)
			m.cacheRead = nonNegative(intOf(mu.CacheReadInputTokens))
			m.cacheCreation = nonNegative(intOf(mu.CacheCreationInputTokens))
		}
		out, err := json.Marshal(payload)
		if err != nil {
			return nil, false
		}
		m.settle(res)
		return out, true
	case "message_delta":
		if mu, ok := decodeMessagesUsage(payload["usage"]); ok && mu.OutputTokens != nil {
			m.sawDelta = true
			if mu.InputTokens != nil {
				m.sawStart = true
				m.input = nonNegative(*mu.InputTokens)
			}
			if mu.CacheReadInputTokens != nil {
				m.cacheRead = nonNegative(*mu.CacheReadInputTokens)
			}
			if mu.CacheCreationInputTokens != nil {
				m.cacheCreation = nonNegative(*mu.CacheCreationInputTokens)
			}
			res.usage.CompletionTokens = nonNegative(*mu.OutputTokens)
			res.usage.CompletionTokensDetails.ReasoningTokens = mu.OutputTokensDetails.ReasoningTokens
			if c := mu.cost(); c != "" {
				res.usage.Cost = c
			}
			m.settle(res)
		}
	case "content_block_delta":
		if delta, _ := payload["delta"].(map[string]any); delta != nil && delta["type"] == "text_delta" {
			t, _ := delta["text"].(string)
			res.contentChars += len(t)
		}
	}
	// Everything else is relayed as the upstream wrote it: no other event
	// carries the model name, and re-encoding would change nothing a client
	// reads.
	return e.data, true
}

// settle writes what has been read so far into the result. Usage counts as
// read only once both halves have arrived; a stream that never reports its
// output is one this service cannot meter.
func (m *messagesStream) settle(res *streamResult) {
	res.usage.PromptTokens = m.input + m.cacheRead + m.cacheCreation
	res.usage.PromptTokensDetails.CachedTokens = m.cacheRead
	res.usage.TotalTokens = res.usage.PromptTokens + res.usage.CompletionTokens
	res.haveUsage = m.sawStart && m.sawDelta
}

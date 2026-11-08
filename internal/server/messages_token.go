package server

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/pnuops/pickle-llm-gateway/internal/config"
	"github.com/pnuops/pickle-llm-gateway/internal/snapshot"
	"github.com/pnuops/pickle-llm-gateway/internal/spool"
)

// The Messages surface on the self-hosted axis. The request goes to the
// serving process's own Messages endpoint as it does to the vendor's; what
// differs is what a caller may send, how large an input may be, and how many
// turns may run at once on the upstream. Whether an upstream serves this
// surface at all is its Messages setting (see config.Upstream.Messages).

// messagesTokenParams are the top-level fields forwarded for a self-hosted
// model on this surface. Any other field is removed before forwarding, so
// replacing the serving process cannot silently change what a caller may
// send, and the request goes on without it (see messagesTokenStripUnknown).
var messagesTokenParams = map[string]bool{
	"model":          true,
	"messages":       true,
	"system":         true,
	"max_tokens":     true,
	"stop_sequences": true,
	"stream":         true,
	"temperature":    true,
	"top_p":          true,
	"top_k":          true,
	"tools":          true,
	"tool_choice":    true,
}

// messagesTokenStrip are removed rather than refused. An agent client sends
// them on its own, from what it believes about the model, and its user has no
// setting that stops it; refusing would refuse every turn. Removing thinking
// and output_config keeps reasoning at the upstream's default, which is what
// refusing them on chat achieves (see reasoning.go): the serving process
// honours output_config's effort and may honour thinking in a later release.
// metadata describes the caller to the platform's own box, which has no use
// for it; context_management and service_tier ask for vendor features this
// upstream does not have.
var messagesTokenStrip = map[string]bool{
	"thinking":           true,
	"output_config":      true,
	"metadata":           true,
	"context_management": true,
	"service_tier":       true,
}

// messagesTokenRefused are refused rather than removed. They are one serving
// process's own switches for reasoning, refused on chat for that reason; a
// caller who sends one asks for a change in how the answer is generated, and
// removing it would hand back the default as if it had been honoured.
var messagesTokenRefused = map[string]bool{
	"chat_template_kwargs":  true,
	"thinking_token_budget": true,
}

func messagesTokenAxisParam(name string) *refusal {
	if messagesTokenParams[name] {
		return nil
	}
	return &refusal{e: errUnsupportedParam(name)}
}

// messagesTokenStripUnknown removes every field off the forwarded list but
// the refused ones. Agent clients add top-level fields release by release
// (beta features, vendor hints), and their users cannot stop them; refusing
// an unknown one refused every turn of every user of that release until the
// list caught up. The removed names are logged and counted, so a field that
// starts arriving can be judged on what it is.
func messagesTokenStripUnknown(name string) bool {
	return !messagesTokenRefused[name]
}

// strippedFieldsLogged bounds what one request puts in the log: the names
// are the caller's, and a body may carry any number of them.
const (
	strippedFieldsLogged   = 16
	strippedFieldNameBytes = 64
)

// noteStrippedFields logs and counts the fields removed from one request.
func (s *Server) noteStrippedFields(ev *spool.Event, names []string) {
	s.metrics.messagesFieldsStripped.Add(int64(len(names)))
	sort.Strings(names)
	if len(names) > strippedFieldsLogged {
		names = names[:strippedFieldsLogged]
	}
	for i, n := range names {
		if len(n) > strippedFieldNameBytes {
			names[i] = n[:strippedFieldNameBytes]
		}
	}
	s.log.Info("unknown fields removed", "keyId", ev.KeyID, "model", ev.PublicModelName, "fields", names)
}

// messagesTokenAdmit reports whether a self-hosted model may be served on this
// surface, judged on its primary upstream the way reasoning is.
func (s *Server) messagesTokenAdmit(model *snapshot.Model) bool {
	up, ok := s.cfg.Upstreams[strings.ToLower(model.UpstreamRef)]
	return ok && up.Messages == config.MessagesOpen
}

// messagesTokenUpstream reports whether one attempt may go to up: only the
// primary upstream the turn was admitted on. Its input was counted against
// that upstream's cap and it holds that upstream's turn place, so sending it
// to a fallback, open or not, would run it on a box whose bounds it never
// passed. A fallback is skipped the way one without a credential is on the
// money axis, and a primary in cooldown fails the turn instead of moving it.
func messagesTokenUpstream(model *snapshot.Model, up config.Upstream) bool {
	return up.Messages == config.MessagesOpen && strings.EqualFold(up.Ref, model.UpstreamRef)
}

// fenceMessagesTokenTools is the self-hosted tool profile on this surface:
// client tools only, in the Messages shape. A server tool names something the
// vendor runs, and a self-hosted box runs nothing on the caller's behalf.
func fenceMessagesTokenTools(params map[string]json.RawMessage) *apiError {
	names := make(map[string]bool)
	if raw := params[toolsField]; !nullOrAbsent(raw) {
		var tools []json.RawMessage
		if json.Unmarshal(raw, &tools) != nil {
			return invalidMessagesTokenTool(toolsField)
		}
		for _, rawTool := range tools {
			name, ok := messagesClientTool(rawTool)
			if !ok {
				return invalidMessagesTokenTool(toolsField)
			}
			names[name] = true
		}
	}
	raw := params[toolChoiceField]
	if nullOrAbsent(raw) {
		return nil
	}
	choice, ok := tokenToolObject(raw)
	if !ok {
		return invalidMessagesTokenTool(toolChoiceField)
	}
	var kind string
	if json.Unmarshal(choice["type"], &kind) != nil {
		return invalidMessagesTokenTool(toolChoiceField)
	}
	if raw, present := choice["disable_parallel_tool_use"]; present && !jsonBoolOrNull(raw) {
		return invalidMessagesTokenTool(toolChoiceField)
	}
	switch kind {
	case "auto", "any", "none":
		if !tokenToolFields(choice, "type", "disable_parallel_tool_use") {
			return invalidMessagesTokenTool(toolChoiceField)
		}
		return nil
	case "tool":
		var name string
		if !tokenToolFields(choice, "type", "name", "disable_parallel_tool_use") ||
			json.Unmarshal(choice["name"], &name) != nil || !names[name] {
			return invalidMessagesTokenTool(toolChoiceField)
		}
		return nil
	}
	return invalidMessagesTokenTool(toolChoiceField)
}

// messagesClientTool reads one client tool definition. Only the protocol
// object is inspected; input_schema is JSON Schema data and is left alone.
func messagesClientTool(raw json.RawMessage) (string, bool) {
	tool, ok := tokenToolObject(raw)
	if !ok || !tokenToolFields(tool, "type", "name", "description", "input_schema", "cache_control", "strict") {
		return "", false
	}
	if raw, present := tool["type"]; present {
		var kind string
		if json.Unmarshal(raw, &kind) != nil || kind != "custom" {
			return "", false
		}
	}
	var name string
	if json.Unmarshal(tool["name"], &name) != nil || !messagesToolName(name) {
		return "", false
	}
	if raw, present := tool["description"]; present {
		var description string
		if json.Unmarshal(raw, &description) != nil {
			return "", false
		}
	}
	schema := bytes.TrimSpace(tool["input_schema"])
	if len(schema) == 0 || schema[0] != '{' {
		return "", false
	}
	if raw, present := tool["cache_control"]; present && !nullOrAbsent(raw) {
		if b := bytes.TrimSpace(raw); len(b) == 0 || b[0] != '{' {
			return "", false
		}
	}
	if raw, present := tool["strict"]; present && !jsonBoolOrNull(raw) {
		return "", false
	}
	return name, true
}

// messagesToolName follows the vendor's rule for a client tool name, which
// is longer than chat's: agent clients prefix tool names with the server
// that provides them.
func messagesToolName(name string) bool {
	if len(name) == 0 || len(name) > 128 {
		return false
	}
	for _, c := range []byte(name) {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '-') {
			return false
		}
	}
	return true
}

func jsonBoolOrNull(raw json.RawMessage) bool {
	switch string(bytes.TrimSpace(raw)) {
	case "true", "false", "null":
		return true
	}
	return false
}

func invalidMessagesTokenTool(field string) *apiError {
	e := errInvalidParamValue(field)
	e.message = field + "에는 클라이언트가 실행하는 도구(name, description, input_schema)와 올바른 도구 선택만 사용할 수 있어요. 서버 도구나 확장 필드를 제거하고 도구 정의를 확인해 주세요."
	return &e
}

// countFields are the parts of a Messages request the token counter reads.
// Everything else changes how an answer is generated, not how long the input
// is, and some of it (thinking, output_config) is refused on this axis.
var countFields = []string{"model", "messages", "system", "tools", "tool_choice"}

// countInputTimeout bounds one call to the upstream's counter. Counting is
// tokenisation on the serving process's front end, well under a second for
// an input at the cap; a counter that takes longer is treated as unavailable.
const countInputTimeout = 5 * time.Second

// countMessagesTokens asks up how many input tokens the request is, with the
// model already renamed to the upstream's name.
func (s *Server) countMessagesTokens(ctx context.Context, up config.Upstream, model *snapshot.Model, params map[string]json.RawMessage) (int, *attemptError) {
	body := make(map[string]json.RawMessage, len(countFields))
	for _, f := range countFields {
		if v, ok := params[f]; ok {
			body[f] = v
		}
	}
	b, err := json.Marshal(body)
	if err != nil {
		return 0, &attemptError{err: err}
	}
	ctx, cancel := context.WithTimeout(ctx, countInputTimeout)
	defer cancel()
	resp, ae := s.do(ctx, up, attemptSpec{
		client: s.client,
		method: http.MethodPost,
		path:   "/messages/count_tokens",
		body:   b,
		cred:   up.APIKey,
		// A relayed rejection names the model the way the caller does.
		upstreamModel: model.UpstreamModel,
		publicModel:   model.PublicName,
	})
	if ae != nil {
		return 0, ae
	}
	defer resp.Body.Close()
	var out struct {
		InputTokens *int `json:"input_tokens"`
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if err != nil || json.Unmarshal(raw, &out) != nil || out.InputTokens == nil || *out.InputTokens < 0 {
		return 0, &attemptError{err: errUpstreamStatus, kind: failureInvalidResponse}
	}
	return *out.InputTokens, nil
}

// messagesInputWithinCap refuses a request whose input is above up's cap. It
// returns the counted input, or 0 when the body was admitted without
// counting or the counter was unavailable. A counter that fails admits the
// request: the cap protects other callers' latency, not the upstream, whose
// own context limit still refuses an input that cannot fit.
func (s *Server) messagesInputWithinCap(ctx context.Context, up config.Upstream, model *snapshot.Model, params map[string]json.RawMessage,
	bodyBytes int, refuse func(apiError, string)) (int, bool) {
	limit := up.MessagesMaxInputTokens
	if limit <= 0 || fitsWithoutCounting(bodyBytes, limit) {
		return 0, true
	}
	n, ae := s.countMessagesTokens(ctx, up, model, params)
	if ae != nil {
		if ctx.Err() == nil {
			s.metrics.messagesCountFailures.Add(1)
			s.log.Warn("input count failed, admitting uncounted", "upstreamRef", up.Ref, "error", ae.err)
		}
		return 0, true
	}
	if n > limit {
		s.metrics.messagesInputRefused.Add(1)
		refuse(errAgentInputTooLong(n, limit), spool.StatusBadRequest)
		return n, false
	}
	return n, true
}

// countSkipMargin is how far under the cap a body must be, in bytes, to be
// admitted without counting. The serving process's tokeniser is byte-level, so
// a token is at least one byte of the input, and what the template adds around
// the messages is far smaller than the JSON syntax the byte count includes.
const countSkipMargin = 2048

// fitsWithoutCounting reports whether a body is certainly under a token cap.
func fitsWithoutCounting(bodyBytes, maxTokens int) bool {
	return bodyBytes+countSkipMargin <= maxTokens
}

// chatInputIsLong reports whether a self-hosted chat request on up is long
// enough to take an agent turn place: up serves agents with a turn bound and
// a chat threshold, and the prompt counted by up's own tokenizer reaches it.
// A body too short in bytes to reach it is not counted. A count that fails
// leaves the request outside the bound, as a failed count admits a turn
// past the input cap: the bound protects other callers' latency, and chat
// was outside it until the threshold existed.
func (s *Server) chatInputIsLong(ctx context.Context, up config.Upstream, model *snapshot.Model, params map[string]json.RawMessage, bodyBytes int) bool {
	limit := up.MessagesChatInputTokens
	if up.Messages != config.MessagesOpen || up.MessagesMaxConcurrent <= 0 || limit <= 0 ||
		fitsWithoutCounting(bodyBytes, limit-1) {
		return false
	}
	n, ae := s.countChatTokens(ctx, up, model, params)
	if ae != nil {
		if ctx.Err() == nil {
			s.metrics.messagesCountFailures.Add(1)
			s.log.Warn("chat input count failed, leaving it outside the turn bound", "upstreamRef", up.Ref, "error", ae.err)
		}
		return false
	}
	if n < limit {
		return false
	}
	s.metrics.chatLongInputPlaces.Add(1)
	return true
}

// chatCountFields are the parts of a chat request the serving process's
// tokenizer reads to render the prompt.
var chatCountFields = []string{"model", "messages", "tools"}

// tokenizeResponseMax bounds the tokenizer's answer, which lists every token
// id: at a 131,072-token context that is under a megabyte.
const tokenizeResponseMax = 8 << 20

// countChatTokens asks up how many tokens a chat request's rendered prompt
// is, with the model already renamed to the upstream's name. The serving
// process answers on its /tokenize route, which sits beside the versioned
// API rather than under it, so a base URL ending in /v1 loses that suffix.
func (s *Server) countChatTokens(ctx context.Context, up config.Upstream, model *snapshot.Model, params map[string]json.RawMessage) (int, *attemptError) {
	body := make(map[string]json.RawMessage, len(chatCountFields))
	for _, f := range chatCountFields {
		if v, ok := params[f]; ok {
			body[f] = v
		}
	}
	b, err := json.Marshal(body)
	if err != nil {
		return 0, &attemptError{err: err}
	}
	ctx, cancel := context.WithTimeout(ctx, countInputTimeout)
	defer cancel()
	root := up
	root.BaseURL = strings.TrimSuffix(up.BaseURL, "/v1")
	resp, ae := s.do(ctx, root, attemptSpec{
		client:        s.client,
		method:        http.MethodPost,
		path:          "/tokenize",
		body:          b,
		cred:          up.APIKey,
		upstreamModel: model.UpstreamModel,
		publicModel:   model.PublicName,
	})
	if ae != nil {
		return 0, ae
	}
	defer resp.Body.Close()
	var out struct {
		Count *int `json:"count"`
	}
	if json.NewDecoder(io.LimitReader(resp.Body, tokenizeResponseMax)).Decode(&out) != nil || out.Count == nil || *out.Count < 0 {
		return 0, &attemptError{err: errUpstreamStatus, kind: failureInvalidResponse}
	}
	return *out.Count, nil
}

// handleCountTokens answers the Messages token counter. A self-hosted model
// whose upstream opens this surface is counted by that upstream's own
// counter, the number the input cap is judged on. Anything else gets the
// fast 404 the route has always given, so an agent client falls back to its
// own estimate: a paid model, an unknown one, and a request that cannot be
// read far enough to name its model (wrong method, too large, not JSON).
// Authentication comes first, since a count goes to a shared upstream.
//
// It is not metered: counting generates nothing and spends no sequence on
// the upstream, so it takes no per-key limit and writes no usage record. The
// gateway-wide in-flight bound and the ordinary body bound hold it.
func (s *Server) handleCountTokens(w http.ResponseWriter, r *http.Request) {
	doc, keyLookup, modelLookup := s.store.Current()
	if r.Method != http.MethodPost || !doc.ServiceEnabled {
		writeAnthropicError(w, errNotFound, "")
		return
	}
	token, missing := messagesToken(r)
	if missing != nil {
		writeAnthropicError(w, *missing, "")
		return
	}
	key, authErr := s.authenticateToken(token, keyLookup)
	if authErr != nil {
		writeAnthropicError(w, *authErr, "")
		return
	}
	if !key.AllowsEndpoint(snapshot.EndpointChat) {
		writeAnthropicError(w, errEndpointNotAllowed(endpointLabel(snapshot.EndpointChat)), "")
		return
	}
	select {
	case s.inFlight <- struct{}{}:
		defer func() { <-s.inFlight }()
	default:
		writeAnthropicError(w, errServerBusy, "")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, s.cfg.RequestBodyMaxBytes)
	raw, err := io.ReadAll(r.Body)
	var params map[string]json.RawMessage
	if err != nil || json.Unmarshal(raw, &params) != nil {
		writeAnthropicError(w, errNotFound, "")
		return
	}
	req, rf := parseMessages(params)
	if rf != nil {
		writeAnthropicError(w, errNotFound, "")
		return
	}
	model := modelLookup(req.publicModel)
	if model == nil || model.CreditAxis() || !s.messagesTokenAdmit(model) {
		writeAnthropicError(w, errNotFound, "")
		return
	}
	if !key.AllowsModel(model) {
		writeAnthropicError(w, errModelNotAllowed, "")
		return
	}
	// The same tool profile the turn itself would meet, so the two routes
	// refuse the same definitions in the same words.
	if e := fenceMessagesTokenTools(params); e != nil {
		writeAnthropicError(w, *e, "")
		return
	}
	up := s.cfg.Upstreams[strings.ToLower(model.UpstreamRef)]
	params["model"] = json.RawMessage(strconv.Quote(model.UpstreamModel))
	n, ae := s.countMessagesTokens(r.Context(), up, model, params)
	switch {
	case ae == nil:
	case ae.refusal != nil:
		writeAnthropicError(w, *ae.refusal, "")
		return
	case ae.timeout:
		writeAnthropicError(w, errUpstreamTimeout, "")
		return
	default:
		writeAnthropicError(w, errUpstream, "")
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	writeJSON(w, map[string]int{"input_tokens": n})
}

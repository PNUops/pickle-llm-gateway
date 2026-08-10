// The chat-completions path: authenticate, admit against limits, translate
// the public model name, forward, and meter. The gateway parses request and
// response JSON only to validate parameters, swap model names and read token
// usage; content is never inspected beyond size and never stored.
package server

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/pnuops/pickle-llm-gateway/internal/limits"
	"github.com/pnuops/pickle-llm-gateway/internal/spool"
)

// allowedParams are the top-level request fields the gateway forwards. An
// unknown field is refused rather than silently forwarded, so replacing the
// upstream can never silently change what student code is allowed to send.
var allowedParams = map[string]bool{
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

type usage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

func (s *Server) handleChat(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeAPIError(w, errMethod)
		return
	}
	start := s.now()
	ev := spool.Event{EventUUID: spool.NewEventUUID(), RequestedAt: start}
	record := func() {
		ev.LatencyMs = time.Since(start).Milliseconds()
		if err := s.spool.Write(ev); err != nil {
			s.log.Error("usage spool write failed", "error", err)
		}
		s.log.Info("chat request", "keyId", ev.KeyID, "model", ev.PublicModelName,
			"status", ev.Status, "errorType", ev.ErrorType,
			"inputTokens", ev.InputTokens, "outputTokens", ev.OutputTokens,
			"latencyMs", ev.LatencyMs, "ttftMs", ev.TtftMs)
	}
	refuse := func(e apiError, status string) {
		writeAPIError(w, e)
		ev.Status = status
		ev.ErrorType = e.code
		record()
	}

	// One snapshot view per request: every check below reads this state, so a
	// concurrent reload can never mix generations within a request.
	doc, keyLookup, modelLookup := s.store.Current()
	if !doc.ServiceEnabled {
		refuse(errServiceDisabled, spool.StatusAuthRejected)
		return
	}
	key, authErr := s.authenticate(r, keyLookup)
	if authErr != nil {
		refuse(*authErr, spool.StatusAuthRejected)
		return
	}
	ev.KeyID = key.KeyID

	if key.QuotaExhausted {
		refuse(errQuotaExhausted, spool.StatusRateLimited)
		return
	}
	// The gateway-wide cap is checked before any per-key charge: a refusal
	// the student cannot influence must not spend their request budget.
	select {
	case s.inFlight <- struct{}{}:
		defer func() { <-s.inFlight }()
	default:
		refuse(errServerBusy, spool.StatusRateLimited)
		return
	}
	rpm, tpm, conc := s.keyLimits(key)
	release, reason := s.limiter.Acquire(key.KeyID, rpm, tpm, conc)
	switch reason {
	case limits.Rpm:
		refuse(errRateRequests, spool.StatusRateLimited)
		return
	case limits.Tpm:
		refuse(errRateTokens, spool.StatusRateLimited)
		return
	case limits.Concurrency:
		refuse(errRateConcurrency, spool.StatusRateLimited)
		return
	}
	defer release()

	r.Body = http.MaxBytesReader(w, r.Body, s.cfg.RequestBodyMaxBytes)
	rawBody, err := io.ReadAll(r.Body)
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			refuse(errRequestTooLarge, spool.StatusBadRequest)
		} else {
			refuse(errBadJSON, spool.StatusBadRequest)
		}
		return
	}
	var params map[string]json.RawMessage
	if err := json.Unmarshal(rawBody, &params); err != nil {
		refuse(errBadJSON, spool.StatusBadRequest)
		return
	}
	for name := range params {
		if !allowedParams[name] {
			refuse(errUnsupportedParam(name), spool.StatusBadRequest)
			return
		}
	}
	var publicModel string
	if raw, ok := params["model"]; !ok || json.Unmarshal(raw, &publicModel) != nil || publicModel == "" {
		refuse(errMissingParam("model"), spool.StatusBadRequest)
		return
	}
	messagesRaw, ok := params["messages"]
	if !ok {
		refuse(errMissingParam("messages"), spool.StatusBadRequest)
		return
	}
	model := modelLookup(publicModel)
	if model == nil {
		refuse(errModelNotFound, spool.StatusBadRequest)
		return
	}
	ev.PublicModelName = publicModel
	if !key.Allows(publicModel) {
		refuse(errModelNotAllowed, spool.StatusBadRequest)
		return
	}
	up, ok := s.cfg.Upstreams[strings.ToLower(model.UpstreamRef)]
	if !ok {
		s.log.Error("model references an unconfigured upstream", "model", publicModel, "upstreamRef", model.UpstreamRef)
		refuse(errUpstream, spool.StatusUpstreamErr)
		return
	}

	// Output length. A JSON null is what SDKs send for "unset" and is treated
	// as absent; an explicit value above the model cap is refused. Whatever the
	// student sent (on either OpenAI field) is normalized onto the upstream's
	// configured cap field and capped at the model maximum, so the limit lands
	// on the field the upstream actually honors — forwarding the student's
	// field verbatim would let a legacy `max_tokens`-only server ignore a
	// `max_completion_tokens` request and blow past the cap.
	if model.MaxOutputTokens > 0 {
		effectiveCap := model.MaxOutputTokens
		for _, f := range []string{"max_completion_tokens", "max_tokens"} {
			raw, ok := params[f]
			if !ok {
				continue
			}
			delete(params, f) // re-added below on up.CapField
			if string(bytes.TrimSpace(raw)) == "null" {
				continue
			}
			var n int
			if json.Unmarshal(raw, &n) != nil || n <= 0 {
				refuse(errInvalidParamValue(f), spool.StatusBadRequest)
				return
			}
			if n > model.MaxOutputTokens {
				refuse(errOutputTooLong, spool.StatusBadRequest)
				return
			}
			if n < effectiveCap {
				effectiveCap = n
			}
		}
		params[up.CapField] = json.RawMessage(strconv.Itoa(effectiveCap))
	}
	// Input length: token counting needs the model's tokenizer, which the
	// gateway does not have. This guard only refuses what cannot possibly
	// fit (bytes far beyond the token budget); exact enforcement is the
	// upstream's context-window error.
	if model.MaxInputTokens > 0 && len(messagesRaw) > model.MaxInputTokens*6 {
		refuse(errInputTooLong, spool.StatusBadRequest)
		return
	}

	streaming := false
	if raw, ok := params["stream"]; ok {
		_ = json.Unmarshal(raw, &streaming)
	}
	// Usage must always come back from the upstream for metering, but the
	// usage chunk is only forwarded when the student asked for it — clients
	// that never opted in would break on a chunk with an empty choices array.
	studentWantsUsage := false
	if raw, ok := params["stream_options"]; ok {
		var opts struct {
			IncludeUsage bool `json:"include_usage"`
		}
		_ = json.Unmarshal(raw, &opts)
		studentWantsUsage = opts.IncludeUsage
	}
	params["model"] = json.RawMessage(strconv.Quote(model.UpstreamModel))
	if streaming {
		params["stream_options"] = withIncludeUsage(params["stream_options"])
	}
	upBody, err := json.Marshal(params)
	if err != nil {
		refuse(errBadJSON, spool.StatusBadRequest)
		return
	}

	upCtx, cancel := context.WithTimeout(r.Context(), s.cfg.RequestMaxDuration)
	defer cancel()
	upReq, err := http.NewRequestWithContext(upCtx, http.MethodPost, up.BaseURL+"/chat/completions", bytes.NewReader(upBody))
	if err != nil {
		s.log.Error("building upstream request failed", "error", err)
		refuse(errUpstream, spool.StatusUpstreamErr)
		return
	}
	upReq.Header.Set("Content-Type", "application/json")
	if up.APIKey != "" {
		upReq.Header.Set("Authorization", "Bearer "+up.APIKey)
	}

	resp, err := s.client.Do(upReq)
	ev.TtftMs = time.Since(start).Milliseconds()
	if err != nil {
		switch {
		case r.Context().Err() != nil:
			// The student went away; nothing can be written back.
			ev.Status = spool.StatusCanceled
			ev.ErrorType = "client_disconnected"
			record()
		case upCtx.Err() != nil || isTimeout(err):
			refuse(errUpstreamTimeout, spool.StatusTimeout)
		default:
			s.log.Error("upstream request failed", "error", err)
			refuse(errUpstream, spool.StatusUpstreamErr)
		}
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		detail, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		s.log.Warn("upstream refused request", "status", resp.StatusCode, "keyId", key.KeyID,
			"model", publicModel, "detail", string(detail))
		switch resp.StatusCode {
		case http.StatusBadRequest:
			refuse(errUpstreamRejected, spool.StatusUpstreamErr)
		case http.StatusTooManyRequests:
			refuse(errServerBusy, spool.StatusUpstreamErr)
		default:
			refuse(errUpstream, spool.StatusUpstreamErr)
		}
		return
	}

	if !streaming {
		s.finishNonStream(w, resp, model.PublicName, key.KeyID, tpm, &ev, record, len(messagesRaw))
		return
	}
	s.finishStream(w, resp, streamArgs{
		publicName:   model.PublicName,
		keyID:        key.KeyID,
		tpm:          tpm,
		inputBytes:   len(messagesRaw),
		forwardUsage: studentWantsUsage,
		clientCtx:    r.Context(),
		upCtx:        upCtx,
	}, &ev, record)
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

func (s *Server) finishNonStream(w http.ResponseWriter, resp *http.Response, publicName, keyID string, tpm int, ev *spool.Event, record func(), inputBytes int) {
	body, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			writeAPIError(w, errUpstreamTimeout)
			ev.Status = spool.StatusTimeout
			ev.ErrorType = errUpstreamTimeout.code
		} else {
			writeAPIError(w, errUpstream)
			ev.Status = spool.StatusUpstreamErr
			ev.ErrorType = "upstream_read_failed"
		}
		// The upstream may have generated tokens before the read failed; charge
		// an input-side estimate so a client looping large failing requests is
		// still rate-limited rather than metered as free.
		s.settleUsage(ev, usage{}, false, inputBytes, 0)
		s.limiter.ChargeTokens(keyID, tpm, ev.InputTokens+ev.OutputTokens)
		record()
		return
	}
	// A response that does not parse as JSON is not something the gateway can
	// vouch for, and forwarding it verbatim would expose the upstream's model
	// identifiers — answer an upstream error instead.
	var m map[string]any
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	if dec.Decode(&m) != nil {
		writeAPIError(w, errUpstream)
		ev.Status = spool.StatusUpstreamErr
		ev.ErrorType = "upstream_invalid_response"
		record()
		return
	}
	if _, has := m["model"]; has {
		m["model"] = publicName
	}
	var u usage
	haveUsage := false
	if uraw, has := m["usage"]; has && uraw != nil {
		if b, err := json.Marshal(uraw); err == nil && json.Unmarshal(b, &u) == nil {
			haveUsage = true
		}
	}
	// Fallback size for the estimate when the upstream reported no usage.
	contentChars := 0
	if !haveUsage {
		if choices, _ := m["choices"].([]any); len(choices) > 0 {
			if first, _ := choices[0].(map[string]any); first != nil {
				if msg, _ := first["message"].(map[string]any); msg != nil {
					if content, _ := msg["content"].(string); content != "" {
						contentChars = len(content)
					}
				}
			}
		}
	}
	out, err := json.Marshal(m)
	if err != nil {
		writeAPIError(w, errUpstream)
		ev.Status = spool.StatusUpstreamErr
		ev.ErrorType = "upstream_invalid_response"
		record()
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	if _, err := w.Write(out); err != nil {
		ev.Status = spool.StatusCanceled
		ev.ErrorType = "client_disconnected"
	} else {
		ev.Status = spool.StatusOK
	}
	s.settleUsage(ev, u, haveUsage, inputBytes, contentChars)
	s.limiter.ChargeTokens(keyID, tpm, ev.InputTokens+ev.OutputTokens)
	record()
}

type streamArgs struct {
	publicName   string
	keyID        string
	tpm          int
	inputBytes   int
	forwardUsage bool
	clientCtx    context.Context
	upCtx        context.Context
}

func (s *Server) finishStream(w http.ResponseWriter, resp *http.Response, a streamArgs, ev *spool.Event, record func()) {
	fl, _ := w.(http.Flusher)
	w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	if fl != nil {
		fl.Flush()
	}
	writeFailed := false
	writeRaw := func(b []byte) bool {
		if writeFailed {
			return false
		}
		if _, err := w.Write(b); err != nil {
			writeFailed = true
			return false
		}
		if fl != nil {
			fl.Flush()
		}
		return true
	}

	var u usage
	haveUsage := false
	contentChars := 0
	dropped := 0

	// SSE events are framed by blank lines and one event's data may span
	// several `data:` lines, so payloads are assembled per event before
	// parsing. An assembled payload that still does not parse is dropped, not
	// forwarded: a verbatim chunk would leak the upstream model identifier.
	var dataLines [][]byte
	flushEvent := func() bool {
		if len(dataLines) == 0 {
			return true
		}
		payload := bytes.Join(dataLines, []byte("\n"))
		dataLines = nil
		if bytes.Equal(payload, []byte("[DONE]")) {
			return writeRaw([]byte("data: [DONE]\n\n"))
		}
		c, ok := rewriteChunk(payload, a.publicName)
		if !ok {
			// Not valid JSON. A payload that does not even open an object is a
			// heartbeat/keepalive (`ping`) with nothing structured to leak —
			// forward it verbatim. One that looks like a truncated object is
			// dropped rather than risk leaking a partial upstream identifier,
			// and the answer is now incomplete, so the request is marked
			// degraded below.
			if len(payload) == 0 || payload[0] != '{' {
				return writeRaw(append(append([]byte("data: "), payload...), '\n', '\n'))
			}
			dropped++
			return true
		}
		if c.usage != nil {
			u = *c.usage
			haveUsage = true
			if !a.forwardUsage {
				if c.choicesEmpty {
					// The gateway-requested usage chunk; the student did not
					// opt in, so it is consumed for metering and not sent.
					return true
				}
				c.out = c.stripUsage()
			}
		}
		contentChars += c.contentChars
		return writeRaw(append(append([]byte("data: "), c.out...), '\n', '\n'))
	}

	br := bufio.NewReaderSize(resp.Body, 64<<10)
	var readErr error
	for {
		line, err := br.ReadBytes('\n')
		trimmed := bytes.TrimRight(line, "\r\n")
		switch {
		case len(trimmed) == 0 && len(line) > 0:
			flushEvent()
		case len(trimmed) == 0:
		case trimmed[0] == ':':
			// Comment lines are keepalives; forward as-is.
			writeRaw(append(trimmed, '\n', '\n'))
		default:
			if after, ok := bytes.CutPrefix(trimmed, []byte("data:")); ok {
				dataLines = append(dataLines, bytes.TrimSpace(after))
			}
			// Other SSE fields (event:, id:, retry:) are not part of the
			// surface and are dropped.
		}
		if writeFailed {
			break
		}
		if err != nil {
			if !errors.Is(err, io.EOF) {
				readErr = err
			} else {
				flushEvent()
			}
			break
		}
	}
	if dropped > 0 {
		s.log.Warn("dropped unparseable stream chunks", "keyId", a.keyID, "count", dropped)
	}

	// A stream that ends without the upstream's own [DONE] (deadline or an
	// interrupted read) gets a terminal error event plus [DONE] so the client
	// can tell a truncated answer from a complete one, rather than guessing
	// from a missing terminator.
	emitStreamError := func(code, msg string) {
		writeRaw([]byte(`data: {"error":{"message":"` + msg + `","type":"upstream_error","code":"` + code + `"}}` + "\n\n"))
		writeRaw([]byte("data: [DONE]\n\n"))
	}
	switch {
	case writeFailed || a.clientCtx.Err() != nil:
		ev.Status = spool.StatusCanceled
		ev.ErrorType = "client_disconnected"
	case errors.Is(readErr, context.DeadlineExceeded) || errors.Is(a.upCtx.Err(), context.DeadlineExceeded):
		// The gateway's own duration cap, not an upstream fault.
		ev.Status = spool.StatusTimeout
		ev.ErrorType = "request_deadline_exceeded"
		emitStreamError("request_deadline_exceeded", "요청 전체 시간 상한을 초과해 스트림을 종료했습니다.")
	case readErr != nil:
		ev.Status = spool.StatusUpstreamErr
		ev.ErrorType = "upstream_stream_interrupted"
		emitStreamError("upstream_stream_interrupted", "모델 서버 응답이 중간에 끊겼습니다. 응답이 불완전할 수 있습니다.")
	case dropped > 0:
		// The stream reached its end, but some chunks could not be forwarded,
		// so the answer the student received is incomplete. The upstream's
		// [DONE] already went out, so this cannot be signalled inline anymore;
		// record it as degraded rather than let it read as a clean success.
		ev.Status = spool.StatusUpstreamErr
		ev.ErrorType = "upstream_chunk_unreadable"
	default:
		ev.Status = spool.StatusOK
	}
	s.settleUsage(ev, u, haveUsage, a.inputBytes, contentChars)
	s.limiter.ChargeTokens(a.keyID, a.tpm, ev.InputTokens+ev.OutputTokens)
	record()
}

// chunk is one parsed and rewritten SSE payload.
type chunk struct {
	out          []byte
	parsed       map[string]any
	usage        *usage
	contentChars int
	choicesEmpty bool
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
	c := chunk{parsed: m, choicesEmpty: true}
	if _, has := m["model"]; has {
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

// settleUsage fills the event's token counts: exact when the upstream
// reported usage, a byte-based estimate flagged as such otherwise.
func (s *Server) settleUsage(ev *spool.Event, u usage, haveUsage bool, inputBytes, contentChars int) {
	if haveUsage {
		ev.InputTokens = u.PromptTokens
		ev.OutputTokens = u.CompletionTokens
		return
	}
	ev.InputTokens = inputBytes / 4
	ev.OutputTokens = contentChars / 3
	ev.Estimated = true
}

func isTimeout(err error) bool {
	var ne interface{ Timeout() bool }
	if errors.As(err, &ne) {
		return ne.Timeout()
	}
	return errors.Is(err, context.DeadlineExceeded)
}

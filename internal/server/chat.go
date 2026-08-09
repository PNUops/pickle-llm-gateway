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

	doc, _, modelLookup := s.store.Current()
	if !doc.ServiceEnabled {
		refuse(errServiceDisabled, spool.StatusAuthRejected)
		return
	}
	key, authErr := s.authenticate(r)
	if authErr != nil {
		refuse(*authErr, spool.StatusAuthRejected)
		return
	}
	ev.KeyID = key.KeyID

	if key.QuotaExhausted {
		refuse(errQuotaExhausted, spool.StatusRateLimited)
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
	select {
	case s.inFlight <- struct{}{}:
		defer func() { <-s.inFlight }()
	default:
		refuse(errServerBusy, spool.StatusRateLimited)
		return
	}

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

	// Output length: an explicit request above the model cap is refused, no
	// request at all gets the cap injected so the upstream enforces it.
	if model.MaxOutputTokens > 0 {
		seen := false
		for _, f := range []string{"max_completion_tokens", "max_tokens"} {
			raw, ok := params[f]
			if !ok {
				continue
			}
			seen = true
			var n int
			if json.Unmarshal(raw, &n) != nil || n <= 0 || n > model.MaxOutputTokens {
				refuse(errOutputTooLong, spool.StatusBadRequest)
				return
			}
		}
		if !seen {
			params["max_tokens"] = json.RawMessage(strconv.Itoa(model.MaxOutputTokens))
		}
	}
	// Input length: token counting needs the model's tokenizer, which the
	// gateway does not have. This guard only refuses what cannot possibly
	// fit (bytes far beyond the token budget); exact enforcement is the
	// upstream's context-window error.
	if model.MaxInputTokens > 0 && len(messagesRaw) > model.MaxInputTokens*6 {
		refuse(errInputTooLong, spool.StatusBadRequest)
		return
	}

	up, ok := s.cfg.Upstreams[strings.ToLower(model.UpstreamRef)]
	if !ok {
		s.log.Error("model references an unconfigured upstream", "model", publicModel, "upstreamRef", model.UpstreamRef)
		refuse(errUpstream, spool.StatusUpstreamErr)
		return
	}

	streaming := false
	if raw, ok := params["stream"]; ok {
		_ = json.Unmarshal(raw, &streaming)
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

	ctx, cancel := context.WithTimeout(r.Context(), s.cfg.RequestMaxDuration)
	defer cancel()
	upReq, err := http.NewRequestWithContext(ctx, http.MethodPost, up.BaseURL+"/chat/completions", bytes.NewReader(upBody))
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
		case ctx.Err() != nil || isTimeout(err):
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
	s.finishStream(w, resp, model.PublicName, key.KeyID, tpm, &ev, record, len(messagesRaw))
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
		writeAPIError(w, errUpstream)
		ev.Status = spool.StatusUpstreamErr
		ev.ErrorType = "upstream_read_failed"
		record()
		return
	}
	var m map[string]any
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	out := body
	var u usage
	haveUsage := false
	if dec.Decode(&m) == nil {
		if _, has := m["model"]; has {
			m["model"] = publicName
		}
		if uraw, has := m["usage"]; has && uraw != nil {
			if b, err := json.Marshal(uraw); err == nil && json.Unmarshal(b, &u) == nil {
				haveUsage = true
			}
		}
		if b, err := json.Marshal(m); err == nil {
			out = b
		}
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	if _, err := w.Write(out); err != nil {
		ev.Status = spool.StatusCanceled
		ev.ErrorType = "client_disconnected"
	} else {
		ev.Status = spool.StatusOK
	}
	s.settleUsage(ev, u, haveUsage, inputBytes, 0)
	s.limiter.ChargeTokens(keyID, tpm, ev.InputTokens+ev.OutputTokens)
	record()
}

func (s *Server) finishStream(w http.ResponseWriter, resp *http.Response, publicName, keyID string, tpm int, ev *spool.Event, record func(), inputBytes int) {
	fl, _ := w.(http.Flusher)
	w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	if fl != nil {
		fl.Flush()
	}

	br := bufio.NewReaderSize(resp.Body, 64<<10)
	var u usage
	haveUsage := false
	contentChars := 0
	writeFailed := false
	var readErr error
	for {
		line, err := br.ReadBytes('\n')
		if len(line) > 0 {
			out := line
			trimmed := bytes.TrimSpace(line)
			if payload, ok := bytes.CutPrefix(trimmed, []byte("data:")); ok {
				payload = bytes.TrimSpace(payload)
				if !bytes.Equal(payload, []byte("[DONE]")) {
					if rewritten, chunkUsage, delta, ok := rewriteChunk(payload, publicName); ok {
						out = append(append([]byte("data: "), rewritten...), '\n')
						contentChars += delta
						if chunkUsage != nil {
							u = *chunkUsage
							haveUsage = true
						}
					}
				}
			}
			if _, werr := w.Write(out); werr != nil {
				writeFailed = true
				break
			}
			if fl != nil {
				fl.Flush()
			}
		}
		if err != nil {
			if !errors.Is(err, io.EOF) {
				readErr = err
			}
			break
		}
	}

	switch {
	case writeFailed:
		ev.Status = spool.StatusCanceled
		ev.ErrorType = "client_disconnected"
	case readErr != nil:
		ev.Status = spool.StatusUpstreamErr
		ev.ErrorType = "upstream_stream_interrupted"
	default:
		ev.Status = spool.StatusOK
	}
	s.settleUsage(ev, u, haveUsage, inputBytes, contentChars)
	s.limiter.ChargeTokens(keyID, tpm, ev.InputTokens+ev.OutputTokens)
	record()
}

// rewriteChunk swaps the model name in one SSE data payload and pulls token
// usage out of the chunk that carries it. A payload that does not parse is
// forwarded untouched (ok=false), never dropped.
func rewriteChunk(payload []byte, publicName string) (out []byte, u *usage, contentChars int, ok bool) {
	var m map[string]any
	dec := json.NewDecoder(bytes.NewReader(payload))
	dec.UseNumber()
	if dec.Decode(&m) != nil {
		return nil, nil, 0, false
	}
	if _, has := m["model"]; has {
		m["model"] = publicName
	}
	if uraw, has := m["usage"]; has && uraw != nil {
		if b, err := json.Marshal(uraw); err == nil {
			var parsed usage
			if json.Unmarshal(b, &parsed) == nil {
				u = &parsed
			}
		}
	}
	if choices, _ := m["choices"].([]any); len(choices) > 0 {
		if c0, _ := choices[0].(map[string]any); c0 != nil {
			if delta, _ := c0["delta"].(map[string]any); delta != nil {
				if content, _ := delta["content"].(string); content != "" {
					contentChars = len(content)
				}
			}
		}
	}
	b, err := json.Marshal(m)
	if err != nil {
		return nil, nil, 0, false
	}
	return b, u, contentChars, true
}

// settleUsage fills the event's token counts: exact when the upstream
// reported usage, a byte-based estimate flagged as such when the request
// ended before the usage chunk arrived.
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

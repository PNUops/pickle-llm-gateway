// Reasoning on the self-hosted axis: whether a caller may set it, and how the
// request says it to the upstream that answers.
//
// Whether a model reasons when the request does not say is the upstream's own
// default and nothing here changes it. A local server started with thinking off
// stays off; a hosted model that reasons by default keeps reasoning. What the
// upstream's Reasoning setting decides is only whether the caller may change
// that, and the field it travels in.
package server

import (
	"bytes"
	"encoding/json"
	"strings"

	"github.com/pnuops/pickle-llm-gateway/internal/config"
	"github.com/pnuops/pickle-llm-gateway/internal/snapshot"
)

// reasoningEfforts are the values the Chat Completions reasoning_effort field
// documents. Anything else is refused here rather than left for the upstream:
// a template upstream cannot refuse it, because the value never reaches it.
var reasoningEfforts = map[string]bool{
	"none":    true,
	"minimal": true,
	"low":     true,
	"medium":  true,
	"high":    true,
	"xhigh":   true,
}

// reasoningOpen reports whether the caller may set reasoning on this model.
// Judged on the primary upstream, the one the model is declared on. A fallback
// with a different setting is handled per attempt by reasoningForAttempt, so a
// request admitted here is never refused later for the route it happened to
// take.
func (s *Server) reasoningOpen(model *snapshot.Model) bool {
	up, ok := s.cfg.Upstreams[strings.ToLower(model.UpstreamRef)]
	return ok && up.Reasoning != "" && up.Reasoning != config.ReasoningClosed
}

// validReasoningEffort accepts a documented value or JSON null, which SDKs send
// for "unset" and which is treated as absent.
func validReasoningEffort(raw json.RawMessage) bool {
	if string(bytes.TrimSpace(raw)) == "null" {
		return true
	}
	var v string
	if json.Unmarshal(raw, &v) != nil {
		return false
	}
	return reasoningEfforts[v]
}

// reasoningForAttempt rewrites one attempt's reasoning field for the upstream that
// attempt goes to. It runs only for self-hosted models; on the money axis the
// field is the provider's and goes out as written.
//
//   - effort: forwarded as is.
//   - template: replaced by the chat template's thinking switch, off for
//     "none" and on for every other value. The template has no notion of
//     effort, so low and high reach it as the same request.
//   - closed: removed. Only a fallback can be closed here, since a closed
//     primary refuses the field at admission; serving the request without
//     reasoning is better than failing it for the route it took.
//
// A null value is removed whatever the setting. params is not modified; the
// caller gets a copy when anything changes.
func reasoningForAttempt(params map[string]json.RawMessage, field string, up config.Upstream) map[string]json.RawMessage {
	raw, ok := params[field]
	if field == "" || !ok {
		return params
	}
	out := make(map[string]json.RawMessage, len(params)+1)
	for k, v := range params {
		out[k] = v
	}
	delete(out, field)
	if string(bytes.TrimSpace(raw)) == "null" {
		return out
	}
	switch up.Reasoning {
	case config.ReasoningEffort:
		out[field] = raw
	case config.ReasoningTemplate:
		var v string
		_ = json.Unmarshal(raw, &v)
		if v == "none" {
			out["chat_template_kwargs"] = json.RawMessage(`{"enable_thinking":false}`)
		} else {
			out["chat_template_kwargs"] = json.RawMessage(`{"enable_thinking":true}`)
		}
	}
	return out
}

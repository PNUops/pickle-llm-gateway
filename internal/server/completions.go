// The legacy OpenAI completions surface: POST /v1/completions, a prompt in and
// text out. OpenAI keeps it as a legacy route and its SDK still calls it
// through completions.create. Like the other surfaces it forwards the body in
// the shape it came in, with the model renamed.
//
// It serves paid models only. The vendor answers it with Chat's usage and
// stream shape, but it puts every chat model behind the route by turning the
// prompt into a chat message, so this is not raw text continuation; on the
// self-hosted side the route would bypass the chat template the serving
// defaults (thinking off) are enforced through, which nobody has measured.
package server

import (
	"bytes"
	"encoding/json"
	"net/http"

	"github.com/pnuops/pickle-llm-gateway/internal/config"
	"github.com/pnuops/pickle-llm-gateway/internal/snapshot"
	"github.com/pnuops/pickle-llm-gateway/internal/spool"
)

var protoCompletions = &protocol{
	endpoint: spool.EndpointCompletions,
	// The chat grant: a completion is text generation, the same thing a key
	// granted chat may already do on the chat route.
	capability:   snapshot.EndpointChat,
	upstreamPath: "/completions",
	logLine:      "completions request",
	limits: func(c *config.Config) bodyLimits {
		return bodyLimits{
			request:       c.RequestBodyMaxBytes,
			largeRequest:  c.ChatLargeBodyMaxBytes,
			largeResponse: c.ChatLargeResponseMaxBytes,
		}
	},
	parse:      parseCompletions,
	creditOnly: &errModelNotOnCompletions,
	// Unreachable while the surface is closed to the self-hosted axis.
	tokenAxisParam: func(name string) *refusal { return &refusal{e: errUnsupportedParam(name)} },
	// Chat's fences. The vendor reads the same routing and plugin fields here,
	// and the fences read only the fields they name, so a body without them
	// passes.
	fences: []fence{
		fencePresetField,
		fenceCandidates,
		fenceServerToolsStep,
		fenceToolChoice,
		fencePluginsStep,
		fenceChatCapabilities,
	},
	requestHeaders: chatForwardedHeaders,
	capReadFields:  []string{"max_tokens"},
	capWriteField:  "max_tokens",
	shape:          shapeChat,
	// No body capture: the record format holds chat messages, not a prompt.
	capture:         nil,
	noUsageEstimate: true,
	readResponse:    readCompletionsResponse,
	newStream: func(s *Server, a streamArgs, ev *spool.Event) streamExtractor {
		return &chatStream{s: s, a: a, ev: ev, legacyText: true}
	},
	writeError: func(w http.ResponseWriter, e apiError, _ string) { writeAPIError(w, e) },
}

func (s *Server) handleCompletions(w http.ResponseWriter, r *http.Request) {
	s.serveProtocol(protoCompletions, w, r)
}

// parseCompletions reads the model and the prompt, the two fields the route
// requires. The model is required: the candidate-list-only shape chat accepts
// is not documented for this route.
func parseCompletions(params map[string]json.RawMessage) (parsedRequest, *refusal) {
	var req parsedRequest
	raw, ok := params["model"]
	if !ok || json.Unmarshal(raw, &req.publicModel) != nil || req.publicModel == "" {
		return req, &refusal{e: errMissingParam("model")}
	}
	prompt, ok := params["prompt"]
	if !ok || string(bytes.TrimSpace(prompt)) == "null" {
		return req, &refusal{e: errMissingParam("prompt")}
	}
	req.input = prompt
	if raw, ok := params["stream_options"]; ok {
		var opts struct {
			IncludeUsage bool `json:"include_usage"`
		}
		_ = json.Unmarshal(raw, &opts)
		req.wantsUsage = opts.IncludeUsage
	}
	return req, nil
}

// readCompletionsResponse reads a non-streamed completion: chat's answer with
// each choice's text in `text`.
func readCompletionsResponse(m map[string]any, publicName string) responseResult {
	res := readChatResponse(m, publicName)
	res.answer = ""
	if choices, _ := m["choices"].([]any); len(choices) > 0 {
		if first, _ := choices[0].(map[string]any); first != nil {
			res.answer, _ = first["text"].(string)
		}
	}
	return res
}

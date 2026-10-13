package server

import (
	"net/http"
	"strings"
	"testing"

	"github.com/pnuops/pickle-llm-gateway/internal/snapshot"
	"github.com/pnuops/pickle-llm-gateway/internal/spool"
)

// Chat is a grant like images and embeddings: a key without it is refused on
// both axes, before anything about limits or the body, and the refusal names
// the capability rather than hiding the route.
func TestChatNeedsTheChatGrant(t *testing.T) {
	for _, tc := range []struct{ name, body string }{
		{"self-hosted", chatBody},
		{"paid", creditChatBody},
		// Not even read: the grant comes before the body.
		{"unreadable body", `{not json`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, func(d *snapshot.Document) {
				creditDoc(d)
				d.Keys[0].PassthroughEndpoints = []string{snapshot.EndpointImages}
			}, nil)
			status, body := h.chat(t, testToken, tc.body)
			if status != http.StatusForbidden || errCode(t, body) != "endpoint_not_allowed" {
				t.Fatalf("got %d %s", status, body)
			}
			if !strings.Contains(errMessage(t, body), "Chat(chat)") {
				t.Fatalf("refusal does not name the capability: %s", body)
			}
			if h.mock.callCount() != 0 {
				t.Fatal("a refused request reached the upstream")
			}
			evs := h.spoolEvents(t)
			if len(evs) != 1 || evs[0].KeyID != "k-test" || evs[0].Status != spool.StatusAuthRejected ||
				evs[0].ErrorType != "endpoint_not_allowed" || evs[0].Endpoint != spool.EndpointChat {
				t.Fatalf("events: %+v", evs)
			}
		})
	}
}

// The grant is judged before the refusal a key could otherwise earn first: an
// exhausted quota on a key with no credential.
func TestChatGrantComesBeforeTheQuotaRefusal(t *testing.T) {
	h := newHarness(t, func(d *snapshot.Document) {
		d.Keys[0].PassthroughEndpoints = nil
		d.Keys[0].QuotaExhausted = true
	}, nil)
	status, body := h.chat(t, testToken, chatBody)
	if status != http.StatusForbidden || errCode(t, body) != "endpoint_not_allowed" {
		t.Fatalf("got %d %s", status, body)
	}
}

// The model catalogue costs nothing and stays open to a key with no grants.
func TestModelListNeedsNoGrant(t *testing.T) {
	h := newHarness(t, func(d *snapshot.Document) {
		d.Keys[0].PassthroughEndpoints = nil
	}, nil)
	req, _ := http.NewRequest(http.MethodGet, h.gw.URL+"/v1/models", nil)
	req.Header.Set("Authorization", "Bearer "+testToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d", resp.StatusCode)
	}
}

// Each field that asks for a paid part of chat needs its grant on the money
// axis, passes with it, and the fields that do not ask for it explicitly are
// never refused for it.
func TestChatSubCapabilities(t *testing.T) {
	const messages = `"messages":[{"role":"user","content":"hi"}]`
	for _, tc := range []struct {
		name, body, needs string
	}{
		{"web plugin", `{"model":"vendor-model",` + messages + `,"plugins":[{"id":"web"}]}`, snapshot.EndpointWebSearch},
		{"web plugin, other spelling", `{"model":"vendor-model",` + messages + `,"plugins":[{"id":" Web "}]}`, snapshot.EndpointWebSearch},
		{"online model", `{"model":"someone/model:online",` + messages + `}`, snapshot.EndpointWebSearch},
		{"online candidate", `{"model":"vendor-model","models":["vendor-model","someone/model:online"],` + messages + `}`, snapshot.EndpointWebSearch},
		{"online first candidate only", `{"models":["someone/model:online"],` + messages + `}`, snapshot.EndpointWebSearch},
		{"web search tool", `{"model":"vendor-model",` + messages + `,"tools":[{"type":"openrouter:web_search"}]}`, snapshot.EndpointWebSearch},
		{"web search tool nested", `{"model":"vendor-model",` + messages + `,"tools":[{"type":"openrouter:subagent","parameters":{"tools":[{"type":"openrouter:web_search"}]}}]}`, snapshot.EndpointWebSearch},
		{"paid pdf engine", `{"model":"vendor-model",` + messages + `,"plugins":[{"id":"file-parser","pdf":{"engine":"mistral-ocr"}}]}`, snapshot.EndpointPdfOcr},
		{"image output", `{"model":"vendor-model",` + messages + `,"modalities":["text","image"]}`, snapshot.EndpointImages},
		{"image generation tool", `{"model":"vendor-model",` + messages + `,"tools":[{"type":"openrouter:image_generation"}]}`, snapshot.EndpointImages},
		{"web search options", `{"model":"vendor-model",` + messages + `,"web_search_options":{"search_context_size":"low"}}`, ""},
		{"free pdf engine", `{"model":"vendor-model",` + messages + `,"plugins":[{"id":"file-parser","pdf":{"engine":"cloudflare-ai"}}]}`, ""},
		{"pdf engine left to the vendor", `{"model":"vendor-model",` + messages + `,"plugins":[{"id":"file-parser"}]}`, ""},
		{"text output", `{"model":"vendor-model",` + messages + `,"modalities":["text"]}`, ""},
		{"function tool named like a server tool", `{"model":"vendor-model",` + messages + `,"tools":[{"type":"function","function":{"name":"web_search"}}]}`, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			chatOnly := newHarness(t, func(d *snapshot.Document) {
				creditPassthroughDoc(d)
			}, nil)
			status, body := chatOnly.chat(t, testToken, tc.body)
			if tc.needs == "" {
				if status != http.StatusOK {
					t.Fatalf("refused a request that asks for nothing extra: %d %s", status, body)
				}
				return
			}
			if status != http.StatusForbidden || errCode(t, body) != "endpoint_not_allowed" ||
				!strings.Contains(errMessage(t, body), "("+tc.needs+")") {
				t.Fatalf("without %s: %d %s", tc.needs, status, body)
			}
			if chatOnly.mock.callCount() != 0 {
				t.Fatal("a refused request reached the upstream")
			}
			if evs := chatOnly.spoolEvents(t); len(evs) != 1 || evs[0].ErrorType != "endpoint_not_allowed" ||
				evs[0].Status != spool.StatusBadRequest {
				t.Fatalf("events: %+v", evs)
			}
			granted := newHarness(t, func(d *snapshot.Document) {
				creditPassthroughDoc(d)
				d.Keys[0].PassthroughEndpoints = []string{snapshot.EndpointChat, tc.needs}
			}, nil)
			if status, body := granted.chat(t, testToken, tc.body); status != http.StatusOK {
				t.Fatalf("with %s: %d %s", tc.needs, status, body)
			}
		})
	}
}

// A value the judgment cannot read is refused rather than forwarded unjudged.
func TestChatSubCapabilityFieldsThatCannotBeRead(t *testing.T) {
	for _, body := range []string{
		`{"model":"vendor-model","messages":[],"modalities":"image"}`,
		`{"model":"vendor-model","messages":[],"plugins":[{"id":"file-parser","pdf":"mistral-ocr"}]}`,
		`{"model":"vendor-model","messages":[],"plugins":[{"id":"file-parser","pdf":{"engine":7}}]}`,
	} {
		h := newHarness(t, creditPassthroughDoc, nil)
		if status, resp := h.chat(t, testToken, body); status != http.StatusBadRequest ||
			errCode(t, resp) != "invalid_parameter_value" {
			t.Fatalf("%s: %d %s", body, status, resp)
		}
	}
}

// A model the key may not use is the answer before a feature it was not
// granted, the same order the image tool already follows.
func TestModelRefusalComesBeforeTheFeatureRefusal(t *testing.T) {
	h := newHarness(t, func(d *snapshot.Document) {
		creditPassthroughDoc(d)
		d.Keys[0].CreditDeniedModels = []string{"someone/*"}
	}, nil)
	status, body := h.chat(t, testToken, `{"model":"someone/model:online","messages":[]}`)
	if status != http.StatusForbidden || errCode(t, body) != "model_not_allowed" {
		t.Fatalf("got %d %s", status, body)
	}
}

// The sub-capabilities are money-axis features. The self-hosted axis refuses
// those fields as parameters it does not take, before any grant is consulted.
func TestSubCapabilityFieldsOnTheSelfHostedAxis(t *testing.T) {
	h := newHarness(t, nil, nil)
	status, body := h.chat(t, testToken, `{"model":"pickle-general","messages":[],"plugins":[{"id":"web"}]}`)
	if status != http.StatusBadRequest || errCode(t, body) != "unsupported_parameter" {
		t.Fatalf("got %d %s", status, body)
	}
}

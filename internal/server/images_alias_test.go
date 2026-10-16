// POST /v1/images/generations is the OpenAI SDK's image path. It is the same
// route as /v1/images under the SDK's name, with the OpenAI fields the vendor
// cannot honour refused by name.
package server

import (
	"net/http"
	"strings"
	"testing"

	"github.com/pnuops/pickle-llm-gateway/internal/snapshot"
	"github.com/pnuops/pickle-llm-gateway/internal/spool"
)

func TestImagesGenerationsIsTheImagesRoute(t *testing.T) {
	h := newHarness(t, passthroughDoc(snapshot.EndpointImages), nil)

	body := `{"model":"openai/gpt-image-1","prompt":"a cat","n":1,"size":"1024x1024","response_format":"b64_json","partial_images":2}`
	status, resp := h.passthrough(t, http.MethodPost, "/v1/images/generations", testToken, body)
	if status != 200 || !strings.Contains(string(resp), "b64_json") {
		t.Fatalf("alias: %d %s", status, resp)
	}
	path, method, _, raw := h.mock.lastRequest()
	if path != "/images" || method != http.MethodPost {
		t.Fatalf("upstream got %s %s, want POST /images", method, path)
	}
	// The body goes out as sent: nothing rewritten, nothing dropped.
	for _, field := range []string{`"response_format":"b64_json"`, `"partial_images":2`, `"size":"1024x1024"`} {
		if !strings.Contains(string(raw), field) {
			t.Fatalf("%s was not forwarded: %s", field, raw)
		}
	}
	evs := h.spoolEvents(t)
	if ev := evs[len(evs)-1]; ev.Endpoint != spool.EndpointImages || ev.Status != spool.StatusOK {
		t.Fatalf("recorded as %+v", ev)
	}
}

// The images grant governs the alias exactly as it governs /v1/images.
func TestImagesGenerationsNeedsTheImagesGrant(t *testing.T) {
	h := newHarness(t, passthroughDoc(), nil)
	status, body := h.passthrough(t, http.MethodPost, "/v1/images/generations", testToken, imageBody)
	if status != http.StatusForbidden || errCode(t, body) != "endpoint_not_allowed" {
		t.Fatalf("got %d %s", status, body)
	}
}

func TestImagesGenerationsRefusesWhatTheVendorDrops(t *testing.T) {
	h := newHarness(t, passthroughDoc(snapshot.EndpointImages), nil)
	for _, tc := range []struct {
		name, extra, code, says string
	}{
		{"url output", `"response_format":"url"`, "unsupported_parameter", "b64_json"},
		{"unreadable format", `"response_format":1`, "invalid_parameter_value", "response_format"},
		{"style", `"style":"vivid"`, "unsupported_parameter", "prompt"},
		{"moderation", `"moderation":"low"`, "unsupported_parameter", "provider.options.openai.moderation"},
		{"streaming", `"stream":true`, "streaming_not_supported", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := `{"model":"openai/gpt-image-1","prompt":"a cat",` + tc.extra + `}`
			status, resp := h.passthrough(t, http.MethodPost, "/v1/images/generations", testToken, body)
			if status != 400 || errCode(t, resp) != tc.code || !strings.Contains(errMessage(t, resp), tc.says) {
				t.Fatalf("got %d %s", status, resp)
			}
		})
	}
	if h.mock.callCount() != 0 {
		t.Fatalf("a refused request reached the vendor %d times", h.mock.callCount())
	}
	// An explicit null is the SDK's way of saying unset, and passes.
	body := `{"model":"openai/gpt-image-1","prompt":"a cat","response_format":null,"style":null,"moderation":null}`
	if status, resp := h.passthrough(t, http.MethodPost, "/v1/images/generations", testToken, body); status != 200 {
		t.Fatalf("null fields: %d %s", status, resp)
	}
}

// The refusals belong to the OpenAI-shaped path only; /v1/images keeps
// forwarding whatever it is sent.
func TestImagesRouteIsUnchanged(t *testing.T) {
	h := newHarness(t, passthroughDoc(snapshot.EndpointImages), nil)
	body := `{"model":"openai/gpt-image-1","prompt":"a cat","style":"vivid"}`
	if status, resp := h.passthrough(t, http.MethodPost, "/v1/images", testToken, body); status != 200 {
		t.Fatalf("got %d %s", status, resp)
	}
}

// Editing and variations are not opened.
func TestImageEditsAreNotOpened(t *testing.T) {
	h := newHarness(t, passthroughDoc(snapshot.EndpointImages), nil)
	for _, path := range []string{"/v1/images/edits", "/v1/images/variations"} {
		status, body := h.passthrough(t, http.MethodPost, path, testToken, imageBody)
		if status != http.StatusNotFound || errCode(t, body) != "unknown_endpoint" {
			t.Fatalf("%s: %d %s", path, status, body)
		}
	}
}

// A refusal is recorded as a bad request under the refusal's code, and a
// repeated member is judged by the value the vendor would read: the last one.
func TestImagesGenerationsRefusalRecordAndRepeatedMembers(t *testing.T) {
	h := newHarness(t, passthroughDoc(snapshot.EndpointImages), nil)

	status, _ := h.passthrough(t, http.MethodPost, "/v1/images/generations", testToken,
		`{"model":"openai/gpt-image-1","prompt":"a cat","style":"vivid"}`)
	if status != 400 {
		t.Fatalf("got %d", status)
	}
	evs := h.spoolEvents(t)
	if ev := evs[len(evs)-1]; ev.Status != spool.StatusBadRequest || ev.ErrorType != "unsupported_parameter" ||
		ev.Endpoint != spool.EndpointImages {
		t.Fatalf("recorded as %+v", ev)
	}

	urlLast := `{"model":"openai/gpt-image-1","prompt":"a cat","response_format":"b64_json","response_format":"url"}`
	if status, body := h.passthrough(t, http.MethodPost, "/v1/images/generations", testToken, urlLast); status != 400 {
		t.Fatalf("url as the last member passed: %d %s", status, body)
	}
	b64Last := `{"model":"openai/gpt-image-1","prompt":"a cat","response_format":"url","response_format":"b64_json"}`
	if status, body := h.passthrough(t, http.MethodPost, "/v1/images/generations", testToken, b64Last); status != 200 {
		t.Fatalf("b64_json as the last member was refused: %d %s", status, body)
	}
	if _, _, _, raw := h.mock.lastRequest(); strings.Contains(string(raw), `"url"`) {
		t.Fatalf("the vendor got a value that was not judged: %s", raw)
	}
}

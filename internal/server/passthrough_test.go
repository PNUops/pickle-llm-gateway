package server

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/pnuops/pickle-llm-gateway/internal/config"
	"github.com/pnuops/pickle-llm-gateway/internal/snapshot"
	"github.com/pnuops/pickle-llm-gateway/internal/spool"
)

// passthroughDoc is the document these tests start from: a passthrough
// upstream, and a key that holds a credential for it. Which endpoints the key
// is granted is left to each test, because that is the fence under test.
func passthroughDoc(endpoints ...string) func(*snapshot.Document) {
	return func(d *snapshot.Document) {
		d.PassthroughRef = "mock"
		d.Keys[0].UpstreamCredentials = map[string]string{"mock": upstreamCred}
		d.Keys[0].PassthroughEndpoints = endpoints
	}
}

func (h *harness) passthrough(t *testing.T, method, path, token, body string) (int, []byte) {
	t.Helper()
	var reader io.Reader
	if body != "" {
		reader = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, h.gw.URL+path, reader)
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	out, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, out
}

const imageBody = `{"model":"openai/gpt-image-1","prompt":"a cat"}`

// The fence is the whole point of the round: a key that exists today carries
// no endpoint grant, and must not reach a single new path because the gateway
// learned to serve them.
func TestPassthroughEndpointFenceClosedByDefault(t *testing.T) {
	h := newHarness(t, passthroughDoc(), nil)
	for _, tc := range []struct {
		method, path, body string
	}{
		{http.MethodPost, "/v1/images", imageBody},
		{http.MethodGet, "/v1/images/models", ""},
		{http.MethodPost, "/v1/embeddings", `{"model":"openai/text-embedding-3-large","input":"hi"}`},
	} {
		status, body := h.passthrough(t, tc.method, tc.path, testToken, tc.body)
		if status != http.StatusForbidden || errCode(t, body) != "endpoint_not_allowed" {
			t.Fatalf("%s %s: %d %s", tc.method, tc.path, status, body)
		}
	}
	if h.mock.callCount() != 0 {
		t.Fatalf("a fenced request reached the upstream %d times", h.mock.callCount())
	}
}

// 403 and not 404: the paths are public documentation, and a student has to be
// able to tell "not granted" from "does not exist" to know what to ask for.
func TestPassthroughRefusalIsNamedNotHidden(t *testing.T) {
	h := newHarness(t, passthroughDoc(), nil)
	status, body := h.passthrough(t, http.MethodPost, "/v1/images", testToken, imageBody)
	if status != http.StatusForbidden {
		t.Fatalf("status: %d %s", status, body)
	}
	if msg := errMessage(t, body); !strings.Contains(msg, "콘솔") {
		t.Fatalf("the refusal has to say where to look: %s", msg)
	}
	// The path a key was never granted is a different answer from one this
	// service does not serve at all.
	status, body = h.passthrough(t, http.MethodPost, "/v1/audio/speech", testToken, imageBody)
	if status != http.StatusNotFound || errCode(t, body) != "unknown_endpoint" {
		t.Fatalf("out-of-scope path: %d %s", status, body)
	}
}

// One capability, both image routes. Tokens name what a key may do, not where.
func TestPassthroughImagesCapabilityCoversBothRoutes(t *testing.T) {
	h := newHarness(t, passthroughDoc(snapshot.EndpointImages), nil)
	if status, body := h.passthrough(t, http.MethodPost, "/v1/images", testToken, imageBody); status != 200 {
		t.Fatalf("images: %d %s", status, body)
	}
	if status, body := h.passthrough(t, http.MethodGet, "/v1/images/models", testToken, ""); status != 200 {
		t.Fatalf("image models: %d %s", status, body)
	}
	// ...and grants nothing else.
	status, body := h.passthrough(t, http.MethodPost, "/v1/embeddings", testToken,
		`{"model":"openai/text-embedding-3-large","input":"hi"}`)
	if status != http.StatusForbidden || errCode(t, body) != "endpoint_not_allowed" {
		t.Fatalf("embeddings should not ride on the images grant: %d %s", status, body)
	}
}

// The fence runs before the credential check, so a key that was never granted
// the path hears about the path rather than about a money budget.
func TestPassthroughFenceRunsBeforeCredential(t *testing.T) {
	h := newHarness(t, func(d *snapshot.Document) {
		d.PassthroughRef = "mock"
		d.Keys[0].UpstreamCredentials = nil
	}, nil)
	status, body := h.passthrough(t, http.MethodPost, "/v1/images", testToken, imageBody)
	if status != http.StatusForbidden || errCode(t, body) != "endpoint_not_allowed" {
		t.Fatalf("%d %s", status, body)
	}
}

// Past the fence, the credential is the money axis and answers as it does on
// chat: a granted-but-unprovisioned budget is told to wait, everything else is
// told to apply.
func TestPassthroughCredentialAnswers(t *testing.T) {
	h := newHarness(t, func(d *snapshot.Document) {
		d.PassthroughRef = "mock"
		d.Keys[0].PassthroughEndpoints = []string{snapshot.EndpointImages}
	}, nil)
	status, body := h.passthrough(t, http.MethodPost, "/v1/images", testToken, imageBody)
	if status != http.StatusForbidden || errCode(t, body) != "credit_unavailable" {
		t.Fatalf("no credential: %d %s", status, body)
	}

	h2 := newHarness(t, func(d *snapshot.Document) {
		d.PassthroughRef = "mock"
		d.Keys[0].PassthroughEndpoints = []string{snapshot.EndpointImages}
		d.Keys[0].CreditPending = true
	}, nil)
	status, body = h2.passthrough(t, http.MethodPost, "/v1/images", testToken, imageBody)
	if status != http.StatusServiceUnavailable || errCode(t, body) != "credit_pending" {
		t.Fatalf("pending: %d %s", status, body)
	}
}

// The model fence is the one chat uses, reached through the body's `model`.
func TestPassthroughModelFence(t *testing.T) {
	h := newHarness(t, func(d *snapshot.Document) {
		passthroughDoc(snapshot.EndpointImages)(d)
		d.Keys[0].CreditDeniedModels = []string{"openai/gpt-image-1"}
	}, nil)
	status, body := h.passthrough(t, http.MethodPost, "/v1/images", testToken, imageBody)
	if status != http.StatusForbidden || errCode(t, body) != "model_not_allowed" {
		t.Fatalf("denied model: %d %s", status, body)
	}
	if h.mock.callCount() != 0 {
		t.Fatal("a fenced model reached the upstream")
	}
	// The allow list works the same way it does on chat: it is a money fence,
	// so a name outside it is refused whatever path asks for it.
	h2 := newHarness(t, func(d *snapshot.Document) {
		passthroughDoc(snapshot.EndpointImages)(d)
		d.Keys[0].CreditAllowedModels = []string{"openai/*"}
	}, nil)
	if status, body := h2.passthrough(t, http.MethodPost, "/v1/images", testToken, imageBody); status != 200 {
		t.Fatalf("allowed vendor: %d %s", status, body)
	}
	status, body = h2.passthrough(t, http.MethodPost, "/v1/images", testToken,
		`{"model":"google/imagen-4","prompt":"a cat"}`)
	if status != http.StatusForbidden || errCode(t, body) != "model_not_allowed" {
		t.Fatalf("outside the allow list: %d %s", status, body)
	}
}

// A curated self-serve name must not become a billable passthrough request
// here either — the same guard chat has, reached through the same function.
func TestPassthroughRefusesReservedModelName(t *testing.T) {
	h := newHarness(t, passthroughDoc(snapshot.EndpointImages), nil)
	status, body := h.passthrough(t, http.MethodPost, "/v1/images", testToken,
		`{"model":"pickle-general","prompt":"a cat"}`)
	if status != http.StatusNotFound || errCode(t, body) != "model_not_found" {
		t.Fatalf("%d %s", status, body)
	}
	if h.mock.callCount() != 0 {
		t.Fatal("a reserved name left for the upstream")
	}
}

// The request reaches the upstream on the matching path, carrying the key's
// own credential and none of the client's headers.
func TestPassthroughForwarding(t *testing.T) {
	h := newHarness(t, passthroughDoc(snapshot.EndpointImages, snapshot.EndpointEmbeddings), nil)

	req, err := http.NewRequest(http.MethodPost, h.gw.URL+"/v1/images", strings.NewReader(imageBody))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+testToken)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "image/png")
	req.Header.Set("X-Title", "student-app")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status %d", resp.StatusCode)
	}

	path, method, hdr, raw := h.mock.lastRequest()
	if path != "/images" || method != http.MethodPost {
		t.Fatalf("upstream saw %s %s", method, path)
	}
	if _, auth := h.mock.last(); auth != "Bearer "+upstreamCred {
		t.Fatalf("upstream bearer: %q", auth)
	}
	// The body leaves untouched: this surface does not rewrite it.
	if string(raw) != imageBody {
		t.Fatalf("body was rewritten: %s", raw)
	}
	// Accept would break metering (a non-JSON answer carries no usage) and
	// vendor attribution headers name the vendor in vendor-neutral code.
	if got := hdr.Get("Accept"); strings.Contains(got, "image/png") {
		t.Fatalf("client Accept was forwarded: %q", got)
	}
	if hdr.Get("X-Title") != "" {
		t.Fatalf("client X-Title was forwarded: %q", hdr.Get("X-Title"))
	}

	if status, body := h.passthrough(t, http.MethodGet, "/v1/images/models", testToken, ""); status != 200 {
		t.Fatalf("image models: %d %s", status, body)
	}
	if path, method, _, _ := h.mock.lastRequest(); path != "/images/models" || method != http.MethodGet {
		t.Fatalf("catalogue upstream saw %s %s", method, path)
	}
}

// Metering: both usage namings land as exact counts, and `estimated` stays
// false. An embedding response reports only prompt tokens, which is not the
// same thing as reporting nothing.
func TestPassthroughMetering(t *testing.T) {
	h := newHarness(t, passthroughDoc(snapshot.EndpointImages, snapshot.EndpointEmbeddings), nil)
	if status, body := h.passthrough(t, http.MethodPost, "/v1/images", testToken, imageBody); status != 200 {
		t.Fatalf("images: %d %s", status, body)
	}
	if status, body := h.passthrough(t, http.MethodPost, "/v1/embeddings", testToken,
		`{"model":"openai/text-embedding-3-large","input":"hi"}`); status != 200 {
		t.Fatalf("embeddings: %d %s", status, body)
	}
	events := h.spoolEvents(t)
	if len(events) != 2 {
		t.Fatalf("want 2 events, got %d", len(events))
	}
	img, emb := events[0], events[1]
	// The image route reports input_tokens/output_tokens, not the chat naming.
	if img.InputTokens != 11 || img.OutputTokens != 3 || img.Estimated {
		t.Fatalf("image usage: %+v", img)
	}
	if img.PublicModelName != "openai/gpt-image-1" || img.BudgetAxis != snapshot.AxisCredit {
		t.Fatalf("image event: %+v", img)
	}
	if img.UpstreamRef != "mock" || img.Status != spool.StatusOK {
		t.Fatalf("image event: %+v", img)
	}
	if emb.InputTokens != 9 || emb.OutputTokens != 0 || emb.Estimated {
		t.Fatalf("embedding usage: %+v", emb)
	}
}

// A response that carries no usage at all drops to the estimate and says so,
// rather than recording a confident zero.
func TestPassthroughNoUsageIsFlaggedEstimated(t *testing.T) {
	h := newHarness(t, passthroughDoc(snapshot.EndpointImages), nil)
	h.mock.set(func(o *mockOpts) { o.noUsage = true })
	if status, body := h.passthrough(t, http.MethodPost, "/v1/images", testToken, imageBody); status != 200 {
		t.Fatalf("%d %s", status, body)
	}
	events := h.spoolEvents(t)
	if len(events) != 1 || !events[0].Estimated {
		t.Fatalf("want one estimated event, got %+v", events)
	}
}

// A catalogue read spends nothing, so its event records a known zero rather
// than a guessed one.
func TestPassthroughCatalogueReadIsNotEstimated(t *testing.T) {
	h := newHarness(t, passthroughDoc(snapshot.EndpointImages), nil)
	if status, body := h.passthrough(t, http.MethodGet, "/v1/images/models", testToken, ""); status != 200 {
		t.Fatalf("%d %s", status, body)
	}
	events := h.spoolEvents(t)
	if len(events) != 1 {
		t.Fatalf("want 1 event, got %d", len(events))
	}
	if events[0].Estimated || events[0].InputTokens != 0 || events[0].OutputTokens != 0 {
		t.Fatalf("catalogue event: %+v", events[0])
	}
}

// The prefix peek finds `model` past a large member, and says which of the two
// failures happened when it cannot.
func TestPassthroughPrefixPeek(t *testing.T) {
	h := newHarness(t, passthroughDoc(snapshot.EndpointImages), nil)

	// A reference image well under the prefix size: `model` is still found.
	small := `{"image":"` + strings.Repeat("A", 8<<10) + `","model":"openai/gpt-image-1","prompt":"x"}`
	if status, body := h.passthrough(t, http.MethodPost, "/v1/images", testToken, small); status != 200 {
		t.Fatalf("small prefix: %d %s", status, body)
	}

	// One past it: the field is unreadable, and the message says to move it
	// rather than to add it.
	big := `{"image":"` + strings.Repeat("A", passthroughPeekBytes) + `","model":"openai/gpt-image-1","prompt":"x"}`
	status, body := h.passthrough(t, http.MethodPost, "/v1/images", testToken, big)
	if status != http.StatusBadRequest || errCode(t, body) != "missing_parameter" {
		t.Fatalf("large prefix: %d %s", status, body)
	}
	if msg := errMessage(t, body); !strings.Contains(msg, "앞쪽") {
		t.Fatalf("message must say where to move the field: %s", msg)
	}

	// No model at all reads the same way, which is correct: neither request
	// can be fenced, and the fix is the same field.
	status, body = h.passthrough(t, http.MethodPost, "/v1/images", testToken, `{"prompt":"x"}`)
	if status != http.StatusBadRequest || errCode(t, body) != "missing_parameter" {
		t.Fatalf("absent model: %d %s", status, body)
	}

	status, body = h.passthrough(t, http.MethodPost, "/v1/images", testToken, `not json`)
	if status != http.StatusBadRequest || errCode(t, body) != "invalid_json" {
		t.Fatalf("bad json: %d %s", status, body)
	}
}

// `n` is bounded here so the caller hears the limit, instead of hitting the
// response cap and getting a 502 that describes a server fault.
func TestPassthroughNBound(t *testing.T) {
	h := newHarness(t, passthroughDoc(snapshot.EndpointImages), func(c *config.Config) {
		c.PassthroughMaxN = 2
	})
	if status, body := h.passthrough(t, http.MethodPost, "/v1/images", testToken,
		`{"model":"openai/gpt-image-1","prompt":"x","n":2}`); status != 200 {
		t.Fatalf("at the bound: %d %s", status, body)
	}
	status, body := h.passthrough(t, http.MethodPost, "/v1/images", testToken,
		`{"model":"openai/gpt-image-1","prompt":"x","n":3}`)
	if status != http.StatusBadRequest || errCode(t, body) != "invalid_parameter_value" {
		t.Fatalf("over the bound: %d %s", status, body)
	}
	if !strings.Contains(errMessage(t, body), "2") {
		t.Fatalf("the message has to name the limit: %s", errMessage(t, body))
	}
	if h.mock.callCount() != 1 {
		t.Fatalf("a refused n reached the upstream: %d calls", h.mock.callCount())
	}
}

// This surface's caps are its own. Raising or lowering one must not move what
// a chat request may send.
func TestPassthroughCapsAreSeparateFromChat(t *testing.T) {
	h := newHarness(t, passthroughDoc(snapshot.EndpointImages), func(c *config.Config) {
		c.PassthroughRequestBodyMaxBytes = 512
	})
	big := `{"model":"openai/gpt-image-1","prompt":"` + strings.Repeat("a", 1024) + `"}`
	status, body := h.passthrough(t, http.MethodPost, "/v1/images", testToken, big)
	if status != http.StatusBadRequest || errCode(t, body) != "request_too_large" {
		t.Fatalf("passthrough cap: %d %s", status, body)
	}
	// The same size on chat, whose own cap the harness leaves at 1 MiB.
	chatBody := `{"model":"pickle-general","messages":[{"role":"user","content":"` +
		strings.Repeat("a", 1024) + `"}]}`
	if status, body := h.chat(t, testToken, chatBody); status != 200 {
		t.Fatalf("chat must not move: %d %s", status, body)
	}
}

// The passthrough pool is separate from the gateway-wide one, so a surface
// with no slots left cannot take chat down with it.
func TestPassthroughSlotPoolIsSeparate(t *testing.T) {
	h := newHarness(t, passthroughDoc(snapshot.EndpointImages), func(c *config.Config) {
		c.PassthroughMaxInFlight = 1
	})
	// Occupy the single passthrough slot, then prove chat still answers.
	release := make(chan struct{})
	h.mock.set(func(o *mockOpts) { o.delay = 0 })
	go func() {
		h.passthroughStatus(http.MethodPost, "/v1/images", testToken, imageBody)
		close(release)
	}()
	<-release
	if status, body := h.chat(t, testToken, `{"model":"pickle-general","messages":[]}`); status != 200 {
		t.Fatalf("chat: %d %s", status, body)
	}
	if got := h.srv.PassthroughInFlight(); got != 0 {
		t.Fatalf("slot leaked: %d", got)
	}
	// A pool with no capacity at all refuses rather than opening the surface.
	h2 := newHarness(t, passthroughDoc(snapshot.EndpointImages), func(c *config.Config) {
		c.PassthroughMaxInFlight = 0
	})
	status, body := h2.passthrough(t, http.MethodPost, "/v1/images", testToken, imageBody)
	if status != http.StatusServiceUnavailable || errCode(t, body) != "server_busy" {
		t.Fatalf("zero pool: %d %s", status, body)
	}
}

func (h *harness) passthroughStatus(method, path, token, body string) int {
	var reader io.Reader
	if body != "" {
		reader = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, h.gw.URL+path, reader)
	if err != nil {
		return -1
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return -1
	}
	defer resp.Body.Close()
	_, _ = io.ReadAll(resp.Body)
	return resp.StatusCode
}

// The routes must not disturb the models subtree, which is registered as a
// prefix pattern and has to keep winning for GET /v1/models/{id}.
func TestPassthroughRoutingDoesNotShadowModels(t *testing.T) {
	h := newHarness(t, passthroughDoc(snapshot.EndpointImages), nil)
	status, body := h.passthrough(t, http.MethodGet, "/v1/models/pickle-general", testToken, "")
	if status != 200 {
		t.Fatalf("models retrieve: %d %s", status, body)
	}
	var got map[string]any
	if json.Unmarshal(body, &got) != nil || got["id"] != "pickle-general" {
		t.Fatalf("models retrieve body: %s", body)
	}
	if status, body := h.passthrough(t, http.MethodGet, "/v1/models", testToken, ""); status != 200 {
		t.Fatalf("models list: %d %s", status, body)
	}
	// The image catalogue is its own exact pattern, not part of that subtree.
	if status, _ := h.passthrough(t, http.MethodGet, "/v1/images/models", testToken, ""); status != 200 {
		t.Fatalf("image models: %d", status)
	}
	if path, _, _, _ := h.mock.lastRequest(); path != "/images/models" {
		t.Fatalf("image catalogue routed to %s", path)
	}
}

// The wrong method on a served path is a method error, not a 404: the path
// exists and saying otherwise would send the caller looking for a typo.
func TestPassthroughMethodMismatch(t *testing.T) {
	h := newHarness(t, passthroughDoc(snapshot.EndpointImages), nil)
	status, body := h.passthrough(t, http.MethodGet, "/v1/images", testToken, "")
	if status != http.StatusMethodNotAllowed || errCode(t, body) != "method_not_allowed" {
		t.Fatalf("%d %s", status, body)
	}
}

// A document that grants a capability but names no upstream to serve it is a
// deployment fault. It must not come back as a budget refusal, which would
// send the student to apply for something they already hold.
func TestPassthroughWithoutUpstreamRefIsNotABudgetRefusal(t *testing.T) {
	h := newHarness(t, func(d *snapshot.Document) {
		d.Keys[0].PassthroughEndpoints = []string{snapshot.EndpointImages}
		d.Keys[0].UpstreamCredentials = map[string]string{"mock": upstreamCred}
	}, nil)
	status, body := h.passthrough(t, http.MethodPost, "/v1/images", testToken, imageBody)
	if status != http.StatusServiceUnavailable || errCode(t, body) != "service_disabled" {
		t.Fatalf("%d %s", status, body)
	}
}

// An upstream refusal keeps the meaning it has on chat: the money axis running
// out is a limit refusal in the student's own words, not an upstream fault.
func TestPassthroughUpstreamRefusals(t *testing.T) {
	h := newHarness(t, passthroughDoc(snapshot.EndpointImages), nil)
	h.mock.set(func(o *mockOpts) { o.status = http.StatusPaymentRequired })
	status, body := h.passthrough(t, http.MethodPost, "/v1/images", testToken, imageBody)
	if status != http.StatusTooManyRequests || errCode(t, body) != "credit_exhausted" {
		t.Fatalf("402: %d %s", status, body)
	}
	h.mock.set(func(o *mockOpts) {
		o.status = http.StatusBadRequest
		o.errBody = `{"error":{"type":"invalid_request_error"}}`
	})
	status, body = h.passthrough(t, http.MethodPost, "/v1/images", testToken, imageBody)
	if status != http.StatusBadRequest || errCode(t, body) != "upstream_rejected" {
		t.Fatalf("400: %d %s", status, body)
	}
}

// One attempt only. An image generation is expensive and not idempotent, and
// a passthrough model carries no fallback to try.
func TestPassthroughDoesNotRetry(t *testing.T) {
	h := newHarness(t, passthroughDoc(snapshot.EndpointImages), func(c *config.Config) {
		c.UpstreamRetries = 3
	})
	h.mock.set(func(o *mockOpts) { o.status = http.StatusInternalServerError })
	if status, _ := h.passthrough(t, http.MethodPost, "/v1/images", testToken, imageBody); status != http.StatusBadGateway {
		t.Fatalf("status %d", status)
	}
	if got := h.mock.callCount(); got != 1 {
		t.Fatalf("want 1 upstream call, got %d", got)
	}
}

// A response the gateway cannot vouch for is an upstream error, never
// forwarded verbatim.
func TestPassthroughInvalidUpstreamResponse(t *testing.T) {
	h := newHarness(t, passthroughDoc(snapshot.EndpointImages), nil)
	h.mock.set(func(o *mockOpts) { o.rawResp = `{"created":1,` })
	status, body := h.passthrough(t, http.MethodPost, "/v1/images", testToken, imageBody)
	if status != http.StatusBadGateway || errCode(t, body) != "upstream_error" {
		t.Fatalf("%d %s", status, body)
	}
}

// The body reaches the client byte for byte. Passthrough public names are the
// upstream names, so there is nothing to rewrite and re-serialising a large
// base64 payload would only cost a second copy of it.
func TestPassthroughForwardsBodyVerbatim(t *testing.T) {
	h := newHarness(t, passthroughDoc(snapshot.EndpointImages), nil)
	const raw = `{"created":1,"data":[{"b64_json":"aGk="}],"usage":{"input_tokens":11,"output_tokens":3,"cost":0.04}}`
	h.mock.set(func(o *mockOpts) { o.rawResp = raw })
	status, body := h.passthrough(t, http.MethodPost, "/v1/images", testToken, imageBody)
	if status != 200 || string(body) != raw {
		t.Fatalf("%d %s", status, body)
	}
}

// Unauthenticated attempts are refused before the fence and leave no spool
// line, exactly as they do on chat: the spool is a small disk anyone can reach.
func TestPassthroughUnauthenticatedIsNotSpooled(t *testing.T) {
	h := newHarness(t, passthroughDoc(snapshot.EndpointImages), nil)
	if status, _ := h.passthrough(t, http.MethodPost, "/v1/images", "", imageBody); status != 401 {
		t.Fatal("want 401")
	}
	if status, _ := h.passthrough(t, http.MethodPost, "/v1/images", "nope", imageBody); status != 401 {
		t.Fatal("want 401")
	}
	if events := h.spoolEvents(t); len(events) != 0 {
		t.Fatalf("want no events, got %d", len(events))
	}
}

// The bound is on the request, not on one spelling of it: a float or an
// exponent is the same number to the upstream and must not walk past.
func TestPassthroughNBoundIsNotEvadableBySpelling(t *testing.T) {
	h := newHarness(t, passthroughDoc(snapshot.EndpointImages), func(c *config.Config) {
		c.PassthroughMaxN = 2
	})
	for _, body := range []string{
		`{"model":"openai/gpt-image-1","prompt":"x","n":3.0}`,
		`{"model":"openai/gpt-image-1","prompt":"x","n":3e0}`,
		`{"model":"openai/gpt-image-1","prompt":"x","n":2.5}`,
	} {
		status, out := h.passthrough(t, http.MethodPost, "/v1/images", testToken, body)
		if status != http.StatusBadRequest || errCode(t, out) != "invalid_parameter_value" {
			t.Fatalf("%s: %d %s", body, status, out)
		}
	}
	if h.mock.callCount() != 0 {
		t.Fatalf("a refused n reached the upstream: %d calls", h.mock.callCount())
	}
}

// A response bigger than this surface relays is said out loud. Truncating it
// silently would reach the caller as "the upstream is broken", which is both
// wrong and something they cannot act on.
func TestPassthroughResponseCapIsNamed(t *testing.T) {
	h := newHarness(t, passthroughDoc(snapshot.EndpointImages), func(c *config.Config) {
		c.PassthroughResponseMaxBytes = 256
	})
	h.mock.set(func(o *mockOpts) {
		o.rawResp = `{"created":1,"data":[{"b64_json":"` + strings.Repeat("A", 512) + `"}]}`
	})
	status, body := h.passthrough(t, http.MethodPost, "/v1/images", testToken, imageBody)
	if status != http.StatusBadGateway || errCode(t, body) != "upstream_response_too_large" {
		t.Fatalf("%d %s", status, body)
	}
	if !strings.Contains(errMessage(t, body), "n") {
		t.Fatalf("the message has to say what to reduce: %s", errMessage(t, body))
	}
	// A response that fits is unaffected.
	h.mock.set(func(o *mockOpts) { o.rawResp = "" })
	if status, body := h.passthrough(t, http.MethodPost, "/v1/images", testToken, imageBody); status != 200 {
		t.Fatalf("under the cap: %d %s", status, body)
	}
}

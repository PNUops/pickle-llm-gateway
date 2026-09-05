package server

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

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
	msg := errMessage(t, body)
	// The capability, not the path: the grant is a capability and that is the
	// word the approver's screen shows.
	if !strings.Contains(msg, "이미지 생성") || !strings.Contains(msg, "images") {
		t.Fatalf("the refusal has to name the capability: %s", msg)
	}
	if strings.Contains(msg, "/v1/images") {
		t.Fatalf("the refusal must not name the path: %s", msg)
	}
	// And how to get it. This is the one fence a student cannot work around.
	if !strings.Contains(msg, "신청") {
		t.Fatalf("the refusal has to say what to do: %s", msg)
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

// `model` is read from the whole body, so its position in the object does not
// decide whether the request works.
func TestPassthroughReadsModelAnywhereInTheBody(t *testing.T) {
	h := newHarness(t, passthroughDoc(snapshot.EndpointImages), nil)

	// A reference image far past any prefix a peek could have read.
	big := `{"image":"` + strings.Repeat("A", 128<<10) + `","model":"openai/gpt-image-1","prompt":"x"}`
	if status, body := h.passthrough(t, http.MethodPost, "/v1/images", testToken, big); status != 200 {
		t.Fatalf("model after a large member: %d %s", status, body)
	}

	status, body := h.passthrough(t, http.MethodPost, "/v1/images", testToken, `{"prompt":"x"}`)
	if status != http.StatusBadRequest || errCode(t, body) != "missing_parameter" {
		t.Fatalf("absent model: %d %s", status, body)
	}

	status, body = h.passthrough(t, http.MethodPost, "/v1/images", testToken, `not json`)
	if status != http.StatusBadRequest || errCode(t, body) != "invalid_json" {
		t.Fatalf("bad json: %d %s", status, body)
	}
}

// A JSON object may repeat a member and parsers take the last one, so a fence
// that reads one `model` while the upstream reads another is no fence at all.
// The body that leaves must be the one that was judged.
func TestPassthroughDuplicateModelCannotEvadeTheFence(t *testing.T) {
	h := newHarness(t, func(d *snapshot.Document) {
		passthroughDoc(snapshot.EndpointImages)(d)
		d.Keys[0].CreditAllowedModels = []string{"openai/gpt-image-1"}
	}, nil)

	for _, name := range []string{"duplicate in place", "duplicate past any prefix"} {
		var body string
		switch name {
		case "duplicate in place":
			body = `{"model":"openai/gpt-image-1","n":1,"model":"openai/sora-2-pro","prompt":"x"}`
		default:
			body = `{"model":"openai/gpt-image-1","prompt":"` + strings.Repeat("A", 96<<10) +
				`","model":"openai/sora-2-pro"}`
		}
		status, out := h.passthrough(t, http.MethodPost, "/v1/images", testToken, body)
		if status != http.StatusForbidden || errCode(t, out) != "model_not_allowed" {
			t.Fatalf("%s: %d %s", name, status, out)
		}
		if h.mock.callCount() != 0 {
			t.Fatalf("%s: reached the upstream", name)
		}
	}

	// The reserved self-serve prefix falls to the same trick if the fence
	// reads a different member than the upstream does.
	status, out := h.passthrough(t, http.MethodPost, "/v1/images", testToken,
		`{"model":"openai/gpt-image-1","model":"pickle-general","prompt":"x"}`)
	if status != http.StatusNotFound || errCode(t, out) != "model_not_found" {
		t.Fatalf("reserved name: %d %s", status, out)
	}
	if h.mock.callCount() != 0 {
		t.Fatal("a reserved name left for the upstream")
	}
}

// What the upstream receives is what the fence judged, member for member.
func TestPassthroughForwardsTheResolvedBody(t *testing.T) {
	h := newHarness(t, passthroughDoc(snapshot.EndpointImages), nil)
	status, out := h.passthrough(t, http.MethodPost, "/v1/images", testToken,
		`{"model":"openai/gpt-image-1","n":1,"n":2,"prompt":"x"}`)
	if status != 200 {
		t.Fatalf("%d %s", status, out)
	}
	_, _, _, raw := h.mock.lastRequest()
	var got map[string]any
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	if got["model"] != "openai/gpt-image-1" {
		t.Fatalf("upstream model: %v", got["model"])
	}
	// One `n`, and the one the bound was applied to.
	if strings.Count(string(raw), `"n"`) != 1 {
		t.Fatalf("upstream saw a repeated member: %s", raw)
	}
	if got["prompt"] != "x" {
		t.Fatalf("an untouched field was altered: %v", got["prompt"])
	}
}

// A model name past the length guard must not reach the spool or the journal.
// The guard is what bounds a field that is client input all the way through.
func TestPassthroughOversizedModelNameIsNotRecorded(t *testing.T) {
	h := newHarness(t, passthroughDoc(snapshot.EndpointImages), nil)
	junk := strings.Repeat("z", 60<<10)
	status, _ := h.passthrough(t, http.MethodPost, "/v1/images", testToken,
		`{"model":"`+junk+`","prompt":"x"}`)
	if status != http.StatusNotFound {
		t.Fatalf("status %d", status)
	}
	events := h.spoolEvents(t)
	if len(events) != 1 {
		t.Fatalf("want 1 event, got %d", len(events))
	}
	if events[0].PublicModelName != "" {
		t.Fatalf("%d bytes of client input reached the spool", len(events[0].PublicModelName))
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

// The passthrough pool is separate from the gateway-wide one. The property
// that matters is concurrency, so this occupies the passthrough slots and
// holds them there while chat is exercised — a version of this test that only
// made requests in sequence passed with one shared pool, which is to say it
// tested nothing.
func TestPassthroughSlotPoolIsSeparate(t *testing.T) {
	const slots = 2
	release := make(chan struct{})
	var releaseOnce sync.Once
	unpark := func() { releaseOnce.Do(func() { close(release) }) }
	// Registered before anything parks. Without it a failed assertion leaves
	// the parked requests in the upstream forever and the test server's own
	// cleanup waits on them, so a real defect would show up as a hung run
	// rather than a failed one.
	t.Cleanup(unpark)
	var parked sync.WaitGroup
	h := newHarness(t, passthroughDoc(snapshot.EndpointImages), func(c *config.Config) {
		c.PassthroughMaxInFlight = slots
	})
	// Park every passthrough slot inside the upstream call.
	h.mock.set(func(o *mockOpts) { o.block = release })
	parked.Add(slots)
	for i := 0; i < slots; i++ {
		go func() {
			defer parked.Done()
			h.passthroughStatus(http.MethodPost, "/v1/images", testToken, imageBody)
		}()
	}
	// Wait until the upstream has actually been entered by both, so the slots
	// are held rather than merely requested.
	deadline := time.Now().Add(5 * time.Second)
	for h.mock.blockedCount() < slots {
		if time.Now().After(deadline) {
			t.Fatalf("only %d of %d passthrough requests reached the upstream", h.mock.blockedCount(), slots)
		}
		time.Sleep(2 * time.Millisecond)
	}
	if got := h.srv.PassthroughInFlight(); got != slots {
		t.Fatalf("passthrough in flight %d, want %d", got, slots)
	}

	// The surface is full: another passthrough request is refused, and refused
	// without reaching the upstream. The probe carries its own short deadline
	// so that a pool which is not actually bounded fails here with a timeout
	// rather than parking this test forever.
	status, body := h.passthroughProbe(t, imageBody, 3*time.Second)
	if status != http.StatusServiceUnavailable || errCode(t, body) != "server_busy" {
		t.Fatalf("full pool: %d %s", status, body)
	}
	if got := h.mock.blockedCount(); got != slots {
		t.Fatalf("a refused request still reached the upstream: %d parked, want %d", got, slots)
	}
	// ...while chat, which owns a different pool, still answers.
	if status, body := h.chat(t, testToken, `{"model":"pickle-general","messages":[]}`); status != 200 {
		t.Fatalf("chat starved by the passthrough pool: %d %s", status, body)
	}

	unpark()
	parked.Wait()
	if got := h.srv.PassthroughInFlight(); got != 0 {
		t.Fatalf("slot leaked: %d", got)
	}

	// A pool with no capacity at all refuses rather than opening the surface.
	h2 := newHarness(t, passthroughDoc(snapshot.EndpointImages), func(c *config.Config) {
		c.PassthroughMaxInFlight = 0
	})
	status, body = h2.passthrough(t, http.MethodPost, "/v1/images", testToken, imageBody)
	if status != http.StatusServiceUnavailable || errCode(t, body) != "server_busy" {
		t.Fatalf("zero pool: %d %s", status, body)
	}
}

// passthroughProbe is one passthrough request with its own deadline, for the
// tests where hanging is the failure being guarded against.
func (h *harness) passthroughProbe(t *testing.T, body string, timeout time.Duration) (int, []byte) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, h.gw.URL+"/v1/images", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+testToken)
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{Timeout: timeout}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("the request was not refused; it was still running after %s: %v", timeout, err)
	}
	defer resp.Body.Close()
	out, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, out
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

// A usage object carrying only a total must not be recorded as an exact zero.
// The sum is the number every consumer adds up, and it is knowable here.
func TestPassthroughUsageWithOnlyATotal(t *testing.T) {
	h := newHarness(t, passthroughDoc(snapshot.EndpointImages), nil)
	h.mock.set(func(o *mockOpts) {
		o.rawResp = `{"created":1,"data":[{"b64_json":"aGk="}],"usage":{"total_tokens":4200}}`
	})
	if status, body := h.passthrough(t, http.MethodPost, "/v1/images", testToken, imageBody); status != 200 {
		t.Fatalf("%d %s", status, body)
	}
	events := h.spoolEvents(t)
	if len(events) != 1 {
		t.Fatalf("want 1 event, got %d", len(events))
	}
	if got := events[0].InputTokens + events[0].OutputTokens; got != 4200 {
		t.Fatalf("the reported total was lost: %+v", events[0])
	}
	if events[0].Estimated {
		t.Fatalf("a reported total is not an estimate: %+v", events[0])
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

// Each capability is refused in its own words, so a student asking an approver
// names the thing that can actually be granted.
func TestPassthroughRefusalNamesTheCapability(t *testing.T) {
	h := newHarness(t, passthroughDoc(), nil)
	for _, tc := range []struct {
		method, path, body, want string
	}{
		{http.MethodPost, "/v1/images", imageBody, "이미지 생성(images)"},
		{http.MethodGet, "/v1/images/models", "", "이미지 생성(images)"},
		{http.MethodPost, "/v1/embeddings",
			`{"model":"openai/text-embedding-3-large","input":"hi"}`, "임베딩(embeddings)"},
	} {
		_, body := h.passthrough(t, tc.method, tc.path, testToken, tc.body)
		if msg := errMessage(t, body); !strings.Contains(msg, tc.want) {
			t.Fatalf("%s: want %q in %q", tc.path, tc.want, msg)
		}
	}
	// A capability the control plane may add before this build names it still
	// reads sensibly rather than rendering an empty gap.
	if got := endpointLabel("audio"); got != "audio" {
		t.Fatalf("unnamed capability: %q", got)
	}
}

// The old vocabulary must not spread into the strings this round adds. The
// display terms are 유료 모델 and 자체 서빙 모델; 상용 모델 is retired, and the
// axis words belong to the design docs, not to anything a student reads.
func TestPassthroughMessagesUseCurrentVocabulary(t *testing.T) {
	for _, e := range []apiError{
		errEndpointNotAllowed(endpointLabel(snapshot.EndpointImages)),
		errPassthroughResponseTooLarge,
	} {
		for _, retired := range []string{"상용 모델", "예산 축", "금액 축"} {
			if strings.Contains(e.message, retired) {
				t.Fatalf("%s carries retired vocabulary %q: %s", e.code, retired, e.message)
			}
		}
	}
}

// Numbers reach the upstream as the caller wrote them. The top level decodes
// into raw byte slices rather than into any/float64, so a large integer or a
// high-precision decimal is never parsed and re-rendered on the way through —
// a seed that arrived as one number must not leave as another.
func TestPassthroughDoesNotRewriteNumbers(t *testing.T) {
	h := newHarness(t, passthroughDoc(snapshot.EndpointImages), nil)
	const body = `{"model":"openai/gpt-image-1","seed":12345678901234567890123,` +
		`"temperature":0.1000000000000000055511151231257827,"scale":1e400,"n":2}`
	if status, out := h.passthrough(t, http.MethodPost, "/v1/images", testToken, body); status != 200 {
		t.Fatalf("%d %s", status, out)
	}
	_, _, _, raw := h.mock.lastRequest()
	for _, literal := range []string{
		"12345678901234567890123",
		"0.1000000000000000055511151231257827",
		"1e400",
	} {
		if !strings.Contains(string(raw), literal) {
			t.Fatalf("literal %s was rewritten: %s", literal, raw)
		}
	}
	// The bound was applied to n, and n still leaves as it arrived.
	if !strings.Contains(string(raw), `"n":2`) {
		t.Fatalf("n was rewritten: %s", raw)
	}
}

// The envelope an image generation actually returns, measured 2026-09-05. It
// carries the chat naming plus a cost and a per-modality breakdown, and the
// extra members must neither confuse the extraction nor be dropped from what
// the caller receives.
func TestPassthroughMetersTheMeasuredImageEnvelope(t *testing.T) {
	h := newHarness(t, passthroughDoc(snapshot.EndpointImages), nil)
	const resp = `{"created":1757030400,"data":[{"b64_json":"aGk="}],` +
		`"usage":{"prompt_tokens":19,"completion_tokens":1290,"total_tokens":1309,` +
		`"completion_tokens_details":{"image_tokens":1290},"cost":0.242}}`
	h.mock.set(func(o *mockOpts) { o.rawResp = resp })
	status, body := h.passthrough(t, http.MethodPost, "/v1/images", testToken, imageBody)
	if status != 200 {
		t.Fatalf("%d %s", status, body)
	}
	// Forwarded whole: cost and the breakdown are the caller's to read.
	if string(body) != resp {
		t.Fatalf("the envelope was altered: %s", body)
	}
	events := h.spoolEvents(t)
	if len(events) != 1 {
		t.Fatalf("want 1 event, got %d", len(events))
	}
	if events[0].InputTokens != 19 || events[0].OutputTokens != 1290 || events[0].Estimated {
		t.Fatalf("image usage: %+v", events[0])
	}
}

// What a non-JSON upstream answer actually does, which is the question behind
// the header allowlist: forwarding a client's Accept could make the vendor
// return image bytes, and this is the outcome the caller would get.
//
// It is a named 502, not a silent drop to an estimate. That matters for the
// allowlist decision: dropping Accept leaves the caller a working JSON answer
// they can decode, and forwarding it would trade that for an error.
func TestPassthroughNonJSONUpstreamAnswerIsRefused(t *testing.T) {
	h := newHarness(t, passthroughDoc(snapshot.EndpointImages), nil)
	h.mock.set(func(o *mockOpts) { o.rawResp = "\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR" })
	status, body := h.passthrough(t, http.MethodPost, "/v1/images", testToken, imageBody)
	if status != http.StatusBadGateway || errCode(t, body) != "upstream_error" {
		t.Fatalf("%d %s", status, body)
	}
	events := h.spoolEvents(t)
	if len(events) != 1 || events[0].ErrorType != "upstream_invalid_response" {
		t.Fatalf("want one upstream_invalid_response event, got %+v", events)
	}
	// And nothing was metered as if it had succeeded.
	if events[0].Status == spool.StatusOK {
		t.Fatalf("a body we could not read was recorded as a success: %+v", events[0])
	}
}

// The candidate-model fence runs here too. Whether these routes honour the
// field is not something this gateway has to establish: fencing it costs a
// short walk and not fencing it costs a bypass of the money fence.
func TestPassthroughCandidateModelsAreFenced(t *testing.T) {
	h := newHarness(t, func(d *snapshot.Document) {
		passthroughDoc(snapshot.EndpointImages)(d)
		d.Keys[0].CreditDeniedModels = []string{"openai/*-pro"}
	}, nil)
	body := `{"model":"openai/gpt-image-1","prompt":"x","models":["openai/o1-pro"]}`
	status, out := h.passthrough(t, http.MethodPost, "/v1/images", testToken, body)
	if status != http.StatusForbidden || errCode(t, out) != "model_not_allowed" {
		t.Fatalf("%d %s", status, out)
	}
	if h.mock.callCount() != 0 {
		t.Fatal("a fenced candidate reached the upstream")
	}
	// A reserved self-serve name cannot ride in the list either.
	status, out = h.passthrough(t, http.MethodPost, "/v1/images", testToken,
		`{"model":"openai/gpt-image-1","prompt":"x","models":["pickle-general"]}`)
	if status != http.StatusForbidden || errCode(t, out) != "model_not_allowed" {
		t.Fatalf("reserved candidate: %d %s", status, out)
	}
	// Allowed candidates still travel.
	if status, out := h.passthrough(t, http.MethodPost, "/v1/images", testToken,
		`{"model":"openai/gpt-image-1","prompt":"x","models":["openai/gpt-image-2"]}`); status != 200 {
		t.Fatalf("allowed candidate: %d %s", status, out)
	}
}

// Streaming is refused rather than forwarded. Every route this surface opens
// is non-streamed and the metering reads a whole JSON body, so a streamed
// answer would relay with no usage in it and land in the accounting as an
// exact zero — billed to nobody and looking like a success.
func TestPassthroughRefusesStreaming(t *testing.T) {
	h := newHarness(t, passthroughDoc(snapshot.EndpointImages, snapshot.EndpointEmbeddings), nil)
	for _, path := range []string{"/v1/images", "/v1/embeddings"} {
		body := `{"model":"openai/gpt-image-1","prompt":"x","stream":true}`
		status, out := h.passthrough(t, http.MethodPost, path, testToken, body)
		if status != http.StatusBadRequest || errCode(t, out) != "streaming_not_supported" {
			t.Fatalf("%s: %d %s", path, status, out)
		}
		if h.mock.callCount() != 0 {
			t.Fatalf("%s reached the upstream", path)
		}
	}
	// stream:false and an absent stream are both ordinary requests.
	for _, body := range []string{
		`{"model":"openai/gpt-image-1","prompt":"x","stream":false}`,
		`{"model":"openai/gpt-image-1","prompt":"x"}`,
	} {
		if status, out := h.passthrough(t, http.MethodPost, "/v1/images", testToken, body); status != 200 {
			t.Fatalf("%s: %d %s", body, status, out)
		}
	}
}

// The same guards on this surface. A preset and an unreadable stream both fail
// closed here, and the vendor's own model field is noted even though the caller
// can see it: our own records cannot.
func TestPassthroughPresetAndStreamFailClosed(t *testing.T) {
	h := newHarness(t, passthroughDoc(snapshot.EndpointImages), nil)
	for _, tc := range []struct{ name, body, code string }{
		{"preset field", `{"model":"openai/gpt-image-1","preset":"s"}`, "preset_not_supported"},
		{"preset in the model", `{"model":"openai/gpt-image-1@preset/s"}`, ""},
		{"unreadable stream", `{"model":"openai/gpt-image-1","stream":"yes"}`, "invalid_parameter_value"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, out := h.passthrough(t, http.MethodPost, "/v1/images", testToken, tc.body)
			if status == 200 {
				t.Fatalf("served: %s", out)
			}
			if tc.code != "" && errCode(t, out) != tc.code {
				t.Fatalf("%d %s", status, out)
			}
			if h.mock.callCount() != 0 {
				t.Fatal("reached the upstream")
			}
		})
	}
}

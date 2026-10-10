package server

import (
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/pnuops/pickle-llm-gateway/internal/config"
	"github.com/pnuops/pickle-llm-gateway/internal/snapshot"
)

func chatWithHeaders(t *testing.T, h *harness, model string, headers map[string]string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, h.gw.URL+"/v1/chat/completions", strings.NewReader(chatFor(model)))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+testToken)
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status %d", resp.StatusCode)
	}
}

var vendorFeatureHeaders = map[string]string{
	"x-anthropic-beta": "structured-outputs-2025-11-13",
	"x-session-id":     "student-session-1",
}

// refusedVendorHeaders are vendor headers deliberately not forwarded; see
// chatHeaderAllowlist for why each group is out.
var refusedVendorHeaders = map[string]string{
	"HTTP-Referer":             "https://somebody-else.example",
	"X-OpenRouter-Title":       "Student App",
	"X-Title":                  "Student App",
	"X-OpenRouter-Categories":  "education",
	"X-OpenRouter-Metadata":    "enabled",
	"X-OpenRouter-Cache":       "true",
	"X-OpenRouter-Cache-TTL":   "600",
	"X-OpenRouter-Cache-Clear": "true",
}

// A money-axis request carries the vendor feature headers the student sent.
func TestChatForwardsVendorHeadersOnTheMoneyAxis(t *testing.T) {
	h := newHarness(t, passthroughAllowlistDoc(), nil)
	headers := map[string]string{
		// Never the client's to set.
		"X-Forwarded-For": "203.0.113.9",
		"User-Agent":      "student-machine/1.0",
	}
	for k, v := range vendorFeatureHeaders {
		headers[k] = v
	}
	for k, v := range refusedVendorHeaders {
		headers[k] = v
	}
	chatWithHeaders(t, h, "openai/gpt-4o-mini", headers)
	_, _, got, _ := h.mock.lastRequest()
	for k, v := range vendorFeatureHeaders {
		if got.Get(k) != v {
			t.Fatalf("%s: upstream got %q, want %q", k, got.Get(k), v)
		}
	}
	for k := range refusedVendorHeaders {
		if got.Get(k) != "" {
			t.Fatalf("%s was forwarded: %q", k, got.Get(k))
		}
	}
	if got.Get("X-Forwarded-For") == "203.0.113.9" {
		t.Fatal("client X-Forwarded-For was forwarded")
	}
	if strings.Contains(got.Get("User-Agent"), "student-machine") {
		t.Fatalf("client User-Agent was forwarded: %q", got.Get("User-Agent"))
	}
	if _, auth := h.mock.last(); auth != "Bearer "+keyCred {
		t.Fatalf("upstream bearer: %q", auth)
	}
}

// A self-hosted model gets none of them.
func TestChatForwardsNoHeadersToSelfHosted(t *testing.T) {
	h := newHarness(t, nil, nil)
	chatWithHeaders(t, h, "pickle-general", vendorFeatureHeaders)
	_, _, got, _ := h.mock.lastRequest()
	for k := range vendorFeatureHeaders {
		if got.Get(k) != "" {
			t.Fatalf("%s reached a self-hosted upstream: %q", k, got.Get(k))
		}
	}
}

// An overlong value is dropped, not cut.
func TestChatDropsOverlongHeaderValues(t *testing.T) {
	h := newHarness(t, passthroughAllowlistDoc(), nil)
	chatWithHeaders(t, h, "openai/gpt-4o-mini", map[string]string{
		"x-anthropic-beta": strings.Repeat("a", forwardedHeaderValueMax+1),
		"x-session-id":     "kept",
	})
	_, _, got, _ := h.mock.lastRequest()
	if got.Get("X-Anthropic-Beta") != "" || got.Get("X-Session-Id") != "kept" {
		t.Fatalf("got beta %d bytes, session %q", len(got.Get("X-Anthropic-Beta")), got.Get("X-Session-Id"))
	}
}

// The image and embedding routes keep their own empty allowlist.
func TestPassthroughStillForwardsNoClientHeaders(t *testing.T) {
	h := newHarness(t, passthroughDoc(snapshot.EndpointImages), nil)
	req, err := http.NewRequest(http.MethodPost, h.gw.URL+"/v1/images", strings.NewReader(imageBody))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+testToken)
	for k, v := range vendorFeatureHeaders {
		req.Header.Set(k, v)
	}
	for k, v := range refusedVendorHeaders {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	_, _, got, _ := h.mock.lastRequest()
	for k := range vendorFeatureHeaders {
		if got.Get(k) != "" {
			t.Fatalf("%s reached the passthrough upstream", k)
		}
	}
}

// vendorResponse answers every upstream call with the given status, headers
// and body.
func vendorResponse(status int, hdr map[string]string, body string) *http.Client {
	return &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		h := make(http.Header)
		for k, v := range hdr {
			h.Set(k, v)
		}
		return &http.Response{StatusCode: status, Header: h, Request: r,
			Body: io.NopCloser(strings.NewReader(body))}, nil
	})}
}

const okCompletion = `{"model":"openai/gpt-4o-mini","choices":[{"index":0,"message":{"role":"assistant","content":"hi"}}],"usage":{"prompt_tokens":1,"completion_tokens":1}}`

var vendorResponseHeaders = map[string]string{
	"X-Generation-Id":              "gen-1760000000-AbC123",
	"X-RateLimit-Limit":            "20",
	"X-OpenRouter-Cache-Status":    "HIT",
	"X-OpenRouter-Cache-Source-Id": "gen-someone-else",
	"Set-Cookie":                   "vendor=1",
	"X-Request-Id":                 "vendor-request",
	"Server":                       "vendor-edge",
}

func chatResponseHeaders(t *testing.T, h *harness, model string, stream bool) http.Header {
	t.Helper()
	body := `{"model":"` + model + `","messages":[{"role":"user","content":"hi"}]}`
	if stream {
		body = `{"model":"` + model + `","stream":true,"messages":[{"role":"user","content":"hi"}]}`
	}
	_, _, hdr := h.chatHeaders(t, testToken, body)
	return hdr
}

// Only the generation id comes back, and only on the money axis.
func TestChatRelaysOnlyTheGenerationID(t *testing.T) {
	h := newHarness(t, passthroughAllowlistDoc(), nil)
	h.srv.client = vendorResponse(200, vendorResponseHeaders, okCompletion)
	hdr := chatResponseHeaders(t, h, "openai/gpt-4o-mini", false)
	if hdr.Get("X-Generation-Id") != "gen-1760000000-AbC123" {
		t.Fatalf("generation id not relayed: %v", hdr)
	}
	for _, k := range []string{"X-RateLimit-Limit", "X-OpenRouter-Cache-Status", "X-OpenRouter-Cache-Source-Id", "Set-Cookie", "Server"} {
		if hdr.Get(k) != "" {
			t.Fatalf("%s reached the client: %q", k, hdr.Get(k))
		}
	}
	if hdr.Get("X-Request-Id") == "vendor-request" {
		t.Fatal("the vendor's request id replaced the gateway's")
	}

	stream := "data: " + okCompletion + "\n\ndata: [DONE]\n\n"
	h.srv.client = vendorResponse(200, vendorResponseHeaders, stream)
	if hdr := chatResponseHeaders(t, h, "openai/gpt-4o-mini", true); hdr.Get("X-Generation-Id") != "gen-1760000000-AbC123" {
		t.Fatalf("generation id not relayed on a stream: %v", hdr)
	}
}

func TestChatRelaysNoVendorHeadersFromSelfHosted(t *testing.T) {
	h := newHarness(t, nil, nil)
	h.srv.client = vendorResponse(200, vendorResponseHeaders, okCompletion)
	if hdr := chatResponseHeaders(t, h, "pickle-general", false); hdr.Get("X-Generation-Id") != "" {
		t.Fatalf("a self-hosted upstream's header was relayed: %v", hdr)
	}
}

func TestGenerationIDShapeIsChecked(t *testing.T) {
	h := newHarness(t, passthroughAllowlistDoc(), nil)
	for _, bad := range []string{"gen 1", strings.Repeat("a", 129), "gen-1;evil", "../gen"} {
		h.srv.client = vendorResponse(200, map[string]string{"X-Generation-Id": bad}, okCompletion)
		if hdr := chatResponseHeaders(t, h, "openai/gpt-4o-mini", false); hdr.Get("X-Generation-Id") != "" {
			t.Fatalf("%q was relayed", bad)
		}
	}
}

// The vendor's wait comes back on its throttled and unavailable answers, and
// not on a 402, where it describes the account's budget.
func TestChatRelaysTheVendorsRetryAfter(t *testing.T) {
	for _, tc := range []struct {
		status int
		code   string
		want   string
	}{
		{http.StatusTooManyRequests, "server_busy", "7"},
		{http.StatusServiceUnavailable, "upstream_error", "7"},
		{http.StatusPaymentRequired, "credit_exhausted", ""},
	} {
		h := newHarness(t, passthroughAllowlistDoc(), nil)
		h.srv.client = vendorResponse(tc.status, map[string]string{"Retry-After": "7"}, `{"error":{"message":"x"}}`)
		_, body, hdr := h.chatHeaders(t, testToken, chatFor("openai/gpt-4o-mini"))
		if errCode(t, body) != tc.code || hdr.Get("Retry-After") != tc.want {
			t.Fatalf("%d: code %s, Retry-After %q, want %s %q", tc.status, errCode(t, body), hdr.Get("Retry-After"), tc.code, tc.want)
		}
	}
	h := newHarness(t, passthroughAllowlistDoc(), nil)
	h.srv.client = vendorResponse(http.StatusTooManyRequests, map[string]string{"Retry-After": "99999"}, `{}`)
	if _, _, hdr := h.chatHeaders(t, testToken, chatFor("openai/gpt-4o-mini")); hdr.Get("Retry-After") != "" {
		t.Fatalf("an unbounded wait was relayed: %q", hdr.Get("Retry-After"))
	}
	self := newHarness(t, nil, nil)
	self.srv.client = vendorResponse(http.StatusTooManyRequests, map[string]string{"Retry-After": "7"}, `{}`)
	if _, _, hdr := self.chatHeaders(t, testToken, chatBody); hdr.Get("Retry-After") != "" {
		t.Fatalf("a self-hosted upstream's wait was relayed: %q", hdr.Get("Retry-After"))
	}
}

func TestPassthroughRelaysGenerationIDAndRetryAfter(t *testing.T) {
	h := newHarness(t, passthroughDoc(snapshot.EndpointImages), nil)
	h.srv.passthroughClient = vendorResponse(200, vendorResponseHeaders,
		`{"created":1,"data":[{"b64_json":"aGk="}],"usage":{"prompt_tokens":1,"completion_tokens":1}}`)
	req, _ := http.NewRequest(http.MethodPost, h.gw.URL+"/v1/images", strings.NewReader(imageBody))
	req.Header.Set("Authorization", "Bearer "+testToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.Header.Get("X-Generation-Id") != "gen-1760000000-AbC123" || resp.Header.Get("X-RateLimit-Limit") != "" {
		t.Fatalf("passthrough headers: %v", resp.Header)
	}
	h.srv.passthroughClient = vendorResponse(http.StatusTooManyRequests, map[string]string{"Retry-After": "4"}, `{}`)
	req, _ = http.NewRequest(http.MethodPost, h.gw.URL+"/v1/images", strings.NewReader(imageBody))
	req.Header.Set("Authorization", "Bearer "+testToken)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.Header.Get("Retry-After") != "4" {
		t.Fatalf("passthrough Retry-After: %q", resp.Header.Get("Retry-After"))
	}
}

// A vendor answer the gateway refuses carries no generation id: the refusal
// is the gateway's own error.
func TestRefusedAnswerCarriesNoGenerationID(t *testing.T) {
	h := newHarness(t, passthroughAllowlistDoc(), nil)
	h.srv.client = vendorResponse(200, map[string]string{"X-Generation-Id": "gen-1"}, "not json")
	status, body, hdr := h.chatHeaders(t, testToken, chatFor("openai/gpt-4o-mini"))
	if status != http.StatusBadGateway || errCode(t, body) != "upstream_error" || hdr.Get("X-Generation-Id") != "" {
		t.Fatalf("got %d %s, id %q", status, body, hdr.Get("X-Generation-Id"))
	}
}

// Repeated lines of an allowed header forward only the first.
func TestRepeatedHeaderLinesForwardOnlyTheFirst(t *testing.T) {
	h := newHarness(t, passthroughAllowlistDoc(), nil)
	req, err := http.NewRequest(http.MethodPost, h.gw.URL+"/v1/chat/completions", strings.NewReader(chatFor("openai/gpt-4o-mini")))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+testToken)
	for i := 0; i < 50; i++ {
		req.Header.Add("x-anthropic-beta", "beta-"+strings.Repeat("x", i))
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	_, _, got, _ := h.mock.lastRequest()
	if v := got.Values("X-Anthropic-Beta"); len(v) != 1 || v[0] != "beta-" {
		t.Fatalf("forwarded %d values: %v", len(v), v)
	}
}

// With a fallback, the longest wait any upstream asked for is the one relayed.
func TestLongestRetryAfterAcrossUpstreams(t *testing.T) {
	h := newHarness(t, func(d *snapshot.Document) {
		d.Models = append(d.Models, snapshot.Model{PublicName: "paid-alias", UpstreamRef: "mock",
			FallbackRef: "backup", UpstreamModel: "vendor/m", BudgetAxis: snapshot.AxisCredit})
		d.Keys[0].UpstreamCredentials = map[string]string{"mock": keyCred, "backup": keyCred}
	}, func(c *config.Config) {
		c.Upstreams["backup"] = config.Upstream{Ref: "backup", BaseURL: "http://backup.invalid", CapField: "max_completion_tokens"}
	})
	calls := 0
	h.srv.client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		calls++
		wait := "30"
		if calls > 1 {
			wait = "2"
		}
		hdr := make(http.Header)
		hdr.Set("Retry-After", wait)
		return &http.Response{StatusCode: http.StatusTooManyRequests, Header: hdr, Request: r,
			Body: io.NopCloser(strings.NewReader(`{}`))}, nil
	})}
	_, body, hdr := h.chatHeaders(t, testToken, chatFor("paid-alias"))
	if calls < 2 || errCode(t, body) != "server_busy" || hdr.Get("Retry-After") != "30" {
		t.Fatalf("calls %d, %s, Retry-After %q", calls, body, hdr.Get("Retry-After"))
	}
}

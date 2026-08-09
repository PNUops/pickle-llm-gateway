package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pnuops/pickle-llm-gateway/internal/config"
	"github.com/pnuops/pickle-llm-gateway/internal/limits"
	"github.com/pnuops/pickle-llm-gateway/internal/snapshot"
	"github.com/pnuops/pickle-llm-gateway/internal/spool"
)

const (
	testToken     = "pickle-test-token"
	upstreamModel = "internal-model-x"
	upstreamCred  = "upstream-secret-cred"
)

type mockOpts struct {
	delay       time.Duration
	status      int
	errBody     string
	rawResp     string        // non-stream: raw body override (may be invalid JSON)
	noUsage     bool          // non-stream: omit the usage field
	splitEvent  bool          // stream: split the first event across two data lines
	brokenChunk bool          // stream: emit an unparseable data payload first
	chunkDelay  time.Duration // stream: pause before each chunk
}

type upstreamMock struct {
	mu       sync.Mutex
	lastBody map[string]json.RawMessage
	lastAuth string
	opts     mockOpts
}

func (u *upstreamMock) set(f func(*mockOpts)) {
	u.mu.Lock()
	defer u.mu.Unlock()
	f(&u.opts)
}

func (u *upstreamMock) last() (map[string]json.RawMessage, string) {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.lastBody, u.lastAuth
}

func (u *upstreamMock) handler(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	params := map[string]json.RawMessage{}
	_ = json.Unmarshal(body, &params)
	u.mu.Lock()
	u.lastBody = params
	u.lastAuth = r.Header.Get("Authorization")
	cp := u.opts
	u.mu.Unlock()

	if cp.delay > 0 {
		time.Sleep(cp.delay)
	}
	if cp.status != 0 && cp.status != http.StatusOK {
		w.WriteHeader(cp.status)
		_, _ = io.WriteString(w, cp.errBody)
		return
	}
	streaming := false
	if raw, ok := params["stream"]; ok {
		_ = json.Unmarshal(raw, &streaming)
	}
	if !streaming {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case cp.rawResp != "":
			_, _ = io.WriteString(w, cp.rawResp)
		case cp.noUsage:
			_, _ = io.WriteString(w, `{"id":"chatcmpl-t1","object":"chat.completion","model":"`+upstreamModel+`","choices":[{"index":0,"message":{"role":"assistant","content":"안녕하세요"},"finish_reason":"stop"}]}`)
		default:
			_, _ = io.WriteString(w, `{"id":"chatcmpl-t1","object":"chat.completion","created":1700000000,"model":"`+upstreamModel+`","choices":[{"index":0,"message":{"role":"assistant","content":"안녕하세요"},"finish_reason":"stop"}],"usage":{"prompt_tokens":7,"completion_tokens":5,"total_tokens":12}}`)
		}
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	fl := w.(http.Flusher)
	emit := func(raw string) {
		if cp.chunkDelay > 0 {
			time.Sleep(cp.chunkDelay)
		}
		_, _ = io.WriteString(w, raw)
		fl.Flush()
	}
	if cp.brokenChunk {
		emit("data: {broken \"model\": \"" + upstreamModel + "\"\n\n")
	}
	if cp.splitEvent {
		// One JSON event across two data lines: legal SSE, joined with \n.
		emit("data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"model\":\"" + upstreamModel + "\",\n" +
			"data: \"choices\":[{\"index\":0,\"delta\":{\"content\":\"안녕\"}}]}\n\n")
	} else {
		emit(`data: {"id":"c1","object":"chat.completion.chunk","model":"` + upstreamModel + `","choices":[{"index":0,"delta":{"role":"assistant","content":"안녕"}}]}` + "\n\n")
	}
	emit(`data: {"id":"c1","object":"chat.completion.chunk","model":"` + upstreamModel + `","choices":[{"index":0,"delta":{"content":"하세요"}}]}` + "\n\n")
	emit(`data: {"id":"c1","object":"chat.completion.chunk","model":"` + upstreamModel + `","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}` + "\n\n")
	emit(`data: {"id":"c1","object":"chat.completion.chunk","model":"` + upstreamModel + `","choices":[],"usage":{"prompt_tokens":7,"completion_tokens":5,"total_tokens":12}}` + "\n\n")
	emit("data: [DONE]\n\n")
}

type harness struct {
	gw       *httptest.Server
	mock     *upstreamMock
	spoolDir string
	snapPath string
	store    *snapshot.Store
	snapMod  time.Time
}

func defaultDoc() snapshot.Document {
	return snapshot.Document{
		Generation:     1,
		ServiceEnabled: true,
		Models: []snapshot.Model{{
			PublicName: "pnu-general", UpstreamRef: "mock",
			UpstreamModel: upstreamModel, MaxOutputTokens: 4096,
		}},
		Keys: []snapshot.Key{{
			KeyID: "k-test", TokenHash: snapshot.HashToken(testToken), Status: snapshot.KeyActive,
		}},
	}
}

func newHarness(t *testing.T, mutateDoc func(*snapshot.Document), mutateCfg func(*config.Config)) *harness {
	t.Helper()
	mock := &upstreamMock{}
	up := httptest.NewServer(http.HandlerFunc(mock.handler))
	t.Cleanup(up.Close)

	doc := defaultDoc()
	if mutateDoc != nil {
		mutateDoc(&doc)
	}
	dir := t.TempDir()
	h := &harness{
		mock:     mock,
		spoolDir: filepath.Join(dir, "spool"),
		snapPath: filepath.Join(dir, "snapshot.json"),
		snapMod:  time.Now().Add(-time.Hour),
	}
	h.writeSnapshot(t, doc)

	store, err := snapshot.Open(h.snapPath, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	h.store = store
	sp, err := spool.Open(h.spoolDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sp.Close() })

	cfg := &config.Config{
		RequestBodyMaxBytes: 1 << 20,
		UpstreamHeaderWait:  5 * time.Second,
		RequestMaxDuration:  30 * time.Second,
		MaxInFlight:         16,
		DefaultRpm:          1000,
		DefaultTpm:          1_000_000,
		DefaultConcurrency:  8,
		Upstreams: map[string]config.Upstream{
			"mock": {Ref: "mock", BaseURL: up.URL, APIKey: upstreamCred, CapField: "max_completion_tokens"},
		},
	}
	if mutateCfg != nil {
		mutateCfg(cfg)
	}
	srv := New(cfg, store, limits.New(nil), sp, slog.New(slog.DiscardHandler))
	h.gw = httptest.NewServer(srv.Handler())
	t.Cleanup(h.gw.Close)
	return h
}

// writeSnapshot bumps the mtime one minute per write so the poller's
// change detection always fires in tests.
func (h *harness) writeSnapshot(t *testing.T, doc snapshot.Document) {
	t.Helper()
	raw, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(h.snapPath, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	h.snapMod = h.snapMod.Add(time.Minute)
	if err := os.Chtimes(h.snapPath, h.snapMod, h.snapMod); err != nil {
		t.Fatal(err)
	}
}

func (h *harness) chat(t *testing.T, token, body string) (int, []byte) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, h.gw.URL+"/v1/chat/completions", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	req.Header.Set("Content-Type", "application/json")
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

func errCode(t *testing.T, body []byte) string {
	t.Helper()
	var e struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &e); err != nil {
		t.Fatalf("not an error envelope: %s", body)
	}
	return e.Error.Code
}

func (h *harness) spoolEvents(t *testing.T) []spool.Event {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(h.spoolDir, "usage-*.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	var evs []spool.Event
	for _, f := range files {
		raw, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		for _, line := range bytes.Split(raw, []byte("\n")) {
			if len(bytes.TrimSpace(line)) == 0 {
				continue
			}
			var ev spool.Event
			if err := json.Unmarshal(line, &ev); err != nil {
				t.Fatalf("bad spool line %q: %v", line, err)
			}
			evs = append(evs, ev)
		}
	}
	return evs
}

const chatBody = `{"model":"pnu-general","messages":[{"role":"user","content":"MARKER-PROMPT-CONTENT"}]}`

func TestAuthRefusals(t *testing.T) {
	past := time.Now().Add(-time.Hour)
	h := newHarness(t, func(d *snapshot.Document) {
		d.Keys = append(d.Keys,
			snapshot.Key{KeyID: "k-rev", TokenHash: snapshot.HashToken("pickle-revoked"), Status: snapshot.KeyRevoked},
			snapshot.Key{KeyID: "k-sus", TokenHash: snapshot.HashToken("pickle-suspended"), Status: snapshot.KeySuspended},
			snapshot.Key{KeyID: "k-exp", TokenHash: snapshot.HashToken("pickle-expired"), Status: snapshot.KeyActive, ExpiresAt: &past},
		)
	}, nil)

	cases := []struct {
		name, token, code string
		status            int
	}{
		{"no key", "", "missing_api_key", 401},
		{"unknown key", "pickle-nope", "invalid_api_key", 401},
		{"revoked", "pickle-revoked", "api_key_revoked", 401},
		{"suspended", "pickle-suspended", "account_suspended", 403},
		{"expired", "pickle-expired", "api_key_expired", 401},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			status, body := h.chat(t, c.token, chatBody)
			if status != c.status || errCode(t, body) != c.code {
				t.Fatalf("got %d %s, want %d %s", status, errCode(t, body), c.status, c.code)
			}
		})
	}
	evs := h.spoolEvents(t)
	if len(evs) != len(cases) {
		t.Fatalf("spool has %d events, want %d", len(evs), len(cases))
	}
	for _, ev := range evs {
		if ev.Status != spool.StatusAuthRejected {
			t.Fatalf("event status %s, want AUTH_REJECTED", ev.Status)
		}
	}
}

func TestChatNonStream(t *testing.T) {
	h := newHarness(t, nil, nil)
	status, body := h.chat(t, testToken, chatBody)
	if status != 200 {
		t.Fatalf("status %d: %s", status, body)
	}
	var resp map[string]any
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatal(err)
	}
	if resp["model"] != "pnu-general" {
		t.Fatalf("model in response = %v, want the public name", resp["model"])
	}
	if bytes.Contains(body, []byte(upstreamModel)) {
		t.Fatal("upstream model name leaked to the client")
	}

	sent, auth := h.mock.last()
	var sentModel string
	_ = json.Unmarshal(sent["model"], &sentModel)
	if sentModel != upstreamModel {
		t.Fatalf("upstream got model %q", sentModel)
	}
	if auth != "Bearer "+upstreamCred {
		t.Fatalf("upstream got auth %q", auth)
	}
	// The gateway injects the model's output cap when the request has none.
	var maxTok int
	if err := json.Unmarshal(sent["max_completion_tokens"], &maxTok); err != nil || maxTok != 4096 {
		t.Fatalf("upstream max_completion_tokens = %s", sent["max_completion_tokens"])
	}

	evs := h.spoolEvents(t)
	if len(evs) != 1 {
		t.Fatalf("spool has %d events", len(evs))
	}
	ev := evs[0]
	if ev.Status != spool.StatusOK || ev.InputTokens != 7 || ev.OutputTokens != 5 || ev.Estimated {
		t.Fatalf("unexpected event: %+v", ev)
	}
	if ev.KeyID != "k-test" || ev.PublicModelName != "pnu-general" {
		t.Fatalf("unexpected event identity: %+v", ev)
	}
}

func TestChatStream(t *testing.T) {
	h := newHarness(t, nil, nil)
	status, body := h.chat(t, testToken, `{"model":"pnu-general","stream":true,"messages":[{"role":"user","content":"hi"}]}`)
	if status != 200 {
		t.Fatalf("status %d: %s", status, body)
	}
	text := string(body)
	if !strings.Contains(text, `"model":"pnu-general"`) {
		t.Fatalf("chunks not rewritten to the public name:\n%s", text)
	}
	if strings.Contains(text, upstreamModel) {
		t.Fatal("upstream model name leaked in the stream")
	}
	if !strings.Contains(text, "data: [DONE]") {
		t.Fatal("terminator not forwarded")
	}
	if !strings.Contains(text, "안녕") || !strings.Contains(text, "하세요") {
		t.Fatal("content deltas lost")
	}
	// The student did not opt into include_usage, so the gateway-requested
	// usage chunk is consumed for metering, never forwarded.
	if strings.Contains(text, "prompt_tokens") {
		t.Fatalf("usage chunk forwarded to a client that did not ask for it:\n%s", text)
	}

	sent, _ := h.mock.last()
	var opts map[string]bool
	if err := json.Unmarshal(sent["stream_options"], &opts); err != nil || !opts["include_usage"] {
		t.Fatalf("include_usage not forced: %s", sent["stream_options"])
	}

	evs := h.spoolEvents(t)
	if len(evs) != 1 || evs[0].Status != spool.StatusOK || evs[0].InputTokens != 7 || evs[0].OutputTokens != 5 || evs[0].Estimated {
		t.Fatalf("unexpected event: %+v", evs)
	}
}

func TestSpoolNeverCarriesContent(t *testing.T) {
	h := newHarness(t, nil, nil)
	if status, _ := h.chat(t, testToken, chatBody); status != 200 {
		t.Fatal("request failed")
	}
	files, _ := filepath.Glob(filepath.Join(h.spoolDir, "usage-*.jsonl"))
	for _, f := range files {
		raw, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		for _, needle := range []string{"MARKER-PROMPT-CONTENT", "안녕하세요", "messages"} {
			if bytes.Contains(raw, []byte(needle)) {
				t.Fatalf("spool contains %q", needle)
			}
		}
	}
}

func TestUnsupportedAndMissingParams(t *testing.T) {
	h := newHarness(t, nil, nil)
	status, body := h.chat(t, testToken, `{"model":"pnu-general","messages":[],"logit_bias":{}}`)
	if status != 400 || errCode(t, body) != "unsupported_parameter" {
		t.Fatalf("got %d %s", status, body)
	}
	status, body = h.chat(t, testToken, `{"messages":[]}`)
	if status != 400 || errCode(t, body) != "missing_parameter" {
		t.Fatalf("got %d %s", status, body)
	}
	status, body = h.chat(t, testToken, `{"model":"pnu-general"}`)
	if status != 400 || errCode(t, body) != "missing_parameter" {
		t.Fatalf("got %d %s", status, body)
	}
	status, body = h.chat(t, testToken, `not json`)
	if status != 400 || errCode(t, body) != "invalid_json" {
		t.Fatalf("got %d %s", status, body)
	}
}

func TestModelChecks(t *testing.T) {
	h := newHarness(t, func(d *snapshot.Document) {
		d.Models = append(d.Models, snapshot.Model{
			PublicName: "pnu-restricted", UpstreamRef: "mock", UpstreamModel: "other"})
		d.Keys[0].AllowedModels = []string{"pnu-general"}
	}, nil)

	status, body := h.chat(t, testToken, `{"model":"pnu-none","messages":[]}`)
	if status != 404 || errCode(t, body) != "model_not_found" {
		t.Fatalf("got %d %s", status, body)
	}
	status, body = h.chat(t, testToken, `{"model":"pnu-restricted","messages":[]}`)
	if status != 403 || errCode(t, body) != "model_not_allowed" {
		t.Fatalf("got %d %s", status, body)
	}
}

func TestModelsListFiltered(t *testing.T) {
	h := newHarness(t, func(d *snapshot.Document) {
		d.Models = append(d.Models, snapshot.Model{
			PublicName: "pnu-restricted", UpstreamRef: "mock", UpstreamModel: "other"})
		d.Keys[0].AllowedModels = []string{"pnu-general"}
	}, nil)
	req, _ := http.NewRequest(http.MethodGet, h.gw.URL+"/v1/models", nil)
	req.Header.Set("Authorization", "Bearer "+testToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var list struct {
		Object string `json:"object"`
		Data   []struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&list); err != nil {
		t.Fatal(err)
	}
	if list.Object != "list" || len(list.Data) != 1 || list.Data[0].ID != "pnu-general" {
		t.Fatalf("unexpected model list: %+v", list)
	}
}

func TestOutputCapRefusal(t *testing.T) {
	h := newHarness(t, nil, nil)
	status, body := h.chat(t, testToken, `{"model":"pnu-general","messages":[],"max_tokens":999999}`)
	if status != 400 || errCode(t, body) != "output_limit_exceeded" {
		t.Fatalf("got %d %s", status, body)
	}
}

func TestRpmLimit(t *testing.T) {
	h := newHarness(t, func(d *snapshot.Document) {
		d.Keys[0].Limits.Rpm = 1
	}, nil)
	if status, body := h.chat(t, testToken, chatBody); status != 200 {
		t.Fatalf("first request refused: %s", body)
	}
	status, body := h.chat(t, testToken, chatBody)
	if status != 429 || errCode(t, body) != "rate_limit_requests" {
		t.Fatalf("got %d %s", status, body)
	}
}

func TestConcurrencyLimit(t *testing.T) {
	h := newHarness(t, func(d *snapshot.Document) {
		d.Keys[0].Limits.Concurrency = 1
	}, nil)
	h.mock.set(func(u *mockOpts) { u.delay = 400 * time.Millisecond })

	type result struct {
		status int
		code   string
	}
	results := make(chan result, 2)
	for range 2 {
		go func() {
			status, body := h.chat(t, testToken, chatBody)
			code := ""
			if status != 200 {
				code = errCode(t, body)
			}
			results <- result{status, code}
		}()
		time.Sleep(50 * time.Millisecond)
	}
	a, b := <-results, <-results
	if a.status > b.status {
		a, b = b, a
	}
	if a.status != 200 || b.status != 429 || b.code != "rate_limit_concurrency" {
		t.Fatalf("got %+v and %+v", a, b)
	}
}

func TestQuotaExhaustedFlag(t *testing.T) {
	h := newHarness(t, func(d *snapshot.Document) {
		d.Keys[0].QuotaExhausted = true
	}, nil)
	status, body := h.chat(t, testToken, chatBody)
	if status != 429 || errCode(t, body) != "quota_exhausted" {
		t.Fatalf("got %d %s", status, body)
	}
}

func TestServiceDisabled(t *testing.T) {
	h := newHarness(t, func(d *snapshot.Document) {
		d.ServiceEnabled = false
	}, nil)
	status, body := h.chat(t, testToken, chatBody)
	if status != 503 || errCode(t, body) != "service_disabled" {
		t.Fatalf("got %d %s", status, body)
	}
}

// Revocation must take effect on the next snapshot refresh: the file changes,
// Refresh runs (in production every poll interval), and the very next request
// is refused.
func TestSnapshotReloadRevokesKey(t *testing.T) {
	h := newHarness(t, nil, nil)
	if status, _ := h.chat(t, testToken, chatBody); status != 200 {
		t.Fatal("initial request refused")
	}
	doc := defaultDoc()
	doc.Generation = 2
	doc.Keys[0].Status = snapshot.KeyRevoked
	h.writeSnapshot(t, doc)
	h.store.Refresh()
	status, body := h.chat(t, testToken, chatBody)
	if status != 401 || errCode(t, body) != "api_key_revoked" {
		t.Fatalf("got %d %s", status, body)
	}
}

func TestUpstreamErrorShaping(t *testing.T) {
	h := newHarness(t, nil, nil)
	h.mock.set(func(u *mockOpts) {
		u.status = 500
		u.errBody = `{"error":"boom at https://internal-host:9999/secret-path"}`
	})
	status, body := h.chat(t, testToken, chatBody)
	if status != 502 || errCode(t, body) != "upstream_error" {
		t.Fatalf("got %d %s", status, body)
	}
	if bytes.Contains(body, []byte("internal-host")) {
		t.Fatal("upstream error detail leaked to the client")
	}

	h.mock.set(func(u *mockOpts) { u.status = 400; u.errBody = `{"error":{"message":"bad param"}}` })
	status, body = h.chat(t, testToken, chatBody)
	if status != 400 || errCode(t, body) != "upstream_rejected" {
		t.Fatalf("got %d %s", status, body)
	}

	evs := h.spoolEvents(t)
	for _, ev := range evs {
		if ev.Status != spool.StatusUpstreamErr {
			t.Fatalf("event status %s, want UPSTREAM_ERROR", ev.Status)
		}
	}
}

func TestUpstreamTimeout(t *testing.T) {
	h := newHarness(t, nil, func(c *config.Config) {
		c.UpstreamHeaderWait = 100 * time.Millisecond
	})
	h.mock.set(func(u *mockOpts) { u.delay = 2 * time.Second })
	status, body := h.chat(t, testToken, chatBody)
	if status != 504 || errCode(t, body) != "upstream_timeout" {
		t.Fatalf("got %d %s", status, body)
	}
	evs := h.spoolEvents(t)
	if len(evs) != 1 || evs[0].Status != spool.StatusTimeout {
		t.Fatalf("unexpected events: %+v", evs)
	}
}

func TestHealthzAndUnknownPath(t *testing.T) {
	h := newHarness(t, nil, nil)
	resp, err := http.Get(h.gw.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 || !bytes.Contains(raw, []byte(`"generation":1`)) {
		t.Fatalf("healthz: %d %s", resp.StatusCode, raw)
	}

	resp2, err := http.Get(h.gw.URL + "/v1/embeddings")
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()
	raw2, _ := io.ReadAll(resp2.Body)
	if resp2.StatusCode != 404 || errCode(t, raw2) != "unknown_endpoint" {
		t.Fatalf("unknown path: %d %s", resp2.StatusCode, raw2)
	}
}

func TestRequestBodyCap(t *testing.T) {
	h := newHarness(t, nil, func(c *config.Config) { c.RequestBodyMaxBytes = 200 })
	big := fmt.Sprintf(`{"model":"pnu-general","messages":[{"role":"user","content":"%s"}]}`,
		strings.Repeat("a", 500))
	status, body := h.chat(t, testToken, big)
	if status != 400 || errCode(t, body) != "request_too_large" {
		t.Fatalf("got %d %s", status, body)
	}
}

func TestStreamUsageChunkOnlyWhenRequested(t *testing.T) {
	h := newHarness(t, nil, nil)
	status, body := h.chat(t, testToken, `{"model":"pnu-general","stream":true,"stream_options":{"include_usage":true},"messages":[{"role":"user","content":"hi"}]}`)
	if status != 200 || !strings.Contains(string(body), `"prompt_tokens":7`) {
		t.Fatalf("opted-in stream lost its usage chunk: %d %s", status, body)
	}
}

func TestServerBusyDoesNotChargeRpm(t *testing.T) {
	h := newHarness(t, func(d *snapshot.Document) { d.Keys[0].Limits.Rpm = 2 },
		func(c *config.Config) { c.MaxInFlight = 1 })
	h.mock.set(func(u *mockOpts) { u.delay = 500 * time.Millisecond })

	done := make(chan int, 1)
	go func() {
		req, err := http.NewRequest(http.MethodPost, h.gw.URL+"/v1/chat/completions", strings.NewReader(chatBody))
		if err != nil {
			done <- -1
			return
		}
		req.Header.Set("Authorization", "Bearer "+testToken)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			done <- -1
			return
		}
		defer resp.Body.Close()
		_, _ = io.Copy(io.Discard, resp.Body)
		done <- resp.StatusCode
	}()
	time.Sleep(100 * time.Millisecond)
	status, body := h.chat(t, testToken, chatBody)
	if status != 503 || errCode(t, body) != "server_busy" {
		t.Fatalf("expected server_busy while saturated, got %d %s", status, body)
	}
	if s := <-done; s != 200 {
		t.Fatalf("occupying request failed: %d", s)
	}
	h.mock.set(func(u *mockOpts) { u.delay = 0 })
	// rpm is 2 and only the completed request was charged, so this passes.
	// Before the reorder the busy refusal also charged a token and this
	// request would be refused with rate_limit_requests.
	if status, body := h.chat(t, testToken, chatBody); status != 200 {
		t.Fatalf("busy refusal charged the rpm bucket: %d %s", status, body)
	}
}

func TestNullMaxTokensTreatedAsAbsent(t *testing.T) {
	h := newHarness(t, nil, nil)
	status, body := h.chat(t, testToken, `{"model":"pnu-general","messages":[],"max_tokens":null}`)
	if status != 200 {
		t.Fatalf("null max_tokens refused: %d %s", status, body)
	}
	sent, _ := h.mock.last()
	if _, has := sent["max_tokens"]; has {
		t.Fatal("null max_tokens forwarded upstream")
	}
	var n int
	if err := json.Unmarshal(sent["max_completion_tokens"], &n); err != nil || n != 4096 {
		t.Fatalf("cap not injected after null: %s", sent["max_completion_tokens"])
	}
}

func TestInvalidMaxTokensValue(t *testing.T) {
	h := newHarness(t, nil, nil)
	status, body := h.chat(t, testToken, `{"model":"pnu-general","messages":[],"max_tokens":"lots"}`)
	if status != 400 || errCode(t, body) != "invalid_parameter_value" {
		t.Fatalf("got %d %s", status, body)
	}
}

func TestLowercaseBearerScheme(t *testing.T) {
	h := newHarness(t, nil, nil)
	req, err := http.NewRequest(http.MethodPost, h.gw.URL+"/v1/chat/completions", strings.NewReader(chatBody))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "bearer "+testToken)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		raw, _ := io.ReadAll(resp.Body)
		t.Fatalf("lowercase scheme refused: %d %s", resp.StatusCode, raw)
	}
}

func TestCapFieldPerUpstream(t *testing.T) {
	h := newHarness(t, nil, func(c *config.Config) {
		u := c.Upstreams["mock"]
		u.CapField = "max_tokens"
		c.Upstreams["mock"] = u
	})
	if status, body := h.chat(t, testToken, chatBody); status != 200 {
		t.Fatalf("%d %s", status, body)
	}
	sent, _ := h.mock.last()
	var n int
	if err := json.Unmarshal(sent["max_tokens"], &n); err != nil || n != 4096 {
		t.Fatalf("legacy cap field not used: %s", sent["max_tokens"])
	}
}

func TestSplitEventRewritten(t *testing.T) {
	h := newHarness(t, nil, nil)
	h.mock.set(func(u *mockOpts) { u.splitEvent = true })
	status, body := h.chat(t, testToken, `{"model":"pnu-general","stream":true,"messages":[]}`)
	text := string(body)
	if status != 200 || strings.Contains(text, upstreamModel) {
		t.Fatalf("split event leaked the upstream model:\n%s", text)
	}
	if !strings.Contains(text, `"model":"pnu-general"`) || !strings.Contains(text, "안녕") {
		t.Fatalf("split event lost content:\n%s", text)
	}
}

func TestUnparseableChunkDropped(t *testing.T) {
	h := newHarness(t, nil, nil)
	h.mock.set(func(u *mockOpts) { u.brokenChunk = true })
	status, body := h.chat(t, testToken, `{"model":"pnu-general","stream":true,"messages":[]}`)
	text := string(body)
	if status != 200 || strings.Contains(text, upstreamModel) || strings.Contains(text, "{broken") {
		t.Fatalf("unparseable chunk forwarded:\n%s", text)
	}
	if !strings.Contains(text, "data: [DONE]") {
		t.Fatal("stream did not finish")
	}
}

func TestNonStreamInvalidUpstreamJSON(t *testing.T) {
	h := newHarness(t, nil, nil)
	h.mock.set(func(u *mockOpts) { u.rawResp = "<html>error page naming " + upstreamModel + "</html>" })
	status, body := h.chat(t, testToken, chatBody)
	if status != 502 || errCode(t, body) != "upstream_error" {
		t.Fatalf("got %d %s", status, body)
	}
	if bytes.Contains(body, []byte(upstreamModel)) {
		t.Fatal("invalid upstream body forwarded")
	}
}

func TestNonStreamWithoutUsageEstimates(t *testing.T) {
	h := newHarness(t, nil, nil)
	h.mock.set(func(u *mockOpts) { u.noUsage = true })
	if status, body := h.chat(t, testToken, chatBody); status != 200 {
		t.Fatalf("%d %s", status, body)
	}
	evs := h.spoolEvents(t)
	if len(evs) != 1 || !evs[0].Estimated || evs[0].OutputTokens == 0 || evs[0].InputTokens == 0 {
		t.Fatalf("no-usage response not estimated: %+v", evs)
	}
}

func TestStreamDeadlineReported(t *testing.T) {
	h := newHarness(t, nil, func(c *config.Config) { c.RequestMaxDuration = 300 * time.Millisecond })
	h.mock.set(func(u *mockOpts) { u.chunkDelay = 150 * time.Millisecond })
	status, body := h.chat(t, testToken, `{"model":"pnu-general","stream":true,"messages":[]}`)
	text := string(body)
	if status != 200 {
		t.Fatalf("stream status %d", status)
	}
	if !strings.Contains(text, "request_deadline_exceeded") || !strings.Contains(text, "data: [DONE]") {
		t.Fatalf("deadline not reported to the client:\n%s", text)
	}
	evs := h.spoolEvents(t)
	if len(evs) != 1 || evs[0].Status != spool.StatusTimeout || evs[0].ErrorType != "request_deadline_exceeded" {
		t.Fatalf("unexpected event: %+v", evs)
	}
}

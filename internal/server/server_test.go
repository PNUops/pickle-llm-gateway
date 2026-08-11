package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/pnuops/pickle-llm-gateway/internal/bodies"
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
	brokenChunk bool          // stream: emit a truncated JSON object payload first
	heartbeat   bool          // stream: emit a non-JSON keepalive (data: ping) first
	abortMid    bool          // stream: hijack and close after one chunk, no [DONE]
	chunkDelay  time.Duration // stream: pause before each chunk
	failNext    int           // fail this many next calls with a 502, then serve
}

type upstreamMock struct {
	mu       sync.Mutex
	lastBody map[string]json.RawMessage
	lastAuth string
	opts     mockOpts
	calls    int
}

// callCount is how many requests reached the upstream. A test that asserts
// only the client-visible outcome cannot tell one attempt from three, and the
// difference is what the upstream bills.
func (u *upstreamMock) callCount() int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.calls
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
	u.calls++
	cp := u.opts
	u.mu.Unlock()

	if cp.delay > 0 {
		time.Sleep(cp.delay)
	}
	if cp.failNext > 0 {
		u.mu.Lock()
		u.opts.failNext--
		u.mu.Unlock()
		w.WriteHeader(http.StatusBadGateway)
		return
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
	// noUsage applies to streams too: an upstream may simply ignore
	// stream_options.include_usage, and that is the case the estimate path
	// exists for. The knob used to be honored only on the non-stream branch,
	// so a stream test that set it silently tested nothing.
	w.Header().Set("Content-Type", "text/event-stream")
	fl := w.(http.Flusher)
	emit := func(raw string) {
		if cp.chunkDelay > 0 {
			time.Sleep(cp.chunkDelay)
		}
		_, _ = io.WriteString(w, raw)
		fl.Flush()
	}
	if cp.abortMid {
		// One good chunk, then close the raw connection with no [DONE] — the
		// gateway's stream read fails with a non-EOF error.
		emit(`data: {"id":"c1","object":"chat.completion.chunk","model":"` + upstreamModel + `","choices":[{"index":0,"delta":{"content":"안녕"}}]}` + "\n\n")
		if hj, ok := w.(http.Hijacker); ok {
			conn, _, err := hj.Hijack()
			if err == nil {
				_ = conn.Close()
			}
		}
		return
	}
	if cp.heartbeat {
		emit("data: ping\n\n")
	}
	if cp.brokenChunk {
		emit("data: {\"id\":\"c1\",\"model\":\"" + upstreamModel + "\",\"choi\n\n")
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
	if !cp.noUsage {
		emit(`data: {"id":"c1","object":"chat.completion.chunk","model":"` + upstreamModel + `","choices":[],"usage":{"prompt_tokens":7,"completion_tokens":5,"total_tokens":12}}` + "\n\n")
	}
	emit("data: [DONE]\n\n")
}

type harness struct {
	gw       *httptest.Server
	srv      *Server
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
	// Open the store with the upstreams the server actually has, exactly as
	// main does — otherwise a test that adds a fallback upstream would see the
	// document refused for naming an upstream the store was not told about.
	store, err := snapshot.OpenFile(h.snapPath, slog.New(slog.DiscardHandler),
		snapshot.Options{KnownUpstreams: cfg.UpstreamRefs()})
	if err != nil {
		t.Fatal(err)
	}
	h.store = store
	srv := New(cfg, store, limits.New(nil), sp, slog.New(slog.DiscardHandler))
	h.srv = srv
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

// chatStatus is a goroutine-safe variant of chat that returns the status code
// (or -1 on transport error) without calling t.Fatal off the test goroutine.
func (h *harness) chatStatus(token, body string) int {
	req, err := http.NewRequest(http.MethodPost, h.gw.URL+"/v1/chat/completions", strings.NewReader(body))
	if err != nil {
		return -1
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return -1
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode
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
	// Only the refusals that resolved to a key are spooled, and each names its
	// owner. The two that did not — no key, unknown key — have nobody to
	// account to, and writing a line for them would let anyone on the internet
	// fill a small disk with a loop.
	evs := h.spoolEvents(t)
	wantIDs := []string{"k-rev", "k-sus", "k-exp"}
	if len(evs) != len(wantIDs) {
		t.Fatalf("spool has %d events, want %d (only the attributable refusals)", len(evs), len(wantIDs))
	}
	for i, want := range wantIDs {
		if evs[i].KeyID != want {
			t.Fatalf("event %d attributed to %q, want %q — a rejection nobody can be told about is worth little", i, evs[i].KeyID, want)
		}
		if evs[i].Status != spool.StatusAuthRejected {
			t.Fatalf("event %d status = %q", i, evs[i].Status)
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

	// Status codes only, collected off the test goroutine (no t.Fatal there).
	results := make(chan int, 2)
	for range 2 {
		go func() { results <- h.chatStatus(testToken, chatBody) }()
		time.Sleep(50 * time.Millisecond)
	}
	a, b := <-results, <-results
	if a > b {
		a, b = b, a
	}
	if a != 200 || b != 429 {
		t.Fatalf("got %d and %d, want 200 and 429", a, b)
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
	h.store.Refresh(context.Background())
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

func TestCapFieldTranslation(t *testing.T) {
	// Upstream honors only the legacy field; a student sending the modern one
	// must have it translated so the cap actually applies.
	h := newHarness(t, nil, func(c *config.Config) {
		u := c.Upstreams["mock"]
		u.CapField = "max_tokens"
		c.Upstreams["mock"] = u
	})
	if status, body := h.chat(t, testToken, `{"model":"pnu-general","messages":[],"max_completion_tokens":100}`); status != 200 {
		t.Fatalf("%d %s", status, body)
	}
	sent, _ := h.mock.last()
	if _, has := sent["max_completion_tokens"]; has {
		t.Fatal("modern field forwarded verbatim to a legacy upstream")
	}
	var n int
	if err := json.Unmarshal(sent["max_tokens"], &n); err != nil || n != 100 {
		t.Fatalf("cap not translated onto max_tokens: %s", sent["max_tokens"])
	}
}

func TestHeartbeatForwardedContentNotDropped(t *testing.T) {
	h := newHarness(t, nil, nil)
	h.mock.set(func(u *mockOpts) { u.heartbeat = true })
	status, body := h.chat(t, testToken, `{"model":"pnu-general","stream":true,"messages":[]}`)
	text := string(body)
	if status != 200 || !strings.Contains(text, "data: ping") {
		t.Fatalf("heartbeat not forwarded:\n%s", text)
	}
	// A non-JSON keepalive is harmless, so the request stays OK.
	evs := h.spoolEvents(t)
	if len(evs) != 1 || evs[0].Status != spool.StatusOK {
		t.Fatalf("heartbeat marked the request non-OK: %+v", evs)
	}
}

func TestDroppedChunkMarksDegraded(t *testing.T) {
	h := newHarness(t, nil, nil)
	h.mock.set(func(u *mockOpts) { u.brokenChunk = true })
	status, body := h.chat(t, testToken, `{"model":"pnu-general","stream":true,"messages":[]}`)
	text := string(body)
	if status != 200 || strings.Contains(text, upstreamModel) {
		t.Fatalf("truncated chunk leaked the model or wrong status:\n%s", text)
	}
	// The dropped content means the answer is incomplete: not a clean OK.
	evs := h.spoolEvents(t)
	if len(evs) != 1 || evs[0].Status != spool.StatusUpstreamErr || evs[0].ErrorType != "upstream_chunk_unreadable" {
		t.Fatalf("dropped chunk not recorded as degraded: %+v", evs)
	}
}

func TestStreamInterruptAnnouncesError(t *testing.T) {
	h := newHarness(t, nil, nil)
	h.mock.set(func(u *mockOpts) { u.abortMid = true })
	status, body := h.chat(t, testToken, `{"model":"pnu-general","stream":true,"messages":[]}`)
	text := string(body)
	if status != 200 {
		t.Fatalf("stream status %d", status)
	}
	// The client must be able to tell a truncated answer from a complete one:
	// an error event plus a terminal [DONE].
	if !strings.Contains(text, "upstream_stream_interrupted") || !strings.Contains(text, "data: [DONE]") {
		t.Fatalf("interruption not announced to the client:\n%s", text)
	}
	evs := h.spoolEvents(t)
	if len(evs) != 1 || evs[0].Status != spool.StatusUpstreamErr {
		t.Fatalf("interruption not recorded: %+v", evs)
	}
}

func TestRestrictedModelHiddenAndDenied(t *testing.T) {
	// A RESTRICTED model is invisible and unusable to a key with an empty
	// allow list; a key that names it may use it.
	h := newHarness(t, func(d *snapshot.Document) {
		d.Models = append(d.Models, snapshot.Model{
			PublicName: "pnu-internal", UpstreamRef: "mock", UpstreamModel: "secret",
			Visibility: snapshot.ModelRestricted})
		d.Keys = append(d.Keys, snapshot.Key{
			KeyID: "k-priv", TokenHash: snapshot.HashToken("pickle-priv"), Status: snapshot.KeyActive,
			AllowedModels: []string{"pnu-general", "pnu-internal"}})
	}, nil)

	// Open key: list excludes the restricted model, chat is denied, retrieve 404.
	req, _ := http.NewRequest(http.MethodGet, h.gw.URL+"/v1/models", nil)
	req.Header.Set("Authorization", "Bearer "+testToken)
	resp, _ := http.DefaultClient.Do(req)
	raw, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if bytes.Contains(raw, []byte("pnu-internal")) {
		t.Fatal("restricted model listed to an open key")
	}
	if status, body := h.chat(t, testToken, `{"model":"pnu-internal","messages":[]}`); status != 403 || errCode(t, body) != "model_not_allowed" {
		t.Fatalf("open key reached a restricted model: %d %s", status, body)
	}
	// Privileged key: allowed.
	if status, _ := h.chat(t, "pickle-priv", chatBody); status != 200 {
		t.Fatalf("privileged key denied a public model: %d", status)
	}
	if status, body := h.chat(t, "pickle-priv", `{"model":"pnu-internal","messages":[{"role":"user","content":"hi"}]}`); status != 200 {
		t.Fatalf("privileged key denied its restricted model: %d %s", status, body)
	}
}

func TestForwardCompatNestedUnknownField(t *testing.T) {
	// An unknown field inside keys[] must be ignored (forward compatibility for
	// the future api sync), while an unknown TOP-LEVEL field still fails.
	dir := t.TempDir()
	path := filepath.Join(dir, "snapshot.json")
	hash := snapshot.HashToken(testToken)
	good := `{"formatVersion":1,"generation":1,"serviceEnabled":true,` +
		`"models":[{"publicName":"pnu-general","upstreamRef":"mock","upstreamModel":"m","futureModelField":true}],` +
		`"keys":[{"keyId":"k","tokenHash":"` + hash + `","status":"ACTIVE","limits":{},"workspaceId":"ws-7"}]}`
	if err := os.WriteFile(path, []byte(good), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := snapshot.OpenFile(path, slog.New(slog.DiscardHandler), snapshot.Options{KnownUpstreams: []string{"mock"}}); err != nil {
		t.Fatalf("nested unknown field rejected: %v", err)
	}
	bad := `{"generation":1,"serviceEnabled":true,"models":[],"keys":[],"unknownTopLevel":1}`
	if err := os.WriteFile(path, []byte(bad), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := snapshot.OpenFile(path, slog.New(slog.DiscardHandler), snapshot.Options{}); err == nil {
		t.Fatal("unknown top-level field accepted")
	}
}

func TestHealthzDepthAndRequestID(t *testing.T) {
	h := newHarness(t, nil, nil)
	resp, err := http.Get(h.gw.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var hz map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&hz); err != nil {
		t.Fatal(err)
	}
	if hz["status"] != "ok" {
		t.Fatalf("healthz status: %v", hz)
	}
	for _, k := range []string{"generation", "snapshotAgeSeconds", "snapshotReloadStuck"} {
		if _, ok := hz[k]; !ok {
			t.Fatalf("healthz missing %q: %v", k, hz)
		}
	}

	// Every chat response carries X-Request-Id matching the metered event.
	req, _ := http.NewRequest(http.MethodPost, h.gw.URL+"/v1/chat/completions", strings.NewReader(chatBody))
	req.Header.Set("Authorization", "Bearer "+testToken)
	cr, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	rid := cr.Header.Get("X-Request-Id")
	cr.Body.Close()
	if rid == "" {
		t.Fatal("no X-Request-Id header")
	}
	evs := h.spoolEvents(t)
	if len(evs) != 1 || evs[0].EventUUID != rid {
		t.Fatalf("X-Request-Id %q does not match spooled event %+v", rid, evs)
	}
	if evs[0].Generation != 1 {
		t.Fatalf("event generation not recorded: %+v", evs[0])
	}
}

func TestModelRetrieve(t *testing.T) {
	h := newHarness(t, nil, nil)
	req, _ := http.NewRequest(http.MethodGet, h.gw.URL+"/v1/models/pnu-general", nil)
	req.Header.Set("Authorization", "Bearer "+testToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var m map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&m); err != nil {
		t.Fatal(err)
	}
	if m["id"] != "pnu-general" || m["object"] != "model" {
		t.Fatalf("unexpected retrieve: %v", m)
	}
	if c, _ := m["created"].(float64); c == 0 {
		t.Fatal("created is 0 (renders as 1970)")
	}
	// Unknown model id → 404.
	req2, _ := http.NewRequest(http.MethodGet, h.gw.URL+"/v1/models/pnu-none", nil)
	req2.Header.Set("Authorization", "Bearer "+testToken)
	resp2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatal(err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != 404 {
		t.Fatalf("unknown model retrieve status %d", resp2.StatusCode)
	}
}

func TestErrorTypesAreOpenAISet(t *testing.T) {
	valid := map[string]bool{
		"invalid_request_error": true, "authentication_error": true,
		"permission_error": true, "rate_limit_error": true, "server_error": true,
	}
	h := newHarness(t, nil, nil)
	// Trigger a server-side error (unconfigured upstream via 5xx passthrough).
	h.mock.set(func(u *mockOpts) { u.status = 500; u.errBody = "boom" })
	_, body := h.chat(t, testToken, chatBody)
	var e struct {
		Error struct{ Type string } `json:"error"`
	}
	if err := json.Unmarshal(body, &e); err != nil {
		t.Fatal(err)
	}
	if !valid[e.Error.Type] {
		t.Fatalf("error type %q is outside the OpenAI set", e.Error.Type)
	}
}

// bodySink spins a sink pointed at a recording server, so a test can assert
// both what was captured and — more importantly — what was not.
type capturedBodies struct {
	mu      sync.Mutex
	records []bodies.Record
}

func (c *capturedBodies) handler(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Records []bodies.Record `json:"records"`
	}
	_ = json.NewDecoder(r.Body).Decode(&in)
	c.mu.Lock()
	c.records = append(c.records, in.Records...)
	c.mu.Unlock()
	w.WriteHeader(http.StatusOK)
}

func (c *capturedBodies) all() []bodies.Record {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]bodies.Record(nil), c.records...)
}

// withBodyCapture attaches a live capture channel to the harness.
func (h *harness) withBodyCapture(t *testing.T) *capturedBodies {
	t.Helper()
	cb := &capturedBodies{}
	srv := httptest.NewServer(http.HandlerFunc(cb.handler))
	t.Cleanup(srv.Close)
	sink := bodies.New(srv.URL, "tok", 64, 1, 5*time.Second, slog.New(slog.DiscardHandler))
	h.srv.SetBodySink(sink)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go sink.Run(ctx)
	return cb
}

func waitForRecords(t *testing.T, cb *capturedBodies, want int) []bodies.Record {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if got := cb.all(); len(got) >= want {
			return got
		}
		time.Sleep(20 * time.Millisecond)
	}
	return cb.all()
}

func TestBodyCaptureOnlyForOptedInKeys(t *testing.T) {
	h := newHarness(t, func(d *snapshot.Document) {
		d.Keys[0].RecordBodies = true
		d.Keys = append(d.Keys, snapshot.Key{
			KeyID: "k-plain", TokenHash: snapshot.HashToken("pickle-plain"),
			Status: snapshot.KeyActive})
	}, nil)
	cb := h.withBodyCapture(t)

	// The opted-out key must produce no record at all.
	if status, _ := h.chat(t, "pickle-plain", chatBody); status != 200 {
		t.Fatal("opted-out request failed")
	}
	// The opted-in key produces exactly one, carrying both sides.
	if status, _ := h.chat(t, testToken, chatBody); status != 200 {
		t.Fatal("opted-in request failed")
	}
	got := waitForRecords(t, cb, 1)
	if len(got) != 1 {
		t.Fatalf("expected exactly the opted-in record, got %d: %+v", len(got), got)
	}
	if got[0].KeyID != "k-test" {
		t.Fatalf("captured the wrong key: %+v", got[0])
	}
	if !bytes.Contains(got[0].Request, []byte("MARKER-PROMPT-CONTENT")) {
		t.Fatalf("prompt not captured: %s", got[0].Request)
	}
	if got[0].Response != "안녕하세요" {
		t.Fatalf("answer not captured: %q", got[0].Response)
	}
	if got[0].EventUUID == "" {
		t.Fatal("record carries no event id to join on")
	}
}

func TestBodyCaptureStreamAssembles(t *testing.T) {
	h := newHarness(t, func(d *snapshot.Document) { d.Keys[0].RecordBodies = true }, nil)
	cb := h.withBodyCapture(t)
	if status, _ := h.chat(t, testToken, `{"model":"pnu-general","stream":true,"messages":[{"role":"user","content":"hi"}]}`); status != 200 {
		t.Fatal("stream failed")
	}
	got := waitForRecords(t, cb, 1)
	if len(got) != 1 || got[0].Response != "안녕하세요" {
		t.Fatalf("streamed answer not assembled: %+v", got)
	}
}

// The spool is the accounting record and must never carry text, opt-in or not.
// This is the guarantee the whole separation rests on.
func TestSpoolNeverCarriesBodiesEvenWhenCapturing(t *testing.T) {
	h := newHarness(t, func(d *snapshot.Document) { d.Keys[0].RecordBodies = true }, nil)
	cb := h.withBodyCapture(t)
	if status, _ := h.chat(t, testToken, chatBody); status != 200 {
		t.Fatal("request failed")
	}
	waitForRecords(t, cb, 1)

	files, _ := filepath.Glob(filepath.Join(h.spoolDir, "usage-*.jsonl"))
	if len(files) == 0 {
		t.Fatal("no spool file written")
	}
	for _, f := range files {
		raw, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		// Content itself, and the field names a body record would arrive under
		// (as JSON keys, so `requestedAt` is not mistaken for a leak).
		for _, needle := range []string{"MARKER-PROMPT-CONTENT", "안녕하세요", `"messages"`, `"request"`, `"response"`} {
			if bytes.Contains(raw, []byte(needle)) {
				t.Fatalf("spool leaked %q: %s", needle, raw)
			}
		}
	}
}

// Without a sink, an opted-in key still captures nothing: the delivery channel
// is what makes capture possible, so a gateway with no control plane collects
// no text anywhere.
func TestNoCaptureWithoutChannel(t *testing.T) {
	h := newHarness(t, func(d *snapshot.Document) { d.Keys[0].RecordBodies = true }, nil)
	if status, _ := h.chat(t, testToken, chatBody); status != 200 {
		t.Fatal("request failed")
	}
	files, _ := filepath.Glob(filepath.Join(h.spoolDir, "usage-*.jsonl"))
	for _, f := range files {
		raw, _ := os.ReadFile(f)
		if bytes.Contains(raw, []byte("MARKER-PROMPT-CONTENT")) {
			t.Fatal("text was recorded with no delivery channel configured")
		}
	}
}

func TestRetryAfterAndRateLimitHeaders(t *testing.T) {
	h := newHarness(t, func(d *snapshot.Document) { d.Keys[0].Limits.Rpm = 1 }, nil)

	req, _ := http.NewRequest(http.MethodPost, h.gw.URL+"/v1/chat/completions", strings.NewReader(chatBody))
	req.Header.Set("Authorization", "Bearer "+testToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("first request: %d", resp.StatusCode)
	}
	if resp.Header.Get("X-RateLimit-Limit-Requests") != "1" {
		t.Fatalf("no rate-limit ceiling header: %q", resp.Header.Get("X-RateLimit-Limit-Requests"))
	}
	if resp.Header.Get("X-RateLimit-Remaining-Requests") != "0" {
		t.Fatalf("remaining header wrong: %q", resp.Header.Get("X-RateLimit-Remaining-Requests"))
	}

	// The refused follow-up must say when to come back.
	req2, _ := http.NewRequest(http.MethodPost, h.gw.URL+"/v1/chat/completions", strings.NewReader(chatBody))
	req2.Header.Set("Authorization", "Bearer "+testToken)
	resp2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatal(err)
	}
	resp2.Body.Close()
	if resp2.StatusCode != 429 {
		t.Fatalf("second request: %d", resp2.StatusCode)
	}
	after := resp2.Header.Get("Retry-After")
	if after == "" {
		t.Fatal("429 carried no Retry-After")
	}
	if n, err := strconv.Atoi(after); err != nil || n <= 0 || n > 120 {
		t.Fatalf("implausible Retry-After: %q", after)
	}
}

func TestUpstreamRetryThenSuccess(t *testing.T) {
	h := newHarness(t, nil, func(c *config.Config) { c.UpstreamRetries = 1 })
	// Fail once, then serve normally: the student never sees the failure.
	h.mock.set(func(u *mockOpts) { u.failNext = 1 })
	status, body := h.chat(t, testToken, chatBody)
	if status != 200 {
		t.Fatalf("a retryable failure was not retried: %d %s", status, body)
	}
}

func TestUpstreamFallback(t *testing.T) {
	var fallbackHits int
	var mu sync.Mutex
	fb := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		fallbackHits++
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"id":"fb","object":"chat.completion","model":"other","choices":[{"index":0,"message":{"role":"assistant","content":"fallback"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`)
	}))
	defer fb.Close()

	h := newHarness(t, func(d *snapshot.Document) {
		d.Models[0].FallbackRef = "backup"
	}, func(c *config.Config) {
		c.UpstreamRetries = 0
		c.Upstreams["backup"] = config.Upstream{Ref: "backup", BaseURL: fb.URL, CapField: "max_completion_tokens"}
	})
	// The primary is down for good; the fallback answers.
	h.mock.set(func(u *mockOpts) { u.status = http.StatusBadGateway })
	status, body := h.chat(t, testToken, chatBody)
	if status != 200 {
		t.Fatalf("fallback did not take over: %d %s", status, body)
	}
	mu.Lock()
	hits := fallbackHits
	mu.Unlock()
	if hits != 1 {
		t.Fatalf("fallback hit %d times", hits)
	}
	// The public model name still holds on the fallback's answer.
	if !bytes.Contains(body, []byte(`"model":"pnu-general"`)) {
		t.Fatalf("fallback answer leaked its own model name: %s", body)
	}
	// That hiding is exactly why the event has to say which upstream answered:
	// the two are different models, often billed to different people, and
	// nothing else on this host records the difference.
	events := h.spoolEvents(t)
	last := events[len(events)-1]
	if last.UpstreamRef != "backup" {
		t.Fatalf("event says upstreamRef=%q, want the upstream that actually served it", last.UpstreamRef)
	}
	if last.Attempts != 2 {
		t.Fatalf("attempts = %d, want both tries counted", last.Attempts)
	}
}

func TestUpstreamRefusalIsNotRetriedOrFailedOver(t *testing.T) {
	var fallbackHits int
	var mu sync.Mutex
	fb := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		fallbackHits++
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer fb.Close()

	h := newHarness(t, func(d *snapshot.Document) { d.Models[0].FallbackRef = "backup" },
		func(c *config.Config) {
			c.UpstreamRetries = 2
			c.Upstreams["backup"] = config.Upstream{Ref: "backup", BaseURL: fb.URL, CapField: "max_completion_tokens"}
		})
	// A 400 is the request's own fault: repeating it anywhere is pointless.
	h.mock.set(func(u *mockOpts) { u.status = http.StatusBadRequest; u.errBody = `{"error":"bad"}` })
	status, body := h.chat(t, testToken, chatBody)
	if status != 400 || errCode(t, body) != "upstream_rejected" {
		t.Fatalf("got %d %s", status, body)
	}
	mu.Lock()
	hits := fallbackHits
	mu.Unlock()
	if hits != 0 {
		t.Fatalf("a refusal was failed over to the fallback %d times", hits)
	}
}

func TestAdminMetrics(t *testing.T) {
	h := newHarness(t, nil, nil)
	if status, _ := h.chat(t, testToken, chatBody); status != 200 {
		t.Fatal("request failed")
	}
	admin := httptest.NewServer(h.srv.AdminHandler())
	defer admin.Close()

	resp, err := http.Get(admin.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var m struct {
		InFlight     int              `json:"inFlight"`
		ByStatus     map[string]int64 `json:"requestsByStatus"`
		InputTokens  int64            `json:"inputTokens"`
		OutputTokens int64            `json:"outputTokens"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&m); err != nil {
		t.Fatal(err)
	}
	if m.ByStatus["OK"] != 1 || m.InputTokens != 7 || m.OutputTokens != 5 {
		t.Fatalf("metrics did not observe the request: %+v", m)
	}

	// The student-facing route must not carry the metrics surface.
	pub, err := http.Get(h.gw.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Body.Close()
	if pub.StatusCode != 404 {
		t.Fatalf("metrics reachable on the public listener: %d", pub.StatusCode)
	}
}

// A completion that timed out waiting for headers may well be generating right
// now. Repeating it bills a second answer nobody reads.
func TestTimeoutIsNotRetried(t *testing.T) {
	var calls int
	var mu sync.Mutex
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		calls++
		mu.Unlock()
		time.Sleep(400 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer slow.Close()

	h := newHarness(t, nil, func(c *config.Config) {
		c.UpstreamRetries = 2
		c.UpstreamHeaderWait = 80 * time.Millisecond
		c.Upstreams["mock"] = config.Upstream{Ref: "mock", BaseURL: slow.URL, CapField: "max_completion_tokens"}
	})
	status, body := h.chat(t, testToken, chatBody)
	if status != 504 || errCode(t, body) != "upstream_timeout" {
		t.Fatalf("got %d %s", status, body)
	}
	mu.Lock()
	got := calls
	mu.Unlock()
	if got != 1 {
		t.Fatalf("a timed-out completion was sent %d times", got)
	}
}

// A throttling upstream must not be hammered, and the student should be told
// the service is busy rather than that something broke.
func TestUpstreamThrottleIsNotRetriedAndSaysBusy(t *testing.T) {
	var calls int
	var mu sync.Mutex
	h := newHarness(t, nil, func(c *config.Config) { c.UpstreamRetries = 2 })
	throttling := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		calls++
		mu.Unlock()
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer throttling.Close()
	h.srv.cfg.Upstreams["mock"] = config.Upstream{Ref: "mock", BaseURL: throttling.URL, CapField: "max_completion_tokens"}

	status, body := h.chat(t, testToken, chatBody)
	if status != 503 || errCode(t, body) != "server_busy" {
		t.Fatalf("got %d %s", status, body)
	}
	mu.Lock()
	got := calls
	mu.Unlock()
	if got != 1 {
		t.Fatalf("a throttling upstream was called %d times", got)
	}
}

// The captured prompt has its own cap. Without one the only bound is the
// request-body limit, and a queue of records at that size is more memory than
// this host has. A cut prompt cannot stay a messages array — cutting JSON
// mid-way produces something no parser takes — so it arrives as a string and
// says so.
func TestBodyCaptureCapsTheRequest(t *testing.T) {
	h := newHarness(t, func(d *snapshot.Document) {
		d.Keys[0].RecordBodies = true
	}, func(c *config.Config) {
		c.RequestBodyMaxBytes = 4 << 20
	})
	cb := h.withBodyCapture(t)

	long := strings.Repeat("가", bodies.RequestCapBytes) // multi-byte on purpose
	body := `{"model":"pnu-general","messages":[{"role":"user","content":"` + long + `"}]}`
	if status, resp := h.chat(t, testToken, body); status != 200 {
		t.Fatalf("chat failed: %d %s", status, resp)
	}
	recs := waitForRecords(t, cb, 1)
	if len(recs) != 1 {
		t.Fatalf("captured %d records", len(recs))
	}
	r := recs[0]
	if !r.RequestTruncated {
		t.Fatal("an oversized prompt was captured whole")
	}
	if len(r.Request) > bodies.RequestCapBytes+64 {
		t.Fatalf("captured request is %d bytes, over the cap", len(r.Request))
	}
	var asString string
	if err := json.Unmarshal(r.Request, &asString); err != nil {
		t.Fatalf("a truncated request must still be valid JSON: %v", err)
	}
	if !utf8.ValidString(asString) {
		t.Fatal("the cut landed inside a rune")
	}
	if r.ResponseTruncated {
		t.Fatal("the response flag fired for a truncated request")
	}
}

// The one surface an operator or a monitor reads. Its failure mode is silence:
// a gateway serving a three-day-old key set answering {"status":"ok"} is
// exactly the shape of every silent-freeze defect this codebase has had.
func TestHealthzReportsDegradedWhenTheDocumentIsNotBeingApplied(t *testing.T) {
	h := newHarness(t, nil, nil)
	if err := os.WriteFile(h.snapPath, []byte("{ this is not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	h.snapMod = h.snapMod.Add(time.Minute)
	if err := os.Chtimes(h.snapPath, h.snapMod, h.snapMod); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		h.store.Refresh(t.Context())
	}
	resp, err := http.Get(h.gw.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var body map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body["status"] != "degraded" {
		t.Fatalf("health says %v while the served state is going stale: %v", body["status"], body)
	}
	if body["snapshotReloadStuck"] != true {
		t.Fatalf("snapshotReloadStuck = %v", body["snapshotReloadStuck"])
	}
	if n, _ := body["reloadFailures"].(float64); n != 3 {
		t.Fatalf("reloadFailures = %v, want 3", body["reloadFailures"])
	}
	if s, _ := body["lastError"].(string); s == "" {
		t.Fatal("no lastError: the operator is told something is wrong but not what")
	}
}

// A retry that fires once is correct; one that fires three times bills three
// times. TestTimeoutIsNotRetried covers "must not retry"; this is the positive
// case, which asserted only a 200 and so could not see over-retrying.
func TestRetrySucceedsAndIsCountedExactlyOnce(t *testing.T) {
	h := newHarness(t, nil, func(c *config.Config) { c.UpstreamRetries = 3 })
	h.mock.set(func(u *mockOpts) { u.failNext = 1 })
	status, body := h.chat(t, testToken, chatBody)
	if status != 200 {
		t.Fatalf("status %d: %s", status, body)
	}
	if got := h.mock.callCount(); got != 2 {
		t.Fatalf("upstream called %d times, want exactly 2 (one failure, one retry)", got)
	}
	evs := h.spoolEvents(t)
	last := evs[len(evs)-1]
	if last.Attempts != 2 {
		t.Fatalf("event records %d attempts, want 2 — the cost of a request is not what it looks like", last.Attempts)
	}
}

// An upstream may simply ignore stream_options.include_usage. Without the
// estimate, every streamed request against such an upstream is metered as zero
// tokens — billed as free and never charging the token bucket.
func TestStreamWithoutUsageIsEstimated(t *testing.T) {
	h := newHarness(t, nil, nil)
	h.mock.set(func(u *mockOpts) { u.noUsage = true })
	status, body := h.chat(t, testToken, `{"model":"pnu-general","stream":true,"messages":[{"role":"user","content":"hi"}]}`)
	if status != 200 {
		t.Fatalf("status %d: %s", status, body)
	}
	evs := h.spoolEvents(t)
	ev := evs[len(evs)-1]
	if !ev.Estimated {
		t.Fatal("an upstream that reported no usage produced an event claiming exact counts")
	}
	if ev.InputTokens <= 0 || ev.OutputTokens <= 0 {
		t.Fatalf("metered %d/%d tokens: the request was billed as free", ev.InputTokens, ev.OutputTokens)
	}
}

// Nothing proved the request path charges the token bucket at all — only that
// the bucket works when charged directly. A wrong key id, the wrong count, or
// no charge at all leaves the token limit permanently unreachable.
func TestTokenLimitIsChargedFromTheRequestPath(t *testing.T) {
	h := newHarness(t, func(d *snapshot.Document) {
		// The bucket is charged after the fact and admission only requires it
		// out of debt, so the limit has to be under one response's total (12)
		// for the second request to be the one refused.
		d.Keys[0].Limits.Tpm = 10
	}, nil)
	if status, body := h.chat(t, testToken, chatBody); status != 200 {
		t.Fatalf("first request: %d %s", status, body)
	}
	status, body := h.chat(t, testToken, chatBody)
	if status != 429 {
		t.Fatalf("second request got %d, want 429 — the token bucket is never charged: %s", status, body)
	}
	if code := errCode(t, body); code != "rate_limit_tokens" {
		t.Fatalf("refused with %q, want rate_limit_tokens", code)
	}
}

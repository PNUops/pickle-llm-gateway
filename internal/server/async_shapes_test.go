package server

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/pnuops/pickle-llm-gateway/internal/config"
	"github.com/pnuops/pickle-llm-gateway/internal/snapshot"
	"github.com/pnuops/pickle-llm-gateway/internal/spool"
)

// vendorRecorder is a scripted vendor for the async routes: one handler, and
// the requests it saw.
type vendorRecorder struct {
	mu   sync.Mutex
	seen []vendorSeen
}

type vendorSeen struct {
	method, path, query string
	body                []byte
}

func useVendor(t *testing.T, h *harness, handle func(w http.ResponseWriter, r *http.Request)) *vendorRecorder {
	t.Helper()
	rec := &vendorRecorder{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		rec.mu.Lock()
		rec.seen = append(rec.seen, vendorSeen{r.Method, r.URL.Path, r.URL.RawQuery, raw})
		rec.mu.Unlock()
		handle(w, r)
	}))
	t.Cleanup(srv.Close)
	h.srv.cfg.Upstreams["mock"] = config.Upstream{Ref: "mock", BaseURL: srv.URL, APIKey: upstreamCred}
	return rec
}

func (v *vendorRecorder) requests() []vendorSeen {
	v.mu.Lock()
	defer v.mu.Unlock()
	return append([]vendorSeen(nil), v.seen...)
}

func writeVendor(w http.ResponseWriter, status int, body string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = io.WriteString(w, body)
}

func anthropicCall(t *testing.T, h *harness, method, path, body string) (int, []byte, http.Header) {
	t.Helper()
	var reader io.Reader
	if body != "" {
		reader = strings.NewReader(body)
	}
	req, _ := http.NewRequest(method, h.gw.URL+path, reader)
	req.Header.Set("X-Api-Key", testToken)
	req.Header.Set("Anthropic-Version", "2023-06-01")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, raw, resp.Header
}

// The batch model goes through the key's curation list, as chat does: a
// model the same key's chat refuses is refused in a batch too.
func TestBatchModelGoesThroughTheCurationList(t *testing.T) {
	h := newHarness(t, func(d *snapshot.Document) {
		passthroughDoc(snapshot.EndpointBatches)(d)
		d.Keys[0].AllowedModels = []string{"anthropic/claude-sonnet-4.5"}
	}, nil)
	ledger := newFakeLedger(t, h)
	acceptBatches(h)
	status, body := h.passthrough(t, http.MethodPost, "/v1/batches", testToken, batchBody)
	if status != http.StatusForbidden || errCode(t, body) != "model_not_allowed" {
		t.Fatalf("curated-out model: %d %s", status, body)
	}
	if intents, _, _, _ := ledger.counts(); intents != 0 || h.mock.callCount() != 0 {
		t.Fatal("the vendor or the ledger was reached")
	}
}

// Only the documented members travel; an unread member could name another
// model or endpoint.
func TestBatchRefusesUndocumentedMembers(t *testing.T) {
	h, ledger := batchHarness(t)
	acceptBatches(h)
	for _, body := range []string{
		`{"endpoint":"/v1/chat/completions","model":"openai/gpt-4o-mini","requests":[{"custom_id":"a",` +
			`"url":"/v1/embeddings","body":{"messages":[{"role":"user","content":"hi"}]}}]}`,
		`{"endpoint":"/v1/chat/completions","model":"openai/gpt-4o-mini","metadata":{},"requests":[{"custom_id":"a",` +
			`"body":{"messages":[{"role":"user","content":"hi"}]}}]}`,
	} {
		status, raw := h.passthrough(t, http.MethodPost, "/v1/batches", testToken, body)
		if status != http.StatusBadRequest || errCode(t, raw) != "unsupported_parameter" {
			t.Fatalf("undocumented member: %d %s", status, raw)
		}
	}
	if intents, _, _, _ := ledger.counts(); intents != 0 || h.mock.callCount() != 0 {
		t.Fatal("the vendor or the ledger was reached")
	}
}

// The vendor refuses a body whose requests come before the other members.
func TestBatchForwardsRequestsLast(t *testing.T) {
	h, _ := batchHarness(t)
	acceptBatches(h)
	body := `{"requests":[{"custom_id":"a","body":{"messages":[{"role":"user","content":"hi"}]}}],` +
		`"completion_window":"24h","model":"openai/gpt-4o-mini","endpoint":"/v1/chat/completions"}`
	if status, raw := h.passthrough(t, http.MethodPost, "/v1/batches", testToken, body); status != http.StatusAccepted {
		t.Fatalf("submit: %d %s", status, raw)
	}
	_, _, _, sent := h.mock.lastRequest()
	at := strings.Index(string(sent), `"requests"`)
	for _, member := range []string{`"endpoint"`, `"model"`, `"completion_window"`} {
		if i := strings.Index(string(sent), member); i < 0 || i > at {
			t.Fatalf("%s is not before requests: %s", member, sent)
		}
	}
}

// After the vendor accepted, a bind refusal other than OBJECT_TAKEN answers
// success: a 503 would be retried into a second billed batch.
func TestBatchBindRefusalAfterAcceptanceAnswersSuccess(t *testing.T) {
	h, ledger := batchHarness(t)
	acceptBatches(h)
	ledger.refuse("bind", http.StatusNotFound, "INTENT_UNKNOWN")
	status, body := h.passthrough(t, http.MethodPost, "/v1/batches", testToken, batchBody)
	if status != http.StatusAccepted {
		t.Fatalf("bind refusal: %d %s", status, body)
	}
	if ev := lastEvent(t, h); ev.Object == nil || ev.Object.ObjectID != "batch_abc" {
		t.Fatalf("no late bind on the event: %+v", ev)
	}
	ledger.refuse("bind", http.StatusConflict, "OBJECT_TAKEN")
	status, body = h.passthrough(t, http.MethodPost, "/v1/batches", testToken, batchBody)
	if status != http.StatusServiceUnavailable {
		t.Fatalf("object taken: %d %s", status, body)
	}
}

// Any terminal batch can carry a price, and a usage member of a surprising
// type costs that member, not the owner's answer.
func TestBatchPriceSettlesOnEveryTerminalStatusLeniently(t *testing.T) {
	h, ledger := batchHarness(t)
	ledger.own("k-test", "BATCH", "batch_exp")
	h.mock.set(func(o *mockOpts) {
		o.rawResp = `{"id":"batch_exp","status":"expired","model":"openai/gpt-4o-mini",` +
			`"usage":{"prompt_tokens":"10","completion_tokens":4,"cost":"0.0007","is_byok":"no"},"results":null}`
	})
	if status, body := h.passthrough(t, http.MethodGet, "/v1/batches/batch_exp", testToken, ""); status != http.StatusOK {
		t.Fatalf("read: %d %s", status, body)
	}
	var settled *spool.Event
	for _, ev := range h.spoolEvents(t) {
		if ev.Endpoint == spool.EndpointBatchCost {
			e := ev
			settled = &e
		}
	}
	if settled == nil || settled.CostUsd != "0.0007" || settled.OutputTokens != 4 || settled.InputTokens != 0 {
		t.Fatalf("settlement: %+v", settled)
	}
}

// A scan that stops at its page bound says so, rather than ending the list.
func TestBatchListSaysWhenTheScanStopsEarly(t *testing.T) {
	h, ledger := batchHarness(t)
	ledger.own("k-test", "BATCH", "batch_far")
	page := 0
	var mu sync.Mutex
	useVendor(t, h, func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		page++
		n := page
		mu.Unlock()
		writeVendor(w, http.StatusOK, `{"data":[{"id":"batch_other`+strings.Repeat("x", n)+`"}],"has_more":true}`)
	})
	status, body := h.passthrough(t, http.MethodGet, "/v1/batches", testToken, "")
	if status != http.StatusOK {
		t.Fatalf("list: %d %s", status, body)
	}
	var out struct {
		Data      []json.RawMessage `json:"data"`
		HasMore   bool              `json:"has_more"`
		Truncated bool              `json:"truncated"`
	}
	if json.Unmarshal(body, &out) != nil || len(out.Data) != 0 || !out.Truncated || out.HasMore {
		t.Fatalf("early end: %s", body)
	}
	if page != batchListMaxPages {
		t.Fatalf("pages read: %d", page)
	}
}

func TestBatchListAcceptsATimestampBound(t *testing.T) {
	h, _ := batchHarness(t)
	rec := useVendor(t, h, func(w http.ResponseWriter, r *http.Request) {
		writeVendor(w, http.StatusOK, `{"data":[],"has_more":false}`)
	})
	status, body := h.passthrough(t, http.MethodGet, "/v1/batches?created_after=2026-08-20T00:00:00Z", testToken, "")
	if status != http.StatusOK {
		t.Fatalf("list: %d %s", status, body)
	}
	if seen := rec.requests(); len(seen) != 1 || !strings.Contains(seen[0].query, "created_after=2026-08-20T00%3A00%3A00Z") {
		t.Fatalf("vendor saw %+v", seen)
	}
}

// The Anthropic SDK's create becomes the vendor's batch targeting
// /v1/messages, and the answer is a message_batch.
func TestMessagesBatchCreateTranslates(t *testing.T) {
	h, ledger := batchHarness(t)
	rec := useVendor(t, h, func(w http.ResponseWriter, r *http.Request) {
		writeVendor(w, http.StatusAccepted, `{"id":"batch_m1","object":"batch","status":"validating",`+
			`"created_at":1782100000,"expires_at":1782186400,"request_counts":{"total":2,"completed":0,"failed":0}}`)
	})
	body := `{"requests":[` +
		`{"custom_id":"q-1","params":{"model":"anthropic/claude-sonnet-4.5","max_tokens":64,"messages":[{"role":"user","content":"hi"}]}},` +
		`{"custom_id":"q-2","params":{"model":"anthropic/claude-sonnet-4.5","max_tokens":64,"messages":[{"role":"user","content":"yo"}]}}]}`
	status, raw, _ := anthropicCall(t, h, http.MethodPost, "/v1/messages/batches", body)
	if status != http.StatusOK {
		t.Fatalf("create: %d %s", status, raw)
	}
	var batch struct {
		ID               string         `json:"id"`
		Type             string         `json:"type"`
		ProcessingStatus string         `json:"processing_status"`
		RequestCounts    map[string]int `json:"request_counts"`
		CreatedAt        string         `json:"created_at"`
		ResultsURL       *string        `json:"results_url"`
	}
	if json.Unmarshal(raw, &batch) != nil || batch.ID != "batch_m1" || batch.Type != "message_batch" ||
		batch.ProcessingStatus != "in_progress" || batch.RequestCounts["processing"] != 2 ||
		batch.CreatedAt != "2026-06-22T03:46:40Z" || batch.ResultsURL != nil {
		t.Fatalf("message_batch: %s", raw)
	}
	seen := rec.requests()
	if len(seen) != 1 || seen[0].path != "/batches" {
		t.Fatalf("vendor saw %+v", seen)
	}
	var sent struct {
		Endpoint string `json:"endpoint"`
		Model    string `json:"model"`
		Requests []struct {
			CustomID string                     `json:"custom_id"`
			Body     map[string]json.RawMessage `json:"body"`
			Params   json.RawMessage            `json:"params"`
		} `json:"requests"`
	}
	if json.Unmarshal(seen[0].body, &sent) != nil || sent.Endpoint != "/v1/messages" ||
		sent.Model != "anthropic/claude-sonnet-4.5" || len(sent.Requests) != 2 ||
		sent.Requests[1].CustomID != "q-2" || sent.Requests[0].Body["max_tokens"] == nil || sent.Requests[0].Params != nil {
		t.Fatalf("vendor body: %s", seen[0].body)
	}
	if !ledger.owns("k-test", "BATCH", "batch_m1") {
		t.Fatal("the batch was not bound")
	}
}

func TestMessagesBatchRefusesWhatDoesNotTranslate(t *testing.T) {
	h, ledger := batchHarness(t)
	ledger.own("k-test", "BATCH", "batch_m1")
	rec := useVendor(t, h, func(w http.ResponseWriter, r *http.Request) {
		writeVendor(w, http.StatusAccepted, `{"id":"batch_m9","status":"validating"}`)
	})
	cases := []struct {
		method, path, body string
		status             int
		code               string
	}{
		{http.MethodPost, "/v1/messages/batches", `{"requests":[` +
			`{"custom_id":"a","params":{"model":"anthropic/claude-sonnet-4.5","messages":[{"role":"user","content":"hi"}]}},` +
			`{"custom_id":"b","params":{"model":"openai/gpt-4o-mini","messages":[{"role":"user","content":"hi"}]}}]}`,
			400, "batch_mixed_models"},
		{http.MethodPost, "/v1/messages/batches", `{"requests":[` +
			`{"custom_id":"has space","params":{"model":"anthropic/claude-sonnet-4.5","messages":[{"role":"user","content":"hi"}]}}]}`,
			400, "invalid_parameter_value"},
		{http.MethodPost, "/v1/messages/batches", `{"requests":[` +
			`{"custom_id":"a","params":{"model":"anthropic/claude-sonnet-4.5","stream":true,"messages":[{"role":"user","content":"hi"}]}}]}`,
			400, ""},
		{http.MethodPost, "/v1/messages/batches/batch_m1/cancel", "", 400, "batch_cancel_unsupported"},
		{http.MethodGet, "/v1/messages/batches?before_id=batch_m1", "", 400, "unsupported_parameter"},
	}
	for _, c := range cases {
		status, raw, _ := anthropicCall(t, h, c.method, c.path, c.body)
		var env anthropicEnvelope
		if status != c.status || json.Unmarshal(raw, &env) != nil || env.Type != "error" ||
			(c.code != "" && env.Error.Code != c.code) {
			t.Fatalf("%s %s: %d %s", c.method, c.path, status, raw)
		}
	}
	if len(rec.requests()) != 0 {
		t.Fatalf("the vendor was reached: %+v", rec.requests())
	}
}

// A finished batch reads as ended with counts from its results, and its
// results file holds one Anthropic line per vendor result.
func TestMessagesBatchResultsTranslate(t *testing.T) {
	h, ledger := batchHarness(t)
	ledger.own("k-test", "BATCH", "batch_m2")
	ledger.own("k-other", "BATCH", "batch_x")
	useVendor(t, h, func(w http.ResponseWriter, r *http.Request) {
		writeVendor(w, http.StatusOK, `{"id":"batch_m2","object":"batch","status":"completed","model":"anthropic/claude-sonnet-4.5",`+
			`"created_at":1782100000,"finalized_at":1782103600,"request_counts":{"total":3,"completed":2,"failed":1},`+
			`"usage":{"prompt_tokens":20,"completion_tokens":8,"cost":0.001},"results":[`+
			`{"id":"r1","custom_id":"ok","response":{"status_code":200,"request_id":"req_1","body":{"type":"message","content":[]}},"error":null},`+
			`{"id":"r2","custom_id":"bad","response":null,"error":{"type":"invalid_request_error","message":"max_tokens too big","param":"max_tokens"}},`+
			`{"id":"r3","custom_id":"limit","response":{"status_code":429,"request_id":null,"body":{"error":{"message":"slow down"}}},"error":null}]}`)
	})
	status, raw, _ := anthropicCall(t, h, http.MethodGet, "/v1/messages/batches/batch_m2", "")
	var batch struct {
		ProcessingStatus string         `json:"processing_status"`
		RequestCounts    map[string]int `json:"request_counts"`
		EndedAt          string         `json:"ended_at"`
		ResultsURL       string         `json:"results_url"`
	}
	if status != http.StatusOK || json.Unmarshal(raw, &batch) != nil || batch.ProcessingStatus != "ended" ||
		batch.RequestCounts["succeeded"] != 1 || batch.RequestCounts["errored"] != 2 ||
		batch.EndedAt != "2026-06-22T04:46:40Z" || batch.ResultsURL != "/v1/messages/batches/batch_m2/results" {
		t.Fatalf("retrieve: %d %s", status, raw)
	}
	if strings.Contains(string(raw), `"results":`) {
		t.Fatalf("the batch object carries the results: %s", raw)
	}
	status, raw, header := anthropicCall(t, h, http.MethodGet, batch.ResultsURL, "")
	if status != http.StatusOK || header.Get("Content-Type") != "application/x-jsonl" {
		t.Fatalf("results: %d %s", status, raw)
	}
	lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
	if len(lines) != 3 {
		t.Fatalf("results: %s", raw)
	}
	want := []string{
		`{"custom_id":"ok","result":{"message":{"type":"message","content":[]},"type":"succeeded"}}`,
		`{"custom_id":"bad","result":{"error":{"error":{"message":"max_tokens too big","type":"invalid_request_error"},"request_id":null,"type":"error"},"type":"errored"}}`,
		`{"custom_id":"limit","result":{"error":{"error":{"message":"slow down","type":"rate_limit_error"},"request_id":null,"type":"error"},"type":"errored"}}`,
	}
	for i := range want {
		if lines[i] != want[i] {
			t.Fatalf("line %d:\n got %s\nwant %s", i, lines[i], want[i])
		}
	}
	status, raw, _ = anthropicCall(t, h, http.MethodGet, "/v1/messages/batches/batch_x/results", "")
	if status != http.StatusNotFound {
		t.Fatalf("another key's results: %d %s", status, raw)
	}
}

func TestMessagesBatchResultsWaitForTheEnd(t *testing.T) {
	h, ledger := batchHarness(t)
	ledger.own("k-test", "BATCH", "batch_m3")
	useVendor(t, h, func(w http.ResponseWriter, r *http.Request) {
		writeVendor(w, http.StatusOK, `{"id":"batch_m3","status":"in_progress","request_counts":{"total":1,"completed":0,"failed":0}}`)
	})
	status, raw, _ := anthropicCall(t, h, http.MethodGet, "/v1/messages/batches/batch_m3/results", "")
	if status != http.StatusConflict {
		t.Fatalf("results of a running batch: %d %s", status, raw)
	}
}

func TestMessagesBatchDeleteAndList(t *testing.T) {
	h, ledger := batchHarness(t)
	ledger.own("k-test", "BATCH", "batch_m4")
	ledger.own("k-other", "BATCH", "batch_theirs")
	useVendor(t, h, func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodDelete:
			writeVendor(w, http.StatusOK, `{"id":"batch_m4","object":"batch","deletion":{"openrouter":"deleted"}}`)
		default:
			writeVendor(w, http.StatusOK, `{"object":"list","data":[{"id":"batch_m4","status":"in_progress",`+
				`"request_counts":{"total":1,"completed":0,"failed":0}},{"id":"batch_theirs","status":"completed"}],"has_more":false}`)
		}
	})
	status, raw, _ := anthropicCall(t, h, http.MethodGet, "/v1/messages/batches?limit=500", "")
	var list struct {
		Data []struct {
			ID   string `json:"id"`
			Type string `json:"type"`
		} `json:"data"`
		LastID  string `json:"last_id"`
		HasMore bool   `json:"has_more"`
	}
	if status != http.StatusOK || json.Unmarshal(raw, &list) != nil || len(list.Data) != 1 ||
		list.Data[0].ID != "batch_m4" || list.Data[0].Type != "message_batch" || list.LastID != "batch_m4" {
		t.Fatalf("list: %d %s", status, raw)
	}
	status, raw, _ = anthropicCall(t, h, http.MethodDelete, "/v1/messages/batches/batch_m4", "")
	if status != http.StatusOK || string(raw) != `{"id":"batch_m4","type":"message_batch_deleted"}` {
		t.Fatalf("delete: %d %s", status, raw)
	}
	if ledger.owns("k-test", "BATCH", "batch_m4") {
		t.Fatal("the deleted batch is still owned")
	}
}

func videoHarness(t *testing.T) (*harness, *fakeLedger) {
	t.Helper()
	h := newHarness(t, passthroughDoc(snapshot.EndpointVideos), nil)
	return h, newFakeLedger(t, h)
}

const videoBody = `{"model":"google/veo-3.1","prompt":"a cat","duration":4}`

func TestVideoSubmitOwnsTheJobAndRewritesItsAddresses(t *testing.T) {
	h, ledger := videoHarness(t)
	rec := useVendor(t, h, func(w http.ResponseWriter, r *http.Request) {
		writeVendor(w, http.StatusAccepted, `{"id":"gen-vid-1","polling_url":"https://openrouter.ai/api/v1/videos/gen-vid-1","status":"pending"}`)
	})
	status, raw := h.passthrough(t, http.MethodPost, "/v1/videos", testToken, videoBody)
	if status != http.StatusAccepted || !strings.Contains(string(raw), `"polling_url":"/v1/videos/gen-vid-1"`) ||
		strings.Contains(string(raw), "openrouter.ai") {
		t.Fatalf("submit: %d %s", status, raw)
	}
	if seen := rec.requests(); len(seen) != 1 || seen[0].path != "/videos" {
		t.Fatalf("vendor saw %+v", seen)
	}
	if !ledger.owns("k-test", "VIDEO", "gen-vid-1") {
		t.Fatal("the job was not bound")
	}
	if ev := lastEvent(t, h); ev.Endpoint != spool.EndpointVideos || ev.Object == nil || ev.Object.Kind != "VIDEO" {
		t.Fatalf("event: %+v", ev)
	}
}

func TestVideoSubmitRefusals(t *testing.T) {
	h, ledger := videoHarness(t)
	ledger.own("k-other", "VIDEO", "gen-vid-theirs")
	rec := useVendor(t, h, func(w http.ResponseWriter, r *http.Request) {
		writeVendor(w, http.StatusAccepted, `{"id":"gen-vid-2","status":"pending"}`)
	})
	for _, c := range []struct {
		body   string
		status int
		code   string
	}{
		{`{"model":"google/veo-3.1","prompt":"x","callback_url":"https://example.com/hook"}`, 400, "unsupported_parameter"},
		{`{"model":"google/veo-3.1","prompt":"x","previous_job_id":"gen-vid-theirs"}`, 404, "object_not_found"},
		{`{"prompt":"x"}`, 400, "missing_parameter"},
	} {
		status, raw := h.passthrough(t, http.MethodPost, "/v1/videos", testToken, c.body)
		if status != c.status || errCode(t, raw) != c.code {
			t.Fatalf("%s: %d %s", c.body, status, raw)
		}
	}
	if len(rec.requests()) != 0 {
		t.Fatal("the vendor was reached")
	}

	noGrant := newHarness(t, passthroughDoc(), nil)
	newFakeLedger(t, noGrant)
	status, raw := noGrant.passthrough(t, http.MethodPost, "/v1/videos", testToken, videoBody)
	if status != http.StatusForbidden || errCode(t, raw) != "endpoint_not_allowed" {
		t.Fatalf("no videos grant: %d %s", status, raw)
	}
}

func TestVideoPollRewritesAndSettlesOnce(t *testing.T) {
	h, ledger := videoHarness(t)
	ledger.own("k-test", "VIDEO", "gen-vid-3")
	useVendor(t, h, func(w http.ResponseWriter, r *http.Request) {
		writeVendor(w, http.StatusOK, `{"id":"gen-vid-3","status":"completed",`+
			`"polling_url":"https://openrouter.ai/api/v1/videos/gen-vid-3",`+
			`"unsigned_urls":["https://openrouter.ai/api/v1/videos/gen-vid-3/content?index=0"],`+
			`"urls":["https://storage.example/signed"],"usage":{"cost":0.25,"is_byok":false}}`)
	})
	for i := 0; i < 2; i++ {
		status, raw := h.passthrough(t, http.MethodGet, "/v1/videos/gen-vid-3", testToken, "")
		if status != http.StatusOK || strings.Contains(string(raw), "openrouter.ai") ||
			strings.Contains(string(raw), "storage.example") ||
			!strings.Contains(string(raw), `"unsigned_urls":["/v1/videos/gen-vid-3/content?index=0"]`) {
			t.Fatalf("poll: %d %s", status, raw)
		}
	}
	var ids []string
	for _, ev := range h.spoolEvents(t) {
		if ev.Endpoint == spool.EndpointVideoCost {
			if ev.CostUsd != "0.25" {
				t.Fatalf("settlement: %+v", ev)
			}
			ids = append(ids, ev.EventUUID)
		}
	}
	if len(ids) != 2 || ids[0] != ids[1] {
		t.Fatalf("settlements: %v", ids)
	}
	status, raw := h.passthrough(t, http.MethodGet, "/v1/videos/gen-vid-other", testToken, "")
	if status != http.StatusNotFound {
		t.Fatalf("not this key's job: %d %s", status, raw)
	}
}

func TestVideoContentStreamsTheOwnersBytes(t *testing.T) {
	h, ledger := videoHarness(t)
	ledger.own("k-test", "VIDEO", "gen-vid-4")
	rec := useVendor(t, h, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "video/mp4")
		_, _ = io.WriteString(w, "MP4BYTES")
	})
	status, raw := h.passthrough(t, http.MethodGet, "/v1/videos/gen-vid-4/content?index=0", testToken, "")
	if status != http.StatusOK || string(raw) != "MP4BYTES" {
		t.Fatalf("content: %d %s", status, raw)
	}
	if seen := rec.requests(); len(seen) != 1 || seen[0].path != "/videos/gen-vid-4/content" || seen[0].query != "index=0" {
		t.Fatalf("vendor saw %+v", seen)
	}
	status, _ = h.passthrough(t, http.MethodGet, "/v1/videos/gen-vid-5/content", testToken, "")
	if status != http.StatusNotFound || len(rec.requests()) != 1 {
		t.Fatalf("another job's content: %d", status)
	}
}

// Any 2xx is an acceptance: a 201 for a video still binds it.
func TestVideoAnyTwoHundredIsAnAcceptance(t *testing.T) {
	h, ledger := videoHarness(t)
	useVendor(t, h, func(w http.ResponseWriter, r *http.Request) {
		writeVendor(w, http.StatusCreated, `{"id":"gen-vid-201","status":"pending","urls":["https://signed.example/x"]}`)
	})
	status, raw := h.passthrough(t, http.MethodPost, "/v1/videos", testToken, videoBody)
	if status != http.StatusCreated || strings.Contains(string(raw), "signed.example") {
		t.Fatalf("submit: %d %s", status, raw)
	}
	if !ledger.owns("k-test", "VIDEO", "gen-vid-201") {
		t.Fatal("an accepted job was not bound")
	}
	if _, _, _, releases := ledger.counts(); len(releases) != 0 {
		t.Fatalf("an accepted job was released: %v", releases)
	}
}

// Once the vendor may have accepted, a failure tells the SDK not to retry.
func TestSubmitFailureAfterAcceptanceSaysDoNotRetry(t *testing.T) {
	h, _ := batchHarness(t)
	useVendor(t, h, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = io.WriteString(w, `{"id":`) // the body breaks off
		if hj, ok := w.(http.Hijacker); ok {
			conn, _, _ := hj.Hijack()
			_ = conn.Close()
		}
	})
	req, _ := http.NewRequest(http.MethodPost, h.gw.URL+"/v1/batches", strings.NewReader(batchBody))
	req.Header.Set("Authorization", "Bearer "+testToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode < 500 || resp.Header.Get("X-Should-Retry") != "false" {
		t.Fatalf("status %d, X-Should-Retry %q", resp.StatusCode, resp.Header.Get("X-Should-Retry"))
	}
}

func TestVideoRefusesUndocumentedMembers(t *testing.T) {
	h, _ := videoHarness(t)
	rec := useVendor(t, h, func(w http.ResponseWriter, r *http.Request) {
		writeVendor(w, http.StatusAccepted, `{"id":"gen-vid-6","status":"pending"}`)
	})
	status, raw := h.passthrough(t, http.MethodPost, "/v1/videos", testToken,
		`{"model":"google/veo-3.1","prompt":"x","source_job_id":"gen-vid-theirs"}`)
	if status != http.StatusBadRequest || errCode(t, raw) != "unsupported_parameter" || len(rec.requests()) != 0 {
		t.Fatalf("undocumented member: %d %s", status, raw)
	}
}

// A catalogue model's output bound applies to batch items as it does on chat:
// a larger cap is refused, and an item naming none gets the bound.
func TestBatchItemsTakeTheModelsOutputBound(t *testing.T) {
	h := newHarness(t, func(d *snapshot.Document) {
		passthroughDoc(snapshot.EndpointBatches)(d)
		d.Models = append(d.Models, snapshot.Model{PublicName: "vendor/bounded", UpstreamRef: "mock",
			UpstreamModel: "vendor/bounded", BudgetAxis: snapshot.AxisCredit, MaxOutputTokens: 100})
	}, nil)
	newFakeLedger(t, h)
	acceptBatches(h)
	over := `{"endpoint":"/v1/chat/completions","model":"vendor/bounded","requests":[{"custom_id":"a",` +
		`"body":{"max_tokens":500,"messages":[{"role":"user","content":"hi"}]}}]}`
	status, raw := h.passthrough(t, http.MethodPost, "/v1/batches", testToken, over)
	if status != http.StatusBadRequest || errCode(t, raw) != "output_limit_exceeded" {
		t.Fatalf("over the bound: %d %s", status, raw)
	}
	none := `{"endpoint":"/v1/chat/completions","model":"vendor/bounded","requests":[{"custom_id":"a",` +
		`"body":{"messages":[{"role":"user","content":"hi"}]}}]}`
	if status, raw := h.passthrough(t, http.MethodPost, "/v1/batches", testToken, none); status != http.StatusAccepted {
		t.Fatalf("no cap: %d %s", status, raw)
	}
	_, _, _, sent := h.mock.lastRequest()
	if !strings.Contains(string(sent), `"max_tokens":100`) {
		t.Fatalf("the bound was not written: %s", sent)
	}
}

// The vendor fails a whole batch when one request is invalid; its results
// answer says why instead of an empty file.
func TestMessagesBatchFailedAsAWholeSaysWhy(t *testing.T) {
	h, ledger := batchHarness(t)
	ledger.own("k-test", "BATCH", "batch_f")
	useVendor(t, h, func(w http.ResponseWriter, r *http.Request) {
		writeVendor(w, http.StatusOK, `{"id":"batch_f","status":"failed","request_counts":{"total":2,"completed":0,"failed":1},`+
			`"results":null,"error":{"message":"Batch request 'bad-1' has no input."}}`)
	})
	status, raw, _ := anthropicCall(t, h, http.MethodGet, "/v1/messages/batches/batch_f/results", "")
	if status != http.StatusBadRequest || !strings.Contains(string(raw), "batch_failed") ||
		!strings.Contains(string(raw), "bad-1") {
		t.Fatalf("failed batch results: %d %s", status, raw)
	}
}

func TestBatchModelWithoutABatchProviderIsARefusal(t *testing.T) {
	h, ledger := batchHarness(t)
	useVendor(t, h, func(w http.ResponseWriter, r *http.Request) {
		writeVendor(w, http.StatusNotFound, `{"error":{"code":404,"message":"only has :batch endpoints that require your own provider key"}}`)
	})
	status, raw := h.passthrough(t, http.MethodPost, "/v1/batches", testToken, batchBody)
	if status != http.StatusBadRequest || errCode(t, raw) != "batch_model_unavailable" {
		t.Fatalf("no batch provider: %d %s", status, raw)
	}
	if _, _, _, releases := ledger.counts(); len(releases) != 1 || releases[0]["reason"] != "NOT_CREATED" {
		t.Fatalf("releases: %v", releases)
	}
}

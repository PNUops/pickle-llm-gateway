package server

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pnuops/pickle-llm-gateway/internal/config"
	"github.com/pnuops/pickle-llm-gateway/internal/ownership"
	"github.com/pnuops/pickle-llm-gateway/internal/snapshot"
	"github.com/pnuops/pickle-llm-gateway/internal/spool"
)

// fakeLedger stands in for the api's /internal/llm/objects calls.
type fakeLedger struct {
	mu       sync.Mutex
	owned    map[string]bool // keyId + "/" + kind + "/" + objectId
	intents  int
	binds    int
	lookups  int
	releases []map[string]any
	fail     map[string]int // path suffix -> status to answer
}

func newFakeLedger(t *testing.T, h *harness) *fakeLedger {
	t.Helper()
	l := &fakeLedger{owned: map[string]bool{}, fail: map[string]int{}}
	srv := httptest.NewServer(http.HandlerFunc(l.handle))
	t.Cleanup(srv.Close)
	h.srv.SetOwnership(ownership.New(srv.URL, "ledger-token", ownership.Options{
		Timeout:    time.Second,
		BindBudget: 300 * time.Millisecond,
		BindDelays: []time.Duration{10 * time.Millisecond},
	}))
	return l
}

func (l *fakeLedger) own(keyID, kind, id string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.owned[keyID+"/"+kind+"/"+id] = true
}

func (l *fakeLedger) owns(keyID, kind, id string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.owned[keyID+"/"+kind+"/"+id]
}

func (l *fakeLedger) setFail(path string, status int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.fail[path] = status
}

func (l *fakeLedger) counts() (intents, binds, lookups int, releases []map[string]any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.intents, l.binds, l.lookups, append([]map[string]any(nil), l.releases...)
}

func (l *fakeLedger) handle(w http.ResponseWriter, r *http.Request) {
	raw, _ := io.ReadAll(r.Body)
	var body map[string]any
	_ = json.Unmarshal(raw, &body)
	path := strings.TrimPrefix(r.URL.Path, "/internal/llm/objects/")
	l.mu.Lock()
	defer l.mu.Unlock()
	switch path {
	case "intents":
		l.intents++
	case "bind":
		l.binds++
	case "lookup":
		l.lookups++
	case "release":
		l.releases = append(l.releases, body)
	}
	if status := l.fail[path]; status != 0 {
		w.WriteHeader(status)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	key, _ := body["keyId"].(string)
	kind, _ := body["kind"].(string)
	switch path {
	case "intents":
		w.WriteHeader(http.StatusCreated)
		_, _ = io.WriteString(w, `{"intentId":"11111111-2222-4333-8444-555555555555"}`)
	case "bind":
		id, _ := body["objectId"].(string)
		l.owned[key+"/"+kind+"/"+id] = true
		_, _ = io.WriteString(w, `{"state":"OWNED"}`)
	case "lookup":
		ids, _ := body["objectIds"].([]any)
		owned := []string{}
		for _, v := range ids {
			if s, _ := v.(string); l.owned[key+"/"+kind+"/"+s] {
				owned = append(owned, s)
			}
		}
		out, _ := json.Marshal(map[string]any{"owned": owned})
		_, _ = w.Write(out)
	case "release":
		if id, _ := body["objectId"].(string); id != "" {
			delete(l.owned, key+"/"+kind+"/"+id)
		}
		_, _ = io.WriteString(w, `{"released":true}`)
	}
}

const batchBody = `{"endpoint":"/v1/chat/completions","model":"openai/gpt-4o-mini",` +
	`"requests":[{"custom_id":"a","body":{"messages":[{"role":"user","content":"hi"}]}}]}`

const batchAccepted = `{"id":"batch_abc","object":"batch","status":"validating"}`

func batchHarness(t *testing.T, endpoints ...string) (*harness, *fakeLedger) {
	t.Helper()
	h := newHarness(t, passthroughDoc(append([]string{snapshot.EndpointBatches}, endpoints...)...), nil)
	return h, newFakeLedger(t, h)
}

func acceptBatches(h *harness) {
	h.mock.set(func(o *mockOpts) { o.status = http.StatusAccepted; o.errBody = batchAccepted })
}

func TestBatchSubmitRecordsTheOwnerBeforeAnswering(t *testing.T) {
	h, ledger := batchHarness(t)
	acceptBatches(h)

	status, body := h.passthrough(t, http.MethodPost, "/v1/batches", testToken, batchBody)
	if status != http.StatusAccepted || !strings.Contains(string(body), "batch_abc") {
		t.Fatalf("submit: %d %s", status, body)
	}
	path, method, _, raw := h.mock.lastRequest()
	if path != "/batches" || method != http.MethodPost || !strings.Contains(string(raw), `"custom_id":"a"`) {
		t.Fatalf("upstream saw %s %s %s", method, path, raw)
	}
	if !ledger.owns("k-test", "BATCH", "batch_abc") {
		t.Fatal("the batch was not bound to the key")
	}
	ev := lastEvent(t, h)
	if ev.Endpoint != spool.EndpointBatches || ev.Object == nil || ev.Object.ObjectID != "batch_abc" ||
		ev.Object.IntentID == "" || ev.Object.Kind != "BATCH" || ev.Object.Compensated {
		t.Fatalf("event: %+v object %+v", ev, ev.Object)
	}
	expires, err := time.Parse(time.RFC3339, ev.Object.ExpiresAt)
	if err != nil || expires.Before(time.Now().Add(29*24*time.Hour)) {
		t.Fatalf("expiry is the end of retention, got %q", ev.Object.ExpiresAt)
	}
}

func TestBatchNeedsItsOwnGrantAndTheTargetsGrant(t *testing.T) {
	h := newHarness(t, passthroughDoc(), nil)
	ledger := newFakeLedger(t, h)
	status, body := h.passthrough(t, http.MethodPost, "/v1/batches", testToken, batchBody)
	if status != http.StatusForbidden || errCode(t, body) != "endpoint_not_allowed" {
		t.Fatalf("without batches: %d %s", status, body)
	}

	h, ledger = batchHarness(t)
	embed := `{"endpoint":"/v1/embeddings","model":"openai/text-embedding-3-small",` +
		`"requests":[{"custom_id":"a","body":{"input":"hi"}}]}`
	status, body = h.passthrough(t, http.MethodPost, "/v1/batches", testToken, embed)
	if status != http.StatusForbidden || !strings.Contains(errMessage(t, body), "embeddings") {
		t.Fatalf("embeddings batch without embeddings: %d %s", status, body)
	}
	if intents, _, _, _ := ledger.counts(); intents != 0 || h.mock.callCount() != 0 {
		t.Fatal("a refused batch reached the ledger or the vendor")
	}
}

func TestBatchRefusesWhatItCannotServe(t *testing.T) {
	h, ledger := batchHarness(t)
	acceptBatches(h)
	cases := map[string]string{
		"callback": `{"endpoint":"/v1/chat/completions","model":"openai/gpt-4o-mini","callback_url":"https://example.com/x",` +
			`"requests":[{"custom_id":"a","body":{"messages":[{"role":"user","content":"hi"}]}}]}`,
		"other model": `{"endpoint":"/v1/chat/completions","model":"openai/gpt-4o-mini",` +
			`"requests":[{"custom_id":"a","body":{"model":"openai/gpt-5","messages":[{"role":"user","content":"hi"}]}}]}`,
		"stream": `{"endpoint":"/v1/chat/completions","model":"openai/gpt-4o-mini",` +
			`"requests":[{"custom_id":"a","body":{"stream":true,"messages":[{"role":"user","content":"hi"}]}}]}`,
		"unknown endpoint": `{"endpoint":"/v1/images","model":"openai/gpt-image-1",` +
			`"requests":[{"custom_id":"a","body":{"prompt":"x"}}]}`,
		"no requests": `{"endpoint":"/v1/chat/completions","model":"openai/gpt-4o-mini","requests":[]}`,
		"stateful responses": `{"endpoint":"/v1/responses","model":"openai/gpt-4o-mini",` +
			`"requests":[{"custom_id":"a","body":{"input":"hi","store":true}}]}`,
	}
	for name, body := range cases {
		status, out := h.passthrough(t, http.MethodPost, "/v1/batches", testToken, body)
		if status != http.StatusBadRequest {
			t.Errorf("%s: %d %s", name, status, out)
		}
	}
	if intents, _, _, _ := ledger.counts(); intents != 0 || h.mock.callCount() != 0 {
		t.Fatal("a refused batch reached the ledger or the vendor")
	}
}

// A body inside a batch is fenced as it would be on its own route.
func TestBatchBodiesGoThroughTheirRoutesFences(t *testing.T) {
	h, _ := batchHarness(t)
	acceptBatches(h)
	web := `{"endpoint":"/v1/chat/completions","model":"openai/gpt-4o-mini",` +
		`"requests":[{"custom_id":"a","body":{"plugins":[{"id":"web"}],"messages":[{"role":"user","content":"hi"}]}}]}`
	status, body := h.passthrough(t, http.MethodPost, "/v1/batches", testToken, web)
	if status != http.StatusForbidden || errCode(t, body) != "endpoint_not_allowed" {
		t.Fatalf("web search inside a batch without the grant: %d %s", status, body)
	}
	if h.mock.callCount() != 0 {
		t.Fatal("the vendor was called")
	}
}

func TestBatchSubmitStopsWhenTheLedgerCannotRecord(t *testing.T) {
	h, ledger := batchHarness(t)
	acceptBatches(h)
	ledger.setFail("intents", http.StatusInternalServerError)
	status, body := h.passthrough(t, http.MethodPost, "/v1/batches", testToken, batchBody)
	if status != http.StatusServiceUnavailable || errCode(t, body) != "ownership_unavailable" {
		t.Fatalf("intent failure: %d %s", status, body)
	}
	if h.mock.callCount() != 0 {
		t.Fatal("the vendor was called without an intent")
	}
}

func TestBatchVendorRefusalReleasesTheIntent(t *testing.T) {
	h, ledger := batchHarness(t)
	h.mock.set(func(o *mockOpts) {
		o.status = http.StatusBadRequest
		o.errBody = `{"error":{"message":"bad batch"}}`
	})
	status, _ := h.passthrough(t, http.MethodPost, "/v1/batches", testToken, batchBody)
	if status != http.StatusBadRequest {
		t.Fatalf("vendor refusal: %d", status)
	}
	_, _, _, releases := ledger.counts()
	if len(releases) != 1 || releases[0]["reason"] != "NOT_CREATED" || releases[0]["intentId"] == nil {
		t.Fatalf("releases: %v", releases)
	}
}

// A batch cannot be cancelled, so a bind that does not land answers success:
// a 5xx would make an SDK submit, and pay for, the batch again.
func TestBatchBindFailureAnswersSuccessAndBindsLate(t *testing.T) {
	h, ledger := batchHarness(t)
	acceptBatches(h)
	ledger.setFail("bind", http.StatusInternalServerError)
	status, body := h.passthrough(t, http.MethodPost, "/v1/batches", testToken, batchBody)
	if status != http.StatusAccepted {
		t.Fatalf("bind failure: %d %s", status, body)
	}
	ev := lastEvent(t, h)
	if ev.Object == nil || ev.Object.ObjectID != "batch_abc" {
		t.Fatalf("no late bind on the event: %+v", ev)
	}
	// This instance remembers the owner, so the first poll works even with the
	// ledger still failing.
	ledger.setFail("lookup", http.StatusInternalServerError)
	h.mock.set(func(o *mockOpts) { o.status = 0; o.rawResp = `{"id":"batch_abc","status":"in_progress"}` })
	status, body = h.passthrough(t, http.MethodGet, "/v1/batches/batch_abc", testToken, "")
	if status != http.StatusOK {
		t.Fatalf("poll after a late bind: %d %s", status, body)
	}
}

func TestBatchReadIsTheOwnersAlone(t *testing.T) {
	h, ledger := batchHarness(t)
	ledger.own("k-other", "BATCH", "batch_theirs")
	status, body := h.passthrough(t, http.MethodGet, "/v1/batches/batch_theirs", testToken, "")
	if status != http.StatusNotFound || errCode(t, body) != "object_not_found" {
		t.Fatalf("another key's batch: %d %s", status, body)
	}
	status, body = h.passthrough(t, http.MethodGet, "/v1/batches/batch_nobody", testToken, "")
	if status != http.StatusNotFound || errCode(t, body) != "object_not_found" {
		t.Fatalf("no batch: %d %s", status, body)
	}
	if h.mock.callCount() != 0 {
		t.Fatal("the vendor was asked about a batch this key does not own")
	}
	ledger.setFail("lookup", http.StatusInternalServerError)
	status, body = h.passthrough(t, http.MethodGet, "/v1/batches/batch_nobody", testToken, "")
	if status != http.StatusServiceUnavailable || errCode(t, body) != "ownership_unavailable" {
		t.Fatalf("ledger down: %d %s", status, body)
	}
}

// The price arrives on the batch once it is done and the owner may read it
// many times; every read records the same settlement id.
func TestCompletedBatchPriceIsRecordedUnderOneID(t *testing.T) {
	h, ledger := batchHarness(t)
	ledger.own("k-test", "BATCH", "batch_done")
	h.mock.set(func(o *mockOpts) {
		o.rawResp = `{"id":"batch_done","status":"completed","model":"openai/gpt-4o-mini",` +
			`"usage":{"prompt_tokens":10,"completion_tokens":4,"total_tokens":14,"cost":0.0021},"results":[]}`
	})
	for i := 0; i < 2; i++ {
		if status, body := h.passthrough(t, http.MethodGet, "/v1/batches/batch_done", testToken, ""); status != http.StatusOK {
			t.Fatalf("read: %d %s", status, body)
		}
	}
	var settles []spool.Event
	reads := 0
	for _, ev := range h.spoolEvents(t) {
		switch ev.Endpoint {
		case spool.EndpointBatchCost:
			settles = append(settles, ev)
		case spool.EndpointBatchReads:
			reads++
			if ev.CostUsd != "" {
				t.Fatalf("a read carries the price: %+v", ev)
			}
		}
	}
	if reads != 2 || len(settles) != 2 || settles[0].EventUUID != settles[1].EventUUID {
		t.Fatalf("reads %d settles %+v", reads, settles)
	}
	if settles[0].CostUsd != "0.0021" || settles[0].InputTokens != 10 || settles[0].OutputTokens != 4 ||
		settles[0].PublicModelName != "openai/gpt-4o-mini" || len(settles[0].EventUUID) > 64 {
		t.Fatalf("settlement: %+v", settles[0])
	}
}

func TestBatchTheVendorNoLongerHasIsReleased(t *testing.T) {
	h, ledger := batchHarness(t)
	ledger.own("k-test", "BATCH", "batch_old")
	h.mock.set(func(o *mockOpts) { o.status = http.StatusGone; o.errBody = `{"error":{"message":"gone"}}` })
	status, body := h.passthrough(t, http.MethodGet, "/v1/batches/batch_old", testToken, "")
	if status != http.StatusNotFound || errCode(t, body) != "object_not_found" {
		t.Fatalf("expired batch: %d %s", status, body)
	}
	_, _, _, releases := ledger.counts()
	if len(releases) != 1 || releases[0]["reason"] != "VENDOR_GONE" || releases[0]["objectId"] != "batch_old" {
		t.Fatalf("releases: %v", releases)
	}
}

func TestBatchDelete(t *testing.T) {
	h, ledger := batchHarness(t)
	ledger.own("k-test", "BATCH", "batch_run")
	h.mock.set(func(o *mockOpts) { o.status = http.StatusConflict; o.errBody = `{"error":{"message":"running"}}` })
	status, body := h.passthrough(t, http.MethodDelete, "/v1/batches/batch_run", testToken, "")
	if status != http.StatusConflict || errCode(t, body) != "batch_not_finished" {
		t.Fatalf("running batch: %d %s", status, body)
	}
	if !ledger.owns("k-test", "BATCH", "batch_run") {
		t.Fatal("a refused delete released the batch")
	}
	h.mock.set(func(o *mockOpts) { o.status = 0; o.rawResp = `{"id":"batch_run","deleted":true}` })
	status, body = h.passthrough(t, http.MethodDelete, "/v1/batches/batch_run", testToken, "")
	if status != http.StatusOK {
		t.Fatalf("delete: %d %s", status, body)
	}
	_, method, _, _ := h.mock.lastRequest()
	if method != http.MethodDelete || ledger.owns("k-test", "BATCH", "batch_run") {
		t.Fatal("the deleted batch is still owned")
	}
}

// The vendor lists the whole workspace; the answer holds only this key's
// batches, and no id of another key appears anywhere in it.
func TestBatchListShowsOnlyTheKeysBatches(t *testing.T) {
	h, ledger := batchHarness(t)
	ledger.own("k-test", "BATCH", "batch_mine1")
	ledger.own("k-test", "BATCH", "batch_mine2")
	ledger.own("k-other", "BATCH", "batch_theirs")
	var seen []string
	var mu sync.Mutex
	vendor := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen = append(seen, r.URL.RawQuery)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Query().Get("after") == "" {
			_, _ = io.WriteString(w, `{"data":[{"id":"batch_mine1"},{"id":"batch_theirs"}],"has_more":true}`)
			return
		}
		_, _ = io.WriteString(w, `{"data":[{"id":"batch_mine2"}],"has_more":false}`)
	}))
	t.Cleanup(vendor.Close)
	h.srv.cfg.Upstreams["mock"] = config.Upstream{Ref: "mock", BaseURL: vendor.URL, APIKey: upstreamCred}

	status, body := h.passthrough(t, http.MethodGet, "/v1/batches?limit=5&foo=bar", testToken, "")
	if status != http.StatusOK {
		t.Fatalf("list: %d %s", status, body)
	}
	if strings.Contains(string(body), "batch_theirs") {
		t.Fatalf("another key's batch leaked: %s", body)
	}
	var out struct {
		Data    []struct{ ID string } `json:"data"`
		LastID  string                `json:"last_id"`
		HasMore bool                  `json:"has_more"`
	}
	if err := json.Unmarshal(body, &out); err != nil || len(out.Data) != 2 ||
		out.Data[0].ID != "batch_mine1" || out.Data[1].ID != "batch_mine2" || out.LastID != "batch_mine2" || out.HasMore {
		t.Fatalf("list: %s", body)
	}
	for _, q := range seen {
		if strings.Contains(q, "foo") {
			t.Fatalf("an unknown parameter was forwarded: %s", q)
		}
	}

	status, body = h.passthrough(t, http.MethodGet, "/v1/batches?after=batch_theirs", testToken, "")
	if status != http.StatusBadRequest {
		t.Fatalf("another key's cursor: %d %s", status, body)
	}
}

func TestBatchRoutesNeedTheLedger(t *testing.T) {
	h := newHarness(t, passthroughDoc(snapshot.EndpointBatches), nil)
	status, body := h.passthrough(t, http.MethodPost, "/v1/batches", testToken, batchBody)
	if status != http.StatusServiceUnavailable || errCode(t, body) != "ownership_unavailable" {
		t.Fatalf("no ledger client: %d %s", status, body)
	}
	status, body = h.passthrough(t, http.MethodPost, "/v1/batches/batch_x/cancel", testToken, "")
	if status != http.StatusNotFound || errCode(t, body) != "unknown_endpoint" {
		t.Fatalf("a path under a batch: %d %s", status, body)
	}
}

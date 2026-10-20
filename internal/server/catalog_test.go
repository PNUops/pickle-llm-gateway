package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pnuops/pickle-llm-gateway/internal/config"
	"github.com/pnuops/pickle-llm-gateway/internal/snapshot"
)

// vendorCatalogBody is a small catalog in the vendor's shape, one model per
// case the list has to tell apart.
const vendorCatalogBody = `{"data":[
 {"id":"openai/gpt-x","name":"OpenAI: GPT X","created":1790000000,"context_length":128000,
  "top_provider":{"max_completion_tokens":16384},"architecture":{"output_modalities":["text"]},
  "pricing":{"prompt":"0.000001"},"supported_parameters":["tools"]},
 {"id":"google/gem-image","name":"Google: Gem Image","created":1790000001,"context_length":32000,
  "top_provider":{"max_completion_tokens":null},"architecture":{"output_modalities":["image","text"]}},
 {"id":"black/flux","name":"Flux","created":1790000002,"context_length":0,
  "architecture":{"output_modalities":["image"]}},
 {"id":"openai/embed-3","name":"Embed 3","created":1790000003,"context_length":8192,
  "architecture":{"output_modalities":["embeddings"]}},
 {"id":"cohere/rerank-x","name":"Rerank X","created":1790000004,"context_length":4096,
  "architecture":{"output_modalities":["rerank"]}},
 {"id":"eleven/tts","name":"TTS","created":1790000005,"context_length":4096,
  "architecture":{"output_modalities":["speech"]}},
 {"id":"openai/whisper","name":"Whisper","created":1790000006,"context_length":4096,
  "architecture":{"output_modalities":["transcription"]}},
 {"id":"vid/maker","name":"Video","created":1790000007,"architecture":{"output_modalities":["video"]}},
 {"id":"meta/llama:free","name":"Free","created":1790000008,"context_length":8192,
  "architecture":{"output_modalities":["text"]}},
 {"id":"openrouter/auto","name":"Auto","created":1790000009,"context_length":2000000,
  "architecture":{"output_modalities":["text"]}},
 {"id":"vendor-model","name":"Catalogued","created":1790000010,"context_length":1000,
  "architecture":{"output_modalities":["text"]}},
 {"id":"pickle-general","name":"Reserved","created":1790000011,"context_length":1000,
  "architecture":{"output_modalities":["text"]}},
 {"id":"","name":"No id","architecture":{"output_modalities":["text"]}}
]}`

type catalogVendor struct {
	mu     sync.Mutex
	status int
	body   string
	calls  int
	query  string
	auth   string
}

func (v *catalogVendor) handler(w http.ResponseWriter, r *http.Request) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.calls++
	v.query = r.URL.RequestURI()
	v.auth = r.Header.Get("Authorization")
	if v.status != 0 && v.status != 200 {
		w.WriteHeader(v.status)
		return
	}
	_, _ = w.Write([]byte(v.body))
}

func (v *catalogVendor) set(status int, body string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.status, v.body = status, body
}

// catalogHarness is the ordinary harness with a second upstream, "vendor",
// serving as the passthrough upstream and answering the catalog read.
func catalogHarness(t *testing.T, mutateDoc func(*snapshot.Document)) (*harness, *catalogVendor) {
	t.Helper()
	vendor := &catalogVendor{body: vendorCatalogBody}
	srv := httptest.NewServer(http.HandlerFunc(vendor.handler))
	t.Cleanup(srv.Close)
	h := newHarness(t, func(d *snapshot.Document) {
		creditDoc(d)
		d.PassthroughRef = "vendor"
		d.Keys[0].UpstreamCredentials["vendor"] = keyCred
		if mutateDoc != nil {
			mutateDoc(d)
		}
	}, func(c *config.Config) {
		c.Upstreams["vendor"] = config.Upstream{Ref: "vendor", BaseURL: srv.URL, APIKey: "env-key", CapField: "max_completion_tokens"}
	})
	return h, vendor
}

func refreshed(h *harness) bool {
	return h.srv.refreshVendorCatalog(context.Background()) == catalogRead
}

func listIDs(t *testing.T, h *harness, anthropic bool) []string {
	t.Helper()
	status, body := getModels(t, h, "/v1/models", testToken, anthropic)
	if status != 200 {
		t.Fatalf("list: %d %s", status, body)
	}
	var list struct {
		Data []struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &list); err != nil {
		t.Fatal(err)
	}
	ids := make([]string, 0, len(list.Data))
	for _, d := range list.Data {
		ids = append(ids, d.ID)
	}
	return ids
}

func TestCatalogListShowsCallablePaidModels(t *testing.T) {
	h, vendor := catalogHarness(t, nil)
	// Before the first read the list is the snapshot's alone.
	if got := strings.Join(listIDs(t, h, false), ","); got != "pickle-general,vendor-model" {
		t.Fatalf("before the catalog: %s", got)
	}
	if !refreshed(h) {
		t.Fatal("refresh failed")
	}
	if vendor.query != "/models?output_modalities=all" || vendor.auth != "" {
		t.Fatalf("catalog read %q with auth %q", vendor.query, vendor.auth)
	}
	// chat only: text models, including a text-and-image one. Not the
	// image-only, embedding, rerank, speech, transcription or video models,
	// not the free variant, not a router, and not the two names the snapshot
	// governs (the catalogued one is listed once, by its own entry).
	want := "pickle-general,vendor-model,google/gem-image,openai/gpt-x"
	if got := strings.Join(listIDs(t, h, false), ","); got != want {
		t.Fatalf("chat key: %s, want %s", got, want)
	}
	if got := strings.Join(listIDs(t, h, true), ","); got != want {
		t.Fatalf("anthropic shape: %s, want %s", got, want)
	}
}

func TestCatalogListFollowsGrants(t *testing.T) {
	h, _ := catalogHarness(t, func(d *snapshot.Document) {
		d.Keys[0].PassthroughEndpoints = []string{snapshot.EndpointImages, snapshot.EndpointEmbeddings,
			"rerank", "speech", "transcriptions"}
	})
	refreshed(h)
	want := "pickle-general,vendor-model,black/flux,cohere/rerank-x,eleven/tts,google/gem-image,openai/embed-3,openai/whisper"
	if got := strings.Join(listIDs(t, h, false), ","); got != want {
		t.Fatalf("got %s, want %s", got, want)
	}
}

// The list and the retrieve share one judgement: the credit fence and the
// credential decide both.
func TestCatalogListFollowsCreditFence(t *testing.T) {
	h, _ := catalogHarness(t, func(d *snapshot.Document) {
		d.Keys[0].CreditDeniedModels = []string{"openai/*"}
	})
	refreshed(h)
	if got := strings.Join(listIDs(t, h, false), ","); got != "pickle-general,vendor-model,google/gem-image" {
		t.Fatalf("denied vendor still listed: %s", got)
	}
	if status, _ := getModels(t, h, "/v1/models/openai/gpt-x", testToken, false); status != 404 {
		t.Fatalf("retrieve of a denied model: %d", status)
	}

	h, _ = catalogHarness(t, func(d *snapshot.Document) {
		delete(d.Keys[0].UpstreamCredentials, "vendor")
	})
	refreshed(h)
	if got := strings.Join(listIDs(t, h, false), ","); got != "pickle-general,vendor-model" {
		t.Fatalf("key without a passthrough credential: %s", got)
	}
}

func TestCatalogEntryShapes(t *testing.T) {
	h, _ := catalogHarness(t, nil)
	refreshed(h)

	status, body := getModels(t, h, "/v1/models", testToken, false)
	var list struct {
		Data []map[string]any `json:"data"`
	}
	if status != 200 || json.Unmarshal(body, &list) != nil {
		t.Fatalf("%d %s", status, body)
	}
	byID := map[string]map[string]any{}
	for _, d := range list.Data {
		byID[d["id"].(string)] = d
	}
	gpt := byID["openai/gpt-x"]
	if len(gpt) != 5 || gpt["object"] != "model" || gpt["created"] != float64(1790000000) ||
		gpt["owned_by"] != "openai" || gpt["context_length"] != float64(128000) {
		t.Fatalf("openai entry: %v", gpt)
	}
	// The self-served model keeps its old four fields.
	if own := byID["pickle-general"]; len(own) != 4 || own["owned_by"] != "pnu-cloud" {
		t.Fatalf("self-served entry: %v", own)
	}

	status, body = getModels(t, h, "/v1/models", testToken, true)
	list.Data = nil
	if status != 200 || json.Unmarshal(body, &list) != nil {
		t.Fatalf("%d %s", status, body)
	}
	byID = map[string]map[string]any{}
	for _, d := range list.Data {
		byID[d["id"].(string)] = d
	}
	gpt = byID["openai/gpt-x"]
	if len(gpt) != 6 || gpt["type"] != "model" || gpt["display_name"] != "OpenAI: GPT X" ||
		gpt["created_at"] != time.Unix(1790000000, 0).UTC().Format(time.RFC3339) ||
		gpt["max_input_tokens"] != float64(128000) || gpt["max_tokens"] != float64(16384) {
		t.Fatalf("anthropic entry: %v", gpt)
	}
	// An output cap the catalog does not give is left out, not zero.
	if gem := byID["google/gem-image"]; len(gem) != 5 || gem["max_tokens"] != nil {
		t.Fatalf("entry without an output cap: %v", gem)
	}
	if own := byID["pickle-general"]; len(own) != 4 {
		t.Fatalf("self-served anthropic entry: %v", own)
	}

	// The retrieve describes a catalog model the way the list does, and
	// keeps the plain entry for a name the catalog does not carry.
	status, body = getModels(t, h, "/v1/models/openai/gpt-x", testToken, false)
	var one map[string]any
	if status != 200 || json.Unmarshal(body, &one) != nil || one["context_length"] != float64(128000) {
		t.Fatalf("retrieve: %d %s", status, body)
	}
	status, body = getModels(t, h, "/v1/models/openai/not-in-catalog", testToken, false)
	one = nil
	if status != 200 || json.Unmarshal(body, &one) != nil || len(one) != 4 || one["owned_by"] != "pnu-cloud" {
		t.Fatalf("retrieve outside the catalog: %d %s", status, body)
	}
}

// A failed read keeps the last good copy; an empty or unreadable catalog is
// a failure, not a fact.
func TestCatalogFailureKeepsLastCopy(t *testing.T) {
	h, vendor := catalogHarness(t, nil)
	if !refreshed(h) {
		t.Fatal("first refresh failed")
	}
	before := strings.Join(listIDs(t, h, false), ",")
	for _, c := range []struct {
		status int
		body   string
	}{{500, ""}, {200, `{"data":[]}`}, {200, `{"data":`}, {200, `<html>`}} {
		vendor.set(c.status, c.body)
		if refreshed(h) {
			t.Fatalf("%d %q counted as a success", c.status, c.body)
		}
		if got := strings.Join(listIDs(t, h, false), ","); got != before {
			t.Fatalf("%d %q changed the list: %s", c.status, c.body, got)
		}
	}
	if n := h.srv.catalog.failures.Load(); n != 4 {
		t.Fatalf("consecutive failures = %d", n)
	}
	vendor.set(200, vendorCatalogBody)
	if !refreshed(h) || h.srv.catalog.failures.Load() != 0 {
		t.Fatal("recovery")
	}
}

// A copy read from one upstream is not shown once passthrough moves.
func TestCatalogIgnoredForAnotherUpstream(t *testing.T) {
	h, _ := catalogHarness(t, nil)
	refreshed(h)
	moved := *h.srv.catalog.current.Load()
	moved.ref = "elsewhere"
	h.srv.catalog.current.Store(&moved)
	if got := strings.Join(listIDs(t, h, false), ","); got != "pickle-general,vendor-model" {
		t.Fatalf("stale upstream's catalog listed: %s", got)
	}
}

func TestCatalogLoopRetriesThenSettles(t *testing.T) {
	h, vendor := catalogHarness(t, nil)
	vendor.set(503, "")
	h.srv.catalog.retry = 10 * time.Millisecond
	h.srv.catalog.interval = time.Hour
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	h.srv.RunVendorCatalog(ctx)
	deadline := time.Now().Add(5 * time.Second)
	for h.srv.catalog.failures.Load() < 2 {
		if time.Now().After(deadline) {
			t.Fatal("no retry after a failure")
		}
		time.Sleep(5 * time.Millisecond)
	}
	vendor.set(200, vendorCatalogBody)
	for h.srv.catalog.current.Load() == nil {
		if time.Now().After(deadline) {
			t.Fatal("never recovered")
		}
		time.Sleep(5 * time.Millisecond)
	}
	vendor.mu.Lock()
	calls := vendor.calls
	vendor.mu.Unlock()
	time.Sleep(100 * time.Millisecond)
	vendor.mu.Lock()
	defer vendor.mu.Unlock()
	if vendor.calls != calls {
		t.Fatalf("read again before the interval: %d then %d", calls, vendor.calls)
	}
}

// The vendor's own lookup routes are not supported, and each answers as an
// unknown endpoint, before authentication, whichever key asks.
func TestVendorLookupRoutesAreUnknown(t *testing.T) {
	h, _ := catalogHarness(t, nil)
	for _, path := range []string{"/v1/models/openai/gpt-x/endpoints", "/v1/models/openai/gpt-x/endpoints/",
		"/v1/models/OpenAI/GPT-X/ENDPOINTS", "/v1/embeddings/models", "/v1/tools",
		"/v1/tools/openrouter:web_search"} {
		for _, token := range []string{testToken, ""} {
			status, body := getModels(t, h, path, token, false)
			if status != 404 || errCode(t, body) != "unknown_endpoint" {
				t.Fatalf("%s (token %q): %d %s", path, token, status, body)
			}
		}
	}
}

func TestCatalogTooLargeIsAFailure(t *testing.T) {
	h, _ := catalogHarness(t, nil)
	h.srv.catalog.maxBytes = int64(len(vendorCatalogBody) - 10)
	if refreshed(h) || h.srv.catalog.current.Load() != nil {
		t.Fatal("an oversized catalog was accepted")
	}
	h.srv.catalog.maxBytes = int64(len(vendorCatalogBody))
	if !refreshed(h) {
		t.Fatal("a catalog exactly at the bound was refused")
	}
}

// A catalog number written as a float does not fail the read.
func TestCatalogReadsFloatNumbers(t *testing.T) {
	h, vendor := catalogHarness(t, nil)
	vendor.set(200, `{"data":[{"id":"openai/gpt-x","created":1.79e9,"context_length":1.28e5,`+
		`"top_provider":{"max_completion_tokens":-1},"architecture":{"output_modalities":["text"]}}]}`)
	if !refreshed(h) {
		t.Fatal("refresh")
	}
	v := h.srv.catalog.current.Load().byID["openai/gpt-x"]
	if v.Created != 1790000000 || v.ContextLength != 128000 || v.MaxOutput != 0 {
		t.Fatalf("%+v", v)
	}
}

// The list never shows a paid model that the retrieve would refuse: a key
// with a model allow list, and a catalog name under the self-serve prefix.
func TestCatalogListHidesWhatRetrieveRefuses(t *testing.T) {
	h, vendor := catalogHarness(t, func(d *snapshot.Document) {
		d.Keys[0].AllowedModels = []string{"pickle-general"}
	})
	refreshed(h)
	if got := strings.Join(listIDs(t, h, false), ","); got != "pickle-general" {
		t.Fatalf("allow-listed key: %s", got)
	}
	if status, _ := getModels(t, h, "/v1/models/openai/gpt-x", testToken, false); status != 404 {
		t.Fatalf("retrieve on an allow-listed key: %d", status)
	}

	h, vendor = catalogHarness(t, nil)
	vendor.set(200, `{"data":[{"id":"pickle-other","architecture":{"output_modalities":["text"]}},`+
		`{"id":"~openai/gpt-latest","architecture":{"output_modalities":["text"]}}]}`)
	refreshed(h)
	status, body := getModels(t, h, "/v1/models", testToken, false)
	var list struct {
		Data []map[string]any `json:"data"`
	}
	if status != 200 || json.Unmarshal(body, &list) != nil || len(list.Data) != 3 ||
		list.Data[2]["id"] != "~openai/gpt-latest" || list.Data[2]["owned_by"] != "openai" {
		t.Fatalf("%d %s", status, body)
	}
}

// A snapshot that moves passthrough gets the new upstream's catalog at the
// next wake, not at the old copy's next refresh.
func TestCatalogLoopFollowsPassthroughRef(t *testing.T) {
	h, _ := catalogHarness(t, nil)
	h.srv.catalog.retry = 10 * time.Millisecond
	h.srv.catalog.interval = time.Hour
	refreshed(h)
	moved := *h.srv.catalog.current.Load()
	moved.ref = "elsewhere"
	h.srv.catalog.current.Store(&moved)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	h.srv.RunVendorCatalog(ctx)
	deadline := time.Now().Add(5 * time.Second)
	for h.srv.catalog.forRef("vendor") == nil {
		if time.Now().After(deadline) {
			t.Fatal("the copy for the current upstream was never read")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

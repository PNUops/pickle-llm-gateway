package server

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/pnuops/pickle-llm-gateway/internal/snapshot"
	"github.com/pnuops/pickle-llm-gateway/internal/spool"
)

const rerankBody = `{"model":"cohere/rerank-v3.5","query":"q","documents":["a","b"],"top_n":1}`

// rerankAnswer is a vendor rerank answer with the given usage member, or
// none when usage is empty.
func rerankAnswer(usage string) string {
	// The vendor names the model in the provider's own spelling here.
	out := `{"id":"gen-rerank-1","model":"rerank-v3.5","provider":"Cohere",` +
		`"results":[{"index":1,"relevance_score":0.9,"document":{"text":"b"}}]`
	if usage != "" {
		out += `,"usage":` + usage
	}
	return out + `}`
}

// The route forwards to the vendor's rerank path under the rerank grant and
// hands the answer back as it came.
func TestRerankForwards(t *testing.T) {
	h := newHarness(t, passthroughDoc(snapshot.EndpointRerank), nil)
	answer := rerankAnswer(`{"total_tokens":150,"search_units":1,"cost":0.002}`)
	h.mock.set(func(o *mockOpts) { o.rawResp = answer })
	status, body := h.passthrough(t, http.MethodPost, "/v1/rerank", testToken, rerankBody)
	if status != 200 || string(body) != answer {
		t.Fatalf("%d %s", status, body)
	}
	path, method, _, _ := h.mock.lastRequest()
	if path != "/rerank" || method != http.MethodPost {
		t.Fatalf("upstream %s %s", method, path)
	}
	sent, auth := h.mock.last()
	if string(sent["model"]) != `"cohere/rerank-v3.5"` || string(sent["top_n"]) != "1" || auth != "Bearer "+upstreamCred {
		t.Fatalf("forwarded %v with %q", sent, auth)
	}
	ev := lastEvent(t, h)
	if ev.Endpoint != spool.EndpointRerank || ev.Status != spool.StatusOK || ev.PublicModelName != "cohere/rerank-v3.5" ||
		ev.InputTokens != 150 || ev.OutputTokens != 0 || ev.CostUsd != "0.002" || ev.Estimated || ev.ImageCount != 0 ||
		ev.ServedModelName != "" {
		t.Fatalf("record: %+v", ev)
	}
}

// Without the grant the route is shut, in the words the approval screen uses,
// and nothing reaches the vendor. Another passthrough grant does not open it.
func TestRerankNeedsItsOwnGrant(t *testing.T) {
	h := newHarness(t, passthroughDoc(snapshot.EndpointEmbeddings, snapshot.EndpointImages), nil)
	status, body := h.passthrough(t, http.MethodPost, "/v1/rerank", testToken, rerankBody)
	if status != http.StatusForbidden || !strings.Contains(errMessage(t, body), "리랭크(rerank)") {
		t.Fatalf("%d %s", status, body)
	}
	if h.mock.callCount() != 0 {
		t.Fatal("a refused request reached the upstream")
	}
	if ev := lastEvent(t, h); ev.Endpoint != spool.EndpointRerank || ev.Status != spool.StatusAuthRejected {
		t.Fatalf("record: %+v", ev)
	}
}

// The usage shapes the vendor may send. Usage is optional on this route, so
// an answer without it is a success with no counts and no estimate; a price
// with no tokens is kept as it came.
func TestRerankUsageShapes(t *testing.T) {
	for _, tc := range []struct {
		name, usage string
		in          int
		cost        string
	}{
		{"tokens and cost", `{"total_tokens":150,"cost":0.002}`, 150, "0.002"},
		{"tokens only", `{"total_tokens":150,"search_units":1}`, 150, ""},
		{"search units and cost", `{"search_units":1,"cost":0.002}`, 0, "0.002"},
		{"empty usage", `{}`, 0, ""},
		{"no usage", ``, 0, ""},
	} {
		h, g := generationHarnessFor(t, passthroughDoc(snapshot.EndpointRerank), nil,
			[]time.Duration{time.Millisecond}, generationAnswer{404, `{"error":{"message":"Generation not found","code":404}}`})
		h.mock.set(func(o *mockOpts) { o.rawResp = rerankAnswer(tc.usage) })
		if status, body := h.passthrough(t, http.MethodPost, "/v1/rerank", testToken, rerankBody); status != 200 {
			t.Fatalf("%s: %d %s", tc.name, status, body)
		}
		h.srv.DrainCostLookups(context.Background())
		// Only an answer without a price is looked up.
		if looked := g.readCount() > 0; looked != (tc.cost == "") {
			t.Fatalf("%s: %d reads of the generation record", tc.name, g.readCount())
		}
		ev := lastEvent(t, h)
		if ev.Status != spool.StatusOK || ev.ErrorType != "" || ev.InputTokens != tc.in || ev.OutputTokens != 0 ||
			string(ev.CostUsd) != tc.cost || ev.Estimated {
			t.Fatalf("%s: %+v", tc.name, ev)
		}
	}
}

// An answer without a price has it read from the generation record, and the
// count the vendor did not send is not made up beside it. The vendor names a
// rerank's generation only in the body's id, never in the header.
func TestRerankWithoutUsageReadsTheCost(t *testing.T) {
	h, g := generationHarnessFor(t, passthroughDoc(snapshot.EndpointRerank), nil,
		[]time.Duration{10 * time.Millisecond}, generationAnswer{200, generationRecordOK})
	g.noHeader = true
	h.mock.set(func(o *mockOpts) {
		o.rawResp = strings.Replace(rerankAnswer(""), `"gen-rerank-1"`, `"`+testGenerationID+`"`, 1)
	})
	status, body := h.passthrough(t, http.MethodPost, "/v1/rerank", testToken, rerankBody)
	if status != 200 {
		t.Fatalf("%d %s", status, body)
	}
	h.srv.DrainCostLookups(context.Background())
	ev := lastEvent(t, h)
	if ev.Status != spool.StatusOK || ev.CostUsd != "0.000023" || ev.InputTokens != 0 || ev.Estimated {
		t.Fatalf("record: %+v", ev)
	}
	if g.readCount() != 1 {
		t.Fatalf("reads %d", g.readCount())
	}
}

// The fences every passthrough route runs apply here unchanged: the money
// fence on the model, a stream refused, an unreadable body refused.
func TestRerankFences(t *testing.T) {
	h := newHarness(t, func(d *snapshot.Document) {
		passthroughDoc(snapshot.EndpointRerank)(d)
		d.Keys[0].CreditAllowedModels = []string{"voyageai/*"}
	}, nil)
	for _, tc := range []struct {
		name, body string
		status     int
		code       string
	}{
		{"model outside the key's list", rerankBody, http.StatusForbidden, "model_not_allowed"},
		{"stream", `{"model":"voyageai/rerank-2.5","query":"q","documents":["a"],"stream":true}`,
			http.StatusBadRequest, "streaming_not_supported"},
		{"no model", `{"query":"q","documents":["a"]}`, http.StatusBadRequest, "missing_parameter"},
		{"not json", `{"model":`, http.StatusBadRequest, "invalid_json"},
	} {
		status, body := h.passthrough(t, http.MethodPost, "/v1/rerank", testToken, tc.body)
		if status != tc.status || errCode(t, body) != tc.code {
			t.Fatalf("%s: %d %s", tc.name, status, body)
		}
	}
	if h.mock.callCount() != 0 {
		t.Fatal("a refused request reached the upstream")
	}
	if status, body := h.passthrough(t, http.MethodGet, "/v1/rerank", testToken, ""); status != http.StatusMethodNotAllowed {
		t.Fatalf("GET: %d %s", status, body)
	}
}

// An id that is not a string is ignored rather than failing the answer: the
// vendor has already billed it, and the id only serves the cost lookup.
func TestPassthroughOddIDIsIgnored(t *testing.T) {
	for _, id := range []string{`42`, `{"x":1}`, `null`} {
		h := newHarness(t, passthroughDoc(snapshot.EndpointEmbeddings), nil)
		h.mock.set(func(o *mockOpts) {
			o.rawResp = `{"id":` + id + `,"data":[],"usage":{"prompt_tokens":3,"total_tokens":3,"cost":0.1}}`
		})
		if status, body := h.passthrough(t, http.MethodPost, "/v1/embeddings", testToken, embeddingBody); status != 200 {
			t.Fatalf("id %s: %d %s", id, status, body)
		}
	}
}

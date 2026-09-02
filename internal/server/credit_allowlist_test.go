// The CREDIT-axis model allow list: a per-key money fence that leaves the
// self-serving axis alone. These tests pin the two claims the feature rests on
// — a fenced key keeps its self-serving access, and no spelling of a name gets
// around the fence — plus the fail-closed handling of a list this build cannot
// act on.
package server

import (
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/pnuops/pickle-llm-gateway/internal/snapshot"
)

// passthroughAllowlistDoc is a passthrough-enabled document whose key holds a
// credential and the given money fence.
func passthroughAllowlistDoc(patterns ...string) func(*snapshot.Document) {
	return func(d *snapshot.Document) {
		d.PassthroughRef = "mock"
		d.Keys[0].UpstreamCredentials = map[string]string{"mock": keyCred}
		d.Keys[0].CreditAllowedModels = patterns
	}
}

func chatFor(model string) string {
	return `{"model":"` + model + `","messages":[{"role":"user","content":"hi"}]}`
}

// The claim the whole design rests on: fencing the money axis must not fence
// campus capacity. A key restricted to one vendor still calls pickle-general.
func TestCreditAllowlistDoesNotGovernTokenAxis(t *testing.T) {
	h := newHarness(t, passthroughAllowlistDoc("openai/*"), nil)

	status, body := h.chat(t, testToken, chatBody)
	if status != 200 {
		t.Fatalf("self-serving call under a money fence was refused: %d %s", status, body)
	}
}

func TestCreditAllowlistRefusesModelsOutsideIt(t *testing.T) {
	h := newHarness(t, passthroughAllowlistDoc("openai/gpt-4o-mini"), nil)

	status, body := h.chat(t, testToken, chatFor("openai/gpt-4o-mini"))
	if status != 200 {
		t.Fatalf("listed model was refused: %d %s", status, body)
	}
	status, body = h.chat(t, testToken, chatFor("anthropic/claude-sonnet-4"))
	if status != 403 || errCode(t, body) != "model_not_allowed" {
		t.Fatalf("unlisted model: got %d %s, want 403 model_not_allowed", status, body)
	}
	// The refusal is local. A fence that still pays the vendor is not a fence.
	if h.mock.callCount() != 1 {
		t.Fatalf("upstream was called %d times, want only the allowed call", h.mock.callCount())
	}
	// The two fences share a public code but not their advice: this one points
	// at the console, because /v1/models cannot list a commercial model.
	if !strings.Contains(string(body), "콘솔의 키 상세") {
		t.Fatalf("money-fence refusal did not point at the console: %s", body)
	}
	// Sharing the code means the usage record is the only place they can be
	// counted apart.
	evs := h.spoolEvents(t)
	last := evs[len(evs)-1]
	if last.ErrorType != "credit_model_not_allowed" {
		t.Fatalf("refusal recorded errorType=%q, want credit_model_not_allowed", last.ErrorType)
	}
	if last.BudgetAxis != snapshot.AxisCredit || last.PublicModelName != "anthropic/claude-sonnet-4" {
		t.Fatalf("refusal recorded axis=%q model=%q", last.BudgetAxis, last.PublicModelName)
	}
}

func TestCreditAllowlistVendorPrefix(t *testing.T) {
	h := newHarness(t, passthroughAllowlistDoc("openai/*"), nil)

	for _, tc := range []struct {
		model string
		want  int
	}{
		{"openai/gpt-4o-mini", 200},
		{"openai/o4", 200},
		{"anthropic/claude-sonnet-4", 403},
		// The prefix opens one vendor's models, not the bare vendor segment.
		// The gateway has no idea whether "openai/" names anything, so it is a
		// passthrough candidate like any other name and the fence is what stops
		// it — a refusal, not a 404.
		{"openai/", 403},
		// A vendor whose name merely starts with the pattern is a different
		// vendor. Matching on the slash is what keeps them apart.
		{"openai-mirror/gpt-4o", 403},
	} {
		status, body := h.chat(t, testToken, chatFor(tc.model))
		if status != tc.want {
			t.Fatalf("model %q: got %d %s, want %d", tc.model, status, body, tc.want)
		}
	}
}

// Case is not a way around the fence. The reserved-prefix guard has been
// case-insensitive since it was written for the same reason: a name that
// differs only in case is the same billable model.
func TestCreditAllowlistIsCaseInsensitive(t *testing.T) {
	h := newHarness(t, passthroughAllowlistDoc("openai/*"), nil)

	status, body := h.chat(t, testToken, chatFor("OpenAI/GPT-4o-Mini"))
	if status != 200 {
		t.Fatalf("capitalized form of an allowed model was refused: %d %s", status, body)
	}
	// Lower-casing decides admission only. What reaches the vendor is what the
	// caller sent, because the vendor's name space is not ours to fold.
	params, _ := h.mock.last()
	if got := strings.Trim(string(params["model"]), `"`); got != "OpenAI/GPT-4o-Mini" {
		t.Fatalf("upstream received %q, want the name forwarded unchanged", got)
	}
}

// A pattern stored with capitals is lower-cased when the document loads, so a
// control plane that skipped normalization still gets a working fence rather
// than one that silently matches nothing.
func TestCreditAllowlistPatternsNormalizeAtLoad(t *testing.T) {
	h := newHarness(t, passthroughAllowlistDoc("OpenAI/*"), nil)

	status, body := h.chat(t, testToken, chatFor("openai/gpt-4o-mini"))
	if status != 200 {
		t.Fatalf("pattern stored with capitals matched nothing: %d %s", status, body)
	}
}

// The money fence never widens the curation list. Two fences, both must pass.
func TestCreditAllowlistDoesNotWidenCuration(t *testing.T) {
	h := newHarness(t, func(d *snapshot.Document) {
		passthroughAllowlistDoc("openai/*")(d)
		d.Keys[0].AllowedModels = []string{"pickle-general"}
	}, nil)

	status, body := h.chat(t, testToken, chatFor("openai/gpt-4o-mini"))
	if status != 403 || errCode(t, body) != "model_not_allowed" {
		t.Fatalf("curation list was widened by the money fence: %d %s", status, body)
	}
}

// An empty list is unrestricted, which is the state every key was in before
// this field existed. This is the regression guard for that.
func TestEmptyCreditAllowlistRestrictsNothing(t *testing.T) {
	h := newHarness(t, passthroughAllowlistDoc(), nil)

	status, body := h.chat(t, testToken, chatFor("anthropic/claude-sonnet-4"))
	if status != 200 {
		t.Fatalf("key with no money fence was refused: %d %s", status, body)
	}
}

// The models surface must agree with chat: a fenced-out CREDIT model is absent
// from the list and unknown to the retrieve, and the self-serving model stays.
func TestModelsSurfaceAgreesWithCreditAllowlist(t *testing.T) {
	h := newHarness(t, func(d *snapshot.Document) {
		creditDoc(d)
		d.Keys[0].CreditAllowedModels = []string{"openai/*"}
	}, nil)

	listed := get(t, h.gw.URL+"/v1/models", testToken)
	if strings.Contains(listed, "vendor-model") {
		t.Fatalf("fenced-out credit model was listed: %s", listed)
	}
	if !strings.Contains(listed, "pickle-general") {
		t.Fatalf("self-serving model went missing from the list: %s", listed)
	}
	req, err := http.NewRequest(http.MethodGet, h.gw.URL+"/v1/models/vendor-model", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+testToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("retrieve of a fenced-out model returned %d, want 404", resp.StatusCode)
	}
}

// get performs an authenticated GET and returns the body as a string.
func get(t *testing.T, url, token string) string {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

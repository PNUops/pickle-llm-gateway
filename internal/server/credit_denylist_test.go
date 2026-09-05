// The CREDIT-axis deny list as the caller meets it: same refusal as the allow
// list, over the same two surfaces. What is pinned here is that a denial is
// answered locally, that it wins over an allowance, and that it says no more
// to the caller than the allow list does.
package server

import (
	"net/http"
	"strings"
	"testing"

	"github.com/pnuops/pickle-llm-gateway/internal/snapshot"
)

// passthroughDenyDoc is a passthrough-enabled document whose key holds a
// credential, the given allowance and the given denial.
func passthroughDenyDoc(allowed, denied []string) func(*snapshot.Document) {
	return func(d *snapshot.Document) {
		d.PassthroughRef = "mock"
		d.Keys[0].UpstreamCredentials = map[string]string{"mock": keyCred}
		d.Keys[0].CreditAllowedModels = allowed
		d.Keys[0].CreditDeniedModels = denied
	}
}

// A denial is refused exactly like a model outside the allow list: same status,
// same public code, same advice. Telling the two apart would tell a caller who
// is not the approver which list holds their model, which is the list's
// contents leaking one name at a time.
func TestCreditDenyListRefusesLikeTheAllowList(t *testing.T) {
	h := newHarness(t, passthroughDenyDoc(nil, []string{"openai/o1-pro"}), nil)

	status, body := h.chat(t, testToken, chatFor("openai/o1-pro"))
	if status != 403 || errCode(t, body) != "model_not_allowed" {
		t.Fatalf("denied model: got %d %s, want 403 model_not_allowed", status, body)
	}
	if !strings.Contains(string(body), "콘솔의 키 상세") {
		t.Fatalf("denial did not give the same advice as the allow list: %s", body)
	}
	// The refusal is local. A denial that still pays the vendor is not a denial.
	if h.mock.callCount() != 0 {
		t.Fatalf("upstream was called %d times for a denied model", h.mock.callCount())
	}
	evs := h.spoolEvents(t)
	last := evs[len(evs)-1]
	if last.ErrorType != "credit_model_not_allowed" {
		t.Fatalf("refusal recorded errorType=%q, want credit_model_not_allowed", last.ErrorType)
	}
	// An empty allow list still means unrestricted, so everything the deny list
	// does not name goes through.
	status, body = h.chat(t, testToken, chatFor("openai/gpt-4o-mini"))
	if status != 200 {
		t.Fatalf("model the deny list does not name was refused: %d %s", status, body)
	}
}

// The denial wins over the allowance it sits inside, which is the ordering the
// whole feature turns on: a vendor-wide allowance is how these keys are written,
// and the price outliers are carved back out of it one name at a time.
func TestCreditDenialBeatsAllowanceOverHTTP(t *testing.T) {
	h := newHarness(t, passthroughDenyDoc(
		[]string{"openai/*"}, []string{"openai/gpt-5-pro"}), nil)

	status, body := h.chat(t, testToken, chatFor("openai/gpt-5-pro"))
	if status != 403 {
		t.Fatalf("denied model inside a vendor-wide allowance: got %d %s, want 403", status, body)
	}
	status, body = h.chat(t, testToken, chatFor("openai/gpt-4o"))
	if status != 200 {
		t.Fatalf("the rest of the allowance was closed by one denial: %d %s", status, body)
	}
}

// Variant recognition, at the surface where it costs money. ":batch" and
// ":free" are rates of the same model, so a denial written against the family
// has to hold for them; missing them is how a key keeps reaching the most
// expensive name on the list through a suffix.
func TestCreditDenyListCoversModelVariants(t *testing.T) {
	h := newHarness(t, passthroughDenyDoc(
		[]string{"openai/*"}, []string{"openai/*-pro"}), nil)

	for _, model := range []string{
		"openai/gpt-5-pro", "openai/gpt-5-pro:batch", "openai/gpt-5-pro:free",
	} {
		status, body := h.chat(t, testToken, chatFor(model))
		if status != 403 {
			t.Fatalf("model %q: got %d %s, want 403", model, status, body)
		}
	}
	// The family, not the vendor: a name that does not end the denied way is
	// still the caller's to spend on.
	status, body := h.chat(t, testToken, chatFor("openai/gpt-5-nano"))
	if status != 200 {
		t.Fatalf("a model outside the denied family was refused: %d %s", status, body)
	}
	if h.mock.callCount() != 1 {
		t.Fatalf("upstream was called %d times, want only the allowed call", h.mock.callCount())
	}
}

// The denial is answered before the credential, like the allowance and for the
// same reason: a key that holds no money budget and asks for a denied model
// needs a different model, and "no money budget" would send them to apply for
// something that would not help.
func TestCreditDenialAnsweredBeforeTheCredential(t *testing.T) {
	h := newHarness(t, func(d *snapshot.Document) {
		d.PassthroughRef = "mock"
		d.Keys[0].UpstreamCredentials = nil
		d.Keys[0].CreditDeniedModels = []string{"openai/o1-pro"}
	}, nil)

	status, body := h.chat(t, testToken, chatFor("openai/o1-pro"))
	if status != 403 || errCode(t, body) != "model_not_allowed" {
		t.Fatalf("denied model on a key with no credential: got %d %s, "+
			"want the fence's answer rather than the credential's", status, body)
	}
}

// A fence with two halves has to answer the same on both surfaces, or a caller
// reads the catalogue and gets a model that chat then refuses to serve.
func TestModelsSurfaceAgreesWithCreditDenyList(t *testing.T) {
	h := newHarness(t, func(d *snapshot.Document) {
		creditDoc(d)
		d.Keys[0].CreditDeniedModels = []string{"vendor-model"}
	}, nil)

	listed := get(t, h.gw.URL+"/v1/models", testToken)
	if strings.Contains(listed, "vendor-model") {
		t.Fatalf("denied credit model was listed: %s", listed)
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
		t.Fatalf("retrieve of a denied model returned %d, want 404", resp.StatusCode)
	}
	// Chat and the catalogue give the same answer, which is the point of both
	// surfaces asking one function.
	status, body := h.chat(t, testToken, creditChatBody)
	if status != 403 {
		t.Fatalf("chat served a model the catalogue denies: %d %s", status, body)
	}
}

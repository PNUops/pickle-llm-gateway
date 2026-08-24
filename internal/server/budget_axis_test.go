// Budget axes and per-key upstream credentials: the daily token quota governs
// only TOKEN-axis models, CREDIT-axis models require the key's own upstream
// credential (never the gateway-wide env one), and uncatalogued model names
// pass through to the document's passthrough upstream on the CREDIT axis.
package server

import (
	"strings"
	"testing"

	"github.com/pnuops/pickle-llm-gateway/internal/snapshot"
)

const keyCred = "per-key-upstream-cred"

// creditDoc is defaultDoc plus a CREDIT-axis model on the mock upstream and a
// credential for it on the test key.
func creditDoc(d *snapshot.Document) {
	d.Models = append(d.Models, snapshot.Model{
		PublicName: "vendor-model", UpstreamRef: "mock",
		UpstreamModel: upstreamModel, BudgetAxis: snapshot.AxisCredit,
	})
	d.Keys[0].UpstreamCredentials = map[string]string{"mock": keyCred}
}

const creditChatBody = `{"model":"vendor-model","messages":[{"role":"user","content":"hi"}]}`

func TestQuotaExhaustedGovernsOnlyTokenAxis(t *testing.T) {
	h := newHarness(t, func(d *snapshot.Document) {
		creditDoc(d)
		d.Keys[0].QuotaExhausted = true
	}, nil)

	// TOKEN-axis model (pnu-general has no budgetAxis, which means TOKEN):
	// the exhausted daily quota refuses it.
	status, body := h.chat(t, testToken, chatBody)
	if status != 429 || errCode(t, body) != "quota_exhausted" {
		t.Fatalf("token-axis call under exhausted quota: got %d %s", status, body)
	}
	// CREDIT-axis model: the token quota does not apply; the key's own
	// credential serves the request.
	status, body = h.chat(t, testToken, creditChatBody)
	if status != 200 {
		t.Fatalf("credit-axis call under exhausted token quota was refused: %d %s", status, body)
	}
	if _, auth := h.mock.last(); auth != "Bearer "+keyCred {
		t.Fatalf("credit-axis call carried %q, want the key's own credential", auth)
	}
}

func TestCreditModelRequiresKeyCredential(t *testing.T) {
	h := newHarness(t, func(d *snapshot.Document) {
		creditDoc(d)
		d.Keys[0].UpstreamCredentials = nil
	}, nil)

	status, body := h.chat(t, testToken, creditChatBody)
	if status != 403 || errCode(t, body) != "credit_unavailable" {
		t.Fatalf("credit model without a key credential: got %d %s", status, body)
	}
	// The refusal must be local: the env credential exists on the mock
	// upstream block and must never be spent on a key that has no budget.
	if h.mock.callCount() != 0 {
		t.Fatalf("the upstream was called %d times for a key with no credential", h.mock.callCount())
	}
}

func TestCreditModelNeverFallsBackToEnvCredential(t *testing.T) {
	h := newHarness(t, creditDoc, nil)
	status, _ := h.chat(t, testToken, creditChatBody)
	if status != 200 {
		t.Fatalf("credit call failed: %d", status)
	}
	if _, auth := h.mock.last(); auth == "Bearer "+upstreamCred {
		t.Fatal("a credit-axis request went out on the gateway-wide env credential")
	}
	// The TOKEN-axis model still uses the env credential as before.
	if status, _ := h.chat(t, testToken, chatBody); status != 200 {
		t.Fatalf("token call failed: %d", status)
	}
	if _, auth := h.mock.last(); auth != "Bearer "+upstreamCred {
		t.Fatalf("token-axis call carried %q, want the env credential", auth)
	}
}

func TestPassthroughRoutesUnknownModels(t *testing.T) {
	h := newHarness(t, func(d *snapshot.Document) {
		d.PassthroughRef = "mock"
		d.Keys[0].UpstreamCredentials = map[string]string{"mock": keyCred}
	}, nil)

	body := `{"model":"vendor/some-model","messages":[{"role":"user","content":"hi"}]}`
	status, out := h.chat(t, testToken, body)
	if status != 200 {
		t.Fatalf("passthrough call failed: %d %s", status, out)
	}
	params, auth := h.mock.last()
	if got := strings.Trim(string(params["model"]), `"`); got != "vendor/some-model" {
		t.Fatalf("upstream received model %q, want the name forwarded as-is", got)
	}
	if auth != "Bearer "+keyCred {
		t.Fatalf("passthrough call carried %q, want the key's credential", auth)
	}
	// The accounting names the requested model, so per-model spend stays
	// attributable even though the catalog never listed it.
	evs := h.spoolEvents(t)
	last := evs[len(evs)-1]
	if last.PublicModelName != "vendor/some-model" || last.Status != "OK" {
		t.Fatalf("event recorded %q/%q", last.PublicModelName, last.Status)
	}
}

func TestPassthroughRefusalsStayLocal(t *testing.T) {
	h := newHarness(t, func(d *snapshot.Document) {
		d.PassthroughRef = "mock"
		// No credential: passthrough is CREDIT-axis by construction, so the
		// key cannot use it, and the env credential must not step in.
	}, nil)

	status, body := h.chat(t, testToken, `{"model":"vendor/x","messages":[{"role":"user","content":"hi"}]}`)
	if status != 403 || errCode(t, body) != "credit_unavailable" {
		t.Fatalf("passthrough without credential: got %d %s", status, body)
	}
	if h.mock.callCount() != 0 {
		t.Fatal("passthrough without credential still reached the upstream")
	}
}

func TestPassthroughNeverServesSelfServePrefix(t *testing.T) {
	h := newHarness(t, func(d *snapshot.Document) {
		d.PassthroughRef = "mock"
		d.Keys[0].UpstreamCredentials = map[string]string{"mock": keyCred}
	}, nil)

	// A typo under the curated prefix must stay a 404 rather than become a
	// billable request to the commercial provider.
	status, body := h.chat(t, testToken, `{"model":"pnu-generall","messages":[{"role":"user","content":"hi"}]}`)
	if status != 404 || errCode(t, body) != "model_not_found" {
		t.Fatalf("self-serve-prefixed unknown name: got %d %s", status, body)
	}
	if h.mock.callCount() != 0 {
		t.Fatal("a pnu-prefixed unknown name reached the upstream")
	}
}

func TestUnknownModelStays404WithoutPassthrough(t *testing.T) {
	h := newHarness(t, func(d *snapshot.Document) {
		d.Keys[0].UpstreamCredentials = map[string]string{"mock": keyCred}
	}, nil)
	status, body := h.chat(t, testToken, `{"model":"vendor/x","messages":[{"role":"user","content":"hi"}]}`)
	if status != 404 || errCode(t, body) != "model_not_found" {
		t.Fatalf("unknown model with no passthroughRef: got %d %s", status, body)
	}
}

func TestRestrictedKeyListGovernsPassthroughToo(t *testing.T) {
	h := newHarness(t, func(d *snapshot.Document) {
		d.PassthroughRef = "mock"
		d.Keys[0].UpstreamCredentials = map[string]string{"mock": keyCred}
		d.Keys[0].AllowedModels = []string{"pnu-general"}
	}, nil)
	status, body := h.chat(t, testToken, `{"model":"vendor/x","messages":[{"role":"user","content":"hi"}]}`)
	if status != 403 || errCode(t, body) != "model_not_allowed" {
		t.Fatalf("allow-listed key reaching passthrough: got %d %s", status, body)
	}
}

func TestCredentialRefMatchIsCaseInsensitive(t *testing.T) {
	h := newHarness(t, func(d *snapshot.Document) {
		d.Models = append(d.Models, snapshot.Model{
			PublicName: "vendor-model", UpstreamRef: "MOCK",
			UpstreamModel: upstreamModel, BudgetAxis: snapshot.AxisCredit,
		})
		d.Keys[0].UpstreamCredentials = map[string]string{"Mock": keyCred}
	}, nil)
	status, body := h.chat(t, testToken, creditChatBody)
	if status != 200 {
		t.Fatalf("mixed-case ref did not resolve the credential: %d %s", status, body)
	}
	if _, auth := h.mock.last(); auth != "Bearer "+keyCred {
		t.Fatalf("call carried %q", auth)
	}
}

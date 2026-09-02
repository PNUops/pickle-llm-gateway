// Budget axes and per-key upstream credentials: every per-key limit governs
// only TOKEN-axis models, CREDIT-axis models require the key's own upstream
// credential (never the gateway-wide env one), and uncatalogued model names
// pass through to the document's passthrough upstream on the CREDIT axis.
package server

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/pnuops/pickle-llm-gateway/internal/config"
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

	// TOKEN-axis model (pickle-general has no budgetAxis, which means TOKEN):
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
	events := h.spoolEvents(t)
	if len(events) != 2 {
		t.Fatalf("spooled %d events, want TOKEN and CREDIT requests", len(events))
	}
	if events[0].BudgetAxis != snapshot.AxisToken || events[1].BudgetAxis != snapshot.AxisCredit {
		t.Fatalf("request-time budget axes = %v, want TOKEN then CREDIT", []string{events[0].BudgetAxis, events[1].BudgetAxis})
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

// A granted budget whose upstream key has not been created yet is the one
// missing-credential state that ends by itself, so it gets its own answer: a
// 503 a client may retry, rather than a 403 telling it to go ask for a budget
// it already has.
func TestCreditPendingIsToldApartFromNoBudget(t *testing.T) {
	h := newHarness(t, func(d *snapshot.Document) {
		creditDoc(d)
		d.Keys[0].UpstreamCredentials = nil
		d.Keys[0].CreditPending = true
	}, nil)

	status, body, header := h.chatHeaders(t, testToken, creditChatBody)
	if status != 503 || errCode(t, body) != "credit_pending" {
		t.Fatalf("pending credit budget: got %d %s", status, body)
	}
	if got := header.Get("Retry-After"); got != "10" {
		t.Fatalf("Retry-After = %q, want 10", got)
	}
	// Still a local refusal. Being nearly ready is not being ready, and the
	// env credential must not stand in for the key's own.
	if h.mock.callCount() != 0 {
		t.Fatalf("the upstream was called %d times for a pending key", h.mock.callCount())
	}
}

// The flag only ever explains a missing credential. A key that has one is
// serving, whatever the control plane last said about provisioning.
func TestCreditPendingDoesNotBlockAKeyThatHasItsCredential(t *testing.T) {
	h := newHarness(t, func(d *snapshot.Document) {
		creditDoc(d)
		d.Keys[0].CreditPending = true
	}, nil)

	if status, body := h.chat(t, testToken, creditChatBody); status != 200 {
		t.Fatalf("credit call refused despite a credential: %d %s", status, body)
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
	if last.PublicModelName != "vendor/some-model" || last.BudgetAxis != snapshot.AxisCredit || last.Status != "OK" {
		t.Fatalf("event recorded model=%q budgetAxis=%q status=%q", last.PublicModelName, last.BudgetAxis, last.Status)
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
	// billable request to the commercial provider — case variants included:
	// the catalog lookup is case-sensitive, so "PICKLE-general" misses it, and
	// without a case-insensitive guard it would slip through as billable. The
	// retired "pnu-" prefix (renamed 2026-08-25) stays guarded for the same
	// reason: a stale name the catalog no longer lists must 404, not bill.
	// (While a catalog row still carries a retired name, the exact match wins
	// — TestReservedPrefixExactCatalogMatchStillServes.)
	for _, name := range []string{
		"pickle-generall", "PICKLE-general", "Pickle-x",
		"pnu-general", "pnu-generall", "PNU-general",
	} {
		status, body := h.chat(t, testToken,
			`{"model":"`+name+`","messages":[{"role":"user","content":"hi"}]}`)
		if status != 404 || errCode(t, body) != "model_not_found" {
			t.Fatalf("self-serve-prefixed unknown name %q: got %d %s", name, status, body)
		}
	}
	if h.mock.callCount() != 0 {
		t.Fatal("a reserved-prefixed unknown name reached the upstream")
	}
}

func TestQuotaExhaustedKeyWithoutCredentialRefusedEarly(t *testing.T) {
	// Exhausted token quota and no credential: no axis can serve this key, so
	// it is refused before the upstream and before any body handling.
	h := newHarness(t, func(d *snapshot.Document) {
		creditDoc(d)
		d.Keys[0].QuotaExhausted = true
		d.Keys[0].UpstreamCredentials = nil
	}, nil)
	for _, body := range []string{chatBody, creditChatBody} {
		status, out := h.chat(t, testToken, body)
		if status != 429 || errCode(t, out) != "quota_exhausted" {
			t.Fatalf("got %d %s", status, out)
		}
	}
	if h.mock.callCount() != 0 {
		t.Fatal("an unusable key reached the upstream")
	}
}

func TestModelRetrieveAgreesWithPassthrough(t *testing.T) {
	h := newHarness(t, func(d *snapshot.Document) {
		d.PassthroughRef = "mock"
		d.Keys[0].UpstreamCredentials = map[string]string{"mock": keyCred}
	}, nil)
	req, err := http.NewRequest(http.MethodGet, h.gw.URL+"/v1/models/vendor%2Fsome-model", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+testToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("retrieve of a passthrough-served name answered %d", resp.StatusCode)
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
	events := h.spoolEvents(t)
	if len(events) != 1 {
		t.Fatalf("spooled %d events for unknown model, want 1", len(events))
	}
	if events[0].BudgetAxis != "" {
		t.Fatalf("unknown model event has budgetAxis %q", events[0].BudgetAxis)
	}
}

func TestCreditExhaustedIsARefusalNotAnOutage(t *testing.T) {
	// 402 is the money axis working: the student hears "budget spent", the
	// request is not retried, the upstream is not cooled down (one exhausted
	// key must not reorder everybody else's traffic), and the event counts as
	// a limit refusal rather than an upstream fault.
	h := newHarness(t, creditDoc, nil)
	h.mock.set(func(u *mockOpts) { u.status = 402; u.errBody = `{"error":{"code":402}}` })

	status, body := h.chat(t, testToken, creditChatBody)

	if status != 429 || errCode(t, body) != "credit_exhausted" {
		t.Fatalf("402 from the upstream: got %d %s", status, body)
	}
	if h.mock.callCount() != 1 {
		t.Fatalf("a budget refusal was retried: %d upstream calls", h.mock.callCount())
	}
	if h.srv.health.cooling("mock") {
		t.Fatal("a budget refusal put the upstream in cooldown")
	}
	evs := h.spoolEvents(t)
	if last := evs[len(evs)-1]; last.Status != "RATE_LIMITED" {
		t.Fatalf("event recorded as %q, want RATE_LIMITED", last.Status)
	}
}

func TestTokenAxis402IsAnUpstreamFaultNotTheStudentsBudget(t *testing.T) {
	// The same status on a TOKEN-axis model means the platform's own account
	// ran dry, not the key's budget. Telling the student to request a limit
	// increase would send them to fix something they do not own, so it stays
	// an upstream fault — which also keeps the model's fallback in play.
	h := newHarness(t, creditDoc, nil)
	h.mock.set(func(u *mockOpts) { u.status = 402; u.errBody = `{"error":{"code":402}}` })

	status, body := h.chat(t, testToken, chatBody)

	if status != 502 || errCode(t, body) != "upstream_error" {
		t.Fatalf("402 on a token-axis model: got %d %s", status, body)
	}
	evs := h.spoolEvents(t)
	if last := evs[len(evs)-1]; last.Status != "UPSTREAM_ERROR" {
		t.Fatalf("event recorded as %q, want UPSTREAM_ERROR", last.Status)
	}
}

func TestRestrictedKeyListGovernsPassthroughToo(t *testing.T) {
	h := newHarness(t, func(d *snapshot.Document) {
		d.PassthroughRef = "mock"
		d.Keys[0].UpstreamCredentials = map[string]string{"mock": keyCred}
		d.Keys[0].AllowedModels = []string{"pickle-general"}
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

func TestReservedPrefixRetrieveAgreesWithChat(t *testing.T) {
	h := newHarness(t, func(d *snapshot.Document) {
		d.PassthroughRef = "mock"
		d.Keys[0].UpstreamCredentials = map[string]string{"mock": keyCred}
	}, nil)

	get := func(id string) int {
		req, _ := http.NewRequest(http.MethodGet, h.gw.URL+"/v1/models/"+id, nil)
		req.Header.Set("Authorization", "Bearer "+testToken)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}
	// Reserved-prefix names the catalog does not list are 404 on the retrieve
	// surface too — the handler comment says retrieve must agree with chat,
	// and this enforces it for reserved names.
	for _, id := range []string{"pickle-none", "pnu-general", "PNU-general"} {
		if got := get(id); got != 404 {
			t.Fatalf("reserved name %q retrieve: got %d, want 404", id, got)
		}
	}
	// The same key sees an arbitrary commercial name through passthrough, so
	// the 404s above are the reservation, not a broken retrieve surface.
	if got := get("vendor-x"); got != 200 {
		t.Fatalf("passthrough name retrieve: got %d, want 200", got)
	}
}

func TestReservedPrefixExactCatalogMatchStillServes(t *testing.T) {
	// During a prefix transition the old name may still be a catalog row;
	// the reservation governs passthrough only, and an exact catalog match
	// wins. This is what keeps live traffic on the old name working between
	// the gateway deploy and the catalog rename.
	h := newHarness(t, func(d *snapshot.Document) {
		d.PassthroughRef = "mock"
		d.Models = append(d.Models, snapshot.Model{
			PublicName: "pnu-general", UpstreamRef: "mock",
			UpstreamModel: upstreamModel, MaxOutputTokens: 4096,
		})
		d.Keys[0].UpstreamCredentials = map[string]string{"mock": keyCred}
	}, nil)
	status, body := h.chat(t, testToken,
		`{"model":"pnu-general","messages":[{"role":"user","content":"hi"}]}`)
	if status != 200 {
		t.Fatalf("catalogued retired-prefix name: got %d %s", status, body)
	}
}

// The per-key short-window limits ration self-hosted serving capacity, so
// since 2026-09-02 they govern TOKEN-axis requests only. A commercial call
// neither answers to them nor charges them, which is what the tests below
// pin — separately, because each of the three failed differently before.

func TestCreditAxisIgnoresRequestAndTokenLimits(t *testing.T) {
	h := newHarness(t, func(d *snapshot.Document) {
		creditDoc(d)
		d.Keys[0].Limits.Rpm = 1
		d.Keys[0].Limits.Tpm = 10
	}, nil)

	// Sequential calls: the first would exhaust rpm and the first response
	// would put the tpm bucket into debt, refusing everything after it.
	for i := range 3 {
		status, body := h.chat(t, testToken, creditChatBody)
		if status != 200 {
			t.Fatalf("credit-axis call %d refused by a self-serve limit: %d %s", i, status, body)
		}
	}
}

func TestCreditAxisIgnoresConcurrencyLimit(t *testing.T) {
	// Sequential calls can never reach a concurrency limit — in-flight goes
	// 0→1→0 around each one — so this needs two requests genuinely overlapping.
	h := newHarness(t, func(d *snapshot.Document) {
		creditDoc(d)
		d.Keys[0].Limits.Concurrency = 1
	}, nil)
	h.mock.set(func(u *mockOpts) { u.delay = 400 * time.Millisecond })

	codes := make(chan int, 2)
	for range 2 {
		go func() { codes <- h.chatStatus(testToken, creditChatBody) }()
		time.Sleep(50 * time.Millisecond)
	}
	for range 2 {
		if got := <-codes; got != 200 {
			t.Fatalf("overlapping credit-axis call refused: %d", got)
		}
	}
}

func TestCreditAxisUsageDoesNotChargeTheTokenBucket(t *testing.T) {
	// The refusal moving off the credit axis is only half the fix. If the
	// commercial response still charged the shared per-key tpm bucket, the
	// bucket would sit in debt and refuse the *next* self-serve call — the
	// original symptom, minus the error code. There are three charge sites;
	// the streaming one is exercised below.
	for _, stream := range []bool{false, true} {
		t.Run(map[bool]string{false: "non-stream", true: "stream"}[stream], func(t *testing.T) {
			h := newHarness(t, func(d *snapshot.Document) {
				creditDoc(d)
				d.Keys[0].Limits.Tpm = 10
			}, nil)
			body := creditChatBody
			if stream {
				body = `{"model":"vendor-model","messages":[{"role":"user","content":"hi"}],"stream":true}`
			}
			for i := range 3 {
				if status, out := h.chat(t, testToken, body); status != 200 {
					t.Fatalf("credit-axis call %d: %d %s", i, status, out)
				}
			}
			// The token-axis model shares the key and so the bucket.
			if status, out := h.chat(t, testToken, chatBody); status != 200 {
				t.Fatalf("token-axis call after credit-axis traffic: %d %s", status, out)
			}
		})
	}
}

func TestTokenAxisStillAnswersToEveryShortWindowLimit(t *testing.T) {
	// The other half: nothing about the self-serve axis loosened. Each limit
	// refuses on its own code and carries Retry-After, as before.
	t.Run("rpm", func(t *testing.T) {
		h := newHarness(t, func(d *snapshot.Document) { d.Keys[0].Limits.Rpm = 1 }, nil)
		if status, _ := h.chat(t, testToken, chatBody); status != 200 {
			t.Fatalf("first call: %d", status)
		}
		status, body, hdr := h.chatHeaders(t, testToken, chatBody)
		if status != 429 || errCode(t, body) != "rate_limit_requests" {
			t.Fatalf("second call: %d %s", status, body)
		}
		if hdr.Get("Retry-After") == "" {
			t.Fatal("rpm refusal carried no Retry-After")
		}
	})
	t.Run("tpm", func(t *testing.T) {
		h := newHarness(t, func(d *snapshot.Document) { d.Keys[0].Limits.Tpm = 10 }, nil)
		if status, _ := h.chat(t, testToken, chatBody); status != 200 {
			t.Fatalf("first call: %d", status)
		}
		status, body, hdr := h.chatHeaders(t, testToken, chatBody)
		if status != 429 || errCode(t, body) != "rate_limit_tokens" {
			t.Fatalf("second call: %d %s", status, body)
		}
		if hdr.Get("Retry-After") == "" {
			t.Fatal("tpm refusal carried no Retry-After")
		}
	})
	t.Run("concurrency", func(t *testing.T) {
		h := newHarness(t, func(d *snapshot.Document) { d.Keys[0].Limits.Concurrency = 1 }, nil)
		h.mock.set(func(u *mockOpts) { u.delay = 400 * time.Millisecond })
		codes := make(chan int, 2)
		bodies := make(chan []byte, 2)
		for range 2 {
			go func() {
				status, raw := h.chatRaw(testToken, chatBody)
				codes <- status
				bodies <- raw
			}()
			time.Sleep(50 * time.Millisecond)
		}
		got := map[int]int{}
		var refused []byte
		for range 2 {
			c := <-codes
			b := <-bodies
			got[c]++
			if c == 429 {
				refused = b
			}
		}
		if got[200] != 1 || got[429] != 1 {
			t.Fatalf("overlapping token-axis calls: %v", got)
		}
		if code := errCode(t, refused); code != "rate_limit_concurrency" {
			t.Fatalf("refused with %q, want rate_limit_concurrency", code)
		}
	})
}

func TestRateLimitHeadersAreForTheSelfServeAxisOnly(t *testing.T) {
	// The remaining-requests header reports the rpm budget, and the credit
	// axis no longer has one. Reporting a number that governs nothing would
	// be worse than reporting none.
	h := newHarness(t, creditDoc, nil)

	status, body, hdr := h.chatHeaders(t, testToken, chatBody)
	if status != 200 {
		t.Fatalf("token-axis call: %d %s", status, body)
	}
	if hdr.Get("X-RateLimit-Limit-Requests") == "" || hdr.Get("X-RateLimit-Remaining-Requests") == "" {
		t.Fatalf("token-axis response lost its rate-limit headers: %v", hdr)
	}

	status, body, hdr = h.chatHeaders(t, testToken, creditChatBody)
	if status != 200 {
		t.Fatalf("credit-axis call: %d %s", status, body)
	}
	if got := hdr.Get("X-RateLimit-Limit-Requests"); got != "" {
		t.Fatalf("credit-axis response advertised a request ceiling %q", got)
	}
	if got := hdr.Get("X-RateLimit-Remaining-Requests"); got != "" {
		t.Fatalf("credit-axis response advertised a remaining count %q", got)
	}
}

func TestAdmissionRunsBeforeTheDailyQuota(t *testing.T) {
	// Order inside the self-serve branch, which nothing pinned before. A key
	// that exhausted its daily allowance is the one most likely to be in a
	// retry loop; if the quota answered first it would never charge rpm and
	// that loop would have no brake at all. The key holds a credential so the
	// early both-axes-closed refusal does not fire.
	h := newHarness(t, func(d *snapshot.Document) {
		creditDoc(d)
		d.Keys[0].QuotaExhausted = true
		d.Keys[0].Limits.Rpm = 1
	}, nil)

	status, body := h.chat(t, testToken, chatBody)
	if status != 429 || errCode(t, body) != "quota_exhausted" {
		t.Fatalf("first call: %d %s", status, body)
	}
	// The refusal above still spent the single request in the rpm bucket.
	status, body = h.chat(t, testToken, chatBody)
	if status != 429 || errCode(t, body) != "rate_limit_requests" {
		t.Fatalf("second call: %d %s, want rate_limit_requests (admission before quota)", status, body)
	}
}

func TestSelfServeRefusalsNameTheModelAndAxis(t *testing.T) {
	// Moving admission after model resolution changed what a refusal records.
	// These rows used to carry neither field, which is what made the live
	// incident hard to read; the accounting now says which model was asked
	// for and on which axis.
	h := newHarness(t, func(d *snapshot.Document) { d.Keys[0].Limits.Rpm = 1 }, nil)
	if status, _ := h.chat(t, testToken, chatBody); status != 200 {
		t.Fatal("first call should pass")
	}
	if status, _ := h.chat(t, testToken, chatBody); status != 429 {
		t.Fatal("second call should be refused")
	}
	events := h.spoolEvents(t)
	last := events[len(events)-1]
	if last.ErrorType != "rate_limit_requests" {
		t.Fatalf("last event is %q", last.ErrorType)
	}
	if last.PublicModelName != "pickle-general" || last.BudgetAxis != snapshot.AxisToken {
		t.Fatalf("refusal recorded model=%q axis=%q, want the requested model on the TOKEN axis",
			last.PublicModelName, last.BudgetAxis)
	}
}

func TestUnparseableUpstreamResponseChargesTheBucketButNotTheRecord(t *testing.T) {
	// A response that is not JSON is an upstream fault the student cannot
	// influence. It must still cost minute-scale budget, or looping against a
	// broken upstream is free; it must not reach the accounting record, or the
	// api's daily-allowance query — which counts tokens without looking at
	// status — spends the student's quota on that fault.
	//
	// The prompt is 51 bytes of messages, so the input-side estimate is 12
	// tokens, which is why the limit below is 10.
	h := newHarness(t, func(d *snapshot.Document) { d.Keys[0].Limits.Tpm = 10 }, nil)
	h.mock.set(func(u *mockOpts) { u.rawResp = "this is not json" })

	if status, body := h.chat(t, testToken, chatBody); status != 502 {
		t.Fatalf("first call: %d %s", status, body)
	}
	events := h.spoolEvents(t)
	last := events[len(events)-1]
	if last.ErrorType != "upstream_invalid_response" {
		t.Fatalf("last event is %q", last.ErrorType)
	}
	if last.InputTokens != 0 || last.OutputTokens != 0 || last.Estimated {
		t.Fatalf("upstream fault reached the accounting record: in=%d out=%d estimated=%t",
			last.InputTokens, last.OutputTokens, last.Estimated)
	}
	// The bucket did take the charge, so the next request is refused.
	status, body := h.chat(t, testToken, chatBody)
	if status != 429 || errCode(t, body) != "rate_limit_tokens" {
		t.Fatalf("second call: %d %s, want the bucket to have been charged", status, body)
	}
}

func TestOneKeyCanHoldEveryGatewaySlot(t *testing.T) {
	// Accepted consequence of the axis split, pinned so it is a decision and
	// not a surprise. With no per-key concurrency on the credit axis, the
	// gateway-wide cap is all that stands between one commercial client and
	// everybody else — including self-serve traffic, which is what the axis
	// split exists to protect. A queue or a per-key share of the cap would
	// flip this test, and that is the point of having it.
	h := newHarness(t, creditDoc, func(c *config.Config) { c.MaxInFlight = 1 })
	h.mock.set(func(u *mockOpts) { u.delay = 400 * time.Millisecond })

	held := make(chan int, 1)
	go func() { held <- h.chatStatus(testToken, creditChatBody) }()
	time.Sleep(100 * time.Millisecond)

	status, body := h.chat(t, testToken, chatBody)
	if status != 503 || errCode(t, body) != "server_busy" {
		t.Fatalf("self-serve call while a commercial one held the only slot: %d %s", status, body)
	}
	if got := <-held; got != 200 {
		t.Fatalf("the holding call ended %d", got)
	}
}

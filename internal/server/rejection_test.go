package server

import (
	"bytes"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/pnuops/pickle-llm-gateway/internal/snapshot"
	"github.com/pnuops/pickle-llm-gateway/internal/spool"
)

// spoolText is every spool line as written, so a test can assert that a
// string never reached the accounting record in any field.
func spoolText(t *testing.T, h *harness) string {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(h.spoolDir, "usage-*.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	var b strings.Builder
	for _, f := range files {
		raw, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		b.Write(raw)
	}
	return b.String()
}

// The reason a context-window overflow fails is the whole point: the caller
// sees it, in the public name, and nowhere else keeps it.
func TestUpstreamRejectionCarriesTheReason(t *testing.T) {
	h := newHarness(t, nil, nil)
	var logs bytes.Buffer
	h.srv.log = slog.New(slog.NewTextHandler(&logs, nil))
	const reason = "However, you requested 40000 tokens"
	h.mock.set(func(u *mockOpts) {
		u.status = http.StatusBadRequest
		u.errBody = `{"error":{"message":"This model's maximum context length is 32768 tokens. ` + reason +
			` (39000 in the messages, 1000 in the completion). Model ` + upstreamModel +
			` cannot serve it.","type":"BadRequestError","param":null,"code":400}}`
	})
	status, body := h.chat(t, testToken, chatBody)
	if status != http.StatusBadRequest || errCode(t, body) != "upstream_rejected" {
		t.Fatalf("got %d %s", status, body)
	}
	msg := errMessage(t, body)
	if !strings.Contains(msg, "maximum context length is 32768 tokens") || !strings.Contains(msg, reason) {
		t.Fatalf("the reason did not reach the caller: %q", msg)
	}
	if strings.Contains(string(body), upstreamModel) || !strings.Contains(msg, "pickle-general") {
		t.Fatalf("upstream model id was not replaced by the public name: %q", msg)
	}
	if strings.Contains(spoolText(t, h), reason) || strings.Contains(logs.String(), reason) {
		t.Fatal("the upstream message reached the spool or the log")
	}
	evs := h.spoolEvents(t)
	if ev := evs[len(evs)-1]; ev.Status != spool.StatusUpstreamErr || ev.ErrorType != "upstream_rejected" {
		t.Fatalf("usage record changed: %+v", ev)
	}
}

// Everything around the message is the vendor's, and so is anything inside it
// that names an address, a credential or another request.
func TestUpstreamRejectionStripsVendorDetail(t *testing.T) {
	h := newHarness(t, func(d *snapshot.Document) {
		d.Models[0].BudgetAxis = snapshot.AxisCredit
		d.Keys[0].UpstreamCredentials = map[string]string{"mock": upstreamCred}
	}, nil)
	leaks := []string{
		"sk-or-v1-0123456789abcdef0123456789abcdef",
		"sk-other-key-abcdef",
		"https://internal-host:9999/secret-path",
		"10.20.30.40:8000",
		"Bearer some-other-token",
		"a3f9c2e17b4d48e6a1c0b9f8d7e6c5b4a3f2e1d0",
		"metadata-raw-detail",
		"SecretProvider",
	}
	h.mock.set(func(u *mockOpts) {
		u.status = http.StatusBadRequest
		u.errBody = `{"error":{"code":400,"message":"Provider returned error for key ` + leaks[0] +
			` and ` + leaks[1] + ` at ` + leaks[2] + ` via ` + leaks[3] + ` with ` + leaks[4] +
			` request ` + leaks[5] + `: invalid tool schema","metadata":{"raw":"` + leaks[6] +
			`","provider_name":"` + leaks[7] + `"}}}`
	})
	status, body := h.chat(t, testToken, chatBody)
	if status != http.StatusBadRequest || errCode(t, body) != "upstream_rejected" {
		t.Fatalf("got %d %s", status, body)
	}
	for _, leak := range leaks {
		if strings.Contains(string(body), leak) {
			t.Fatalf("%q leaked to the caller: %s", leak, body)
		}
	}
	if msg := errMessage(t, body); !strings.Contains(msg, "invalid tool schema") {
		t.Fatalf("the reason itself was lost: %q", msg)
	}
	if strings.Contains(string(body), upstreamCred) {
		t.Fatal("upstream credential leaked")
	}
}

func TestUpstreamRejectionShapes(t *testing.T) {
	long := strings.Repeat("너무 긴 입력 ", 200)
	tests := []struct {
		name       string
		status     int
		body       string
		wantStatus int
		contains   string
		fixed      bool
	}{
		{"422 keeps its status", http.StatusUnprocessableEntity,
			`{"error":{"message":"messages must not be empty"}}`, http.StatusUnprocessableEntity,
			"messages must not be empty", false},
		{"legacy vLLM object", http.StatusBadRequest,
			`{"object":"error","message":"temperature must be non-negative","type":"BadRequestError"}`,
			http.StatusBadRequest, "temperature must be non-negative", false},
		{"string error is not a message", http.StatusBadRequest, `{"error":"bad"}`,
			http.StatusBadRequest, "", true},
		{"not JSON", http.StatusBadRequest, `<html>bad request</html>`,
			http.StatusBadRequest, "", true},
		{"no message", http.StatusBadRequest, `{"error":{"type":"invalid_request_error"}}`,
			http.StatusBadRequest, "", true},
		{"control characters", http.StatusBadRequest,
			`{"error":{"message":"line one\nline two\u0000\u001b[31m"}}`, http.StatusBadRequest,
			"line one line two", false},
		{"long message is bounded", http.StatusBadRequest,
			`{"error":{"message":"` + long + `"}}`, http.StatusBadRequest, "…", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness(t, nil, nil)
			h.mock.set(func(u *mockOpts) { u.status = tt.status; u.errBody = tt.body })
			status, body := h.chat(t, testToken, chatBody)
			if status != tt.wantStatus || errCode(t, body) != "upstream_rejected" {
				t.Fatalf("got %d %s", status, body)
			}
			msg := errMessage(t, body)
			if tt.fixed && msg != errUpstreamRejected.message {
				t.Fatalf("want the fixed message, got %q", msg)
			}
			if tt.contains != "" && !strings.Contains(msg, tt.contains) {
				t.Fatalf("message %q lacks %q", msg, tt.contains)
			}
			if strings.ContainsAny(msg, "\n\x00\x1b") {
				t.Fatalf("control characters reached the caller: %q", msg)
			}
			if utf8.RuneCountInString(msg) > utf8.RuneCountInString(errUpstreamRejected.message)+rejectionMessageMax+20 {
				t.Fatalf("message is not bounded: %d runes", utf8.RuneCountInString(msg))
			}
		})
	}
}

// The passthrough surface shares the classification, so it shares the reason.
func TestPassthroughRejectionCarriesTheReason(t *testing.T) {
	h := newHarness(t, passthroughDoc(snapshot.EndpointImages), nil)
	h.mock.set(func(o *mockOpts) {
		o.status = http.StatusBadRequest
		o.errBody = `{"error":{"code":400,"message":"size 4096x4096 is not supported","metadata":{"raw":"metadata-raw-detail"}}}`
	})
	status, body := h.passthrough(t, http.MethodPost, "/v1/images", testToken, imageBody)
	if status != http.StatusBadRequest || errCode(t, body) != "upstream_rejected" {
		t.Fatalf("got %d %s", status, body)
	}
	if msg := errMessage(t, body); !strings.Contains(msg, "size 4096x4096 is not supported") ||
		strings.Contains(string(body), "metadata-raw-detail") {
		t.Fatalf("passthrough rejection: %s", body)
	}
}

func TestHideModelName(t *testing.T) {
	if got := hideModelName("Model Internal-Model-X and internal-model-x", "internal-model-x", "pub"); got != "Model pub and pub" {
		t.Fatalf("got %q", got)
	}
	if got := hideModelName("No endpoints found for Llama-3.3-70B.", "nvidia/Llama-3.3-70B", "pub"); got != "No endpoints found for pub." {
		t.Fatalf("basename survived: %q", got)
	}
	// Characters whose lower case changes byte length must not shift the match.
	if got := hideModelName("ẞẞẞ internal-model-x ȺȺȺ", "internal-model-x", "pub"); got != "ẞẞẞ pub ȺȺȺ" {
		t.Fatalf("got %q", got)
	}
}

// The id is swapped while whole. vLLM's validation errors echo the body the
// gateway sent, model included, after the caller's own messages, so a caller
// can pad the prompt until the cut lands inside the id.
func TestRejectionHidesModelAcrossTheCut(t *testing.T) {
	const id = "nvidia/Llama-3.3-70B-Instruct-FP4-0123456789abcdefghijklmnop.v2"
	for pad := 330; pad <= 400; pad++ {
		raw := strings.Repeat("a ", pad/2) + "'model': '" + id + "'"
		got := cleanRejection(raw, id, "pickle-general")
		for _, frag := range []string{"nvidia", "Llama", "Instruct", "FP4", "0123456789"} {
			if strings.Contains(got, frag) {
				t.Fatalf("pad %d: fragment %q of the id survived: %q", pad, frag, got)
			}
		}
	}
}

func TestCleanRejectionAddresses(t *testing.T) {
	in := "vLLM 0.10.1.1 on dgx1.lab.local:8000 via spark2:8000 at [fd00::12]:8000 and 2001:db8:0:0:0:0:0:1 " +
		"10.0.0.5 path /models/hf/qwen3 'x=/srv/m' user@x.ac.kr \u202e at 12:30:45 for 32768 tokens"
	got := cleanRejection(strings.ReplaceAll(in, "\\u202e", "\u202e"), "", "")
	for _, leak := range []string{"dgx1", "spark2", "fd00", "2001:db8", "10.0.0.5", "/models", "/srv", "user@", "\u202e"} {
		if strings.Contains(got, leak) {
			t.Fatalf("%q survived: %q", leak, got)
		}
	}
	// A version string with four small parts is indistinguishable from an
	// address and is redacted; that costs a version number, not a reason.
	for _, keep := range []string{"12:30:45", "32768 tokens"} {
		if !strings.Contains(got, keep) {
			t.Fatalf("%q was redacted: %q", keep, got)
		}
	}
}

// The caller's own model names and ordinary words survive the cleaning; the
// rejection that quotes a model name back is the most common one there is.
// Measured before the fix: every name below came back as [redacted],
// "tokens:12000" as an address and "Error::" as an address.
func TestCleanRejectionKeepsWhatTheCallerSent(t *testing.T) {
	for _, tc := range []struct{ in, keep string }{
		{"openai/gpt-4o-mini-search-preview is not a valid model ID", "openai/gpt-4o-mini-search-preview"},
		{"anthropic/claude-3-7-sonnet-20250219 is not a valid model ID", "anthropic/claude-3-7-sonnet-20250219"},
		{"meta-llama/llama-4-maverick-17b-128e-instruct: context length exceeded", "meta-llama/llama-4-maverick-17b-128e-instruct"},
		{"Input tokens:12000 exceed the limit", "tokens:12000"},
		{"Error:: something odd", "Error:: something odd"},
	} {
		if got := cleanRejection(tc.in, "", ""); !strings.Contains(got, tc.keep) {
			t.Fatalf("%q lost %q: %q", tc.in, tc.keep, got)
		}
	}
}

// What the exemptions must not let through: a key or hash that carries a
// slash, a mixed-case author, and the hosts the address rule exists for.
func TestCleanRejectionStillHidesSecretsAndHosts(t *testing.T) {
	for _, secret := range []string{
		"Zm9vYmFyYmF6cXV4/cXV1eHF1dXhxdXV4cXV1eHF1dXg=",
		"AbCdEfGhIjKlMnOpQr/StUvWxYz0123456789abcdef",
		"abcdefghijklmnopqrstuvwxyz0123456789ABCDEF",
		"abcdefghij0123456789/K7MDENGbPxRfiCYEXAMPLEKEY",
		"abc123/ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefgh",
		"models/qwen3-235b-a22b-instruct-2507-fp8",
	} {
		if got := cleanRejection("bad token "+secret+" here", "", ""); strings.Contains(got, secret) {
			t.Fatalf("%q survived: %q", secret, got)
		}
	}
	for _, host := range []string{"localhost:8000", "dgx1:8000", "vllm-0:8000", "a.b.c:443"} {
		if got := cleanRejection("refused by "+host, "", ""); strings.Contains(got, host) {
			t.Fatalf("%q survived: %q", host, got)
		}
	}
	if got := cleanRejection("at fd00::12 and 2001:db8::1", "", ""); strings.Contains(got, "fd00") || strings.Contains(got, "2001") {
		t.Fatalf("IPv6 survived: %q", got)
	}
}

package server

import (
	"net/http"
	"strings"
	"testing"

	"github.com/pnuops/pickle-llm-gateway/internal/snapshot"
)

func TestProviderWildcardDenialStopsRequestsBeforeUpstream(t *testing.T) {
	h := newHarness(t, passthroughDenyDoc([]string{"*/*"}, []string{"*/*-pro"}), nil)
	for _, body := range []string{
		chatFor("openai/gpt-5-pro"),
		chatFor("google/gemini-pro:batch"),
		chatFor("~google/gemini-pro:nitro"),
		`{"model":"openai/gpt-5","models":["~google/gemini-pro"],"messages":[]}`,
		`{"model":"openai/gpt-5","tools":[{"type":"openrouter:advisor","parameters":{"model":"~google/gemini-pro"}}],"messages":[]}`,
	} {
		status, response := h.chat(t, testToken, body)
		if status != http.StatusForbidden || errCode(t, response) != "model_not_allowed" {
			t.Fatalf("request %s: got %d %s, want 403 model_not_allowed", body, status, response)
		}
	}
	if got := h.mock.callCount(); got != 0 {
		t.Fatalf("denied requests reached upstream %d times", got)
	}
	for _, name := range []string{"openai/gpt-5", "~google/gemini-latest", "pickle-general"} {
		status, body := h.chat(t, testToken, chatFor(name))
		if status != http.StatusOK {
			t.Fatalf("allowed model %q: got %d %s", name, status, body)
		}
	}
}

func TestProviderWildcardAllowanceAcrossProviders(t *testing.T) {
	for _, tc := range []struct {
		pattern, allowed, refused string
	}{
		{"*/gpt-5", "~openai/gpt-5:batch", "openai/gpt-5-pro"},
		{"*/gpt-5-*", "other/gpt-5", "openai/gpt-5:batch"},
		{"*/*-pro", "~other/model-pro:free", "openai/gpt-5"},
	} {
		t.Run(tc.pattern, func(t *testing.T) {
			h := newHarness(t, passthroughAllowlistDoc(tc.pattern), nil)
			status, body := h.chat(t, testToken, chatFor(tc.allowed))
			if status != http.StatusOK {
				t.Fatalf("allowed model %q: got %d %s", tc.allowed, status, body)
			}
			status, body = h.chat(t, testToken, chatFor(tc.refused))
			if status != http.StatusForbidden || errCode(t, body) != "model_not_allowed" {
				t.Fatalf("refused model %q: got %d %s", tc.refused, status, body)
			}
			if got := h.mock.callCount(); got != 1 {
				t.Fatalf("upstream received %d calls, want one allowed call", got)
			}
		})
	}
}

func TestProviderWildcardKeepsRouterNamesFenced(t *testing.T) {
	h := newHarness(t, passthroughAllowlistDoc("*/*"), nil)
	for _, name := range []string{"openrouter/auto", "~openrouter/free", "~~openrouter/auto:nitro"} {
		status, body := h.chat(t, testToken, chatFor(name))
		if status != http.StatusForbidden || errCode(t, body) != "model_not_allowed" {
			t.Fatalf("router %q: got %d %s", name, status, body)
		}
	}
	if h.mock.callCount() != 0 {
		t.Fatal("a router reached upstream through a provider wildcard")
	}
}

func TestModelsSurfaceAppliesProviderWildcard(t *testing.T) {
	h := newHarness(t, func(d *snapshot.Document) {
		passthroughDenyDoc([]string{"*/*"}, []string{"*/*-pro"})(d)
		for _, name := range []string{"openai/gpt-5", "~other/model-pro:batch"} {
			d.Models = append(d.Models, snapshot.Model{
				PublicName: name, UpstreamRef: "mock", UpstreamModel: upstreamModel,
				BudgetAxis: snapshot.AxisCredit,
			})
		}
	}, nil)
	listed := get(t, h.gw.URL+"/v1/models", testToken)
	if !strings.Contains(listed, "openai/gpt-5") || !strings.Contains(listed, "pickle-general") || strings.Contains(listed, "model-pro") {
		t.Fatalf("catalogue disagrees with provider wildcard policy: %s", listed)
	}
	status, body := h.chat(t, testToken, chatFor("~other/model-pro:batch"))
	if status != http.StatusForbidden || h.mock.callCount() != 0 {
		t.Fatalf("catalogue model bypassed denial: %d %s", status, body)
	}
}

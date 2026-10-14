package server

import (
	"encoding/json"
	"testing"

	"github.com/pnuops/pickle-llm-gateway/internal/snapshot"
)

// The vendor resolves a bare claude- name to its anthropic/ model and bills
// that model, so a key's lists judge the name under anthropic/ as well. The
// name forwarded is the one the caller wrote.
func TestBareClaudeNameIsJudgedUnderItsVendor(t *testing.T) {
	for _, c := range []struct {
		name            string
		allowed, denied []string
		model           string
		ok              bool
	}{
		{"deny vendor", nil, []string{"anthropic/*"}, "claude-opus-5-5", false},
		{"deny family", nil, []string{"anthropic/claude-opus*"}, "Claude-Opus-5-5", false},
		{"allow other vendor only", []string{"openai/*"}, nil, "claude-opus-5-5", false},
		{"allow family with a made-up date", []string{"anthropic/claude-opus*"}, nil, "claude-opus-5-5-20990101", true},
		{"allow family, other family asked", []string{"anthropic/claude-haiku*"}, nil, "claude-opus-5-5", false},
		{"no lists", nil, nil, "claude-opus-5-5", true},
		{"allowed but denied", []string{"anthropic/*"}, []string{"anthropic/claude-opus*"}, "claude-opus-5-5", false},
	} {
		t.Run(c.name, func(t *testing.T) {
			h, v := messagesHarness(t, func(d *snapshot.Document) {
				d.Keys[0].CreditAllowedModels = c.allowed
				d.Keys[0].CreditDeniedModels = c.denied
			})
			status, body, _ := sendMessages(t, h, nil, messagesFor(c.model))
			if !c.ok {
				expectAnthropicRefusal(t, status, body, 403, "permission_error", "model_not_allowed")
				if v.callCount() != 0 {
					t.Fatal("a refused name reached the vendor")
				}
				return
			}
			if status != 200 {
				t.Fatalf("%d %s", status, body)
			}
			var sent string
			_ = json.Unmarshal(v.body["model"], &sent)
			if sent != c.model {
				t.Fatalf("forwarded %q, want the name as written", sent)
			}
		})
	}
	// Chat goes through the same lists.
	h := newHarness(t, func(d *snapshot.Document) {
		creditPassthroughDoc(d)
		d.Keys[0].CreditDeniedModels = []string{"anthropic/*"}
	}, nil)
	if status, body := h.chat(t, testToken, `{"model":"claude-opus-5-5","messages":[{"role":"user","content":"hi"}]}`); status != 403 || errCode(t, body) != "model_not_allowed" {
		t.Fatalf("chat: %d %s", status, body)
	}
}

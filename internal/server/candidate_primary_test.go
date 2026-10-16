// A request may name its models through the fallback list alone. The vendor
// documents that shape: `model` is optional when `models` is given, and the
// first entry is then the primary. These tests pin that the first entry goes
// through every check `model` would, and that the list is still fenced whole.
package server

import (
	"encoding/json"
	"testing"

	"github.com/pnuops/pickle-llm-gateway/internal/snapshot"
)

func modelsOnly(models string) string {
	return `{"models":` + models + `,"messages":[{"role":"user","content":"hi"}]}`
}

func TestModelsWithoutModelUsesTheFirstEntry(t *testing.T) {
	h := newHarness(t, passthroughAllowlistDoc(), nil)

	status, body := h.chat(t, testToken, modelsOnly(`["openai/gpt-4o-mini","anthropic/claude-sonnet-4"]`))
	if status != 200 {
		t.Fatalf("models-only request was refused: %d %s", status, body)
	}
	sent, auth := h.mock.last()
	var rest []string
	_ = json.Unmarshal(sent["models"], &rest)
	// Forwarded in the shape the caller sent: no `model`, the list unchanged.
	if _, has := sent["model"]; has || len(rest) != 2 ||
		rest[0] != "openai/gpt-4o-mini" || rest[1] != "anthropic/claude-sonnet-4" {
		t.Fatalf("upstream got model=%s models=%v", sent["model"], rest)
	}
	if auth != "Bearer "+keyCred {
		t.Fatalf("money-axis request did not carry the key's credential: %q", auth)
	}
	evs := h.spoolEvents(t)
	ev := evs[len(evs)-1]
	if ev.PublicModelName != "openai/gpt-4o-mini" || ev.BudgetAxis != snapshot.AxisCredit {
		t.Fatalf("event recorded model=%q axis=%q", ev.PublicModelName, ev.BudgetAxis)
	}
}

// A catalogued money-axis row is sent under its upstream id, as `model` would
// be; the caller's public name never reaches the vendor for the primary.
func TestModelsFirstEntryIsSentByUpstreamName(t *testing.T) {
	h := newHarness(t, func(d *snapshot.Document) {
		passthroughAllowlistDoc()(d)
		d.Models = append(d.Models, snapshot.Model{PublicName: "paid-alias", UpstreamRef: "mock",
			UpstreamModel: "vendor/real-model", BudgetAxis: snapshot.AxisCredit})
	}, nil)

	if status, body := h.chat(t, testToken, modelsOnly(`["paid-alias"]`)); status != 200 {
		t.Fatalf("got %d %s", status, body)
	}
	sent, _ := h.mock.last()
	var rest []string
	_ = json.Unmarshal(sent["models"], &rest)
	if len(rest) != 1 || rest[0] != "vendor/real-model" {
		t.Fatalf("upstream got models=%v", rest)
	}
	evs := h.spoolEvents(t)
	if ev := evs[len(evs)-1]; ev.PublicModelName != "paid-alias" {
		t.Fatalf("event recorded model=%q", ev.PublicModelName)
	}
}

// An empty or null `model` is the same as an absent one.
func TestModelsWithEmptyModel(t *testing.T) {
	h := newHarness(t, passthroughAllowlistDoc(), nil)

	for _, model := range []string{`""`, `null`} {
		body := `{"model":` + model + `,"models":["openai/gpt-4o-mini"],"messages":[{"role":"user","content":"hi"}]}`
		if status, resp := h.chat(t, testToken, body); status != 200 {
			t.Fatalf("model=%s: got %d %s", model, status, resp)
		}
		if sent, _ := h.mock.last(); sent["model"] != nil {
			t.Fatalf("model=%s was forwarded: %s", model, sent["model"])
		}
	}
}

// The deny list reaches the first entry the way it reaches `model`.
func TestModelsFirstEntryAgainstDenyList(t *testing.T) {
	h := newHarness(t, func(d *snapshot.Document) {
		passthroughAllowlistDoc()(d)
		d.Keys[0].CreditDeniedModels = []string{"anthropic/*"}
	}, nil)

	status, body := h.chat(t, testToken, modelsOnly(`["anthropic/claude-sonnet-4"]`))
	if status != 403 || errCode(t, body) != "model_not_allowed" {
		t.Fatalf("denied first entry: %d %s", status, body)
	}
	if h.mock.callCount() != 0 {
		t.Fatal("a denied request reached the vendor")
	}
}

// Every entry is judged, the first one included, and a refusal is local.
func TestModelsWithoutModelIsFencedWhole(t *testing.T) {
	h := newHarness(t, passthroughAllowlistDoc("openai/*"), nil)

	status, body := h.chat(t, testToken, modelsOnly(`["anthropic/claude-sonnet-4","openai/gpt-4o-mini"]`))
	if status != 403 || errCode(t, body) != "model_not_allowed" {
		t.Fatalf("fenced first entry: %d %s", status, body)
	}
	status, body = h.chat(t, testToken, modelsOnly(`["openai/gpt-4o-mini","anthropic/claude-sonnet-4"]`))
	if status != 403 || errCode(t, body) != "model_not_allowed" {
		t.Fatalf("fenced later entry: %d %s", status, body)
	}
	if h.mock.callCount() != 0 {
		t.Fatalf("a fenced request reached the vendor %d times", h.mock.callCount())
	}
}

// The naming rules `model` answers to apply to the first entry.
func TestModelsFirstEntryNamingRules(t *testing.T) {
	h := newHarness(t, passthroughAllowlistDoc("openai/*"), nil)

	for _, tc := range []struct {
		name, models string
		status       int
		code         string
	}{
		{"preset", `["@preset/mine"]`, 400, "preset_not_supported"},
		{"router under a fence", `["openrouter/auto"]`, 403, "model_not_allowed"},
		{"reserved self-serve name", `["pickle-generall"]`, 404, "model_not_found"},
		{"self-served model as a candidate", `["pickle-general"]`, 400, "unsupported_parameter"},
		{"empty first entry", `["","openai/gpt-4o-mini"]`, 400, "invalid_parameter_value"},
		{"not a list of names", `[1,2]`, 400, "invalid_parameter_value"},
		{"empty list", `[]`, 400, "missing_parameter"},
		{"null list", `null`, 400, "missing_parameter"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, body := h.chat(t, testToken, modelsOnly(tc.models))
			if status != tc.status || (tc.code != "" && errCode(t, body) != tc.code) {
				t.Fatalf("got %d %s, want %d %s", status, body, tc.status, tc.code)
			}
		})
	}
	if h.mock.callCount() != 0 {
		t.Fatalf("a refused request reached the vendor %d times", h.mock.callCount())
	}
}

// An explicit model is unchanged: `model` in its upstream name, the list as
// sent.
func TestModelWithModelsIsUnchanged(t *testing.T) {
	h := newHarness(t, passthroughAllowlistDoc(), nil)

	body := `{"model":"openai/gpt-4o","models":["openai/gpt-4o-mini"],"messages":[{"role":"user","content":"hi"}]}`
	if status, resp := h.chat(t, testToken, body); status != 200 {
		t.Fatalf("got %d %s", status, resp)
	}
	sent, _ := h.mock.last()
	var model string
	var rest []string
	_ = json.Unmarshal(sent["model"], &model)
	_ = json.Unmarshal(sent["models"], &rest)
	if model != "openai/gpt-4o" || len(rest) != 1 || rest[0] != "openai/gpt-4o-mini" {
		t.Fatalf("upstream got model=%q models=%v", model, rest)
	}
}

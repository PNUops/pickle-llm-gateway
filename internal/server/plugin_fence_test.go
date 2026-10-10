package server

import (
	"net/http"
	"strings"
	"testing"

	"github.com/pnuops/pickle-llm-gateway/internal/snapshot"
)

// The `plugins` field and server tools left without a model are the seventh
// channel around the money fence: each lets the vendor pick or steer a model
// the fence never judged. The rule is the one the other six follow — a key
// with no model list passes, a fenced key has every name judged, and what
// cannot be judged is refused.

func fencedPluginDoc(d *snapshot.Document) {
	creditPassthroughDoc(d)
	d.Keys[0].CreditDeniedModels = []string{"openai/*-pro", "anthropic/*"}
	d.Keys[0].PassthroughEndpoints = []string{snapshot.EndpointImages}
}

func unfencedPluginDoc(d *snapshot.Document) {
	creditPassthroughDoc(d)
	d.Keys[0].PassthroughEndpoints = []string{snapshot.EndpointImages}
}

func chatWith(field, value string) string {
	return `{"model":"vendor-model","messages":[],"` + field + `":` + value + `}`
}

// expectFenceRefusal checks a refusal from this channel: 403, the shared code,
// advice naming where the model was, nothing upstream, and the existing spool
// type so the contract enumeration does not grow.
func expectFenceRefusal(t *testing.T, h *harness, body, where string) {
	t.Helper()
	status, resp := h.chat(t, testToken, body)
	if status != http.StatusForbidden || errCode(t, resp) != "model_not_allowed" {
		t.Fatalf("%s: %d %s", body, status, resp)
	}
	if msg := errMessage(t, resp); !strings.Contains(msg, where) {
		t.Fatalf("the refusal has to say where the name was (%s): %s", where, msg)
	}
	if h.mock.callCount() != 0 {
		t.Fatalf("%s reached the upstream", body)
	}
	evs := h.spoolEvents(t)
	if len(evs) == 0 || evs[len(evs)-1].ErrorType != "credit_model_not_allowed" {
		t.Fatalf("spooled %+v", evs)
	}
}

func expectPassed(t *testing.T, h *harness, body string) {
	t.Helper()
	if status, resp := h.chat(t, testToken, body); status != 200 {
		t.Fatalf("%s: %d %s", body, status, resp)
	}
}

// Router plugins steer a router's choice with patterns or a score, so a fenced
// key cannot have them judged. Every documented router id is covered, in the
// spellings a prefix or case test would miss.
func TestRouterPluginsRefusedOnFencedKey(t *testing.T) {
	for _, plugin := range []string{
		`{"id":"auto-router","allowed_models":["anthropic/*"]}`,
		`{"id":"auto-router"}`,
		`{"id":"auto-beta-router","allowed_models":["openai/gpt-5"]}`,
		`{"id":"pareto-router","min_coding_score":0.9}`,
		`{"id":"jev-router","models":["openai/*"]}`,
		`{"id":"switchyard-router","judge_model":"openai/gpt-5.4-nano"}`,
		`{"id":" Auto-Router "}`,
	} {
		t.Run(plugin, func(t *testing.T) {
			h := newHarness(t, fencedPluginDoc, nil)
			expectFenceRefusal(t, h, chatWith("plugins", "["+plugin+"]"), "plugins")
		})
	}
}

// A key with no list is bounded only by its money, so the routers stay open.
func TestRouterPluginsPassOnUnfencedKey(t *testing.T) {
	h := newHarness(t, unfencedPluginDoc, nil)
	for _, plugin := range []string{
		`{"id":"auto-router","allowed_models":["anthropic/*"]}`,
		`{"id":"pareto-router"}`,
		`{"id":"switchyard-router","judge_model":"openai/o1-pro"}`,
	} {
		expectPassed(t, h, chatWith("plugins", "["+plugin+"]"))
	}
}

// fusion names its analyst and panel outright, so they are judged rather than
// refused, and its preset is refused on a fenced key because it expands on the
// vendor's side.
func TestFusionPluginModelsAreFenced(t *testing.T) {
	for _, tc := range []struct{ name, plugin string }{
		{"analyst", `{"id":"fusion","model":"openai/o1-pro","analysis_models":["openai/gpt-5"]}`},
		{"panel", `{"id":"fusion","model":"openai/gpt-5","analysis_models":["openai/gpt-5","anthropic/claude-opus-4"]}`},
		{"floating alias", `{"id":"fusion","analysis_models":["~anthropic/claude-opus-latest"]}`},
		{"preset", `{"id":"fusion","preset":"general-high"}`},
		{"nested tool", `{"id":"fusion","tools":[{"type":"openrouter:advisor","parameters":{"model":"openai/o1-pro"}}]}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, fencedPluginDoc, nil)
			body := chatWith("plugins", "["+tc.plugin+"]")
			status, resp := h.chat(t, testToken, body)
			if status != http.StatusForbidden || errCode(t, resp) != "model_not_allowed" {
				t.Fatalf("%d %s", status, resp)
			}
			if h.mock.callCount() != 0 {
				t.Fatal("reached the upstream")
			}
		})
	}

	// Allowed names travel on a fenced key; on an unfenced key so does
	// everything, the preset included.
	h := newHarness(t, fencedPluginDoc, nil)
	expectPassed(t, h, chatWith("plugins",
		`[{"id":"fusion","model":"openai/gpt-5","analysis_models":["openai/gpt-5","google/gemini-pro"]}]`))
	h2 := newHarness(t, unfencedPluginDoc, nil)
	for _, plugin := range []string{
		`{"id":"fusion","model":"openai/o1-pro","analysis_models":["anthropic/claude-opus-4"]}`,
		`{"id":"fusion","preset":"general-high"}`,
	} {
		expectPassed(t, h2, chatWith("plugins", "["+plugin+"]"))
	}
}

// Left without a model, the advisor's model is chosen mid-generation by the
// executing model and fusion's comes from vendor defaults; neither appears in
// the request, so a fenced key must name them.
func TestServerToolsWithoutModelRefusedOnFencedKey(t *testing.T) {
	for _, tools := range []string{
		`[{"type":"openrouter:advisor"}]`,
		`[{"type":"openrouter:advisor","parameters":{"model":null}}]`,
		`[{"type":"openrouter:fusion","parameters":{"analysis_models":["openai/gpt-5"]}}]`,
		`[{"type":"openrouter:fusion","parameters":{"model":"openai/gpt-5"}}]`,
		`[{"type":"OpenRouter:Advisor","parameters":{}}]`,
	} {
		t.Run(tools, func(t *testing.T) {
			h := newHarness(t, fencedPluginDoc, nil)
			expectFenceRefusal(t, h, chatWith("tools", tools), "tools")
		})
	}

	// The subagent falls back to the outer model, which is already judged.
	h := newHarness(t, fencedPluginDoc, nil)
	expectPassed(t, h, chatWith("tools", `[{"type":"openrouter:subagent"}]`))
	expectPassed(t, h, chatWith("tools",
		`[{"type":"openrouter:fusion","parameters":{"model":"openai/gpt-5","analysis_models":["openai/gpt-5"]}}]`))

	// Unfenced, defaults are the key's own business.
	h2 := newHarness(t, unfencedPluginDoc, nil)
	for _, tools := range []string{
		`[{"type":"openrouter:advisor"}]`,
		`[{"type":"openrouter:fusion"}]`,
	} {
		expectPassed(t, h2, chatWith("tools", tools))
	}
}

// The subagent names a worker and carries server tools for it; both run on
// this key.
func TestSubagentModelAndNestedToolsAreFenced(t *testing.T) {
	for _, tools := range []string{
		`[{"type":"openrouter:subagent","parameters":{"model":"openai/o1-pro"}}]`,
		`[{"type":"openrouter:subagent","parameters":{"tools":[{"type":"openrouter:advisor","parameters":{"model":"anthropic/claude-opus-4"}}]}}]`,
		`[{"type":"openrouter:fusion","parameters":{"model":"openai/gpt-5","analysis_models":["openai/gpt-5"],"tools":[{"type":"openrouter:advisor"}]}}]`,
	} {
		t.Run(tools, func(t *testing.T) {
			h := newHarness(t, fencedPluginDoc, nil)
			expectFenceRefusal(t, h, chatWith("tools", tools), "tools")
		})
	}

	// The image capability reaches a nested tool too.
	h := newHarness(t, creditPassthroughDoc, nil) // no image grant
	status, resp := h.chat(t, testToken, chatWith("tools",
		`[{"type":"openrouter:subagent","parameters":{"tools":[{"type":"openrouter:image_generation"}]}}]`))
	if status != http.StatusForbidden || errCode(t, resp) != "endpoint_not_allowed" {
		t.Fatalf("%d %s", status, resp)
	}

	h2 := newHarness(t, unfencedPluginDoc, nil)
	expectPassed(t, h2, chatWith("tools",
		`[{"type":"openrouter:subagent","parameters":{"model":"openai/o1-pro","tools":[{"type":"openrouter:web_search"}]}}]`))
	h3 := newHarness(t, fencedPluginDoc, nil)
	expectPassed(t, h3, chatWith("tools",
		`[{"type":"openrouter:subagent","parameters":{"model":"openai/gpt-5","tools":[{"type":"openrouter:web_search"}]}}]`))
}

// context-compression names no model and switches itself on without the
// field, so refusing the entry would not close it. It is forwarded on purpose
// until a live call shows which models it moves between.
func TestContextCompressionIsForwarded(t *testing.T) {
	h := newHarness(t, fencedPluginDoc, nil)
	expectPassed(t, h, chatWith("plugins", `[{"id":"context-compression","engine":"middle-out"}]`))
	expectPassed(t, h, chatWith("plugins", `[{"id":"web"},{"id":"response-healing"}]`))
}

// What cannot be read cannot be fenced.
func TestUnreadablePluginsIsRefused(t *testing.T) {
	h := newHarness(t, unfencedPluginDoc, nil)
	for _, body := range []string{
		chatWith("plugins", `"auto-router"`),
		chatWith("plugins", `[1]`),
		chatWith("plugins", `[{}]`),
		chatWith("plugins", `[{"id":1}]`),
		chatWith("plugins", `[{"id":"fusion","analysis_models":{"a":1}}]`),
		chatWith("plugins", `[{"id":"fusion","analysis_models":[1]}]`),
		chatWith("plugins", `[{"id":"fusion","tools":"web"}]`),
		chatWith("tools", `[{"type":"openrouter:subagent","parameters":{"tools":{"a":1}}}]`),
	} {
		if status, resp := h.chat(t, testToken, body); status != 400 {
			t.Fatalf("%s: %d %s", body, status, resp)
		}
	}
	if h.mock.callCount() != 0 {
		t.Fatal("an unreadable field reached the upstream")
	}
}

// Nesting past the bound fails closed rather than being forwarded unread.
func TestDeeplyNestedToolsAreRefused(t *testing.T) {
	h := newHarness(t, unfencedPluginDoc, nil)
	inner := `[{"type":"openrouter:web_search"}]`
	for i := 0; i <= maxNestedToolDepth; i++ {
		inner = `[{"type":"openrouter:subagent","parameters":{"tools":` + inner + `}}]`
	}
	if status, resp := h.chat(t, testToken, chatWith("tools", inner)); status != 400 {
		t.Fatalf("%d %s", status, resp)
	}
}

// The passthrough routes run the same fence.
func TestPassthroughFencesPlugins(t *testing.T) {
	h := newHarness(t, func(d *snapshot.Document) {
		passthroughDoc(snapshot.EndpointImages)(d)
		d.Keys[0].CreditDeniedModels = []string{"openai/*-pro"}
	}, nil)
	status, body := h.passthrough(t, http.MethodPost, "/v1/images", testToken,
		`{"model":"openai/gpt-image-1","prompt":"a cat","plugins":[{"id":"auto-router"}]}`)
	if status != http.StatusForbidden || errCode(t, body) != "model_not_allowed" {
		t.Fatalf("%d %s", status, body)
	}
	if h.mock.callCount() != 0 {
		t.Fatal("reached the upstream")
	}
}

// The vendor receives each tool's bytes verbatim and reads exact keys, so the
// fence must read the same keys. A case-variant duplicate once let the fence
// judge one key while the vendor read the other.
func TestCaseVariantToolKeysCannotSplitTheFence(t *testing.T) {
	for _, tools := range []string{
		`[{"type":"openrouter:advisor","Type":"function"}]`,
		`[{"type":"openrouter:advisor","parameters":{"model":"anthropic/claude-x"},"PARAMETERS":{"model":"openai/gpt-5"}}]`,
		`[{"type":"openrouter:subagent","parameters":{"tools":[{"type":"openrouter:advisor","Type":"function"}]}}]`,
	} {
		t.Run(tools, func(t *testing.T) {
			h := newHarness(t, fencedPluginDoc, nil)
			expectFenceRefusal(t, h, chatWith("tools", tools), "tools")
		})
	}
}

// An empty panel judges nothing and may fall back to the vendor's default, so
// on a fenced key it counts as no panel.
func TestEmptyPanelRefusedOnFencedKey(t *testing.T) {
	for _, tc := range []struct{ field, value string }{
		{"tools", `[{"type":"openrouter:fusion","parameters":{"model":"openai/gpt-5","analysis_models":[]}}]`},
		{"tools", `[{"type":"openrouter:advisor","parameters":{"model":""}}]`},
		{"plugins", `[{"id":"fusion","analysis_models":[ ]}]`},
	} {
		t.Run(tc.value, func(t *testing.T) {
			h := newHarness(t, fencedPluginDoc, nil)
			status, resp := h.chat(t, testToken, chatWith(tc.field, tc.value))
			if status != http.StatusForbidden || errCode(t, resp) != "model_not_allowed" {
				t.Fatalf("%d %s", status, resp)
			}
		})
	}
	h := newHarness(t, unfencedPluginDoc, nil)
	expectPassed(t, h, chatWith("plugins", `[{"id":"fusion","analysis_models":[]}]`))
}

// Surrounding space in a tool type does not take the tool out of the fence.
func TestPaddedToolTypeIsStillFenced(t *testing.T) {
	h := newHarness(t, fencedPluginDoc, nil)
	expectFenceRefusal(t, h, chatWith("tools", `[{"type":" openrouter:advisor "}]`), "tools")
}

// null is a field left out, so a key with no list passes it.
func TestNullModelPassesOnUnfencedKey(t *testing.T) {
	h := newHarness(t, unfencedPluginDoc, nil)
	expectPassed(t, h, chatWith("plugins", `[{"id":"fusion","model":null}]`))
	expectPassed(t, h, chatWith("tools", `[{"type":"openrouter:advisor","parameters":{"model":null}}]`))
}

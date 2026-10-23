package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/pnuops/pickle-llm-gateway/internal/config"
	"github.com/pnuops/pickle-llm-gateway/internal/snapshot"
)

func tokenToolsBody(stream bool, fields string) string {
	return fmt.Sprintf(`{"model":"pickle-general","messages":[{"role":"user","content":"hello"}],"stream":%t,%s}`, stream, fields)
}

// Rejections must happen before any shared upstream credential can be used.
// A key's commercial grants must not widen a TOKEN model's tool profile.
func TestTokenToolsRefuseExecutableShapesBeforeUpstream(t *testing.T) {
	cases := map[string]string{
		"hosted search":          `"tools":[{"type":"web_search"}]`,
		"vendor search":          `"tools":[{"type":"openrouter:web_search"}]`,
		"hosted image":           `"tools":[{"type":"image_generation"}]`,
		"hosted container":       `"tools":[{"type":"code_interpreter"}]`,
		"remote mcp":             `"tools":[{"type":"mcp","server_url":"https://example.com/mcp"}]`,
		"hosted subagent":        `"tools":[{"type":"openrouter:subagent","parameters":{"model":"other"}}]`,
		"namespace":              `"tools":[{"type":"namespace","name":"n","tools":[{"type":"web_search"}]}]`,
		"custom":                 `"tools":[{"type":"custom","custom":{"name":"f"}}]`,
		"unknown":                `"tools":[{"type":"future_tool"}]`,
		"type case":              `"tools":[{"type":"Function","function":{"name":"f"}}]`,
		"type whitespace":        `"tools":[{"type":" function ","function":{"name":"f"}}]`,
		"unknown type key":       `"tools":[{"Type":"web_search","type":"function","function":{"name":"f"}}]`,
		"missing type":           `"tools":[{"function":{"name":"f"}}]`,
		"null entry":             `"tools":[null]`,
		"scalar entry":           `"tools":[1]`,
		"nonarray list":          `"tools":{"type":"function","function":{"name":"f"}}`,
		"null function":          `"tools":[{"type":"function","function":null}]`,
		"missing function":       `"tools":[{"type":"function"}]`,
		"missing name":           `"tools":[{"type":"function","function":{}}]`,
		"invalid name":           `"tools":[{"type":"function","function":{"name":"bad name"}}]`,
		"tool routing peer":      `"tools":[{"type":"function","function":{"name":"f"},"model":"other"}]`,
		"tool nested peer":       `"tools":[{"type":"function","function":{"name":"f"},"parameters":{"tools":[{"type":"web_search"}]}}]`,
		"function model":         `"tools":[{"type":"function","function":{"name":"f","model":"other"}}]`,
		"function provider":      `"tools":[{"type":"function","function":{"name":"f","provider":{"only":["other"]}}}]`,
		"function tools":         `"tools":[{"type":"function","function":{"name":"f","tools":[{"type":"web_search"}]}}]`,
		"function cache":         `"tools":[{"type":"function","function":{"name":"f","cache_control":{"type":"ephemeral"}}}]`,
		"bad description":        `"tools":[{"type":"function","function":{"name":"f","description":7}}]`,
		"bad parameters":         `"tools":[{"type":"function","function":{"name":"f","parameters":[]}}]`,
		"bad strict":             `"tools":[{"type":"function","function":{"name":"f","strict":"true"}}]`,
		"duplicate type":         `"tools":[{"type":"web_search","type":"function","function":{"name":"f"}}]`,
		"escaped duplicate":      `"tools":[{"type":"web_search","t\u0079pe":"function","function":{"name":"f"}}]`,
		"duplicate function":     `"tools":[{"type":"function","function":{"name":"x","model":"other"},"function":{"name":"f"}}]`,
		"duplicate name":         `"tools":[{"type":"function","function":{"name":"other","name":"f"}}]`,
		"hosted choice":          `"tool_choice":{"type":"web_search"}`,
		"unknown choice string":  `"tool_choice":"web_search"`,
		"missing choice type":    `"tool_choice":{"function":{"name":"f"}}`,
		"choice without tool":    `"tool_choice":{"type":"function","function":{"name":"f"}}`,
		"choice routing peer":    `"tools":[{"type":"function","function":{"name":"f"}}],"tool_choice":{"type":"function","function":{"name":"f"},"model":"other"}`,
		"choice function peer":   `"tools":[{"type":"function","function":{"name":"f"}}],"tool_choice":{"type":"function","function":{"name":"f","tools":[]}}`,
		"duplicate choice type":  `"tools":[{"type":"function","function":{"name":"f"}}],"tool_choice":{"type":"web_search","type":"function","function":{"name":"f"}}`,
		"allowed hosted":         `"tool_choice":{"type":"allowed_tools","allowed_tools":{"mode":"auto","tools":[{"type":"web_search"}]}}`,
		"allowed unknown name":   `"tools":[{"type":"function","function":{"name":"f"}}],"tool_choice":{"type":"allowed_tools","allowed_tools":{"mode":"required","tools":[{"type":"function","function":{"name":"other"}}]}}`,
		"allowed extra peer":     `"tools":[{"type":"function","function":{"name":"f"}}],"tool_choice":{"type":"allowed_tools","allowed_tools":{"mode":"auto","tools":[{"type":"function","function":{"name":"f"}}],"provider":{}}}`,
		"allowed duplicate mode": `"tools":[{"type":"function","function":{"name":"f"}}],"tool_choice":{"type":"allowed_tools","allowed_tools":{"mode":"none","mode":"auto","tools":[{"type":"function","function":{"name":"f"}}]}}`,
	}
	for _, stream := range []bool{false, true} {
		for name, fields := range cases {
			t.Run(fmt.Sprintf("stream=%t/%s", stream, name), func(t *testing.T) {
				h := newHarness(t, func(d *snapshot.Document) {
					d.Keys[0].PassthroughEndpoints = []string{snapshot.EndpointChat, snapshot.EndpointWebSearch, snapshot.EndpointImages, snapshot.EndpointContainers}
				}, nil)
				status, out := h.chat(t, testToken, tokenToolsBody(stream, fields))
				if status != http.StatusBadRequest {
					t.Errorf("status=%d, want 400: %s", status, out)
				}
				if calls := h.mock.callCount(); calls != 0 {
					t.Errorf("a locally refused tool reached the upstream %d times", calls)
				}
				events := h.spoolEvents(t)
				if len(events) != 1 || events[0].BudgetAxis != snapshot.AxisToken || events[0].Attempts != 0 || events[0].UpstreamRef != "" {
					t.Errorf("refusal must preserve TOKEN axis and zero attempts: %+v", events)
				}
			})
		}
	}
}

func TestTokenFunctionToolsPreserveSchemaData(t *testing.T) {
	const schema = `{"type":"object","properties":{"model":{"type":"string"},"provider":{"type":"string"},"tools":{"type":"array"}},"$defs":{"web_search":{"type":"object"}},"default":{"type":"web_search","model":"other","provider":{"only":["x"]},"tools":[{"type":"mcp"}]},"examples":[{"type":"namespace","tools":[]}],"model":"first","model":"last"}`
	for _, stream := range []bool{false, true} {
		for _, name := range []string{"web_search", "bash", "mcp"} {
			for _, choice := range []string{`"auto"`, `"none"`, `"required"`, `null`, `{"type":"function","function":{"name":"` + name + `"}}`, `{"type":"allowed_tools","allowed_tools":{"mode":"auto","tools":[{"type":"function","function":{"name":"` + name + `"}}]}}`} {
				t.Run(fmt.Sprintf("stream=%t/%s/%s", stream, name, choice), func(t *testing.T) {
					h := newHarness(t, nil, nil)
					fields := `"tools":[{"type":"function","function":{"name":"` + name + `","description":"client function","parameters":` + schema + `,"strict":null}}],"tool_choice":` + choice
					status, out := h.chat(t, testToken, tokenToolsBody(stream, fields))
					if status != http.StatusOK || h.mock.callCount() != 1 {
						t.Fatalf("ordinary function was refused: %d %s", status, out)
					}
					_, _, _, raw := h.mock.lastRequest()
					if !bytes.Contains(raw, []byte(schema)) {
						t.Fatalf("schema data was changed: %s", raw)
					}
					events := h.spoolEvents(t)
					if len(events) != 1 || events[0].BudgetAxis != snapshot.AxisToken || events[0].Attempts != 1 || events[0].InputTokens+events[0].OutputTokens == 0 {
						t.Fatalf("function usage lost ordinary TOKEN accounting: %+v", events)
					}
				})
			}
		}
	}
}

func TestTokenToolsAllowEmptyAndOptionalShapes(t *testing.T) {
	for _, fields := range []string{
		`"tools":null,"tool_choice":null`,
		`"tools":[],"tool_choice":"none"`,
		`"tools":[{"type":"function","function":{"name":"f"}}]`,
		`"tools":[{"type":"function","function":{"name":"f","description":null,"parameters":null,"strict":false}}]`,
	} {
		h := newHarness(t, nil, nil)
		if status, out := h.chat(t, testToken, tokenToolsBody(false, fields)); status != http.StatusOK {
			t.Errorf("%s: %d %s", fields, status, out)
		}
	}
}

func tokenToolFallbackHarness(t *testing.T) (*harness, *upstreamMock) {
	t.Helper()
	fallback := &upstreamMock{}
	fb := httptest.NewServer(http.HandlerFunc(fallback.handler))
	t.Cleanup(fb.Close)
	h := newHarness(t, func(d *snapshot.Document) {
		d.Models[0].FallbackRef = "backup"
	}, func(c *config.Config) {
		c.UpstreamRetries = 0
		c.Upstreams["backup"] = config.Upstream{Ref: "backup", BaseURL: fb.URL, APIKey: "other-shared-secret", CapField: "max_completion_tokens"}
	})
	return h, fallback
}

func TestTokenToolsStayFencedAcrossFallbackAndCooldown(t *testing.T) {
	for _, stream := range []bool{false, true} {
		for _, cooling := range []bool{false, true} {
			t.Run(fmt.Sprintf("stream=%t/cooling=%t", stream, cooling), func(t *testing.T) {
				h, fallback := tokenToolFallbackHarness(t)
				h.mock.set(func(o *mockOpts) { o.status = http.StatusBadGateway })
				if cooling {
					for range 3 {
						h.srv.health.recordFailure("mock")
					}
					if !h.srv.health.cooling("mock") {
						t.Fatal("primary did not enter cooldown")
					}
				}
				bad := `"tools":[{"type":"mcp","server_url":"https://example.com/mcp"}]`
				if status, out := h.chat(t, testToken, tokenToolsBody(stream, bad)); status != http.StatusBadRequest {
					t.Errorf("unsafe fallback request: %d %s", status, out)
				}
				if h.mock.callCount() != 0 || fallback.callCount() != 0 {
					t.Errorf("unsafe request reached primary=%d/fallback=%d", h.mock.callCount(), fallback.callCount())
				}
				// A normal function still reaches the configured fallback. No
				// routing choice, model rewrite or credential binding changes.
				valid := `"tools":[{"type":"function","function":{"name":"web_search","parameters":{"type":"object","properties":{"model":{"type":"string"}}}}}],"tool_choice":{"type":"function","function":{"name":"web_search"}}`
				beforePrimary, beforeFallback := h.mock.callCount(), fallback.callCount()
				if status, out := h.chat(t, testToken, tokenToolsBody(stream, valid)); status != http.StatusOK {
					t.Fatalf("safe fallback request: %d %s", status, out)
				}
				wantPrimary := 1
				if cooling {
					wantPrimary = 0
				}
				if h.mock.callCount()-beforePrimary != wantPrimary || fallback.callCount()-beforeFallback != 1 {
					t.Fatalf("unexpected primary/fallback calls: %d/%d", h.mock.callCount()-beforePrimary, fallback.callCount()-beforeFallback)
				}
				body, cred := fallback.last()
				if string(body["model"]) != `"`+upstreamModel+`"` || cred != "Bearer other-shared-secret" {
					t.Fatalf("fallback target or credential changed: model=%s auth=%q", body["model"], cred)
				}
				var sentTools []map[string]json.RawMessage
				if json.Unmarshal(body["tools"], &sentTools) != nil || len(sentTools) != 1 || string(sentTools[0]["type"]) != `"function"` {
					t.Fatalf("fallback did not receive the ordinary function: %s", body["tools"])
				}
			})
		}
	}
}

func TestTokenToolProfileDoesNotChangeCreditTools(t *testing.T) {
	const model = "anthropic/claude-sonnet-4.5"
	for _, stream := range []bool{false, true} {
		for _, tool := range []string{
			`{"type":"custom","custom":{"name":"f"}}`,
			`{"type":"mcp","server_label":"s","server_url":"https://example.com/mcp"}`,
			`{"type":"namespace","name":"n","tools":[{"type":"function","function":{"name":"f"}}]}`,
		} {
			h := newHarness(t, creditPassthroughDoc, nil)
			body := strings.Replace(tokenToolsBody(stream, `"tools":[`+tool+`]`), "pickle-general", model, 1)
			if status, out := h.chat(t, testToken, body); status != http.StatusOK || h.mock.callCount() != 1 {
				t.Errorf("CREDIT tool profile changed: %d %s", status, out)
			}
		}
	}
}

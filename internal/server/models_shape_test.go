package server

import (
	"encoding/json"
	"io"
	"net/http"
	"testing"

	"github.com/pnuops/pickle-llm-gateway/internal/snapshot"
)

func getModels(t *testing.T, h *harness, path, token string, anthropic bool) (int, []byte) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, h.gw.URL+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if anthropic {
		req.Header.Set("Anthropic-Version", "2023-06-01")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, out
}

func TestModelsAnthropicShape(t *testing.T) {
	h := newHarness(t, creditDoc, nil)
	status, body := getModels(t, h, "/v1/models?limit=1000", testToken, true)
	if status != 200 {
		t.Fatalf("%d %s", status, body)
	}
	var page struct {
		Data []struct {
			Type, ID, DisplayName, CreatedAt string
		} `json:"data"`
		HasMore *bool   `json:"has_more"`
		FirstID *string `json:"first_id"`
		LastID  *string `json:"last_id"`
		Object  *string `json:"object"`
	}
	if err := json.Unmarshal(body, &page); err != nil {
		t.Fatal(err)
	}
	var raw struct {
		Data []map[string]any `json:"data"`
	}
	_ = json.Unmarshal(body, &raw)
	if len(page.Data) != 2 || page.HasMore == nil || *page.HasMore || page.Object != nil {
		t.Fatalf("page: %s", body)
	}
	first := raw.Data[0]
	if first["type"] != "model" || first["id"] != "pickle-general" || first["display_name"] != "pickle-general" ||
		first["created_at"] != "2026-01-01T00:00:00Z" {
		t.Fatalf("entry: %v", first)
	}
	if *page.FirstID != "pickle-general" || *page.LastID != "vendor-model" {
		t.Fatalf("page ends: %s", body)
	}

	// The same route without the header keeps the OpenAI shape.
	status, body = getModels(t, h, "/v1/models", testToken, false)
	var list struct {
		Object string           `json:"object"`
		Data   []map[string]any `json:"data"`
	}
	if status != 200 || json.Unmarshal(body, &list) != nil || list.Object != "list" ||
		len(list.Data) != 2 || list.Data[0]["object"] != "model" {
		t.Fatalf("openai shape: %d %s", status, body)
	}
}

func TestModelsAnthropicEmptyPage(t *testing.T) {
	h := newHarness(t, func(d *snapshot.Document) { d.Models = []snapshot.Model{} }, nil)
	status, body := getModels(t, h, "/v1/models", testToken, true)
	if status != 200 || string(body) != `{"data":[],"has_more":false,"first_id":null,"last_id":null}`+"\n" {
		t.Fatalf("%d %q", status, body)
	}
}

func TestModelsAnthropicRetrieveAndRefusals(t *testing.T) {
	h := newHarness(t, creditDoc, nil)
	status, body := getModels(t, h, "/v1/models/vendor-model", testToken, true)
	var one map[string]any
	if status != 200 || json.Unmarshal(body, &one) != nil || one["type"] != "model" || one["id"] != "vendor-model" {
		t.Fatalf("retrieve: %d %s", status, body)
	}
	cases := []struct {
		path, token string
		status      int
		typ, code   string
	}{
		{"/v1/models/nope", testToken, 404, "not_found_error", "model_not_found"},
		{"/v1/models", "", 401, "authentication_error", "missing_api_key"},
	}
	for _, c := range cases {
		status, body := getModels(t, h, c.path, c.token, true)
		var e struct {
			Type  string `json:"type"`
			Error struct {
				Type, Code string
			} `json:"error"`
		}
		if json.Unmarshal(body, &e) != nil || status != c.status || e.Type != "error" ||
			e.Error.Type != c.typ || e.Error.Code != c.code {
			t.Fatalf("%s: %d %s", c.path, status, body)
		}
	}
	// Without the header the refusal keeps the OpenAI envelope.
	status, body = getModels(t, h, "/v1/models/nope", testToken, false)
	if status != 404 || errCode(t, body) != "model_not_found" {
		t.Fatalf("openai refusal: %d %s", status, body)
	}
}

// An Anthropic SDK lists models with the key in x-api-key, as it sends its
// messages. Without the header the route reads only the bearer, as before.
func TestModelsAnthropicReadsAPIKeyHeader(t *testing.T) {
	h := newHarness(t, creditDoc, nil)
	get := func(anthropic bool) (int, []byte) {
		req, err := http.NewRequest(http.MethodGet, h.gw.URL+"/v1/models", nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("X-Api-Key", testToken)
		if anthropic {
			req.Header.Set("Anthropic-Version", "2023-06-01")
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		out, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, out
	}
	if status, body := get(true); status != 200 {
		t.Fatalf("x-api-key with anthropic-version: %d %s", status, body)
	}
	status, body := get(false)
	if status != 401 || errCode(t, body) != "missing_api_key" {
		t.Fatalf("x-api-key alone: %d %s", status, body)
	}
	// The Anthropic refusal names both headers, as on /v1/messages.
	status, body = getModels(t, h, "/v1/models", "", true)
	var e struct {
		Error struct{ Message string } `json:"error"`
	}
	if status != 401 || json.Unmarshal(body, &e) != nil || e.Error.Message != errMissingKeyMessages.message {
		t.Fatalf("missing key: %d %s", status, body)
	}
}

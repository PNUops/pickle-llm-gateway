package server

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/pnuops/pickle-llm-gateway/internal/spool"
)

func TestLocalFencesReportKnownZeroAttempts(t *testing.T) {
	for _, passthrough := range []bool{false, true} {
		t.Run(map[bool]string{false: "protocol", true: "passthrough"}[passthrough], func(t *testing.T) {
			var h *harness
			if passthrough {
				h = newHarness(t, passthroughDoc(), nil)
			} else {
				h = newHarness(t, nil, nil)
			}
			if passthrough {
				status, _ := h.passthrough(t, http.MethodPost, "/v1/images", testToken, imageBody)
				if status != http.StatusForbidden {
					t.Fatalf("local endpoint fence = %d, want 403", status)
				}
			} else {
				status, _ := h.chat(t, testToken, `{"model":"missing-model","messages":[{"role":"user","content":"hello"}]}`)
				if status != http.StatusNotFound {
					t.Fatalf("local model fence = %d, want 404", status)
				}
			}
			if h.mock.callCount() != 0 {
				t.Fatal("a local fence contacted an upstream")
			}
			events := h.spoolEvents(t)
			if len(events) != 1 {
				t.Fatalf("events = %d, want one", len(events))
			}
			wire, err := json.Marshal(events[0])
			if err != nil {
				t.Fatal(err)
			}
			var body map[string]any
			if err := json.Unmarshal(wire, &body); err != nil {
				t.Fatal(err)
			}
			if value, exists := body["attempts"]; !exists || value != float64(0) {
				t.Fatalf("local attempts = %v (present=%t), want explicit zero", value, exists)
			}
			if events[0].UpstreamRef != "" {
				t.Fatal("local refusal invented an upstream identity")
			}
		})
	}
}

func recordedAttempts(event spool.Event) int {
	if event.Attempts == nil {
		return -1
	}
	return *event.Attempts
}

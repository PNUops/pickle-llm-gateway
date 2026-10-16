package server

import (
	"strings"
	"testing"
)

// The vendor names thinking output thinking_tokens on the Messages route.
func TestMessagesRecordsThinkingTokens(t *testing.T) {
	h, v := messagesHarness(t, nil)
	v.answer = strings.Replace(vendorMessageAnswer, `"cost":0.0012`,
		`"cost":0.0012,"output_tokens_details":{"thinking_tokens":3}`, 1)
	if status, body, _ := sendMessages(t, h, nil, messagesFor("vendor-model")); status != 200 {
		t.Fatalf("%d %s", status, body)
	}
	if ev := lastEvent(t, h); ev.ReasoningTokens != 3 {
		t.Fatalf("non-stream record: %+v", ev)
	}

	v.stream = vendorEvent("message_start", vendorStreamStart) +
		vendorEvent("message_delta", `{"type":"message_delta","delta":{"stop_reason":"end_turn"},`+
			`"usage":{"output_tokens":7,"output_tokens_details":{"thinking_tokens":5}}}`) +
		vendorEvent("message_stop", vendorStreamStop)
	if status, body, _ := sendMessages(t, h, nil,
		`{"model":"vendor-model","stream":true,"messages":[{"role":"user","content":"hi"}]}`); status != 200 {
		t.Fatalf("%d %s", status, body)
	}
	if ev := lastEvent(t, h); ev.ReasoningTokens != 5 || ev.OutputTokens != 7 {
		t.Fatalf("stream record: %+v", ev)
	}
	// The chat spelling is still read where a provider sends it.
	v.answer = strings.Replace(vendorMessageAnswer, `"cost":0.0012`,
		`"cost":0.0012,"output_tokens_details":{"reasoning_tokens":2}`, 1)
	_, _, _ = sendMessages(t, h, nil, messagesFor("vendor-model"))
	if ev := lastEvent(t, h); ev.ReasoningTokens != 2 {
		t.Fatalf("reasoning_tokens record: %+v", ev)
	}
}

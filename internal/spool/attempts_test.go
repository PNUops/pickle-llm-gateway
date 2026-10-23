package spool

import (
	"encoding/json"
	"testing"
)

func TestAttemptPresenceSurvivesLegacySpoolReshipping(t *testing.T) {
	for _, tc := range []struct {
		name, wire string
		present    bool
		value      float64
	}{
		{"legacy unknown", `{"eventUuid":"legacy","status":"RATE_LIMITED"}`, false, 0},
		{"explicit unknown", `{"eventUuid":"unknown","status":"RATE_LIMITED","attempts":null}`, false, 0},
		{"known zero", `{"eventUuid":"local","status":"RATE_LIMITED","attempts":0}`, true, 0},
		{"known positive", `{"eventUuid":"remote","status":"OK","attempts":2}`, true, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var event Event
			if err := json.Unmarshal([]byte(tc.wire), &event); err != nil {
				t.Fatal(err)
			}
			// Typed readers must retain presence when re-encoding an archive.
			// The current reporter separately forwards raw JSON messages.
			batch, err := json.Marshal([]Event{event})
			if err != nil {
				t.Fatal(err)
			}
			var sent []map[string]any
			if err := json.Unmarshal(batch, &sent); err != nil {
				t.Fatal(err)
			}
			value, present := sent[0]["attempts"]
			if present != tc.present || present && value != tc.value {
				t.Fatalf("attempt presence/value = %t/%v, want %t/%v", present, value, tc.present, tc.value)
			}
		})
	}
}

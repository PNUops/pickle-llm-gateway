package snapshot

import (
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"testing"
)

func TestCreditModelPolicyCases(t *testing.T) {
	var cases struct {
		PatternCases []struct {
			Pattern, Name string
			Expected      bool
		}
		PolicyCases []struct {
			Allowed, Denied []string
			Name            string
			Expected        bool
		}
		ValidPatterns, InvalidPatterns []string
	}
	raw, err := os.ReadFile("testdata/credit_model_policy_cases.json")
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &cases); err != nil {
		t.Fatal(err)
	}
	if len(cases.PatternCases) == 0 || len(cases.PolicyCases) == 0 || len(cases.ValidPatterns) == 0 || len(cases.InvalidPatterns) == 0 {
		t.Fatal("policy cases must include matching, policies and valid/invalid patterns")
	}
	for _, tc := range cases.PatternCases {
		if got := MatchesCreditModel(tc.Pattern, tc.Name); got != tc.Expected {
			t.Errorf("MatchesCreditModel(%q, %q) = %v, want %v", tc.Pattern, tc.Name, got, tc.Expected)
		}
	}
	for _, tc := range cases.PolicyCases {
		key := Key{CreditAllowedModels: tc.Allowed, CreditDeniedModels: tc.Denied}
		if got := key.AllowsCreditModel(creditModel(tc.Name)); got != tc.Expected {
			t.Errorf("policy allowed=%v denied=%v for %q = %v, want %v", tc.Allowed, tc.Denied, tc.Name, got, tc.Expected)
		}
	}
	for _, pattern := range cases.ValidPatterns {
		if !creditModelPattern.MatchString(pattern) {
			t.Errorf("valid pattern %q was rejected", pattern)
		}
	}
	for _, pattern := range cases.InvalidPatterns {
		if creditModelPattern.MatchString(pattern) {
			t.Errorf("invalid pattern %q was accepted", pattern)
		}
	}
}

func TestProviderWildcardSurvivesRawDocumentLoad(t *testing.T) {
	for _, field := range []string{"creditAllowedModels", "creditDeniedModels"} {
		t.Run(field, func(t *testing.T) {
			hash := HashToken(field)
			body := fmt.Sprintf(`{"generation":1,"serviceEnabled":true,
			  "models":[{"publicName":"pickle-general","upstreamRef":"mock","upstreamModel":"m"}],
			  "keys":[{"keyId":"k","tokenHash":%q,"status":"ACTIVE","limits":{},
			           %q:["*/GPT-5"," */gpt-5-* ","*/*-pro","*/*"]}]}`, hash, field)
			s := openDoc(t, body)
			_, byHash, _ := s.Current()
			key := byHash(hash)
			if key == nil || s.RejectedEntries() != 0 {
				t.Fatalf("valid wildcard patterns dropped the key: %d rejected entries", s.RejectedEntries())
			}
			got := key.CreditAllowedModels
			if field == "creditDeniedModels" {
				got = key.CreditDeniedModels
			}
			if want := []string{"*/gpt-5", "*/gpt-5-*", "*/*-pro", "*/*"}; !slices.Equal(got, want) {
				t.Fatalf("loaded patterns = %v, want %v", got, want)
			}
		})
	}
}

func TestProviderPolicyFromGeneratedSyncDocument(t *testing.T) {
	raw, err := os.ReadFile("testdata/credit_model_sync.json")
	if err != nil {
		t.Fatal(err)
	}
	st, err := build(raw, known("openrouter"), true)
	if err != nil {
		t.Fatal(err)
	}
	if st.rejected != 0 || len(st.byHash) != 1 {
		t.Fatalf("sync document loaded %d keys with %d rejected entries", len(st.byHash), st.rejected)
	}
	for _, key := range st.byHash {
		if !slices.Equal(key.CreditAllowedModels, []string{"*/*"}) || !slices.Equal(key.CreditDeniedModels, []string{"*/*-pro"}) {
			t.Fatalf("sync document changed policy: allowed=%v denied=%v", key.CreditAllowedModels, key.CreditDeniedModels)
		}
		if key.AllowsCreditModel(creditModel("~other/model-pro:batch")) {
			t.Fatal("the generated policy admitted a denied alias variant")
		}
		if !key.AllowsCreditModel(creditModel("~other/model")) {
			t.Fatal("the generated policy refused an allowed alias")
		}
		if key.AllowsCreditModel(creditModel("openrouter/auto")) {
			t.Fatal("the generated policy admitted a router")
		}
	}
}

func TestInvalidProviderWildcardDropsKey(t *testing.T) {
	for _, field := range []string{"creditAllowedModels", "creditDeniedModels"} {
		for _, pattern := range []string{"open*/*", "~*/*", "*openai/*", "**/*", "*", "*/", "*/*gpt*", "*/a/b"} {
			t.Run(field+"/"+pattern, func(t *testing.T) {
				hash := HashToken(field + pattern)
				body := fmt.Sprintf(`{"generation":1,"serviceEnabled":true,
				  "models":[{"publicName":"pickle-general","upstreamRef":"mock","upstreamModel":"m"}],
				  "keys":[{"keyId":"k","tokenHash":%q,"status":"ACTIVE","limits":{},%q:[%q]}]}`, hash, field, pattern)
				s := openDoc(t, body)
				_, byHash, _ := s.Current()
				if byHash(hash) != nil || s.RejectedEntries() == 0 {
					t.Fatal("the key with an invalid pattern was not rejected")
				}
			})
		}
	}
}

func TestProviderWildcardDoesNotGovernTokenAxis(t *testing.T) {
	key := Key{CreditAllowedModels: []string{"*/*"}, CreditDeniedModels: []string{"*/*"}}
	if !key.AllowsCreditModel(&Model{PublicName: "pickle-general", BudgetAxis: AxisToken}) {
		t.Fatal("paid model lists refused a self-serving model")
	}
}

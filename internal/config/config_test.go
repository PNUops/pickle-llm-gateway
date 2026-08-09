package config

import (
	"strings"
	"testing"
	"time"
)

func setRequired(t *testing.T) {
	t.Setenv("LLMGW_LISTEN", "127.0.0.1:0")
	t.Setenv("LLMGW_SNAPSHOT_PATH", "/tmp/snapshot.json")
	t.Setenv("LLMGW_SPOOL_DIR", "/tmp/spool")
	t.Setenv("LLMGW_UPSTREAM_OPENAI_BASE_URL", "https://api.openai.example/v1/")
	t.Setenv("LLMGW_UPSTREAM_OPENAI_API_KEY", "sk-test")
}

func TestFromEnvDefaults(t *testing.T) {
	setRequired(t)
	cfg, err := FromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.SnapshotPollInterval != 5*time.Second || cfg.DefaultRpm != 20 || cfg.MaxInFlight != 64 {
		t.Fatalf("defaults not applied: %+v", cfg)
	}
	up, ok := cfg.Upstreams["openai"]
	if !ok || up.APIKey != "sk-test" {
		t.Fatalf("upstream not parsed: %+v", cfg.Upstreams)
	}
	if strings.HasSuffix(up.BaseURL, "/") {
		t.Fatalf("base URL keeps trailing slash: %s", up.BaseURL)
	}
}

func TestFromEnvReportsAllMissing(t *testing.T) {
	// Only one variable set: every other requirement must appear in the error.
	t.Setenv("LLMGW_LISTEN", "127.0.0.1:0")
	_, err := FromEnv()
	if err == nil {
		t.Fatal("config accepted without snapshot, spool and upstream")
	}
	for _, want := range []string{"SNAPSHOT_PATH", "SPOOL_DIR", "UPSTREAM"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error does not mention %s: %v", want, err)
		}
	}
}

func TestFromEnvRejectsBadValues(t *testing.T) {
	setRequired(t)
	t.Setenv("LLMGW_DEFAULT_RPM", "-3")
	if _, err := FromEnv(); err == nil {
		t.Fatal("negative rpm accepted")
	}
	setRequired(t)
	t.Setenv("LLMGW_DEFAULT_RPM", "")
	t.Setenv("LLMGW_SNAPSHOT_POLL_INTERVAL", "soon")
	if _, err := FromEnv(); err == nil {
		t.Fatal("unparseable duration accepted")
	}
}

func TestUpstreamKeyWithoutBaseURL(t *testing.T) {
	ups, errs := upstreamsFromEnv([]string{"LLMGW_UPSTREAM_ORPHAN_API_KEY=x"})
	if len(ups) != 0 || len(errs) == 0 {
		t.Fatalf("orphan API key accepted: %v %v", ups, errs)
	}
}

func TestMultipleUpstreams(t *testing.T) {
	ups, errs := upstreamsFromEnv([]string{
		"LLMGW_UPSTREAM_OPENAI_BASE_URL=https://a.example/v1",
		"LLMGW_UPSTREAM_OPENAI_API_KEY=k1",
		"LLMGW_UPSTREAM_VLLM_BASE_URL=http://198.51.100.10:8000/v1",
	})
	if len(errs) != 0 {
		t.Fatal(errs)
	}
	if len(ups) != 2 || ups["vllm"].APIKey != "" || ups["openai"].APIKey != "k1" {
		t.Fatalf("upstreams: %+v", ups)
	}
}

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
	if cfg.SnapshotPollInterval != 5*time.Second || cfg.DefaultRpm != 20 || cfg.MaxInFlight != 16 {
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

func TestCapFieldParsing(t *testing.T) {
	ups, errs := upstreamsFromEnv([]string{
		"LLMGW_UPSTREAM_A_BASE_URL=https://a.example/v1",
		"LLMGW_UPSTREAM_B_BASE_URL=https://b.example/v1",
		"LLMGW_UPSTREAM_B_CAP_FIELD=max_tokens",
	})
	if len(errs) != 0 {
		t.Fatal(errs)
	}
	if ups["a"].CapField != "max_completion_tokens" {
		t.Fatalf("default cap field not applied: %+v", ups["a"])
	}
	if ups["b"].CapField != "max_tokens" {
		t.Fatalf("explicit cap field lost: %+v", ups["b"])
	}
	_, errs = upstreamsFromEnv([]string{
		"LLMGW_UPSTREAM_C_BASE_URL=https://c.example/v1",
		"LLMGW_UPSTREAM_C_CAP_FIELD=max_new_tokens",
	})
	if len(errs) == 0 {
		t.Fatal("unknown cap field accepted")
	}
}

// Every knob the README documents must actually be read; a documented default
// that cannot be changed is worse than an undocumented one.
func TestDocumentedBodyKnobsAreRead(t *testing.T) {
	setRequired(t)
	t.Setenv("LLMGW_BODY_QUEUE_SIZE", "4096")
	t.Setenv("LLMGW_BODY_BATCH_SIZE", "7")
	t.Setenv("LLMGW_UPSTREAM_RETRIES", "0")
	cfg, err := FromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.BodyQueueSize != 4096 || cfg.BodyBatchSize != 7 {
		t.Fatalf("body knobs ignored: queue=%d batch=%d", cfg.BodyQueueSize, cfg.BodyBatchSize)
	}
	if cfg.UpstreamRetries != 0 {
		t.Fatalf("retries not settable to 0: %d", cfg.UpstreamRetries)
	}
}

// Settings that are about upstreams in general share the UPSTREAM_ prefix with
// per-upstream declarations. Parsing them as a declaration failed startup.
func TestGeneralUpstreamSettingsAreNotParsedAsDeclarations(t *testing.T) {
	setRequired(t)
	t.Setenv("LLMGW_UPSTREAM_RETRIES", "3")
	t.Setenv("LLMGW_UPSTREAM_HEADER_WAIT", "45s")
	cfg, err := FromEnv()
	if err != nil {
		t.Fatalf("a general upstream setting broke startup: %v", err)
	}
	if cfg.UpstreamRetries != 3 || cfg.UpstreamHeaderWait != 45*time.Second {
		t.Fatalf("not applied: retries=%d wait=%v", cfg.UpstreamRetries, cfg.UpstreamHeaderWait)
	}
	if _, stray := cfg.Upstreams["retries"]; stray {
		t.Fatal("a general setting became an upstream")
	}
	// A real typo in a per-upstream name must still fail.
	setRequired(t)
	t.Setenv("LLMGW_UPSTREAM_OPENAI_BASEURL", "https://x.example")
	if _, err := FromEnv(); err == nil {
		t.Fatal("a misspelled upstream field was accepted")
	}
}

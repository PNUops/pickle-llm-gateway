package config

import (
	"os"
	"regexp"
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

func TestProbeIntervalDefaultsAndOverride(t *testing.T) {
	ups, errs := upstreamsFromEnv([]string{
		"LLMGW_UPSTREAM_LOCAL_BASE_URL=http://127.0.0.1:8000/v1",
		"LLMGW_UPSTREAM_PUBLIC_BASE_URL=https://openrouter.example/v1",
		"LLMGW_UPSTREAM_PROXY_BASE_URL=https://proxy.example/v1",
		"LLMGW_UPSTREAM_PROXY_PROBE_INTERVAL=75s",
	})
	if len(errs) != 0 {
		t.Fatal(errs)
	}
	if got := ups["local"].ProbeInterval; got != time.Minute {
		t.Fatalf("private upstream probe interval = %v, want 1m", got)
	}
	if got := ups["public"].ProbeInterval; got != 5*time.Minute {
		t.Fatalf("public upstream probe interval = %v, want 5m", got)
	}
	if got := ups["proxy"].ProbeInterval; got != 75*time.Second {
		t.Fatalf("explicit probe interval = %v, want 75s", got)
	}
	_, errs = upstreamsFromEnv([]string{
		"LLMGW_UPSTREAM_BAD_BASE_URL=https://bad.example/v1",
		"LLMGW_UPSTREAM_BAD_PROBE_INTERVAL=never",
	})
	if len(errs) == 0 {
		t.Fatal("invalid probe interval accepted")
	}
	_, errs = upstreamsFromEnv([]string{
		"LLMGW_UPSTREAM_FAST_BASE_URL=https://fast.example/v1",
		"LLMGW_UPSTREAM_FAST_PROBE_INTERVAL=500ms",
	})
	if len(errs) == 0 {
		t.Fatal("too-short probe interval accepted")
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

// The reserved-name list that keeps a general setting from being parsed as an
// upstream declaration is hand-maintained, and it was added after a name
// collision stopped the process from starting at all. Pinning the two names it
// holds today only remembers that incident; this reads every variable the
// README documents and asserts the whole set is accepted, so the next addition
// cannot reintroduce the same failure quietly.
func TestEveryDocumentedEnvVarIsAccepted(t *testing.T) {
	raw, err := os.ReadFile("../../README.md")
	if err != nil {
		t.Fatal(err)
	}
	names := map[string]bool{}
	for _, m := range regexp.MustCompile(`LLMGW_[A-Z0-9_]+`).FindAllString(string(raw), -1) {
		names[m] = true
	}
	if len(names) < 20 {
		t.Fatalf("only %d variables found in the README; the extraction is wrong", len(names))
	}
	// Values that parse for every type the config reads.
	value := func(n string) string {
		switch {
		case strings.HasSuffix(n, "_INTERVAL"), strings.HasSuffix(n, "_WAIT"),
			strings.HasSuffix(n, "_DURATION"), strings.HasSuffix(n, "_TIMEOUT"):
			return "5s"
		case n == "LLMGW_SNAPSHOT_SOURCE":
			return "file"
		case strings.HasSuffix(n, "_PUSH"), strings.HasSuffix(n, "_CAPTURE"),
			strings.HasSuffix(n, "_RESET"):
			return "on"
		case strings.HasSuffix(n, "_BYTES"), strings.HasSuffix(n, "_SIZE"),
			strings.HasSuffix(n, "_DAYS"), strings.HasSuffix(n, "_RETRIES"),
			strings.HasSuffix(n, "_RPM"), strings.HasSuffix(n, "_TPM"),
			strings.HasSuffix(n, "_CONCURRENCY"), strings.HasSuffix(n, "_IN_FLIGHT"),
			strings.HasSuffix(n, "_MAX_N"):
			return "7"
		case strings.HasSuffix(n, "_LISTEN"):
			return "127.0.0.1:9999"
		case strings.HasSuffix(n, "_PATH"), strings.HasSuffix(n, "_DIR"):
			return t.TempDir()
		case strings.HasSuffix(n, "_URL"):
			return "https://example.invalid/v1"
		}
		return "x"
	}
	// The README writes the upstream block as a shape (`LLMGW_UPSTREAM_<REF>_BASE_URL`),
	// which the extraction sees as a name ending in an underscore. Those are
	// placeholders, not variables.
	declared := map[string]bool{}
	for n := range names {
		if strings.HasSuffix(n, "_") {
			continue
		}
		if after, ok := strings.CutPrefix(n, "LLMGW_UPSTREAM_"); ok {
			if ref, _, found := strings.Cut(after, "_"); found && !reservedUpstreamSettings[ref] {
				declared[strings.ToLower(ref)] = true
			}
		}
		t.Setenv(n, value(n))
	}
	// One real upstream so the config has something to serve.
	t.Setenv("LLMGW_UPSTREAM_MOCK_BASE_URL", "https://example.invalid/v1")
	t.Setenv("LLMGW_UPSTREAM_MOCK_API_KEY", "k")
	declared["mock"] = true

	cfg, err := FromEnv()
	if err != nil {
		t.Fatalf("a configuration of every documented variable was rejected: %v", err)
	}
	for ref := range cfg.Upstreams {
		if !declared[ref] {
			t.Fatalf("a general setting was parsed as an upstream declaration: %q", ref)
		}
	}
}

// The passthrough surface's four bounds carry defaults, and every one of them
// is settable on its own. The deployed environment is the authority on the
// values; what this pins is that the wiring exists and that chat's own four
// stay where they were.
func TestPassthroughLimitsDefaultAndOverride(t *testing.T) {
	setRequired(t)
	cfg, err := FromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.PassthroughRequestBodyMaxBytes != 8<<20 || cfg.PassthroughResponseMaxBytes != 32<<20 {
		t.Fatalf("byte defaults: %d %d", cfg.PassthroughRequestBodyMaxBytes, cfg.PassthroughResponseMaxBytes)
	}
	if cfg.PassthroughHeaderWait != 180*time.Second || cfg.PassthroughMaxInFlight != 16 {
		t.Fatalf("wait/pool defaults: %v %d", cfg.PassthroughHeaderWait, cfg.PassthroughMaxInFlight)
	}
	if cfg.PassthroughMaxN != 4 {
		t.Fatalf("n default: %d", cfg.PassthroughMaxN)
	}
	// The four the chat path reads are untouched by all of this.
	if cfg.RequestBodyMaxBytes != 2<<20 || cfg.UpstreamHeaderWait != 60*time.Second || cfg.MaxInFlight != 16 {
		t.Fatalf("chat limits moved: %d %v %d", cfg.RequestBodyMaxBytes, cfg.UpstreamHeaderWait, cfg.MaxInFlight)
	}

	t.Setenv("LLMGW_PASSTHROUGH_REQUEST_BODY_MAX_BYTES", "1048576")
	t.Setenv("LLMGW_PASSTHROUGH_RESPONSE_MAX_BYTES", "20971520")
	t.Setenv("LLMGW_PASSTHROUGH_HEADER_WAIT", "300s")
	t.Setenv("LLMGW_PASSTHROUGH_MAX_IN_FLIGHT", "6")
	t.Setenv("LLMGW_PASSTHROUGH_MAX_N", "10")
	cfg, err = FromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.PassthroughRequestBodyMaxBytes != 1<<20 || cfg.PassthroughResponseMaxBytes != 20<<20 {
		t.Fatalf("byte overrides: %d %d", cfg.PassthroughRequestBodyMaxBytes, cfg.PassthroughResponseMaxBytes)
	}
	if cfg.PassthroughHeaderWait != 300*time.Second || cfg.PassthroughMaxInFlight != 6 || cfg.PassthroughMaxN != 10 {
		t.Fatalf("overrides: %v %d %d", cfg.PassthroughHeaderWait, cfg.PassthroughMaxInFlight, cfg.PassthroughMaxN)
	}
	// Overriding the passthrough copies still leaves chat where it was.
	if cfg.RequestBodyMaxBytes != 2<<20 || cfg.UpstreamHeaderWait != 60*time.Second {
		t.Fatalf("chat limits moved: %d %v", cfg.RequestBodyMaxBytes, cfg.UpstreamHeaderWait)
	}
}

func TestPassthroughLimitsRejectBadValues(t *testing.T) {
	setRequired(t)
	t.Setenv("LLMGW_PASSTHROUGH_RESPONSE_MAX_BYTES", "0")
	if _, err := FromEnv(); err == nil {
		t.Fatal("a zero response cap must not start the daemon")
	}
	t.Setenv("LLMGW_PASSTHROUGH_RESPONSE_MAX_BYTES", "")
	t.Setenv("LLMGW_PASSTHROUGH_MAX_IN_FLIGHT", "-1")
	if _, err := FromEnv(); err == nil {
		t.Fatal("a negative slot count must not start the daemon")
	}
}

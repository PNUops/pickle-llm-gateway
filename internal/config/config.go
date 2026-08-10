// Package config reads the gateway configuration from the environment. The
// gateway fails closed: anything that decides who gets through or where
// requests go must be present at startup, and only tuning knobs carry
// defaults.
package config

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Upstream is one OpenAI-compatible target the snapshot's upstreamRef can
// select. The API key never appears in the snapshot or in logs; it lives in
// the environment only.
type Upstream struct {
	Ref     string // lowercase reference name, as used in the snapshot
	BaseURL string // e.g. https://api.openai.com/v1
	APIKey  string // may be empty for an unauthenticated upstream (a local vLLM)
	// CapField is the request field the gateway injects the per-model output
	// cap into. Modern OpenAI-compatible servers take max_completion_tokens
	// (the default); servers that only honor the legacy field are configured
	// with max_tokens here, otherwise the cap would be silently ignored.
	CapField string
}

// SnapshotSource names where the authorization document comes from.
const (
	SourceFile = "file" // a local document an operator maintains
	SourceHTTP = "http" // the control plane (pickle-api) serves it
)

// Config is everything the daemon needs to run.
type Config struct {
	Listen string // address for the public-facing HTTP listener
	// AdminListen serves metrics and health on a separate address. Empty
	// disables it. Bind it to loopback or the infra bridge only — it is not
	// behind the public route and must never become reachable from it.
	AdminListen  string
	SnapshotPath string // authorization document (or, on http, its local cache)
	SpoolDir     string // usage event spool directory

	// SnapshotSource selects the document transport. The default stays "file"
	// so enabling the control plane is a deliberate act, not a consequence of
	// upgrading the binary.
	SnapshotSource string
	ControlBaseURL string        // control-plane root, e.g. http://172.30.1.20:8080
	ControlToken   string        // bearer for the internal link
	ControlTimeout time.Duration // per-poll deadline

	SnapshotPollInterval time.Duration
	RequestBodyMaxBytes  int64
	UpstreamHeaderWait   time.Duration // how long to wait for upstream response headers
	RequestMaxDuration   time.Duration // hard cap on one request, streaming included
	MaxInFlight          int           // gateway-wide concurrent request cap
	// UpstreamRetries is how many extra attempts one upstream gets after a
	// transient failure, before the model's fallback (if any) is tried. Only
	// ever applied before any of the response has reached the client.
	UpstreamRetries int

	// Fallback limits for keys whose snapshot entry sets none.
	DefaultRpm         int
	DefaultTpm         int
	DefaultConcurrency int

	// SpoolRetentionDays bounds the usage spool on the gateway's small disk.
	// Old day-files are deleted once past it; 0 disables pruning.
	SpoolRetentionDays int

	// UsagePush ships spooled usage events to the control plane. Off by
	// default: the spool is written either way, so turning this on later loses
	// nothing that happened before.
	UsagePush         bool
	UsageBatchSize    int
	UsagePushInterval time.Duration

	// BodyCapture opens the prompt/response capture channel. Off by default,
	// and even on, a key records nothing unless its snapshot entry opted in.
	// Captured text never touches this host's disk, so the channel has to
	// exist before anything is captured at all.
	BodyCapture   bool
	BodyQueueSize int
	BodyBatchSize int

	// AllowGenerationReset lets the snapshot load a document whose generation
	// is below the persisted high-water (an operator deliberately resetting the
	// sequence). Off by default so a restored old snapshot fails closed.
	AllowGenerationReset bool

	Upstreams map[string]Upstream // by lowercase ref
}

// UpstreamRefs returns the configured upstream reference names.
func (c *Config) UpstreamRefs() []string {
	refs := make([]string, 0, len(c.Upstreams))
	for ref := range c.Upstreams {
		refs = append(refs, ref)
	}
	return refs
}

const envPrefix = "LLMGW_"

// FromEnv builds the configuration. Missing required values are reported all
// at once so the operator fixes one startup, not five.
func FromEnv() (*Config, error) {
	cfg := &Config{
		Listen:               getenv("LISTEN", ""),
		SnapshotPath:         getenv("SNAPSHOT_PATH", ""),
		SpoolDir:             getenv("SPOOL_DIR", ""),
		AdminListen:          getenv("ADMIN_LISTEN", ""),
		SnapshotSource:       getenv("SNAPSHOT_SOURCE", SourceFile),
		ControlBaseURL:       strings.TrimRight(getenv("CONTROL_BASE_URL", ""), "/"),
		ControlToken:         getenv("CONTROL_TOKEN", ""),
		ControlTimeout:       10 * time.Second,
		SnapshotPollInterval: 5 * time.Second,
		RequestBodyMaxBytes:  2 << 20,
		UpstreamHeaderWait:   60 * time.Second,
		RequestMaxDuration:   10 * time.Minute,
		// Sized for the gateway LXC's memory (512 MB): each in-flight request
		// can transiently hold a few MB of request/response buffers, so the
		// default stays well under the unit's MemoryMax. Raise it (and the
		// LXC memory + MemoryMax together) for a larger host.
		MaxInFlight:        16,
		UpstreamRetries:    1,
		DefaultRpm:         20,
		DefaultTpm:         20000,
		DefaultConcurrency: 2,
		SpoolRetentionDays: 90,
		UsageBatchSize:     500,
		UsagePushInterval:  30 * time.Second,
		BodyQueueSize:      256,
		BodyBatchSize:      20,
		Upstreams:          map[string]Upstream{},
	}

	var errs []string
	need := func(name, val string) {
		if val == "" {
			errs = append(errs, envPrefix+name+" is required")
		}
	}
	need("LISTEN", cfg.Listen)
	need("SNAPSHOT_PATH", cfg.SnapshotPath)
	need("SPOOL_DIR", cfg.SpoolDir)

	switch cfg.SnapshotSource {
	case SourceFile:
	case SourceHTTP:
		// The control plane decides who gets through, so an unset endpoint or
		// token must stop the gateway rather than silently fall back to a file
		// that may be stale or absent.
		need("CONTROL_BASE_URL", cfg.ControlBaseURL)
		need("CONTROL_TOKEN", cfg.ControlToken)
	default:
		errs = append(errs, envPrefix+"SNAPSHOT_SOURCE must be "+SourceFile+" or "+SourceHTTP)
	}

	for _, opt := range []struct {
		name string
		dst  *int
	}{
		{"MAX_IN_FLIGHT", &cfg.MaxInFlight},
		{"BODY_QUEUE_SIZE", &cfg.BodyQueueSize},
		{"BODY_BATCH_SIZE", &cfg.BodyBatchSize},
		{"DEFAULT_RPM", &cfg.DefaultRpm},
		{"DEFAULT_TPM", &cfg.DefaultTpm},
		{"DEFAULT_CONCURRENCY", &cfg.DefaultConcurrency},
	} {
		if v := getenv(opt.name, ""); v != "" {
			n, err := strconv.Atoi(v)
			if err != nil || n <= 0 {
				errs = append(errs, envPrefix+opt.name+" must be a positive integer")
				continue
			}
			*opt.dst = n
		}
	}
	if v := getenv("UPSTREAM_RETRIES", ""); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			errs = append(errs, envPrefix+"UPSTREAM_RETRIES must be a non-negative integer")
		} else {
			cfg.UpstreamRetries = n
		}
	}
	if v := getenv("SPOOL_RETENTION_DAYS", ""); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			errs = append(errs, envPrefix+"SPOOL_RETENTION_DAYS must be a non-negative integer")
		} else {
			cfg.SpoolRetentionDays = n
		}
	}
	for _, opt := range []struct {
		name string
		dst  *time.Duration
	}{
		{"SNAPSHOT_POLL_INTERVAL", &cfg.SnapshotPollInterval},
		{"CONTROL_TIMEOUT", &cfg.ControlTimeout},
		{"USAGE_PUSH_INTERVAL", &cfg.UsagePushInterval},
		{"UPSTREAM_HEADER_WAIT", &cfg.UpstreamHeaderWait},
		{"REQUEST_MAX_DURATION", &cfg.RequestMaxDuration},
	} {
		if v := getenv(opt.name, ""); v != "" {
			d, err := time.ParseDuration(v)
			if err != nil || d <= 0 {
				errs = append(errs, envPrefix+opt.name+" must be a positive duration (e.g. 5s)")
				continue
			}
			*opt.dst = d
		}
	}
	if v := getenv("ALLOW_GENERATION_RESET", ""); v == "1" || strings.EqualFold(v, "true") {
		cfg.AllowGenerationReset = true
	}
	if v := getenv("USAGE_PUSH", ""); v == "on" || v == "1" || strings.EqualFold(v, "true") {
		cfg.UsagePush = true
		// Shipping needs somewhere to ship to, whichever way the document
		// arrives — an operator may push usage while still editing the
		// document by hand.
		need("CONTROL_BASE_URL", cfg.ControlBaseURL)
		need("CONTROL_TOKEN", cfg.ControlToken)
	}
	if v := getenv("BODY_CAPTURE", ""); v == "on" || v == "1" || strings.EqualFold(v, "true") {
		cfg.BodyCapture = true
		need("CONTROL_BASE_URL", cfg.ControlBaseURL)
		need("CONTROL_TOKEN", cfg.ControlToken)
	}
	if v := getenv("USAGE_BATCH_SIZE", ""); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			errs = append(errs, envPrefix+"USAGE_BATCH_SIZE must be a positive integer")
		} else {
			cfg.UsageBatchSize = n
		}
	}
	if v := getenv("REQUEST_BODY_MAX_BYTES", ""); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n <= 0 {
			errs = append(errs, envPrefix+"REQUEST_BODY_MAX_BYTES must be a positive integer")
		} else {
			cfg.RequestBodyMaxBytes = n
		}
	}

	ups, upErrs := upstreamsFromEnv(os.Environ())
	cfg.Upstreams = ups
	errs = append(errs, upErrs...)
	if len(ups) == 0 {
		errs = append(errs, "at least one upstream is required ("+envPrefix+"UPSTREAM_<REF>_BASE_URL)")
	}

	if len(errs) > 0 {
		sort.Strings(errs)
		return nil, fmt.Errorf("config: %s", strings.Join(errs, "; "))
	}
	return cfg, nil
}

// reservedUpstreamSettings are names under the UPSTREAM_ prefix that configure
// upstream handling as a whole rather than declaring one.
var reservedUpstreamSettings = map[string]bool{
	"RETRIES":     true,
	"HEADER_WAIT": true,
}

// upstreamsFromEnv collects LLMGW_UPSTREAM_<REF>_BASE_URL / _API_KEY pairs.
// <REF> is matched case-insensitively against the snapshot's upstreamRef.
func upstreamsFromEnv(environ []string) (map[string]Upstream, []string) {
	ups := map[string]Upstream{}
	var errs []string
	const p = envPrefix + "UPSTREAM_"
	for _, kv := range environ {
		name, val, ok := strings.Cut(kv, "=")
		if !ok || !strings.HasPrefix(name, p) {
			continue
		}
		rest := strings.TrimPrefix(name, p)
		// Settings that are about upstreams in general, not about one named
		// upstream, share this prefix. Without this they parse as an upstream
		// declaration and fail the whole startup — which is exactly what
		// LLMGW_UPSTREAM_RETRIES did.
		if reservedUpstreamSettings[rest] {
			continue
		}
		var ref, field string
		switch {
		case strings.HasSuffix(rest, "_BASE_URL"):
			ref, field = strings.TrimSuffix(rest, "_BASE_URL"), "base"
		case strings.HasSuffix(rest, "_API_KEY"):
			ref, field = strings.TrimSuffix(rest, "_API_KEY"), "key"
		case strings.HasSuffix(rest, "_CAP_FIELD"):
			ref, field = strings.TrimSuffix(rest, "_CAP_FIELD"), "cap"
		default:
			errs = append(errs, name+" is not a recognized upstream field (_BASE_URL, _API_KEY or _CAP_FIELD)")
			continue
		}
		if ref == "" {
			errs = append(errs, name+" has an empty upstream reference")
			continue
		}
		id := strings.ToLower(ref)
		u := ups[id]
		u.Ref = id
		switch field {
		case "base":
			u.BaseURL = strings.TrimRight(val, "/")
		case "key":
			u.APIKey = val
		case "cap":
			u.CapField = val
		}
		ups[id] = u
	}
	for id, u := range ups {
		if u.BaseURL == "" {
			errs = append(errs, "upstream "+id+" is missing "+envPrefix+"UPSTREAM_"+strings.ToUpper(id)+"_BASE_URL")
			delete(ups, id)
			continue
		}
		switch u.CapField {
		case "":
			u.CapField = "max_completion_tokens"
			ups[id] = u
		case "max_completion_tokens", "max_tokens":
		default:
			errs = append(errs, "upstream "+id+" has an unknown cap field "+u.CapField+" (max_completion_tokens or max_tokens)")
			delete(ups, id)
		}
	}
	return ups, errs
}

func getenv(name, def string) string {
	if v := os.Getenv(envPrefix + name); v != "" {
		return v
	}
	return def
}

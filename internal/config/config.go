// Package config reads the gateway configuration from the environment. The
// gateway fails closed: anything that decides who gets through or where
// requests go must be present at startup, and only tuning knobs carry
// defaults.
package config

import (
	"fmt"
	"net"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Upstream is one OpenAI-compatible target the snapshot's upstreamRef can
// select. This env-level API key never appears in the snapshot or in logs; it
// lives in the environment only, and it serves TOKEN-axis models. CREDIT-axis
// models never use it — they require the per-key credential the snapshot
// carries (snapshot.Key.UpstreamCredentials), which is the one deliberate
// exception to "the snapshot holds no usable secret".
type Upstream struct {
	Ref     string // lowercase reference name, as used in the snapshot
	BaseURL string // e.g. https://api.openai.com/v1
	APIKey  string // may be empty for an unauthenticated upstream (a local vLLM)
	// CapField is the request field the gateway injects the per-model output
	// cap into. Modern OpenAI-compatible servers take max_completion_tokens
	// (the default); servers that only honor the legacy field are configured
	// with max_tokens here, otherwise the cap would be silently ignored.
	CapField string
	// ProbeInterval controls the read-only GET /models observation loop. A
	// private or loopback literal defaults to the on-prem cadence; a public
	// address or hostname defaults to the external cadence. Operators can set
	// it explicitly when DNS or a proxy hides that topology.
	ProbeInterval time.Duration
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

	// The passthrough surface's own copies of the four values above. Chat runs
	// on the fields above and does not move; these govern only the routes the
	// passthrough surface opens, so an answer about image sizes can never
	// change what a chat request may send or hold.
	//
	// The separate slot pool is what makes the memory arithmetic provable.
	// A request is held three times over — the bytes as read, the decoded
	// top-level members, and the body re-serialized for the upstream, which is
	// what makes the model fence sound (see passthrough.go). The response is
	// held once, because it is forwarded verbatim rather than re-marshalled.
	// Taking the measured 1.75x amplification (2026-09-02) on the response
	// side as the conservative bound, the ceiling these caps guarantee is
	//
	//	(PassthroughRequestBodyMaxBytes*3 + PassthroughResponseMaxBytes*1.75) * PassthroughMaxInFlight
	//
	// which at the defaults below is about 2,096 MiB. That is the number to
	// check a host against, and it is the whole reason this pool is separate:
	// it holds whatever the chat pool is sized to, so raising one cannot move
	// the other's arithmetic. The same caps applied to the gateway-wide pool
	// would give a figure in the tens of gigabytes.
	//
	// The expected load is far below the ceiling. An image response measured
	// 2026-09-05 is 13.09 MiB at 4K and 1.61 MiB at the default resolution
	// (base64 expands the image 1.333x), so sixteen concurrent 4K generations
	// sit at about 366 MiB.
	//
	// The response cap, the header wait and the slot count come from that
	// measurement. An 8 MiB response cap could not hold a 4K image at all, and
	// 32 MiB leaves room above one; a 4K generation takes 32 seconds and is
	// not streamed, so the 60s chat header wait has under twice the margin
	// while 180s has room.
	//
	// The request cap is the vendor's own documented limit, 25 MB, rather than
	// a number of ours. Where the vendor accepts a request there is no reason
	// for this gateway to be the thing that refuses it, so a value picked here
	// would only ever be a smaller ceiling with nothing behind it. It covers
	// both shapes an image edit can take: a reference image as a plain URL
	// costs nothing, and a 4K one inline as a data URL fits once base64 has
	// expanded it.
	//
	// There is deliberately no bound on `n`. The vendor does not cap it, and
	// the reason one was considered here — that an oversized response would be
	// truncated into an unexplained 502 — no longer holds: the response cap
	// refuses that case by name (see errPassthroughResponseTooLarge), so a
	// resource limit does the work a policy limit would have done, and says so.
	//
	// The deployed environment remains the authority — a host with a different
	// memory budget wants different numbers, and the unit's MemoryHigh and
	// GOMEMLIMIT move with them.
	PassthroughRequestBodyMaxBytes int64
	PassthroughResponseMaxBytes    int64
	PassthroughHeaderWait          time.Duration
	PassthroughMaxInFlight         int

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
		// Sized for a 512 MB gateway container, which is the smallest one this
		// daemon is expected to run in. Measured 2026-09-02 against this
		// binary: an in-flight request costs about 75 KB with a typical
		// response and about 240 KB with a 128 KB one (32k output tokens, the
		// realistic ceiling), plus exactly two file descriptors; a single core
		// sustained ~2,000 req/s. So this default is conservative by a wide
		// margin and the binding constraint is the container, not the request.
		// Raise it — with the container's memory, the unit's MemoryMax,
		// MemoryHigh, GOMEMLIMIT and LimitNOFILE together — for a larger host.
		// The ceilings that matter answer to upstreamResponseCapBytes, not to
		// this default: a response near that cap costs roughly 13 MB to hold,
		// decode and re-serialise, so raising the cap moves how many such
		// responses it takes to exhaust the container.
		MaxInFlight:     16,
		UpstreamRetries: 1,
		// See the field comments for what the measurement decided and what is
		// still provisional. The header wait is the value images actually
		// need: a generation is not streamed, so the upstream sends no headers
		// until it has finished producing the image.
		PassthroughRequestBodyMaxBytes: 25 << 20,
		PassthroughResponseMaxBytes:    32 << 20,
		PassthroughHeaderWait:          180 * time.Second,
		PassthroughMaxInFlight:         16,
		DefaultRpm:                     20,
		DefaultTpm:                     20000,
		DefaultConcurrency:             2,
		SpoolRetentionDays:             90,
		UsageBatchSize:                 500,
		UsagePushInterval:              30 * time.Second,
		BodyQueueSize:                  256,
		BodyBatchSize:                  20,
		Upstreams:                      map[string]Upstream{},
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
		{"PASSTHROUGH_MAX_IN_FLIGHT", &cfg.PassthroughMaxInFlight},
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
		{"PASSTHROUGH_HEADER_WAIT", &cfg.PassthroughHeaderWait},
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
	for _, opt := range []struct {
		name string
		dst  *int64
	}{
		{"REQUEST_BODY_MAX_BYTES", &cfg.RequestBodyMaxBytes},
		{"PASSTHROUGH_REQUEST_BODY_MAX_BYTES", &cfg.PassthroughRequestBodyMaxBytes},
		{"PASSTHROUGH_RESPONSE_MAX_BYTES", &cfg.PassthroughResponseMaxBytes},
	} {
		if v := getenv(opt.name, ""); v != "" {
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil || n <= 0 {
				errs = append(errs, envPrefix+opt.name+" must be a positive integer")
				continue
			}
			*opt.dst = n
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
		case strings.HasSuffix(rest, "_PROBE_INTERVAL"):
			ref, field = strings.TrimSuffix(rest, "_PROBE_INTERVAL"), "probe"
		default:
			errs = append(errs, name+" is not a recognized upstream field (_BASE_URL, _API_KEY, _CAP_FIELD or _PROBE_INTERVAL)")
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
		case "probe":
			d, err := time.ParseDuration(val)
			if err != nil || d < 10*time.Second {
				errs = append(errs, name+" must be at least 10s (e.g. 60s)")
				continue
			}
			u.ProbeInterval = d
		}
		ups[id] = u
	}
	for id, u := range ups {
		if u.BaseURL == "" {
			errs = append(errs, "upstream "+id+" is missing "+envPrefix+"UPSTREAM_"+strings.ToUpper(id)+"_BASE_URL")
			delete(ups, id)
			continue
		}
		if u.ProbeInterval == 0 {
			u.ProbeInterval = defaultProbeInterval(u.BaseURL)
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

const (
	onPremProbeInterval   = time.Minute
	externalProbeInterval = 5 * time.Minute
)

// defaultProbeInterval applies the product's two observation cadences without
// doing DNS at startup. Literal private/loopback/link-local addresses and
// localhost are on-prem; public literals and hostnames are external. A local
// hostname or a proxy that hides the destination uses the explicit per-
// upstream override instead of making startup depend on name resolution.
func defaultProbeInterval(baseURL string) time.Duration {
	u, err := url.Parse(baseURL)
	if err != nil {
		return externalProbeInterval
	}
	host := strings.ToLower(u.Hostname())
	if host == "localhost" {
		return onPremProbeInterval
	}
	ip := net.ParseIP(host)
	if ip != nil && (ip.IsPrivate() || ip.IsLoopback() || ip.IsLinkLocalUnicast()) {
		return onPremProbeInterval
	}
	return externalProbeInterval
}

func getenv(name, def string) string {
	if v := os.Getenv(envPrefix + name); v != "" {
		return v
	}
	return def
}

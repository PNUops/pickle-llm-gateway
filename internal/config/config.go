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
	// Reasoning says whether a self-hosted (TOKEN-axis) model served here
	// accepts the caller's reasoning_effort, and in which dialect it goes out:
	// ReasoningClosed refuses the field, ReasoningEffort forwards it as is,
	// ReasoningTemplate turns it into the chat template's thinking switch. It
	// is a property of the upstream because the serving capacity a reasoning
	// request spends is the upstream's, so moving a model to another upstream
	// takes that upstream's decision with it. It does not decide whether the
	// model reasons when the field is absent: that is the upstream's default.
	Reasoning string
	// ProbeInterval controls the read-only GET /models observation loop. A
	// private or loopback literal defaults to the on-prem cadence; a public
	// address or hostname defaults to the external cadence. Operators can set
	// it explicitly when DNS or a proxy hides that topology.
	ProbeInterval time.Duration
}

// Reasoning settings for an upstream; see Upstream.Reasoning.
const (
	ReasoningClosed   = "closed"
	ReasoningEffort   = "effort"
	ReasoningTemplate = "template"
)

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
	// A chat body above RequestBodyMaxBytes is admitted only through a pool
	// of its own, so the memory it can hold is a multiplication with a small
	// count in it rather than one with the gateway-wide slot count. Measured
	// 2026-10-10: a request costs three to four times its size while it is
	// decoded and re-serialised, about twice once GOMEMLIMIT presses, and eight
	// 25 MiB requests held at once peaked at 752 MiB. With the gateway-wide
	// pool instead, 25 MiB would reach the unit's MemoryMax in a few dozen
	// requests, all of which one key can send.
	//
	// The pool is judged on the declared Content-Length, before the body is
	// read: the edge buffers the whole body and sets the header, and a request
	// that declares nothing or declares a small size is still read through the
	// small cap, so lying about the length cannot reach the large one. The
	// request cap is the vendor's documented 25 MB, the same as passthrough.
	//
	// A slot is held until the upstream's response headers arrive and then
	// released together with every reference to the request body; a stream
	// that runs for ten minutes after that does not keep it. The pool has no
	// per-key share: one key may take every slot, and the next large request
	// from anyone is refused until one frees. The per-key share that
	// existed until 2026-10-14 is gone and its variable,
	// LLMGW_CHAT_LARGE_BODY_SLOTS_PER_KEY, is no longer read, so an
	// environment file that still sets it starts as before.
	// ChatLargeBodySlots = 0 turns the pool off, which is the old behaviour:
	// anything above RequestBodyMaxBytes is refused.
	//
	// The same pool admits a non-streamed response that outgrows the 8 MiB
	// relay bound, and one stream event that does, up to
	// ChatLargeResponseMaxBytes. With no slot free the response is refused by
	// name, after the vendor has billed it.
	//
	// The response bound is what sizes the pool, not the request bound. A 4K
	// image generation answered 53.4 MB in one non-streamed body (a 36 MB
	// base64 image plus a 17 MB reasoning signature), and streamed the picture
	// as one event of the same size, so the earlier 32 MiB refused a single
	// picture. 128 MiB holds that answer about two and a half times over.
	// Measured 2026-10-14 with answers of that shape: one such answer costs
	// four to five times its size while it is read, decoded and re-serialised,
	// and about 3.2 times once GOMEMLIMIT presses (the bytes read, the decoded
	// value and the output are all live at the moment of marshalling, so three
	// is a floor; the pressed figure was taken with the heap over the limit).
	// One stream event costs the same. The slot is held until the answer has
	// been written, so the copy waiting on a slow reader is counted too. A
	// 25 MiB request costs three to four times its size, so a slot is sized by
	// the response: about 410 MiB at 128 MiB with GOMEMLIMIT pressing.
	//
	// The slot count is what the container decides. The default is for an
	// 8 GiB container (unit GOMEMLIMIT 5120 MiB): three slots (about 1.2 GiB)
	// plus the passthrough pool below (about 0.6 GiB) leave about 3.2 GiB under
	// GOMEMLIMIT for traffic that takes no slot, against an estimated class
	// peak of about 3 GiB with LLMGW_MAX_IN_FLIGHT at 1,024. A 16 GiB container
	// takes sixteen. The 128 MiB bound does not move with the container: it
	// was chosen so as not to be raised again, and a smaller host gives up
	// slots, not that.
	// These figures come from the measured multipliers, not from a run at
	// these values; raising either bound or the slot count moves the ladder.
	ChatLargeBodyMaxBytes     int64
	ChatLargeResponseMaxBytes int64
	ChatLargeBodySlots        int
	// UpstreamRetries is how many extra attempts one upstream gets after a
	// transient failure, before the model's fallback (if any) is tried. Only
	// ever applied before any of the response has reached the client.
	UpstreamRetries int

	// The passthrough surface's own copies of the four values above. Chat runs
	// on the fields above and does not move; these govern only the routes the
	// passthrough surface opens, so an answer about image sizes can never
	// change what a chat request may send or hold.
	//
	// Admission has the same two tiers as chat. A request up to
	// RequestBodyMaxBytes, judged on its declared length, and an answer up to
	// the 8 MiB relay bound take no slot of this surface's own and are bounded
	// by MaxInFlight alone, like a small chat request; each costs well under
	// the about 40 MiB a small chat request can (2 MiB three times over plus
	// 8 MiB 1.75 times over). That is what lets an embedding class send dozens
	// of small calls at once. A request past that bound, or an answer that
	// crosses the relay bound, takes one of PassthroughMaxInFlight large-body
	// slots, refused without one: a large request with 429 large_request_busy
	// before the vendor is called, a large answer by name after it has billed
	// (see errPassthroughResponseTooLarge). The variable keeps its name so an
	// environment file that sets it still starts; it now counts large bodies
	// only.
	//
	// The pool is its own, not chat's, for two reasons. A slot here costs a
	// different amount (below), so one shared count would have to price every
	// slot at the dearer surface's figure. And a large passthrough request
	// holds its slot through the whole call, about half a minute for a 4K
	// picture, where chat lets go at the headers, so sharing would let image
	// edits hold the slots a chat answer the vendor has already billed needs.
	//
	// A slot is held from the moment it is taken until the answer has been
	// written, covering a large request and its answer together. A request is
	// held three times over — the bytes as read, the decoded top-level
	// members, and the body re-serialized for the upstream, which is what
	// makes the model fence sound (see passthrough.go). The response is held
	// once, because it is forwarded verbatim rather than re-marshalled. Taking
	// the measured 1.75x amplification (2026-09-02) on the response side as the
	// conservative bound, the ceiling these caps guarantee is
	//
	//	(PassthroughRequestBodyMaxBytes*3 + PassthroughResponseMaxBytes*1.75) * PassthroughMaxInFlight
	//
	// which at the defaults below is about 598 MiB (two at 299 MiB; a 16 GiB
	// container takes twelve, about 3,588 MiB). That is the number to check a
	// host against, and it holds whatever the chat pool is sized to, so
	// raising one cannot move the other's arithmetic.
	//
	// Measured 2026-10-14, sixteen 120 MiB answers held at once peaked at
	// 3.3 GB of heap, about 1.75 times each, as the formula assumes.
	//
	// The response cap is chat's, 128 MiB. A later model answered a 4K picture
	// at 36 MB of base64 alone (34.3 MiB), so two 4K pictures in one call
	// (n=2) come to about 69 MiB and three to about 103 MiB; a smaller cap
	// would refuse them after the vendor had billed both. The slot count is
	// what pays for it: three slots here (897 MiB) beside chat's three
	// (1,229 MiB) would leave 2,994 MiB under an 8 GiB container's GOMEMLIMIT,
	// short of the 3 GiB class estimate, so the default is two. A 4K
	// generation takes 32 seconds and is not streamed, so the 60s chat header
	// wait had under twice the margin while 180s has room.
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

// ordinaryResponseBytes is the relay bound a response or stream event gets
// without a large-body slot (upstreamResponseCapBytes in the server).
const ordinaryResponseBytes = 8 << 20

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
		// The passthrough's value. Long reasoning and vendor server-tool
		// chains send no headers until they start answering; the large-body
		// pool bounds what a longer wait can hold.
		UpstreamHeaderWait:        180 * time.Second,
		ChatLargeBodyMaxBytes:     25 << 20,
		ChatLargeResponseMaxBytes: 128 << 20,
		ChatLargeBodySlots:        3,
		RequestMaxDuration:        10 * time.Minute,
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
		PassthroughResponseMaxBytes:    128 << 20,
		PassthroughHeaderWait:          180 * time.Second,
		PassthroughMaxInFlight:         2,
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
	// Zero is meaningful here: it closes the large-body pool.
	if v := getenv("CHAT_LARGE_BODY_SLOTS", ""); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			errs = append(errs, envPrefix+"CHAT_LARGE_BODY_SLOTS must be a non-negative integer")
		} else {
			cfg.ChatLargeBodySlots = n
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
		{"CHAT_LARGE_BODY_MAX_BYTES", &cfg.ChatLargeBodyMaxBytes},
		{"CHAT_LARGE_RESPONSE_MAX_BYTES", &cfg.ChatLargeResponseMaxBytes},
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

	if cfg.ChatLargeBodySlots > 0 && cfg.ChatLargeBodyMaxBytes < cfg.RequestBodyMaxBytes {
		errs = append(errs, envPrefix+"CHAT_LARGE_BODY_MAX_BYTES must not be below "+envPrefix+"REQUEST_BODY_MAX_BYTES")
	}
	// At or under the ordinary 8 MiB relay bound the large response path
	// never admits anything, which would turn it off without a word.
	if cfg.ChatLargeBodySlots > 0 && cfg.ChatLargeResponseMaxBytes <= ordinaryResponseBytes {
		errs = append(errs, envPrefix+"CHAT_LARGE_RESPONSE_MAX_BYTES must be above 8 MiB, the ordinary relay bound, while the large-body pool is open")
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
		case strings.HasSuffix(rest, "_REASONING"):
			ref, field = strings.TrimSuffix(rest, "_REASONING"), "reasoning"
		default:
			errs = append(errs, name+" is not a recognized upstream field (_BASE_URL, _API_KEY, _CAP_FIELD, _PROBE_INTERVAL or _REASONING)")
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
		case "reasoning":
			u.Reasoning = val
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
			continue
		}
		switch u.Reasoning {
		case "":
			u.Reasoning = ReasoningClosed
			ups[id] = u
		case ReasoningClosed, ReasoningEffort, ReasoningTemplate:
		default:
			errs = append(errs, "upstream "+id+" has an unknown reasoning setting "+u.Reasoning+
				" ("+ReasoningClosed+", "+ReasoningEffort+" or "+ReasoningTemplate+")")
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

package snapshot

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/pnuops/pickle-llm-gateway/internal/version"
)

// Source is where the authorization document comes from. Two exist: the local
// file an operator maintains today, and the control plane that will serve the
// same document over HTTP once the api owns the state. The document format is
// identical either way — only the transport differs, which is the whole point
// of having written the file format as the wire format from the start.
type Source interface {
	// Load returns the document when the source has one newer than served.
	// changed=false means "nothing new"; raw is then meaningless. An error
	// leaves the caller on its last good state.
	Load(ctx context.Context, served int64) (raw []byte, changed bool, err error)
	// Accept is called once the caller has validated and applied the document
	// Load returned. A source must not treat a document as delivered until
	// then: one that is offered and rejected has to keep being offered, or the
	// rejection stops being visible.
	Accept()
	// Name identifies the source in logs and on the health endpoint.
	Name() string
}

// --- file source -------------------------------------------------------------

// FileSource polls a local document and reports a change when the file's
// identity (mtime, size) moved. Identity rather than content because the
// writers replace the file by rename, so a changed file is always a new inode
// with a new mtime.
type FileSource struct {
	path           string
	lastModTime    time.Time
	lastSize       int64
	pendingModTime time.Time
	pendingSize    int64
}

// NewFileSource reads the document from path.
func NewFileSource(path string) *FileSource { return &FileSource{path: path} }

func (s *FileSource) Name() string { return "file:" + s.path }

func (s *FileSource) Load(_ context.Context, served int64) ([]byte, bool, error) {
	fi, err := os.Stat(s.path)
	if err != nil {
		return nil, false, fmt.Errorf("snapshot file: %w", err)
	}
	// served == 0 means the caller has no state yet (startup), so read
	// regardless of whether the file looks unchanged to us.
	if served != 0 && fi.ModTime().Equal(s.lastModTime) && fi.Size() == s.lastSize {
		return nil, false, nil
	}
	raw, err := os.ReadFile(s.path)
	if err != nil {
		return nil, false, fmt.Errorf("snapshot file: %w", err)
	}
	// The identity is remembered by Accept, not here: a document that the
	// caller then rejects (unparseable, or a generation rollback) must keep
	// being offered, so the failure keeps being counted and the health surface
	// keeps saying the served state is stale. Stamping it here would make the
	// next poll report "unchanged", which reads as healthy.
	s.pendingModTime = fi.ModTime()
	s.pendingSize = fi.Size()
	return raw, true, nil
}

// Accept records that the caller took the last offered document.
func (s *FileSource) Accept() {
	s.lastModTime = s.pendingModTime
	s.lastSize = s.pendingSize
}

// --- control-plane source ----------------------------------------------------

// SyncRequest is what the gateway reports on every poll. The control plane
// decides everything and anything the gateway says about itself is a claim,
// not a measurement — but the claims matter, because this is the only channel
// that carries them. The api never calls the gateway, so a fact that is not on
// this request does not reach the control plane at all.
//
// AppliedGeneration standing still is the symptom of every silent freeze, and
// on its own it does not say why. The rest of these fields are the why.
type SyncRequest struct {
	AppliedGeneration int64 `json:"appliedGeneration"`
	// SupportedFormat is the highest document format this build understands.
	// Serving a format above it is what a lockstep deploy would look like, so
	// the writer is told the reader's ceiling instead of having to assume it.
	SupportedFormat int    `json:"supportedFormat"`
	AgentVersion    string `json:"agentVersion,omitempty"`
	// StartedAt marks this process. A value that moved since the last poll is
	// a restart, which is otherwise invisible to the control plane.
	StartedAt time.Time `json:"startedAt,omitzero"`
	InFlight  int       `json:"inFlight"`
	// MaxInFlight is the denominator for InFlight; without it a load figure
	// cannot be read as anything.
	MaxInFlight int `json:"maxInFlight,omitempty"`
	// UpstreamRefs are the upstreams this host has configured. A model naming
	// anything else is dropped on load, so the writer can check a name before
	// it is stored rather than discovering it as a dropped entry.
	UpstreamRefs []string `json:"upstreamRefs,omitempty"`
	// RejectedEntries counts entries of the applied document this build could
	// not act on: the gateway is enforcing less than the document describes.
	RejectedEntries int `json:"rejectedEntries,omitempty"`
	// ReloadFailures and LastError describe a document that is not being
	// applied at all — the case where the served state is quietly going stale.
	ReloadFailures    int64  `json:"reloadFailures,omitempty"`
	LastError         string `json:"lastError,omitempty"`
	BodiesDropped     int64  `json:"bodiesDropped,omitempty"`
	UsageShipFailures int64  `json:"usageShipFailures,omitempty"`
	// SpoolWriteFailures counts usage events that could not be written to the
	// outbox at all. Those are gone before shipping ever sees them, so the api
	// would otherwise just receive quietly incomplete accounting.
	SpoolWriteFailures int64 `json:"spoolWriteFailures,omitempty"`
	// UpstreamObservationFormat versions the optional observation block. Its
	// presence is what lets the control plane distinguish an older gateway
	// from a newer gateway reporting no upstreams; absence must never be read as
	// "every upstream was deconfigured".
	UpstreamObservationFormat int                   `json:"upstreamObservationFormat,omitempty"`
	Upstreams                 []UpstreamObservation `json:"upstreams"`
	// Usage queue gauges describe delivery completeness. Loss counters alone
	// cannot distinguish a quiet gateway from one whose durable outbox is
	// growing because the control plane is unavailable.
	LastUsageShipSuccessAt time.Time `json:"lastUsageShipSuccessAt,omitzero"`
	UsageQueueObservedAt   time.Time `json:"usageQueueObservedAt,omitzero"`
	OldestUnshippedEventAt time.Time `json:"oldestUnshippedEventAt,omitzero"`
	QueuedUsageEvents      int64     `json:"queuedUsageEvents,omitempty"`
	QueuedUsageBytes       int64     `json:"queuedUsageBytes,omitempty"`
	UsageQueueScanFailures int64     `json:"usageQueueScanFailures,omitempty"`
}

// UpstreamObservation is one configured upstream's read-only state. Passive
// request outcomes and active probes are deliberately separate: a probe must
// never mutate routing cooldown, and a quiet service must not look healthy
// merely because nobody called it.
type UpstreamObservation struct {
	Ref     string                     `json:"ref"`
	Passive PassiveUpstreamObservation `json:"passive"`
	Active  ActiveUpstreamObservation  `json:"active"`
	Catalog CatalogObservation         `json:"catalog"`
}

type PassiveUpstreamObservation struct {
	LastAttemptAt       time.Time `json:"lastAttemptAt,omitzero"`
	LastSuccessAt       time.Time `json:"lastSuccessAt,omitzero"`
	LastFailureAt       time.Time `json:"lastFailureAt,omitzero"`
	LastFailureType     string    `json:"lastFailureType,omitempty"`
	ConsecutiveFailures int       `json:"consecutiveFailures,omitempty"`
	CooldownUntil       time.Time `json:"cooldownUntil,omitzero"`
}

type ActiveUpstreamObservation struct {
	Status              string    `json:"status"`
	IntervalSeconds     int64     `json:"intervalSeconds,omitempty"`
	LastAttemptAt       time.Time `json:"lastAttemptAt,omitzero"`
	LastSuccessAt       time.Time `json:"lastSuccessAt,omitzero"`
	LastFailureAt       time.Time `json:"lastFailureAt,omitzero"`
	LastFailureType     string    `json:"lastFailureType,omitempty"`
	ConsecutiveFailures int       `json:"consecutiveFailures,omitempty"`
	LatencyMs           int64     `json:"latencyMs,omitempty"`
	ModelCount          int       `json:"modelCount,omitempty"`
}

type CatalogObservation struct {
	Status               string   `json:"status"`
	ExpectedModelCount   int      `json:"expectedModelCount,omitempty"`
	MissingModelCount    int      `json:"missingModelCount,omitempty"`
	UnexpectedModelCount int      `json:"unexpectedModelCount,omitempty"`
	MissingPublicModels  []string `json:"missingPublicModels,omitempty"`
}

// SyncGauges are the live numbers the poll reports. They come from components
// the snapshot package does not own (the request server, the reporter, the
// body sink), so they arrive through one closure set at startup rather than by
// threading dependencies through the store.
type SyncGauges struct {
	InFlight                  int
	MaxInFlight               int
	UpstreamRefs              []string
	RejectedEntries           int
	ReloadFailures            int64
	LastError                 string
	BodiesDropped             int64
	UsageShipFailures         int64
	SpoolWriteFailures        int64
	StartedAt                 time.Time
	UpstreamObservationFormat int
	Upstreams                 []UpstreamObservation
	LastUsageShipSuccessAt    time.Time
	UsageQueueObservedAt      time.Time
	OldestUnshippedEventAt    time.Time
	QueuedUsageEvents         int64
	QueuedUsageBytes          int64
	UsageQueueScanFailures    int64
}

// maxReportedError bounds LastError. The text is a Go error string, so it is
// short by construction; the bound exists because it crosses a trust boundary
// into somebody else's log and database column.
const maxReportedError = 500

// sanitizeReported makes an error string safe to hand to another service:
// control characters out (they corrupt logs and terminals), length bounded.
func sanitizeReported(s string) string {
	if s == "" {
		return ""
	}
	var b strings.Builder
	for _, r := range s {
		if r < 0x20 || r == 0x7f {
			r = ' '
		}
		b.WriteRune(r)
		if b.Len() >= maxReportedError {
			break
		}
	}
	return strings.TrimSpace(b.String())
}

// HTTPSource fetches the document from the control plane. The response is
// either the bare generation (nothing changed) or the whole document — the
// same omit-when-unchanged convention the relay sync uses, so an unchanged
// poll costs one small response rather than a full key set.
//
// It also keeps a local cache of the last document it accepted. A restart
// while the control plane is unreachable then still comes up serving the last
// known state instead of failing closed with no authorization at all; the
// generation guard still refuses anything older than what was already served.
type HTTPSource struct {
	client    *http.Client
	url       string
	token     string
	cachePath string
	// pending holds the document Load returned but the caller has not yet
	// accepted. Caching it before validation would let a document the Store
	// refuses (a rollback, an unconfigured upstream) become what a later
	// restart loads — and then the gateway would refuse to start at all.
	pending []byte
	log     *slog.Logger
	// gauges reports what the gateway can say about itself; nil until the rest
	// of the process is wired, and never load-bearing.
	gauges    func() SyncGauges
	delivered bool
}

// NewHTTPSource builds a control-plane source. cachePath is where the accepted
// document is mirrored for restart resilience.
func NewHTTPSource(baseURL, token, cachePath string, timeout time.Duration) *HTTPSource {
	return &HTTPSource{
		client:    &http.Client{Timeout: timeout},
		url:       baseURL + "/internal/llm/sync",
		token:     token,
		cachePath: cachePath,
		log:       slog.New(slog.DiscardHandler),
	}
}

// SetLogger attaches a logger. Without one the cache-write failures below are
// invisible, and restart resilience is exactly the thing you find out about at
// the worst moment.
func (s *HTTPSource) SetLogger(log *slog.Logger) {
	if log != nil {
		s.log = log
	}
}

// SetGauges supplies the self-report attached to each poll. It is set after
// the store exists, so the very first load reports nothing but the generation.
func (s *HTTPSource) SetGauges(f func() SyncGauges) { s.gauges = f }

// Accept caches the document the caller just applied, so a restart while the
// control plane is unreachable comes up on a document that was known good.
func (s *HTTPSource) Accept() {
	if s.pending == nil {
		return
	}
	s.writeCache(s.pending)
	s.pending = nil
}

func (s *HTTPSource) Name() string { return "control-plane" }

func (s *HTTPSource) Load(ctx context.Context, served int64) ([]byte, bool, error) {
	raw, changed, err := s.fetch(ctx, served)
	if err == nil {
		if changed {
			s.pending = raw
			s.delivered = true
			return raw, changed, nil
		}
		// "Unchanged" answered to a caller with nothing to serve leaves the
		// gateway unable to start. The control plane should not do that — the
		// gateway reports appliedGeneration 0 — but a cached document from the
		// last run is a better answer than refusing to come up, and the
		// generation guard still refuses anything older than what was served.
		if served == 0 && !s.delivered {
			if cached, cerr := os.ReadFile(s.cachePath); cerr == nil {
				s.log.Warn("control plane reported no change to a gateway with no state; starting on the cached document",
					"cache", s.cachePath)
				s.delivered = true
				s.pending = nil // already on disk
				return cached, true, nil
			}
		}
		s.delivered = true
		return raw, changed, nil
	}
	// Startup with an unreachable control plane: serve the cache rather than
	// refuse to start. Once a document has been delivered in this process the
	// caller's last good state is better than the cache, so the error stands.
	if !s.delivered {
		if cached, cerr := os.ReadFile(s.cachePath); cerr == nil {
			s.delivered = true
			// Already on disk: re-writing it on Accept would be a no-op, and
			// leaving pending nil keeps it that way.
			s.pending = nil
			return cached, true, nil
		}
	}
	return nil, false, err
}

func (s *HTTPSource) fetch(ctx context.Context, served int64) ([]byte, bool, error) {
	req := SyncRequest{
		AppliedGeneration: served,
		SupportedFormat:   SupportedFormat,
		AgentVersion:      version.String(),
	}
	if s.gauges != nil {
		g := s.gauges()
		req.InFlight = g.InFlight
		req.MaxInFlight = g.MaxInFlight
		req.UpstreamRefs = g.UpstreamRefs
		req.RejectedEntries = g.RejectedEntries
		req.ReloadFailures = g.ReloadFailures
		req.LastError = sanitizeReported(g.LastError)
		req.BodiesDropped = g.BodiesDropped
		req.UsageShipFailures = g.UsageShipFailures
		req.SpoolWriteFailures = g.SpoolWriteFailures
		req.StartedAt = g.StartedAt
		req.UpstreamObservationFormat = g.UpstreamObservationFormat
		req.Upstreams = g.Upstreams
		req.LastUsageShipSuccessAt = g.LastUsageShipSuccessAt
		req.UsageQueueObservedAt = g.UsageQueueObservedAt
		req.OldestUnshippedEventAt = g.OldestUnshippedEventAt
		req.QueuedUsageEvents = g.QueuedUsageEvents
		req.QueuedUsageBytes = g.QueuedUsageBytes
		req.UsageQueueScanFailures = g.UsageQueueScanFailures
	}
	body, err := json.Marshal(req)
	if err != nil {
		return nil, false, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, s.url, bytes.NewReader(body))
	if err != nil {
		return nil, false, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+s.token)
	resp, err := s.client.Do(httpReq)
	if err != nil {
		return nil, false, fmt.Errorf("control plane: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		// The body may carry a reason but may also be anything at all; keep a
		// bounded prefix for the log and nothing else.
		detail, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, false, fmt.Errorf("control plane: HTTP %d: %s", resp.StatusCode, bytes.TrimSpace(detail))
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxDocumentBytes))
	if err != nil {
		return nil, false, fmt.Errorf("control plane: %w", err)
	}
	// Unchanged is signalled by omitting the document members, never by an
	// empty array — an empty array means "no keys at all", which is a real and
	// very different state.
	var probe struct {
		Keys   *json.RawMessage `json:"keys"`
		Models *json.RawMessage `json:"models"`
	}
	if err := json.Unmarshal(raw, &probe); err != nil {
		return nil, false, fmt.Errorf("control plane: %w", err)
	}
	if probe.Keys == nil && probe.Models == nil {
		return nil, false, nil
	}
	return raw, true, nil
}

// maxDocumentBytes bounds a control-plane response. The document is a key set,
// not user data; a response beyond this is a malfunction, not a big campus.
const maxDocumentBytes = 32 << 20

// writeCache mirrors an accepted document for restart resilience. A failure
// here is not fatal — the running gateway is fine — but it must be visible:
// silently, restart resilience would simply be off, and the discovery would
// come during the outage it exists for.
func (s *HTTPSource) writeCache(raw []byte) {
	fail := func(err error) {
		s.log.Error("snapshot cache write failed; a restart during a control-plane outage will have no document",
			"cache", s.cachePath, "error", err)
	}
	tmp, err := os.CreateTemp(filepath.Dir(s.cachePath), ".snapshot-cache-*")
	if err != nil {
		fail(err)
		return
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(raw); err != nil {
		tmp.Close()
		fail(err)
		return
	}
	if err := tmp.Chmod(0o640); err != nil {
		tmp.Close()
		fail(err)
		return
	}
	if err := tmp.Close(); err != nil {
		fail(err)
		return
	}
	if err := os.Rename(tmp.Name(), s.cachePath); err != nil {
		fail(err)
	}
}

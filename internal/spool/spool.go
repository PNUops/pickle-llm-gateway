// Package spool records one usage event per request as a JSONL line. The
// spool is the gateway's outbox: a future control-plane reporter reads these
// files and ships them in batches, deduplicating on eventUuid, so the format
// here is already the wire format. Prompt and response bodies have no field
// to land in, by design.
package spool

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// Event statuses, mirroring what the request path can observe.
const (
	StatusOK           = "OK"
	StatusAuthRejected = "AUTH_REJECTED"
	StatusRateLimited  = "RATE_LIMITED"
	StatusBadRequest   = "BAD_REQUEST"
	StatusUpstreamErr  = "UPSTREAM_ERROR"
	StatusTimeout      = "TIMEOUT"
	StatusCanceled     = "CANCELED"
)

// Event is one request's accounting record.
type Event struct {
	EventUUID string `json:"eventUuid"`
	// Generation is the snapshot generation in force for this request, so a
	// later reader can tell which model mapping and limits applied — needed
	// because the public model name is stable across upstream-model swaps.
	Generation      int64  `json:"generation,omitempty"`
	KeyID           string `json:"keyId,omitempty"`
	PublicModelName string `json:"publicModelName,omitempty"`
	// BudgetAxis snapshots the TOKEN/CREDIT route decision from the model in
	// force for this request. It stays empty when no valid model or passthrough
	// route was resolved, so older spool lines and local unknown-model refusals
	// omit the additive field.
	BudgetAxis string `json:"budgetAxis,omitempty"`
	// UpstreamRef names the last upstream the request was sent to, and
	// Attempts how many tries it took. The public model name deliberately hides
	// which server answered, so without these a fallback to a second upstream —
	// a different model, often a paid one — is indistinguishable in the
	// accounting from the ordinary path. It cannot be reconstructed later
	// either: nothing else records it. A failed request carries them too: a
	// timeout or a 5xx still costs whatever the upstream had already produced,
	// and a request refused before any upstream was contacted is the only one
	// that leaves both empty.
	UpstreamRef  string    `json:"upstreamRef,omitempty"`
	Attempts     int       `json:"attempts,omitempty"`
	Status       string    `json:"status"`
	ErrorType    string    `json:"errorType,omitempty"`
	InputTokens  int       `json:"inputTokens"`
	OutputTokens int       `json:"outputTokens"`
	Estimated    bool      `json:"estimated,omitempty"`
	LatencyMs    int64     `json:"latencyMs"`
	TtftMs       int64     `json:"ttftMs,omitempty"`
	RequestedAt  time.Time `json:"requestedAt"`
}

// Writer appends events to a per-day file (usage-YYYYMMDD.jsonl, UTC) in the
// spool directory. Writes are serialized; a write failure is returned to the
// caller for logging but never blocks the response that already went out.
type Writer struct {
	mu  sync.Mutex
	dir string
	day string
	f   *os.File
}

// Open ensures the spool directory exists.
func Open(dir string) (*Writer, error) {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("spool: %w", err)
	}
	return &Writer{dir: dir}, nil
}

// Write appends one event.
func (w *Writer) Write(ev Event) error {
	line, err := json.Marshal(ev)
	if err != nil {
		return fmt.Errorf("spool: %w", err)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	day := ev.RequestedAt.UTC().Format("20060102")
	if w.f == nil || day != w.day {
		if w.f != nil {
			_ = w.f.Close()
		}
		f, err := os.OpenFile(filepath.Join(w.dir, "usage-"+day+".jsonl"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o640)
		if err != nil {
			return fmt.Errorf("spool: %w", err)
		}
		w.f = f
		w.day = day
	}
	if _, err := w.f.Write(append(line, '\n')); err != nil {
		return fmt.Errorf("spool: %w", err)
	}
	return nil
}

// It is best-effort: a file that cannot be parsed or removed is left in place
// and the error is returned for logging, never fatal. Retention exists because
// nothing else rotates the spool and the gateway LXC has a small root disk;
// once the api ingests these events it will own the authoritative copy.
//
// keep is asked whether a day file may be deleted. Retention alone is not
// enough once shipping is on: a day the reporter never confirmed is the only
// copy of those events.
type keepFunc func(day string) bool

// Prune deletes spool files whose day is older than retentionDays before now,
// skipping any day that keep reports as not yet shipped. keep may be nil when
// nothing ships (the spool is then the only record and retention is the only
// bound).
func (w *Writer) Prune(now time.Time, retentionDays int, keep keepFunc) error {
	if retentionDays <= 0 {
		return nil
	}
	cutoff := now.UTC().AddDate(0, 0, -retentionDays)
	entries, err := filepath.Glob(filepath.Join(w.dir, "usage-*.jsonl"))
	if err != nil {
		return fmt.Errorf("spool prune: %w", err)
	}
	w.mu.Lock()
	current := w.day
	w.mu.Unlock()
	var firstErr error
	for _, path := range entries {
		base := filepath.Base(path)
		day := strings.TrimSuffix(strings.TrimPrefix(base, "usage-"), ".jsonl")
		if day == current {
			continue
		}
		d, err := time.Parse("20060102", day)
		if err != nil {
			continue
		}
		if !d.Before(cutoff) {
			continue
		}
		if keep != nil && !keep(day) {
			// Past retention but never reported: keeping stale bytes is the
			// lesser harm against losing the only record of that usage.
			continue
		}
		if err := os.Remove(path); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// Close releases the current file.
func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f == nil {
		return nil
	}
	err := w.f.Close()
	w.f = nil
	return err
}

// NewEventUUID returns a random 128-bit identifier in UUIDv4 form. It exists
// so batch ingestion can be retried without double-counting.
func NewEventUUID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand failing means the process has bigger problems; a
		// timestamp keeps the event usable rather than dropping it.
		return fmt.Sprintf("t-%d", time.Now().UnixNano())
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	dst := make([]byte, 32)
	hex.Encode(dst, b[:])
	return string(dst[0:8]) + "-" + string(dst[8:12]) + "-" + string(dst[12:16]) + "-" + string(dst[16:20]) + "-" + string(dst[20:32])
}

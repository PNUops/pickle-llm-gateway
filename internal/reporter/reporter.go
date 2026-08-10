// Package reporter ships spooled usage events to the control plane. The spool
// is the outbox: the request path writes there and never waits for the
// network, and this package walks it from a persisted checkpoint. Delivery is
// at-least-once — a batch that succeeded but whose checkpoint write did not
// land is simply sent again, and the control plane deduplicates on the
// event's uuid.
package reporter

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pnuops/pickle-llm-gateway/internal/version"
)

// checkpoint records how far each day file has been shipped. It is a map, not
// a single cursor, because a request that starts before UTC midnight and ends
// after it appends to *yesterday's* file after today's has already been
// shipped: a single "we are past that day" cursor would skip those events
// forever. Offsets only ever move forward, so re-reading a day costs nothing.
type checkpoint struct {
	Offsets map[string]int64 `json:"offsets"`
}

func (c *checkpoint) offsetOf(day string) int64 {
	if c.Offsets == nil {
		return 0
	}
	return c.Offsets[day]
}

func (c *checkpoint) set(day string, offset int64) {
	if c.Offsets == nil {
		c.Offsets = map[string]int64{}
	}
	c.Offsets[day] = offset
}

// Reporter walks the spool and posts batches. One instance, one goroutine.
type Reporter struct {
	dir        string
	url        string
	token      string
	batchSize  int
	client     *http.Client
	log        *slog.Logger
	ckptPath   string
	ckpt       checkpoint
	loadedCkpt bool

	// shipFailures counts batches the control plane refused permanently and
	// this reporter therefore skipped. Those events are gone: reported so the
	// loss is a number somewhere rather than only a log line.
	shipFailures atomic.Int64
}

// ShipFailures is how many batches were dropped after a permanent refusal.
func (r *Reporter) ShipFailures() int64 { return r.shipFailures.Load() }

// New builds a reporter over the spool directory.
func New(spoolDir, baseURL, token string, batchSize int, timeout time.Duration, log *slog.Logger) *Reporter {
	return &Reporter{
		dir:       spoolDir,
		url:       baseURL + "/internal/llm/usage",
		token:     token,
		batchSize: batchSize,
		client:    &http.Client{Timeout: timeout},
		log:       log,
		ckptPath:  filepath.Join(spoolDir, ".shipped"),
	}
}

// Run ships on an interval until the context ends, backing off when the
// control plane is unavailable. A failure never touches the request path: the
// spool keeps growing and the next pass picks up where this one stopped.
func (r *Reporter) Run(ctx context.Context, interval time.Duration) {
	backoff := interval
	const maxBackoff = 5 * time.Minute
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		sent, err := r.Flush(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			backoff = min(backoff*2, maxBackoff)
			r.log.Error("usage report failed, will retry", "error", err, "retryIn", backoff.String())
			continue
		}
		backoff = interval
		if sent > 0 {
			r.log.Info("usage reported", "events", sent)
		}
	}
}

// Flush ships everything currently spooled, in batches, and returns how many
// events went out. It stops at the first failure so the checkpoint never runs
// ahead of what the control plane accepted.
func (r *Reporter) Flush(ctx context.Context) (int, error) {
	if err := r.loadCheckpoint(); err != nil {
		return 0, err
	}
	files, err := filepath.Glob(filepath.Join(r.dir, "usage-*.jsonl"))
	if err != nil {
		return 0, fmt.Errorf("reporter: %w", err)
	}
	sort.Strings(files)

	total := 0
	for _, path := range files {
		day := dayOf(path)
		if day == "" {
			// Not a spool file this package wrote. Ignoring it keeps one
			// stray name from stalling every real file behind it.
			continue
		}
		sent, err := r.shipFile(ctx, path, day, r.ckpt.offsetOf(day))
		total += sent
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

// ShippedThrough reports the day files that have been fully or partly shipped,
// so retention can refuse to delete what was never reported.
func (r *Reporter) ShippedThrough() (map[string]int64, error) {
	if err := r.loadCheckpoint(); err != nil {
		return nil, err
	}
	out := make(map[string]int64, len(r.ckpt.Offsets))
	maps.Copy(out, r.ckpt.Offsets)
	return out, nil
}

// shipFile posts the complete lines of one day file from offset onward.
func (r *Reporter) shipFile(ctx context.Context, path, day string, offset int64) (int, error) {
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, nil // pruned between the glob and here
		}
		return 0, fmt.Errorf("reporter: %w", err)
	}
	defer f.Close()
	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return 0, fmt.Errorf("reporter: %w", err)
	}

	br := bufio.NewReaderSize(f, 64<<10)
	sent := 0
	batch := make([]json.RawMessage, 0, r.batchSize)
	pos := offset
	batchEnd := offset

	flushBatch := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := r.post(ctx, batch); err != nil {
			if !permanent(err) {
				return err
			}
			// The control plane understood the batch and refused it. Retrying
			// changes nothing, and stopping here would park the checkpoint in
			// front of it forever: every later event — every later day —
			// would queue behind one bad batch until retention deleted the
			// lot. Skipping loses this batch and keeps the channel moving,
			// which is the lesser loss, so it is counted and logged loudly.
			r.shipFailures.Add(1)
			r.log.Error("control plane refused a usage batch permanently, skipping it",
				"day", day, "events", len(batch), "error", err)
		} else {
			sent += len(batch)
		}
		batch = batch[:0]
		r.ckpt.set(day, batchEnd)
		return r.saveCheckpoint()
	}

	for {
		line, err := br.ReadBytes('\n')
		if err != nil {
			// A trailing fragment is a write in progress: leave the offset
			// before it so the next pass reads the whole line.
			break
		}
		pos += int64(len(line))
		trimmed := bytes.TrimSpace(line)
		if len(trimmed) == 0 {
			batchEnd = pos
			continue
		}
		batch = append(batch, json.RawMessage(trimmed))
		batchEnd = pos
		if len(batch) >= r.batchSize {
			if err := flushBatch(); err != nil {
				return sent, err
			}
		}
	}
	if err := flushBatch(); err != nil {
		return sent, err
	}
	return sent, nil
}

// ingestRequest is the wire shape of one batch. Events are forwarded verbatim
// as they were spooled, so the reporter never has to know the event schema —
// a field added by the request path ships without touching this package.
type ingestRequest struct {
	AgentVersion string            `json:"agentVersion,omitempty"`
	Events       []json.RawMessage `json:"events"`
}

// postError carries the status a refusal came with, so the caller can tell a
// control plane that is down from one that has decided.
type postError struct {
	status int
	msg    string
}

func (e *postError) Error() string { return e.msg }

// permanent reports whether retrying this error could ever succeed. A 4xx is
// the control plane saying the batch itself is the problem — except 408 and
// 429, which are timing, not judgement.
func permanent(err error) bool {
	var pe *postError
	if !errors.As(err, &pe) {
		return false
	}
	if pe.status == http.StatusRequestTimeout || pe.status == http.StatusTooManyRequests {
		return false
	}
	return pe.status >= 400 && pe.status < 500
}

func (r *Reporter) post(ctx context.Context, events []json.RawMessage) error {
	body, err := json.Marshal(ingestRequest{AgentVersion: version.String(), Events: events})
	if err != nil {
		return fmt.Errorf("reporter: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("reporter: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+r.token)
	resp, err := r.client.Do(req)
	if err != nil {
		return fmt.Errorf("reporter: %w", err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		return &postError{
			status: resp.StatusCode,
			msg:    fmt.Sprintf("reporter: control plane returned HTTP %d", resp.StatusCode),
		}
	}
	return nil
}

func (r *Reporter) loadCheckpoint() error {
	if r.loadedCkpt {
		return nil
	}
	raw, err := os.ReadFile(r.ckptPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			r.loadedCkpt = true
			return nil // nothing shipped yet
		}
		return fmt.Errorf("reporter checkpoint: %w", err)
	}
	// A corrupt checkpoint would otherwise stall shipping forever; starting
	// over is safe because delivery is deduplicated on the event uuid.
	if err := json.Unmarshal(raw, &r.ckpt); err != nil {
		r.log.Error("unreadable ship checkpoint, restarting from the beginning", "error", err)
		r.ckpt = checkpoint{}
	}
	r.loadedCkpt = true
	return nil
}

func (r *Reporter) saveCheckpoint() error {
	raw, err := json.Marshal(r.ckpt)
	if err != nil {
		return fmt.Errorf("reporter checkpoint: %w", err)
	}
	tmp, err := os.CreateTemp(r.dir, ".shipped-*")
	if err != nil {
		return fmt.Errorf("reporter checkpoint: %w", err)
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(raw); err != nil {
		tmp.Close()
		return fmt.Errorf("reporter checkpoint: %w", err)
	}
	if err := tmp.Chmod(0o640); err != nil {
		tmp.Close()
		return fmt.Errorf("reporter checkpoint: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("reporter checkpoint: %w", err)
	}
	if err := os.Rename(tmp.Name(), r.ckptPath); err != nil {
		return fmt.Errorf("reporter checkpoint: %w", err)
	}
	return nil
}

// dayOf extracts the YYYYMMDD part of a spool file name, and returns "" for
// anything that is not one — including a name that matches the glob but is not
// a date, such as a hand-made copy.
func dayOf(path string) string {
	base := filepath.Base(path)
	if !strings.HasPrefix(base, "usage-") || !strings.HasSuffix(base, ".jsonl") {
		return ""
	}
	day := strings.TrimSuffix(strings.TrimPrefix(base, "usage-"), ".jsonl")
	if _, err := time.Parse("20060102", day); err != nil {
		return ""
	}
	return day
}

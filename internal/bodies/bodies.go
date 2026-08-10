// Package bodies carries opted-in prompt and response text to the control
// plane. It is deliberately a separate channel from the usage spool: the
// service records only counters by default, and the exception that permits
// collecting the text at all is conditioned on keeping that collection apart
// from ordinary operation.
//
// The separation is enforced by construction rather than by care:
//
//   - Records live in memory and go straight out over the network. Nothing
//     here writes to disk, so a gateway with no reachable control plane
//     accumulates no bodies anywhere — the capture is simply lost.
//   - The queue is bounded and drops rather than blocks, so this channel can
//     never slow or fail a student's request.
//   - Capture happens only for keys whose snapshot entry opted in, which the
//     request path checks before it ever builds a record.
package bodies

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/pnuops/pickle-llm-gateway/internal/version"
)

// Record is one request's captured text, joined to its usage event by the
// event id. It carries no token counts or timings — those live on the usage
// event, and duplicating them here would create a second, divergent truth.
type Record struct {
	EventUUID   string    `json:"eventUuid"`
	KeyID       string    `json:"keyId"`
	RequestedAt time.Time `json:"requestedAt"`
	// Request is the messages array exactly as the student sent it.
	Request json.RawMessage `json:"request,omitempty"`
	// Response is the assistant text, assembled from the stream when the
	// answer was streamed.
	Response string `json:"response,omitempty"`
	// Truncated marks a record whose response hit the capture cap.
	Truncated bool `json:"truncated,omitempty"`
}

// Sink accepts records and ships them. A nil Sink is a working "capture
// disabled" value: every method is safe to call on it.
type Sink struct {
	queue  chan Record
	url    string
	token  string
	client *http.Client
	log    *slog.Logger
	batch  int
}

// New builds a sink with a bounded queue. queueSize bounds the memory this
// channel can hold; beyond it, records are dropped with a log line.
func New(baseURL, token string, queueSize, batch int, timeout time.Duration, log *slog.Logger) *Sink {
	return &Sink{
		queue:  make(chan Record, queueSize),
		url:    baseURL + "/internal/llm/bodies",
		token:  token,
		client: &http.Client{Timeout: timeout},
		log:    log,
		batch:  batch,
	}
}

// Enabled reports whether capture should happen at all.
func (s *Sink) Enabled() bool { return s != nil }

// Offer hands over a record without ever blocking the caller. A nil record
// (the ordinary case — capture is off) does nothing. A full queue means the
// control plane is behind; dropping is correct here because the usage event —
// the accounting record — went to the durable spool regardless.
func (s *Sink) Offer(r *Record) {
	if s == nil || r == nil {
		return
	}
	select {
	case s.queue <- *r:
	default:
		s.log.Warn("body capture queue full, dropping a record", "keyId", r.KeyID)
	}
}

// Run drains the queue until the context ends, sending in batches. A failed
// batch is dropped, not retried: these records are a convenience, and holding
// them for a retry is exactly the accumulation this channel avoids.
func (s *Sink) Run(ctx context.Context) {
	if s == nil {
		return
	}
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	pending := make([]Record, 0, s.batch)
	flush := func() {
		if len(pending) == 0 {
			return
		}
		if err := s.post(ctx, pending); err != nil && ctx.Err() == nil {
			s.log.Error("body capture delivery failed, records dropped",
				"count", len(pending), "error", err)
		}
		pending = pending[:0]
	}
	for {
		select {
		case <-ctx.Done():
			return
		case r := <-s.queue:
			pending = append(pending, r)
			if len(pending) >= s.batch {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

type ingestRequest struct {
	AgentVersion string   `json:"agentVersion,omitempty"`
	Records      []Record `json:"records"`
}

func (s *Sink) post(ctx context.Context, records []Record) error {
	body, err := json.Marshal(ingestRequest{AgentVersion: version.String(), Records: records})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+s.token)
	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("control plane returned HTTP %d", resp.StatusCode)
	}
	return nil
}

// ResponseCapBytes bounds how much assistant text one record carries. Long
// answers are cut with Truncated set rather than held whole in memory.
const ResponseCapBytes = 256 << 10

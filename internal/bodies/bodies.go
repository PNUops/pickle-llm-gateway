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
	"sync/atomic"
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
	// RequestTruncated and ResponseTruncated say which side hit its cap. One
	// flag for both would leave a reader unable to tell a cut prompt from a
	// cut answer, and the two mean different things to whoever reads the
	// record later.
	//
	// A truncated request cannot stay a messages array — cutting JSON mid-way
	// produces something no parser will take — so it becomes a JSON string
	// holding the prefix. The field is therefore "the messages array, or a
	// string when RequestTruncated is set".
	RequestTruncated  bool `json:"requestTruncated,omitempty"`
	ResponseTruncated bool `json:"responseTruncated,omitempty"`
}

// size estimates what this record costs in memory and on the wire. Used to
// bound the queue and the batch by bytes rather than by count: the records
// vary by three orders of magnitude, so a count bounds nothing.
func (r *Record) size() int {
	return len(r.Request) + len(r.Response) + recordOverheadBytes
}

// recordOverheadBytes covers the ids, timestamp and JSON punctuation.
const recordOverheadBytes = 256

// Sink accepts records and ships them. A nil Sink is a working "capture
// disabled" value: every method is safe to call on it.
type Sink struct {
	queue  chan Record
	url    string
	token  string
	client *http.Client
	log    *slog.Logger
	batch  int
	// queuedBytes bounds the channel by size as well as by depth. Depth alone
	// is not a bound: a queue of records that are each at their cap holds two
	// orders of magnitude more memory than a queue of ordinary ones, and this
	// process shares a small LXC with nothing to spare.
	queuedBytes atomic.Int64
	maxBytes    int64
	dropped     atomic.Int64
}

// New builds a sink with a bounded queue. queueSize bounds the memory this
// channel can hold; beyond it, records are dropped with a log line.
func New(baseURL, token string, queueSize, batch int, timeout time.Duration, log *slog.Logger) *Sink {
	return &Sink{
		queue:    make(chan Record, queueSize),
		url:      baseURL + "/internal/llm/bodies",
		token:    token,
		client:   &http.Client{Timeout: timeout},
		log:      log,
		batch:    batch,
		maxBytes: queueBytes(queueSize),
	}
}

// queueBytes turns a queue depth into a memory bound. The eighth is because
// records are nothing like their cap in practice — an ordinary prompt and
// answer are a few kilobytes, not three hundred — so sizing for the worst case
// would reserve two orders of magnitude more than the queue ever holds.
//
// The floor is what stops a small depth from meaning "capture nothing": below
// about eight, an eighth of the worst case is less than one maximal record, so
// every large record would be dropped on arrival, forever, with only a warning
// per request to say so. A small depth has to mean "few records", never "no
// records".
func queueBytes(depth int) int64 {
	const maxRecord = RequestCapBytes + ResponseCapBytes + recordOverheadBytes
	if n := int64(depth) * maxRecord / 8; n > maxRecord {
		return n
	}
	return maxRecord
}

// Dropped is how many records were discarded because the queue was full or a
// delivery failed. Nil (capture off) has nothing to drop.
func (s *Sink) Dropped() int64 {
	if s == nil {
		return 0
	}
	return s.dropped.Load()
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
	size := int64(r.size())
	if s.queuedBytes.Load()+size > s.maxBytes {
		s.dropped.Add(1)
		s.log.Warn("body capture queue over its memory bound, dropping a record", "keyId", r.KeyID)
		return
	}
	select {
	case s.queue <- *r:
		s.queuedBytes.Add(size)
	default:
		s.dropped.Add(1)
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
	pendingBytes := 0
	flush := func() {
		if len(pending) == 0 {
			return
		}
		if err := s.post(ctx, pending); err != nil && ctx.Err() == nil {
			s.log.Error("body capture delivery failed, records dropped",
				"count", len(pending), "error", err)
			s.dropped.Add(int64(len(pending)))
		}
		pending = pending[:0]
		pendingBytes = 0
	}
	for {
		select {
		case <-ctx.Done():
			return
		case r := <-s.queue:
			s.queuedBytes.Add(-int64(r.size()))
			pending = append(pending, r)
			pendingBytes += r.size()
			// Two bounds, because either one alone leaves a bad case: many
			// small records make a batch the receiver would rather have split,
			// and a handful of capped ones make one it would refuse outright.
			if len(pending) >= s.batch || pendingBytes >= BatchCapBytes {
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
	// Any 2xx is acceptance; see the same reasoning in the reporter. Here the
	// cost of getting it wrong is worse — a failed body batch is dropped, not
	// retried, so a 201 would silently discard captured text.
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("control plane returned HTTP %d", resp.StatusCode)
	}
	return nil
}

const (
	// ResponseCapBytes bounds how much assistant text one record carries. Long
	// answers are cut, with ResponseTruncated set, rather than held whole in
	// memory.
	ResponseCapBytes = 256 << 10
	// RequestCapBytes bounds the captured prompt the same way. Without it the
	// only bound is the request-body limit, two megabytes, which multiplied by
	// a full queue is more memory than this host has.
	RequestCapBytes = 64 << 10
	// BatchCapBytes bounds one delivery. It is deliberately well under any
	// sane request-size limit on the receiving side: a batch refused for being
	// too large is not retried, so the text would simply be lost.
	BatchCapBytes = 4 << 20
)

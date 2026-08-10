package snapshot

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
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

// SyncRequest is what the gateway reports on every poll. It is deliberately
// small: the control plane decides everything, and anything the gateway says
// about itself is a claim, not a measurement.
type SyncRequest struct {
	AppliedGeneration int64  `json:"appliedGeneration"`
	AgentVersion      string `json:"agentVersion,omitempty"`
	InFlight          int    `json:"inFlight,omitempty"`
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
	// inFlight reports the gateway's current concurrent requests; nil until
	// the server is wired, and never load-bearing.
	inFlight  func() int
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
	}
}

// SetInFlight supplies the in-flight gauge reported on each poll.
func (s *HTTPSource) SetInFlight(f func() int) { s.inFlight = f }

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
	req := SyncRequest{AppliedGeneration: served, AgentVersion: version.String()}
	if s.inFlight != nil {
		req.InFlight = s.inFlight()
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

func (s *HTTPSource) writeCache(raw []byte) {
	tmp, err := os.CreateTemp(filepath.Dir(s.cachePath), ".snapshot-cache-*")
	if err != nil {
		return
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(raw); err != nil {
		tmp.Close()
		return
	}
	if err := tmp.Chmod(0o640); err != nil {
		tmp.Close()
		return
	}
	if err := tmp.Close(); err != nil {
		return
	}
	_ = os.Rename(tmp.Name(), s.cachePath)
}

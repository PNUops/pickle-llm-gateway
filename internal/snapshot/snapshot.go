// Package snapshot holds the gateway's authorization state: which API keys
// exist, which models are served, and which limits apply. The state arrives
// as one JSON document and is swapped atomically, so a request always sees a
// consistent view. Today the document is a local file maintained by the
// operator tooling; the document format is also the response format a future
// control-plane sync will serve, so only the loader knows where it came from.
package snapshot

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync/atomic"
	"time"
)

// Document is the full authorization state, replaced as a whole on every
// load. Partial application is structurally impossible.
type Document struct {
	Generation     int64   `json:"generation"`
	ServiceEnabled bool    `json:"serviceEnabled"`
	Models         []Model `json:"models"`
	Keys           []Key   `json:"keys"`
}

// Model maps one public model name to an upstream target. Students only ever
// see PublicName; UpstreamRef selects a configured upstream block and
// UpstreamModel is the identifier sent to it.
type Model struct {
	PublicName      string `json:"publicName"`
	UpstreamRef     string `json:"upstreamRef"`
	UpstreamModel   string `json:"upstreamModel"`
	MaxInputTokens  int    `json:"maxInputTokens,omitempty"`
	MaxOutputTokens int    `json:"maxOutputTokens,omitempty"`
}

// Key statuses. Anything but ACTIVE refuses requests; the distinction only
// changes the error message.
const (
	KeyActive    = "ACTIVE"
	KeySuspended = "SUSPENDED"
	KeyRevoked   = "REVOKED"
)

// Key is one issued API key. The plaintext never appears anywhere: TokenHash
// is the hex sha256 of the full bearer token, and lookup hashes the presented
// token before comparing.
type Key struct {
	KeyID          string     `json:"keyId"`
	TokenHash      string     `json:"tokenHash"`
	Status         string     `json:"status"`
	ExpiresAt      *time.Time `json:"expiresAt,omitempty"`
	AllowedModels  []string   `json:"allowedModels,omitempty"`
	Limits         Limits     `json:"limits"`
	QuotaExhausted bool       `json:"quotaExhausted,omitempty"`
}

// Limits are the short-window limits the gateway enforces locally. A zero
// value means "not set here" and falls back to the gateway-wide default;
// long-window quotas are decided by whoever produces the document and arrive
// as the QuotaExhausted flag.
type Limits struct {
	Rpm         int `json:"rpm,omitempty"`
	Tpm         int `json:"tpm,omitempty"`
	Concurrency int `json:"concurrency,omitempty"`
}

// Allows reports whether the key may use the given public model name. An
// empty allow list means every served model, which is the hand-maintained
// document's default.
func (k *Key) Allows(publicName string) bool {
	if len(k.AllowedModels) == 0 {
		return true
	}
	for _, m := range k.AllowedModels {
		if m == publicName {
			return true
		}
	}
	return false
}

// state is one loaded document plus the lookup maps derived from it.
type state struct {
	doc      Document
	byHash   map[string]*Key
	byPublic map[string]*Model
	loadedAt time.Time
}

// Store serves the current state and refreshes it in the background. Readers
// never block on a reload.
type Store struct {
	cur  atomic.Pointer[state]
	path string
	log  *slog.Logger

	// file identity of the last successful load; a reload only parses the
	// file when this changed.
	lastModTime time.Time
	lastSize    int64
}

// Open loads the document once and fails if it is missing or invalid: a
// gateway must not start with nothing to authorize against. Later reload
// failures keep the last good state instead, bounding staleness rather than
// dropping traffic.
func Open(path string, log *slog.Logger) (*Store, error) {
	s := &Store{path: path, log: log}
	if err := s.reload(); err != nil {
		return nil, err
	}
	return s, nil
}

// Current returns the state for one request. The pointer is immutable; a
// concurrent reload publishes a new one.
func (s *Store) Current() (Document, func(hash string) *Key, func(publicName string) *Model) {
	st := s.cur.Load()
	return st.doc, func(h string) *Key { return st.byHash[h] }, func(n string) *Model { return st.byPublic[n] }
}

// Generation is the currently served document generation.
func (s *Store) Generation() int64 { return s.cur.Load().doc.Generation }

// LoadedAt is when the current state was read.
func (s *Store) LoadedAt() time.Time { return s.cur.Load().loadedAt }

// Refresh re-reads the document if the file changed since the last successful
// load. Errors are logged, never propagated to request handling.
func (s *Store) Refresh() {
	fi, err := os.Stat(s.path)
	if err != nil {
		s.log.Error("snapshot stat failed, keeping current state", "error", err)
		return
	}
	if fi.ModTime().Equal(s.lastModTime) && fi.Size() == s.lastSize {
		return
	}
	if err := s.reload(); err != nil {
		s.log.Error("snapshot reload failed, keeping current state", "error", err)
	}
}

func (s *Store) reload() error {
	fi, err := os.Stat(s.path)
	if err != nil {
		return fmt.Errorf("snapshot: %w", err)
	}
	raw, err := os.ReadFile(s.path)
	if err != nil {
		return fmt.Errorf("snapshot: %w", err)
	}
	st, err := build(raw)
	if err != nil {
		return fmt.Errorf("snapshot %s: %w", s.path, err)
	}
	prev := s.cur.Load()
	if prev != nil && st.doc.Generation < prev.doc.Generation {
		// A generation moving backwards means the file was replaced with an
		// older document; serving it would silently undo a revocation.
		return fmt.Errorf("snapshot generation went backwards: %d -> %d", prev.doc.Generation, st.doc.Generation)
	}
	s.cur.Store(st)
	s.lastModTime = fi.ModTime()
	s.lastSize = fi.Size()
	s.log.Info("snapshot loaded", "generation", st.doc.Generation, "models", len(st.doc.Models), "keys", len(st.doc.Keys), "serviceEnabled", st.doc.ServiceEnabled)
	return nil
}

func build(raw []byte) (*state, error) {
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.DisallowUnknownFields()
	var doc Document
	if err := dec.Decode(&doc); err != nil {
		return nil, err
	}
	st := &state{
		doc:      doc,
		byHash:   make(map[string]*Key, len(doc.Keys)),
		byPublic: make(map[string]*Model, len(doc.Models)),
		loadedAt: time.Now(),
	}
	for i := range doc.Models {
		m := &doc.Models[i]
		if m.PublicName == "" || m.UpstreamRef == "" || m.UpstreamModel == "" {
			return nil, fmt.Errorf("model %d: publicName, upstreamRef and upstreamModel are all required", i)
		}
		if _, dup := st.byPublic[m.PublicName]; dup {
			return nil, fmt.Errorf("duplicate public model name %q", m.PublicName)
		}
		st.byPublic[m.PublicName] = m
	}
	for i := range doc.Keys {
		k := &doc.Keys[i]
		if k.KeyID == "" {
			return nil, fmt.Errorf("key %d: keyId is required", i)
		}
		if !validHash(k.TokenHash) {
			return nil, fmt.Errorf("key %s: tokenHash must be 64 lowercase hex chars", k.KeyID)
		}
		switch k.Status {
		case KeyActive, KeySuspended, KeyRevoked:
		default:
			return nil, fmt.Errorf("key %s: unknown status %q", k.KeyID, k.Status)
		}
		if _, dup := st.byHash[k.TokenHash]; dup {
			return nil, fmt.Errorf("key %s: tokenHash duplicates another key", k.KeyID)
		}
		st.byHash[k.TokenHash] = k
	}
	return st, nil
}

func validHash(h string) bool {
	if len(h) != 64 {
		return false
	}
	if _, err := hex.DecodeString(h); err != nil {
		return false
	}
	return h == strings.ToLower(h)
}

// HashToken is the one token-hashing rule in the codebase: hex sha256 of the
// full plaintext. The issuing tool and the lookup path both call it.
func HashToken(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

// ErrNotFound reports a missing key on lookup paths that want an error value.
var ErrNotFound = errors.New("api key not found")

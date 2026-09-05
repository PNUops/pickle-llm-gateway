// Package snapshot holds the gateway's authorization state: which API keys
// exist, which models are served, and which limits apply. The state arrives
// as one JSON document and is swapped atomically, so a request always sees a
// consistent view. Today the document is a local file maintained by the
// operator tooling; the document format is also the response format a future
// control-plane sync will serve, so only the loader knows where it came from.
package snapshot

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// SupportedFormat is the highest document format this build understands. The
// gateway reports it on every control-plane poll so the writer can serve the
// format the reader actually has, rather than assuming the two were deployed
// together. Bump it when a change would make an older gateway misread a
// document — not for additions, which are already tolerated.
const SupportedFormat = 1

// Document is the full authorization state, replaced as a whole on every
// load. Partial application is structurally impossible.
//
// The document is also the future api-to-gateway sync response, so its
// compatibility rules are deliberate: unknown fields inside models[] and
// keys[] are ignored, and from the control plane unknown top-level members are
// too, which is what lets the api extend the document without a lockstep
// gateway upgrade. FormatVersion is informational, for the same reason.
type Document struct {
	FormatVersion  int     `json:"formatVersion,omitempty"`
	Generation     int64   `json:"generation"`
	ServiceEnabled bool    `json:"serviceEnabled"`
	Models         []Model `json:"models"`
	Keys           []Key   `json:"keys"`
	// PassthroughRef, when set, names the upstream that serves any public model
	// name the catalog does not list. The commercial provider carries its own
	// ever-changing model catalog, so those names are forwarded as-is instead of
	// being enumerated here; a passthrough model is always on the CREDIT budget
	// axis, and names under the self-serve prefix never pass through (a typo in
	// a curated name must stay a 404, not become a billable request). Empty
	// keeps today's behaviour: unknown model names are 404.
	PassthroughRef string `json:"passthroughRef,omitempty"`
}

// Model visibility. RESTRICTED models are reachable only by a key that names
// them explicitly in allowedModels; PUBLIC (the default) is reachable by any
// key with an empty allow list. This is fail-safe on purpose: adding a new
// model does not silently open it to every existing key.
const (
	ModelPublic     = "PUBLIC"
	ModelRestricted = "RESTRICTED"
)

// Budget axes. A model's axis decides which of a key's two budgets its usage
// counts against and which enforcement applies: TOKEN models are covered by
// the document's QuotaExhausted flag (the daily token allowance, decided by
// the control plane), CREDIT models by the per-key upstream credential whose
// issuer enforces a money limit. The axis is a property of the model row, not
// of the upstream kind — a self-serve model temporarily served by an external
// upstream stays on the TOKEN axis, and swapping the upstream never moves it.
const (
	AxisToken  = "TOKEN"
	AxisCredit = "CREDIT"
)

// reservedModelPrefixes are the prefixes reserved for curated self-serve
// model names: the current one first ("pickle-"), then prefixes retired by a
// rename ("pnu-", retired 2026-08-25) that stay guarded so a stale name in
// student code keeps failing as a 404 instead of turning into a billable
// passthrough request. A future rename is a one-line reorder here — prepend
// the new prefix, keep the old. Every entry must stay lowercase: the guard
// lowercases the candidate before comparing, so a mixed-case entry would
// silently never match (a test pins this).
var reservedModelPrefixes = []string{"pickle-", "pnu-"}

// IsReservedModelName reports whether a public model name sits under a
// reserved self-serve prefix, current or retired. Such a name is served only
// by an exact catalog match and never by passthrough. This function is the
// only door to the list — prefix checks elsewhere would skip the case
// handling, the retired entries, and the tilde below.
//
// A leading tilde is stripped before comparing. The vendor marks its floating
// aliases that way, so the character is meaningful upstream and a caller can
// spell `~pickle-general`; without stripping, that walked past this guard and
// was synthesized as a passthrough model. Nothing is billed for it — no such
// model exists upstream, so the call dies there — but the guard exists so that
// a typo in a curated name stays a 404 instead of leaving for an upstream at
// all, and one character defeated it.
func IsReservedModelName(publicName string) bool {
	lower := strings.ToLower(publicName)
	lower = strings.TrimPrefix(lower, "~")
	for _, p := range reservedModelPrefixes {
		if strings.HasPrefix(lower, p) {
			return true
		}
	}
	return false
}

// Model maps one public model name to an upstream target. Students only ever
// see PublicName; UpstreamRef selects a configured upstream block and
// UpstreamModel is the identifier sent to it.
type Model struct {
	PublicName    string `json:"publicName"`
	UpstreamRef   string `json:"upstreamRef"`
	UpstreamModel string `json:"upstreamModel"`
	// FallbackRef names a second upstream to try when the first one fails
	// before any of the response has reached the client. Optional.
	FallbackRef     string `json:"fallbackRef,omitempty"`
	Visibility      string `json:"visibility,omitempty"` // PUBLIC (default) | RESTRICTED
	MaxInputTokens  int    `json:"maxInputTokens,omitempty"`
	MaxOutputTokens int    `json:"maxOutputTokens,omitempty"`
	// BudgetAxis is TOKEN (default when empty) or CREDIT — see the axis
	// constants. Empty means TOKEN so a document written before the axis
	// existed keeps meaning what it meant.
	BudgetAxis string `json:"budgetAxis,omitempty"`
}

// Restricted reports whether the model is reachable only by keys that name it.
func (m *Model) Restricted() bool { return m.Visibility == ModelRestricted }

// CreditAxis reports whether the model's usage is governed by the money
// budget (per-key upstream credential) rather than the token budget.
func (m *Model) CreditAxis() bool { return m.BudgetAxis == AxisCredit }

// Key statuses. Anything but ACTIVE refuses requests; the distinction only
// changes the error message.
const (
	KeyActive    = "ACTIVE"
	KeySuspended = "SUSPENDED"
	KeyRevoked   = "REVOKED"
)

// Key is one issued API key. The key's own plaintext never appears anywhere:
// TokenHash is the hex sha256 of the full bearer token, and lookup hashes the
// presented token before comparing. UpstreamCredentials is the deliberate
// exception to the old "hashes only" rule: it carries usable upstream secrets
// for this key, which is what makes per-key money limits enforceable by the
// upstream that holds the money.
type Key struct {
	KeyID         string     `json:"keyId"`
	TokenHash     string     `json:"tokenHash"`
	Status        string     `json:"status"`
	ExpiresAt     *time.Time `json:"expiresAt,omitempty"`
	AllowedModels []string   `json:"allowedModels,omitempty"`
	// CreditAllowedModels fences the money axis, and only the money axis. A
	// TOKEN-axis model is reachable whatever this holds: self-serving capacity
	// answers to its own daily quota, and refusing it here would restrict
	// something the control plane did not mean to restrict.
	//
	// Empty means unrestricted, the same as the field above, but the two never
	// substitute for each other — AllowedModels curates which catalogue models
	// a key sees at all, and filling it locks a key out of self-serving models
	// as a side effect. That is why the money fence is a second field rather
	// than a reuse of the first.
	//
	// Entries are exact public names, a whole vendor ("openai/*"), or a model
	// segment opened at one end ("openai/gpt-5-*", "openai/*-pro"), lower-cased
	// at load so a stored capital cannot silently match nothing.
	CreditAllowedModels []string `json:"creditAllowedModels,omitempty"`
	// CreditDeniedModels carves models back out of whatever the field above
	// leaves open, and takes the same entry shapes and the same matcher. It is
	// the money axis only, for the reason given above.
	//
	// A denial wins over an allowance. The two are not symmetric statements
	// about the same list: an allow list says which models an approver chose,
	// and a deny list says which ones nobody may reach whatever else was
	// chosen — the price outliers, mostly. Reading them the other way round
	// would let a wide allowance quietly reopen exactly the models a later
	// decision closed.
	//
	// Empty means this axis restricts nothing, like the field above, so a key
	// carrying neither list is unfenced and a key carrying only this one is
	// fenced out of just these names.
	CreditDeniedModels []string `json:"credit_denied_models,omitempty"`
	Limits             Limits   `json:"limits"`
	QuotaExhausted     bool     `json:"quotaExhausted,omitempty"`
	// UpstreamCredentials maps an upstream ref (lowercased at load) to the
	// bearer this key must use there. CREDIT-axis models require an entry for
	// the serving upstream — there is deliberately no fallback to the
	// gateway-wide env credential, because that would spend a shared budget on
	// behalf of a key that was never granted one. Absent or empty means the
	// key cannot reach CREDIT-axis models at all.
	UpstreamCredentials map[string]string `json:"upstreamCredentials,omitempty"`
	// CreditPending distinguishes the one reason a CREDIT credential can be
	// missing that ends by itself: the key was granted a money budget and its
	// upstream key has not been created yet. Absence alone cannot tell that
	// from "never granted", and the two need opposite answers — apply for a
	// budget, or wait for the one you have. The control plane sets it only
	// for the healing case; every other missing-credential state, a
	// credential the control plane could not decrypt included, arrives false
	// so nobody is told to wait for something that is not coming.
	CreditPending bool `json:"creditPending,omitempty"`
	// RecordBodies opts this key into prompt and response capture. It is the
	// key owner's choice, expressed in the control plane and carried here; the
	// default is off, so a key says nothing about bodies unless someone asked
	// for it. Capture never writes to the usage spool — see internal/bodies.
	RecordBodies bool `json:"recordBodies,omitempty"`
}

// Limits are the short-window limits the gateway enforces locally. All three
// govern TOKEN-axis models only, for the same reason QuotaExhausted does: they
// ration self-hosted serving capacity, and a CREDIT-axis call spends the money
// budget its own upstream credential carries instead. A zero value means "not
// set here" and falls back to the gateway-wide default; long-window quotas are
// decided by whoever produces the document and arrive as the QuotaExhausted
// flag.
type Limits struct {
	Rpm         int `json:"rpm,omitempty"`
	Tpm         int `json:"tpm,omitempty"`
	Concurrency int `json:"concurrency,omitempty"`
}

// CredentialFor returns the key's own bearer for the named upstream, or ""
// when it has none there. Refs are matched case-insensitively, like every
// other upstream-ref comparison.
func (k *Key) CredentialFor(ref string) string {
	if len(k.UpstreamCredentials) == 0 {
		return ""
	}
	return k.UpstreamCredentials[strings.ToLower(ref)]
}

// AllowsModel reports whether the key may use the model. An empty allow list
// means every PUBLIC model; a RESTRICTED model requires an explicit listing.
func (k *Key) AllowsModel(m *Model) bool {
	if len(k.AllowedModels) == 0 {
		return !m.Restricted()
	}
	for _, name := range k.AllowedModels {
		if name == m.PublicName {
			return true
		}
	}
	return false
}

// AllowsCreditModel reports whether the key may spend money on the model. It is
// the one entry point for both money lists, so a caller reaches the whole fence
// by asking this single question and cannot consult half of it.
//
// TOKEN-axis models return true unconditionally, which is the whole point of
// the fields: the lists are a money fence, and a self-serving model has no
// money to fence. Keeping that guard inside this function rather than at the
// call sites means a caller cannot forget the axis and fence the wrong one.
//
// Each list empty means that half restricts nothing, so a key with neither is
// bounded only by the amount granted, as it was before these fields existed.
// A denial wins: it is checked after the allowance and can only take away.
func (k *Key) AllowsCreditModel(m *Model) bool {
	if !m.CreditAxis() {
		return true
	}
	name := strings.ToLower(m.PublicName)
	if len(k.CreditAllowedModels) > 0 && !matchesAnyCreditModel(k.CreditAllowedModels, name) {
		return false
	}
	return !matchesAnyCreditModel(k.CreditDeniedModels, name)
}

// matchesAnyCreditModel reports whether any pattern in one already-normalized
// list covers the name. An empty list matches nothing, which is what lets each
// caller above read "empty" as its own kind of no-restriction.
func matchesAnyCreditModel(patterns []string, lowerName string) bool {
	for _, pattern := range patterns {
		if MatchesCreditModel(pattern, lowerName) {
			return true
		}
	}
	return false
}

// MatchesCreditModel reports whether one already-normalized pattern covers a
// lower-cased public model name. Both money lists are matched by this function,
// so an entry means the same thing whichever list it sits in — a rule that
// allowed differently from how it denied would be two rules to keep in step.
//
// Four shapes for the model segment: an exact name, a whole vendor ("openai/*"),
// a trailing star ("openai/gpt-5-*") and a leading star ("openai/*-pro"). The
// vendor itself never takes a star, because vendor names prefix one another
// (meta and meta-llama), so "openai*" would silently reach a neighbour.
func MatchesCreditModel(pattern, lowerName string) bool {
	// A bare "*" is refused here as well as at load. The loader already drops a
	// key carrying one, so this is the second lock on the same door — and the
	// cheaper one to reason about, since a caller may send "*" as a model name
	// and passthrough would happily synthesize a model called exactly that.
	if pattern == "" || pattern == "*" {
		return false
	}
	vendor, seg, hasVendor := strings.Cut(pattern, "/")
	if !hasVendor {
		// A name with no vendor segment ("pickle-general") is an exact entry.
		return pattern == lowerName
	}
	rest, ok := strings.CutPrefix(lowerName, vendor+"/")
	if !ok || rest == "" {
		return false
	}
	switch {
	case seg == "*":
		return true
	case strings.HasPrefix(seg, "*"):
		// A leading star names a family by its ending, and the endings that
		// matter carry variant suffixes: ":batch" is half price and ":free" is
		// free, but both are the same model as the bare name. Matching the
		// variant-stripped base as well as the whole name is what keeps
		// "openai/*-pro" from fencing gpt-5-pro and paying full price for
		// gpt-5-pro:batch, which is the same model at another rate.
		tail := seg[1:]
		base, _, _ := strings.Cut(rest, ":")
		return strings.HasSuffix(rest, tail) || strings.HasSuffix(base, tail)
		// Deliberately no "is it my own name" case here: nothing says
		// "openai/*-pro" was meant to reach a model called plainly "pro".
	case strings.HasSuffix(seg, "*"):
		// A trailing star is a prefix, and a prefix already covers the variant
		// suffixes, so it needs none of the handling above.
		stem := seg[:len(seg)-1]
		if strings.HasPrefix(rest, stem) && len(rest) > len(stem) {
			return true
		}
		// "openai/gpt-5-*" is written to name the gpt-5 family, and the family
		// includes gpt-5 itself; the separator the author typed before the star
		// is the only thing standing in the way.
		if last := stem[len(stem)-1]; last == '-' || last == '.' || last == ':' {
			return rest == stem[:len(stem)-1]
		}
		return false
	default:
		return rest == seg
	}
}

// creditModelPattern is the shape the control plane is expected to have
// normalized to. The gateway checks rather than trusts: a pattern it cannot act
// on would otherwise be dropped from the list, and dropping the last entry of a
// list turns a fence into no fence at all.
//
// The leading `~` admits the vendor's floating aliases (`~anthropic/claude-
// sonnet-latest`, which always resolves to the newest model of that family).
// They route today — passthrough forwards the name verbatim — so a fence that
// could not spell them was the one place a restricted key was narrower than an
// unrestricted one for no stated reason. `~vendor/*` and `vendor/*` stay
// separate prefixes, which falls out of the matcher below rather than being
// special-cased: an alias points at a model that changes under it, so a fence
// naming the vendor must not silently pick up a moving target the approver
// never chose.
//
// The model segment takes one star and only at an end: a star in the middle
// ("openai/*gpt*") describes a set nobody can predict the size of, and a
// leading star whose tail is empty or ends in a separator ("openai/*-") ends
// up naming most of a vendor by accident.
var creditModelPattern = regexp.MustCompile(
	`^~?[a-z0-9][a-z0-9._:-]*(/([a-z0-9][a-z0-9._:-]*\*?|\*[a-z0-9._:-]*[a-z0-9]|\*))?$`)

// state is one loaded document plus the lookup maps derived from it.
type state struct {
	doc      Document
	byHash   map[string]*Key
	byPublic map[string]*Model
	loadedAt time.Time
	// rejected counts entries dropped for a value this build cannot act on
	// (an unknown status, an upstream that is not configured). Only the
	// control-plane path drops rather than refuses — see Options.
	rejected    int
	dropReasons []string
}

// Store serves the current state and refreshes it in the background. Readers
// never block on a reload.
type Store struct {
	cur    atomic.Pointer[state]
	source Source
	log    *slog.Logger
	known  map[string]bool // configured upstream refs (lowercase); nil = skip the check

	// Consecutive reload failures since the last success, surfaced on the
	// health endpoint: a document that keeps failing to load means the served
	// state is silently going stale, which is otherwise invisible.
	reloadFailures atomic.Int64

	// Generation monotonicity across process restarts. The in-process guard in
	// reload() cannot see generations from before this process started, so the
	// highest generation ever served is persisted in a sidecar file; a document
	// on disk whose generation is below it is a rollback (a restored old backup)
	// and is refused at startup too — otherwise a restart would serve a snapshot
	// the running gateway had correctly rejected, silently reviving revoked keys.
	highWaterPath string
	highWater     int64
	allowGenReset bool
	fromControl   bool

	// lastError is the most recent reload failure, kept so the control plane
	// can be told why its document is not being applied. Without it the only
	// symptom the writer sees is appliedGeneration standing still.
	lastError atomic.Pointer[string]
}

// Options configure a Store beyond the document path.
type Options struct {
	// KnownUpstreams is the set of upstream refs the gateway has configured
	// (any case); a model naming an upstream outside it is rejected at load,
	// so a typo surfaces as a refused snapshot rather than per-request 502s.
	// Nil disables the check (tests that do not exercise upstream wiring).
	KnownUpstreams []string
	// AllowGenerationReset lets a document with a generation below the recorded
	// high-water load anyway (an operator deliberately resetting the sequence).
	AllowGenerationReset bool
	// FromControlPlane says who wrote the document, which is what decides how
	// unforgiving the reader should be. Strictness follows the writer:
	//
	//   - A hand-maintained file is read to the letter. An unknown top-level
	//     member is a typo (`serviceEnable` for `serviceEnabled` would leave
	//     the service quietly on), and a bad entry is an edit the operator is
	//     standing in front of and can fix. Refusing the whole document is the
	//     loudest possible signal and costs nothing.
	//
	//   - A document from the control plane is machine-written by a component
	//     that ships independently. An unknown top-level member is a newer api
	//     talking to an older gateway, and a value this build does not know
	//     (a richer key status, a model naming an upstream not configured on
	//     this host) is the same thing one level down. Refusing the document
	//     for either would freeze authorization on the last good state:
	//     revocations stop arriving, the api keeps getting 200s, and the only
	//     visible symptom is a generation that stops moving. So unknown
	//     top-level members are ignored and an unusable entry is dropped on
	//     its own — a dropped key stops working (fail-closed), a dropped model
	//     404s, and both are local and counted, where a frozen document is
	//     neither.
	//
	// Structural failures — unparseable JSON, a half-written document, a
	// generation that went backwards — still refuse the whole document on both
	// paths. Those are not "a field I do not know"; they are "I do not know
	// what this is".
	FromControlPlane bool
}

// Open loads the document once and fails if it is missing or invalid: a
// gateway must not start with nothing to authorize against. Later reload
// failures keep the last good state instead, bounding staleness rather than
// dropping traffic.
//
// statePath is where the high-water sidecar lives; for the file source it is
// the document's own path, and for the control-plane source it is the local
// cache path, so the guard survives a restart either way.
func Open(ctx context.Context, source Source, statePath string, log *slog.Logger, opts Options) (*Store, error) {
	s := &Store{
		source:        source,
		log:           log,
		highWaterPath: statePath + ".highwater",
		allowGenReset: opts.AllowGenerationReset,
		fromControl:   opts.FromControlPlane,
	}
	if opts.KnownUpstreams != nil {
		s.known = make(map[string]bool, len(opts.KnownUpstreams))
		for _, r := range opts.KnownUpstreams {
			s.known[strings.ToLower(r)] = true
		}
	}
	s.highWater = s.readHighWater()
	if err := s.reload(ctx); err != nil {
		return nil, err
	}
	return s, nil
}

// OpenFile is the file-source convenience used by tests and by the default
// deployment mode.
func OpenFile(path string, log *slog.Logger, opts Options) (*Store, error) {
	return Open(context.Background(), NewFileSource(path), path, log, opts)
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

// ReloadFailures is the count of consecutive failed reloads since the last
// success (0 when healthy). A rising count means the served state is stale.
func (s *Store) ReloadFailures() int64 { return s.reloadFailures.Load() }

// LastError describes the most recent reload failure, or "" when the last
// reload succeeded. It is reported back to the control plane so the writer
// learns why its document is not in force instead of only that it is not.
func (s *Store) LastError() string {
	if p := s.lastError.Load(); p != nil {
		return *p
	}
	return ""
}

// RejectedEntries is how many entries of the current document were dropped for
// a value this build cannot act on. Non-zero means the gateway is enforcing
// less than the writer described, which is otherwise invisible.
func (s *Store) RejectedEntries() int {
	if st := s.cur.Load(); st != nil {
		return st.rejected
	}
	return 0
}

// Refresh asks the source for a newer document. Errors are logged, never
// propagated to request handling: the last good state keeps serving and the
// failure count surfaces on the health endpoint.
func (s *Store) Refresh(ctx context.Context) {
	if err := s.reload(ctx); err != nil {
		s.reloadFailures.Add(1)
		msg := err.Error()
		s.lastError.Store(&msg)
		s.log.Error("snapshot refresh failed, keeping current state", "source", s.source.Name(), "error", err)
		return
	}
	s.reloadFailures.Store(0)
	s.lastError.Store(nil)
}

// reload fetches and, if the source had something new, validates and swaps it.
// A source reporting no change is a success with nothing to do.
func (s *Store) reload(ctx context.Context) error {
	served := int64(0)
	if prev := s.cur.Load(); prev != nil {
		served = prev.doc.Generation
	}
	raw, changed, err := s.source.Load(ctx, served)
	if err != nil {
		return err
	}
	if !changed {
		// Nothing new is success — unless there is nothing to serve yet. A
		// control plane that answers "unchanged" to a caller with no state
		// (a fresh gateway against a control plane at the same generation)
		// would otherwise leave the store empty and every read nil.
		if s.cur.Load() == nil {
			return fmt.Errorf("snapshot from %s: no document to serve (source reported no change on the first load)", s.source.Name())
		}
		return nil
	}
	st, err := build(raw, s.known, s.fromControl)
	if err != nil {
		return fmt.Errorf("snapshot from %s: %w", s.source.Name(), err)
	}
	if st.rejected > 0 {
		s.log.Error("snapshot entries dropped, the gateway is enforcing less than the document describes",
			"source", s.source.Name(), "generation", st.doc.Generation,
			"dropped", st.rejected, "reasons", st.dropReasons)
	}
	// The floor a new document must not drop below is the higher of the
	// in-process generation and the persisted high-water — the first catches a
	// rollback while running, the second catches one across a restart.
	floor := s.highWater
	if prev := s.cur.Load(); prev != nil && prev.doc.Generation > floor {
		floor = prev.doc.Generation
	}
	if st.doc.Generation < floor && !s.allowGenReset {
		return fmt.Errorf("snapshot generation went backwards: %d < %d (a rollback; set LLMGW_ALLOW_GENERATION_RESET to override)", st.doc.Generation, floor)
	}
	s.cur.Store(st)
	// The document is applied; only now may the source treat it as delivered
	// (remember the file identity, write the restart cache). Doing it earlier
	// caches documents this guard rejected and hides the rejection.
	s.source.Accept()
	if st.doc.Generation > s.highWater {
		s.highWater = st.doc.Generation
		s.writeHighWater(st.doc.Generation)
	}
	s.log.Info("snapshot loaded", "source", s.source.Name(), "generation", st.doc.Generation,
		"models", len(st.doc.Models), "keys", len(st.doc.Keys), "serviceEnabled", st.doc.ServiceEnabled)
	return nil
}

// readHighWater returns the persisted highest generation, or 0 if none.
func (s *Store) readHighWater() int64 {
	raw, err := os.ReadFile(s.highWaterPath)
	if err != nil {
		return 0
	}
	n, err := strconv.ParseInt(strings.TrimSpace(string(raw)), 10, 64)
	if err != nil || n < 0 {
		return 0
	}
	return n
}

// writeHighWater persists the high-water atomically. A failure is logged, not
// fatal: the in-process guard still holds for this run, and the next
// successful load rewrites it.
func (s *Store) writeHighWater(gen int64) {
	tmp, err := os.CreateTemp(filepath.Dir(s.highWaterPath), ".highwater-*")
	if err != nil {
		s.log.Error("high-water write failed", "error", err)
		return
	}
	defer os.Remove(tmp.Name())
	if _, err := fmt.Fprintf(tmp, "%d\n", gen); err != nil {
		tmp.Close()
		s.log.Error("high-water write failed", "error", err)
		return
	}
	if err := tmp.Close(); err != nil {
		s.log.Error("high-water write failed", "error", err)
		return
	}
	if err := os.Rename(tmp.Name(), s.highWaterPath); err != nil {
		s.log.Error("high-water write failed", "error", err)
	}
}

func build(raw []byte, known map[string]bool, fromControl bool) (*state, error) {
	// models[] and keys[] are captured as raw messages and decoded leniently
	// below, so an unknown field inside them is ignored rather than rejecting
	// the whole document. How much else is forgiven depends on who wrote the
	// document — see Options.FromControlPlane.
	//
	// The three envelope members are pointers so that "absent" is
	// distinguishable from "zero". A writer that models serviceEnabled as a
	// nullable Boolean and omits nulls — which is exactly how the unchanged
	// response omits models and keys — would otherwise put the whole service
	// into maintenance mode by leaving a field out, and a response carrying
	// only one of the two lists would silently wipe the other.
	var env struct {
		FormatVersion  int                `json:"formatVersion"`
		Generation     int64              `json:"generation"`
		ServiceEnabled *bool              `json:"serviceEnabled"`
		Models         *[]json.RawMessage `json:"models"`
		Keys           *[]json.RawMessage `json:"keys"`
		PassthroughRef string             `json:"passthroughRef"`
	}
	envDec := json.NewDecoder(strings.NewReader(string(raw)))
	if !fromControl {
		envDec.DisallowUnknownFields()
	}
	if err := envDec.Decode(&env); err != nil {
		return nil, err
	}
	if env.ServiceEnabled == nil {
		return nil, errors.New("serviceEnabled is missing: a document that does not say whether the service is on is not a document")
	}
	// Both members must be there. One missing would silently empty the other;
	// both missing empties everything — every key invalid, every model gone —
	// and does it without a single failure signal, because as far as the parser
	// is concerned the document simply says there is nothing. Over the sync
	// link those same bytes are the "nothing changed" answer, which the
	// transport filters out before it ever reaches here (see HTTPSource.fetch);
	// through a file they can only be a truncated or half-written document.
	if env.Models == nil || env.Keys == nil {
		return nil, errors.New("models and keys must both be present: a document missing either one silently empties authorization")
	}
	rawModels, rawKeys := *env.Models, *env.Keys
	doc := Document{
		FormatVersion:  env.FormatVersion,
		Generation:     env.Generation,
		ServiceEnabled: *env.ServiceEnabled,
		Models:         make([]Model, 0, len(rawModels)),
		Keys:           make([]Key, 0, len(rawKeys)),
		PassthroughRef: strings.ToLower(env.PassthroughRef),
	}
	st := &state{
		byHash:   make(map[string]*Key, len(rawKeys)),
		byPublic: make(map[string]*Model, len(rawModels)),
		loadedAt: time.Now(),
	}
	// drop reports an entry the gateway cannot act on. From the control plane
	// it is counted and skipped; from a file it fails the load.
	var dropped []string
	drop := func(format string, args ...any) error {
		reason := fmt.Sprintf(format, args...)
		if !fromControl {
			return errors.New(reason)
		}
		dropped = append(dropped, reason)
		return nil
	}

	// A passthrough target this host has not configured cannot serve anything;
	// disabling passthrough (unknown names go back to 404) is the local,
	// visible failure, where keeping the ref would turn every uncatalogued
	// model name into a per-request upstream error.
	if doc.PassthroughRef != "" && known != nil && !known[doc.PassthroughRef] {
		if derr := drop("passthroughRef %q is not a configured upstream; passthrough disabled", doc.PassthroughRef); derr != nil {
			return nil, derr
		}
		doc.PassthroughRef = ""
	}

	for i, rawModel := range rawModels {
		var m Model
		if err := json.Unmarshal(rawModel, &m); err != nil {
			if derr := drop("model %d: %v", i, err); derr != nil {
				return nil, derr
			}
			continue
		}
		// Upstream refs are matched case-insensitively everywhere; lowering
		// them once here is what lets every later comparison be a plain hit.
		m.UpstreamRef = strings.ToLower(m.UpstreamRef)
		m.FallbackRef = strings.ToLower(m.FallbackRef)
		reason := modelProblem(&m, i, known, st.byPublic)
		if reason != "" {
			if derr := drop("%s", reason); derr != nil {
				return nil, derr
			}
			continue
		}
		doc.Models = append(doc.Models, m)
	}
	for i, rawKey := range rawKeys {
		var k Key
		if err := json.Unmarshal(rawKey, &k); err != nil {
			if derr := drop("key %d: %v", i, err); derr != nil {
				return nil, derr
			}
			continue
		}
		// Credential refs are lowercased once here so every later lookup can
		// be a plain map hit; an empty credential is the same as none. Two
		// refs that collide after lowering would leave map iteration order
		// picking which credential gets spent — that entry is unusable, and
		// dropping it is fail-closed like every other unusable entry.
		credProblem := ""
		if len(k.UpstreamCredentials) > 0 {
			norm := make(map[string]string, len(k.UpstreamCredentials))
			for ref, cred := range k.UpstreamCredentials {
				if cred == "" {
					continue
				}
				lower := strings.ToLower(ref)
				if _, dup := norm[lower]; dup {
					credProblem = fmt.Sprintf(
						"key %s: upstreamCredentials refs collide on %q", k.KeyID, lower)
					break
				}
				norm[lower] = cred
			}
			k.UpstreamCredentials = norm
		}
		if credProblem != "" {
			if derr := drop("%s", credProblem); derr != nil {
				return nil, derr
			}
			continue
		}
		// Both money lists are lower-cased once here, like credential refs
		// above, so every later comparison is against a name lowered the same
		// way. A blank entry is refused rather than skipped, for the same
		// reason a malformed one is; the first version of this loop skipped
		// blanks, and ["  "] widened a fenced key instead of failing.
		var listProblem string
		k.CreditAllowedModels, listProblem = normalizeCreditPatterns(
			k.CreditAllowedModels, k.KeyID, "creditAllowedModels")
		if listProblem == "" {
			k.CreditDeniedModels, listProblem = normalizeCreditPatterns(
				k.CreditDeniedModels, k.KeyID, "credit_denied_models")
		}
		if listProblem != "" {
			if derr := drop("%s", listProblem); derr != nil {
				return nil, derr
			}
			continue
		}
		reason := keyProblem(&k, i, st.byHash)
		if reason != "" {
			if derr := drop("%s", reason); derr != nil {
				return nil, derr
			}
			continue
		}
		doc.Keys = append(doc.Keys, k)
	}
	// The lookup maps point into doc, so they are filled only once both slices
	// have stopped growing — appending to a slice may move its backing array,
	// and a pointer taken before that would address the old one. The problem
	// checks above therefore see partial maps, which is exactly what they need:
	// duplicates are detected against the entries already accepted.
	for i := range doc.Models {
		st.byPublic[doc.Models[i].PublicName] = &doc.Models[i]
	}
	for i := range doc.Keys {
		st.byHash[doc.Keys[i].TokenHash] = &doc.Keys[i]
	}
	st.doc = doc
	st.rejected = len(dropped)
	st.dropReasons = dropped
	return st, nil
}

// normalizeCreditPatterns lower-cases and trims one money list, returning the
// normalized entries and "" — or the untouched list and why the whole key has
// to go, because one entry is a pattern this build cannot act on.
//
// The key goes rather than the entry, and that holds for both lists even
// though the reasoning runs in opposite directions. Drop an entry from the
// allow list and the list shrinks toward empty, which means unrestricted: the
// key silently regains every commercial model. Drop an entry from the deny
// list and the list shrinks toward empty too, which there means nothing is
// denied: the model somebody closed is silently open again. Same conclusion,
// mirrored reasons — which is why the simplification of skipping just the
// unreadable entry is wrong for both, and would be tempting on each of them
// separately. Dropping the key denies service to one key, counts it, and logs
// why, which is the failure this loader is built to prefer.
func normalizeCreditPatterns(patterns []string, keyID, field string) ([]string, string) {
	if len(patterns) == 0 {
		return patterns, ""
	}
	norm := make([]string, 0, len(patterns))
	for _, pattern := range patterns {
		lower := strings.ToLower(strings.TrimSpace(pattern))
		if lower == "" || !creditModelPattern.MatchString(lower) {
			return patterns, fmt.Sprintf(
				"key %s: %s entry %q is not a usable pattern", keyID, field, pattern)
		}
		norm = append(norm, lower)
	}
	return norm, ""
}

// modelProblem returns why a model entry cannot be used, or "" when it can.
// seen holds the public names accepted so far.
func modelProblem(m *Model, i int, known map[string]bool, seen map[string]*Model) string {
	if m.PublicName == "" || m.UpstreamRef == "" || m.UpstreamModel == "" {
		return fmt.Sprintf("model %d: publicName, upstreamRef and upstreamModel are all required", i)
	}
	switch m.Visibility {
	case "", ModelPublic, ModelRestricted:
	default:
		// Not knowing how visible a model is means not knowing who may reach
		// it, so the entry goes rather than being guessed at either way.
		return fmt.Sprintf("model %q: unknown visibility %q", m.PublicName, m.Visibility)
	}
	switch m.BudgetAxis {
	case "", AxisToken, AxisCredit:
	default:
		// The axis decides which budget governs the model; guessing it would
		// either bill a token model against money or exempt a credit model
		// from its credential requirement.
		return fmt.Sprintf("model %q: unknown budgetAxis %q", m.PublicName, m.BudgetAxis)
	}
	if known != nil && !known[strings.ToLower(m.UpstreamRef)] {
		return fmt.Sprintf("model %q references upstream %q, which is not configured", m.PublicName, m.UpstreamRef)
	}
	// The fallback is checked the same way. Skipping it would mean a typo
	// there stays invisible until the outage the fallback exists for.
	if known != nil && m.FallbackRef != "" && !known[strings.ToLower(m.FallbackRef)] {
		return fmt.Sprintf("model %q names fallback upstream %q, which is not configured", m.PublicName, m.FallbackRef)
	}
	if _, dup := seen[m.PublicName]; dup {
		return fmt.Sprintf("duplicate public model name %q", m.PublicName)
	}
	// A name already claimed by an accepted entry must not be claimed again
	// later in the same document either.
	seen[m.PublicName] = nil
	return ""
}

// keyProblem returns why a key entry cannot be used, or "" when it can. seen
// holds the token hashes accepted so far.
func keyProblem(k *Key, i int, seen map[string]*Key) string {
	if k.KeyID == "" {
		return fmt.Sprintf("key %d: keyId is required", i)
	}
	if !validHash(k.TokenHash) {
		return fmt.Sprintf("key %s: tokenHash must be 64 lowercase hex chars", k.KeyID)
	}
	switch k.Status {
	case KeyActive, KeySuspended, KeyRevoked:
	default:
		// A status this build does not know cannot be assumed to mean ACTIVE.
		// Dropping the entry denies the key, which is the safe reading of "I
		// do not know what state this key is in".
		return fmt.Sprintf("key %s: unknown status %q", k.KeyID, k.Status)
	}
	if _, dup := seen[k.TokenHash]; dup {
		return fmt.Sprintf("key %s: tokenHash duplicates another key", k.KeyID)
	}
	seen[k.TokenHash] = nil
	return ""
}

// Validate reports whether raw is a document this build would accept from a
// file — strict about the top level, and refusing any entry it cannot act on.
// It exists so a writer can check its own output before replacing the file the
// gateway reads: a tool that renames first and finds out never finds out at
// all, because the failure surfaces on the reader's side, minutes later, as a
// silently unchanged authorization state.
//
// Upstream names are not checked here: the writer does not know which upstreams
// a given host has configured. That check stays with the gateway.
func Validate(raw []byte) error {
	_, err := build(raw, nil, false)
	return err
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

// Package ownership records and checks which key owns an asynchronous vendor
// object (a file, a batch, a video). The vendor scopes these objects to a
// workspace that every key of a business account shares, so the vendor does
// not keep one key out of another key's objects; the api's ledger does, and
// this package is the gateway's side of it.
//
// A submit is recorded in two phases. Intent runs before the vendor call: if
// the api cannot be reached the vendor is never called, so nothing is spent
// and nothing exists that nobody owns. Bind runs after the vendor accepted
// and is retried for about ten seconds, because a bind that never lands
// leaves an object its owner cannot reach. Lookups are answered from a cache
// of confirmed ownership when they can be, and otherwise from the api.
//
// Every error these calls return is one of two kinds. A refusal (RefusalCode
// is not empty) is a decision the api made about this request and is never
// retried. Anything else satisfies IsUnavailable — the api could not be
// reached, timed out, failed, rejected the link itself, or answered something
// unreadable — and the route answers 503. Nothing outside the async routes
// goes through this package, so every other route keeps working without the
// api.
package ownership

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"time"

	"github.com/pnuops/pickle-llm-gateway/internal/version"
)

// Kind is the type of vendor object. The api refuses any other value.
type Kind string

const (
	KindFile  Kind = "FILE"
	KindBatch Kind = "BATCH"
	KindVideo Kind = "VIDEO"
)

// ReleaseReason says why a ledger row is released.
type ReleaseReason string

const (
	// ReleaseDeleted: a delete through the gateway succeeded at the vendor.
	ReleaseDeleted ReleaseReason = "DELETED"
	// ReleaseVendorGone: the vendor answered 404 or 410 for an object the
	// ledger said was owned. This is how a release that never reached the api
	// heals: the next request finds the object gone.
	ReleaseVendorGone ReleaseReason = "VENDOR_GONE"
	// ReleaseNotCreated: the vendor refused the submit, so the intent never
	// got an object.
	ReleaseNotCreated ReleaseReason = "NOT_CREATED"
)

// Refusal codes the api answers with. A refusal is a decision, never retried.
const (
	CodeKeyNotActive         = "KEY_NOT_ACTIVE"
	CodeNoAccount            = "NO_ACCOUNT"
	CodeIntentUnknown        = "INTENT_UNKNOWN"
	CodeIntentBoundElsewhere = "INTENT_BOUND_ELSEWHERE"
	// CodeObjectTaken means another key's row owns this vendor id. A route
	// that sees it must not undo the submit: the object is that key's.
	CodeObjectTaken = "OBJECT_TAKEN"
)

// maxLookupIDs is the api's per-call bound; longer lists are split.
const maxLookupIDs = 100

// Defaults. A call sits on a client's request, so it gets two seconds; a
// confirmed ownership is kept five minutes, long enough that a client polling
// a job asks the api a few times an hour and short enough that a release on
// another instance is seen soon; ten thousand entries is a couple of MiB.
// They become configuration when a route wires the client, not before: a knob
// nothing reads is not documented.
const (
	DefaultTimeout    = 2 * time.Second
	DefaultCacheTTL   = 5 * time.Minute
	DefaultCacheSize  = 10_000
	DefaultBindBudget = 12 * time.Second
)

// maxObjectIDBytes is the longest vendor id the api stores. A longer one
// cannot be owned, so a lookup answers it absent without asking.
const maxObjectIDBytes = 255

// defaultBindDelays are the pauses between bind attempts: six attempts over
// roughly ten seconds with the per-attempt timeout.
var defaultBindDelays = []time.Duration{
	200 * time.Millisecond, 600 * time.Millisecond, 1500 * time.Millisecond,
	3 * time.Second, 4 * time.Second,
}

// Options adjusts a Client. Zero values take the defaults.
type Options struct {
	Timeout    time.Duration
	BindBudget time.Duration
	CacheTTL   time.Duration
	CacheSize  int
	BindDelays []time.Duration
	Now        func() time.Time
	Sleep      func(context.Context, time.Duration) error
	HTTP       *http.Client
}

// Client talks to the api's /internal/llm/objects calls.
type Client struct {
	base       string
	token      string
	timeout    time.Duration
	bindBudget time.Duration
	bindDelays []time.Duration
	sleep      func(context.Context, time.Duration) error
	http       *http.Client
	cache      *cache
}

// New builds a client for the api at baseURL with the link's bearer.
func New(baseURL, token string, opts Options) *Client {
	if opts.Timeout <= 0 {
		opts.Timeout = DefaultTimeout
	}
	if opts.BindBudget <= 0 {
		opts.BindBudget = DefaultBindBudget
	}
	if opts.CacheTTL <= 0 {
		opts.CacheTTL = DefaultCacheTTL
	}
	if opts.CacheSize <= 0 {
		opts.CacheSize = DefaultCacheSize
	}
	if opts.BindDelays == nil {
		opts.BindDelays = defaultBindDelays
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.Sleep == nil {
		opts.Sleep = sleepContext
	}
	if opts.HTTP == nil {
		opts.HTTP = &http.Client{}
	}
	return &Client{
		base:       baseURL + "/internal/llm/objects",
		token:      token,
		timeout:    opts.Timeout,
		bindBudget: opts.BindBudget,
		bindDelays: opts.BindDelays,
		sleep:      opts.Sleep,
		http:       opts.HTTP,
		cache:      newCache(opts.CacheTTL, opts.CacheSize, opts.Now),
	}
}

// Refusal is a 4xx the api answered with a reason. The route maps the code.
type Refusal struct {
	Status int
	Code   string
}

func (r *Refusal) Error() string {
	return fmt.Sprintf("ownership: refused (HTTP %d %s)", r.Status, r.Code)
}

// unavailable wraps every failure that is not a refusal: transport errors,
// timeouts, 5xx and 429, a 4xx without a refusal code (the link itself was
// rejected — a wrong bearer, a wrong source, a malformed call), and an
// answer that cannot be read. All of them mean this request cannot be
// decided, and a route that acted on half an answer would be worse.
type unavailable struct{ err error }

func (u *unavailable) Error() string { return "ownership: api unavailable: " + u.err.Error() }
func (u *unavailable) Unwrap() error { return u.err }

// IsUnavailable reports whether err means the request cannot be decided,
// which the route answers with 503. Every other error is a Refusal.
func IsUnavailable(err error) bool {
	var u *unavailable
	return errors.As(err, &u)
}

// RefusalCode returns the api's refusal code, or "" when err is not one.
func RefusalCode(err error) string {
	var r *Refusal
	if errors.As(err, &r) {
		return r.Code
	}
	return ""
}

// Intent records that keyID is about to submit an object of this kind and
// returns the intent to bind. Not retried: nothing has been spent yet, and the
// client can retry the whole submit.
func (c *Client) Intent(ctx context.Context, keyID string, kind Kind, eventID string) (string, error) {
	var out struct {
		IntentID string `json:"intentId"`
	}
	err := c.post(ctx, "/intents", map[string]any{
		"agentVersion": version.String(),
		"keyId":        keyID,
		"kind":         kind,
		"eventId":      eventID,
	}, &out)
	if err != nil {
		return "", err
	}
	if out.IntentID == "" {
		return "", &unavailable{errors.New("intent answer without an intentId")}
	}
	return out.IntentID, nil
}

// BindDetails carries what the vendor said about the object, where it said it.
type BindDetails struct {
	SizeBytes *int64
	ExpiresAt *time.Time
}

// Bind attaches the vendor id to an intent and caches the ownership. An
// unavailable api is retried within a total budget (twelve seconds by
// default); a refusal is returned at once. The api answers the same success
// for a repeat, so a bind whose response was lost is safe to send again.
//
// The caller's cancellation is deliberately ignored: the vendor has already
// accepted the object, and a client that hangs up does not make the bind any
// less necessary. Only the budget ends the retries.
func (c *Client) Bind(ctx context.Context, keyID, intentID string, kind Kind, objectID string, d BindDetails) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.bindBudget)
	defer cancel()
	body := map[string]any{
		"agentVersion": version.String(),
		"keyId":        keyID,
		"intentId":     intentID,
		"kind":         kind,
		"objectId":     objectID,
	}
	if d.SizeBytes != nil {
		body["sizeBytes"] = *d.SizeBytes
	}
	if d.ExpiresAt != nil {
		body["expiresAt"] = d.ExpiresAt.UTC().Format(time.RFC3339Nano)
	}
	var err error
	for attempt := 0; ; attempt++ {
		err = c.post(ctx, "/bind", body, nil)
		if err == nil {
			c.cache.put(cacheKey{keyID, kind, objectID})
			return nil
		}
		if !IsUnavailable(err) || attempt >= len(c.bindDelays) {
			return err
		}
		if serr := c.sleep(ctx, c.bindDelays[attempt]); serr != nil {
			return err
		}
	}
}

// Remember caches ownership the gateway is certain of without the api: a
// submit it just made whose bind did not land and whose late bind travels on
// the usage channel. Only this instance learns it; the others answer from the
// api once the late bind arrives.
func (c *Client) Remember(keyID string, kind Kind, objectID string) {
	c.cache.put(cacheKey{keyID, kind, objectID})
}

// Owned answers which of ids keyID owns. Cached confirmations are answered
// locally; the rest are asked of the api in batches of at most 100. If any
// batch cannot be asked the whole call fails, so a route never acts on half
// an answer.
func (c *Client) Owned(ctx context.Context, keyID string, kind Kind, ids []string) (map[string]bool, error) {
	owned := make(map[string]bool, len(ids))
	var ask []string
	seen := make(map[string]bool, len(ids))
	for _, id := range ids {
		if seen[id] || id == "" || len(id) > maxObjectIDBytes {
			continue
		}
		seen[id] = true
		if c.cache.owned(cacheKey{keyID, kind, id}) {
			owned[id] = true
		} else {
			ask = append(ask, id)
		}
	}
	for start := 0; start < len(ask); start += maxLookupIDs {
		end := min(start+maxLookupIDs, len(ask))
		var out struct {
			Owned []string `json:"owned"`
		}
		err := c.post(ctx, "/lookup", map[string]any{
			"agentVersion": version.String(),
			"keyId":        keyID,
			"kind":         kind,
			"objectIds":    ask[start:end],
		}, &out)
		if err != nil {
			return nil, err
		}
		asked := ask[start:end]
		for _, id := range out.Owned {
			// Only ids this call asked about count; anything else in the answer
			// is an api defect and must not widen what the key can reach.
			if slices.Contains(asked, id) {
				owned[id] = true
				c.cache.put(cacheKey{keyID, kind, id})
			}
		}
	}
	return owned, nil
}

// Release ends ownership of an object, or of an intent the vendor refused
// (reason ReleaseNotCreated, with intentID instead of objectID). It reports
// whether a row changed. Not retried: a lost release of an owned object heals
// through ReleaseVendorGone on the next request.
//
// The cache entry goes first, before the api answers. If the release fails,
// the next lookup asks the api, which still says owned, and the vendor's 404
// then releases it; keeping the entry would only hide the object's absence
// for longer.
func (c *Client) Release(ctx context.Context, keyID string, kind Kind, objectID, intentID string, reason ReleaseReason) (bool, error) {
	body := map[string]any{
		"agentVersion": version.String(),
		"keyId":        keyID,
		"kind":         kind,
		"reason":       reason,
	}
	if objectID != "" {
		body["objectId"] = objectID
		c.cache.forget(cacheKey{keyID, kind, objectID})
	}
	if intentID != "" {
		body["intentId"] = intentID
	}
	var out struct {
		Released bool `json:"released"`
	}
	if err := c.post(ctx, "/release", body, &out); err != nil {
		return false, err
	}
	return out.Released, nil
}

func (c *Client) post(ctx context.Context, path string, body any, out any) error {
	raw, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("ownership: %s: %w", path, err)
	}
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+path, bytes.NewReader(raw))
	if err != nil {
		return fmt.Errorf("ownership: %s: %w", path, err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.token)
	resp, err := c.http.Do(req)
	if err != nil {
		return &unavailable{err}
	}
	defer resp.Body.Close()
	answer, err := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if err != nil {
		return &unavailable{err}
	}
	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		if out == nil {
			return nil
		}
		if err := json.Unmarshal(answer, out); err != nil {
			return &unavailable{fmt.Errorf("%s answer: %w", path, err)}
		}
		return nil
	case resp.StatusCode >= 400 && resp.StatusCode < 500 && resp.StatusCode != http.StatusTooManyRequests:
		var refusal struct {
			Code string `json:"code"`
		}
		if json.Unmarshal(answer, &refusal) == nil && refusal.Code != "" {
			return &Refusal{Status: resp.StatusCode, Code: refusal.Code}
		}
		// No refusal code: the link was rejected, not the request. Keep a
		// bounded prefix of the answer for the log.
		return &unavailable{fmt.Errorf("%s: HTTP %d: %s", path, resp.StatusCode, bytes.TrimSpace(answer[:min(len(answer), 512)]))}
	default:
		return &unavailable{fmt.Errorf("%s: HTTP %d", path, resp.StatusCode)}
	}
}

func sleepContext(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

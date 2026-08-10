// Package limits enforces the per-key short-window limits: requests per
// minute, tokens per minute, and concurrent requests. All state is in memory
// on this one gateway process; the counters restart with it, which for
// minute-scale windows only ever errs briefly in the caller's favor.
package limits

import (
	"sync"
	"time"
)

// Reason says which limit refused a request.
type Reason int

const (
	OK Reason = iota
	Rpm
	Tpm
	Concurrency
)

// Limiter tracks per-key state. Keys that disappear from the snapshot stop
// being charged and their state ages out.
type Limiter struct {
	mu      sync.Mutex
	perKey  map[string]*keyState
	now     func() time.Time
	lastGC  time.Time
	gcEvery time.Duration
	idleTTL time.Duration
}

type keyState struct {
	rpm      bucket
	tpm      bucket
	inFlight int
	lastSeen time.Time
}

// bucket is a continuous-refill token bucket. level may go negative: token
// usage is only known after the response, so a large completion is charged
// late and pushes the bucket into debt that must refill before the next
// request passes.
type bucket struct {
	level    float64
	lastFill time.Time
}

func (b *bucket) refill(now time.Time, perMinute int) {
	if b.lastFill.IsZero() {
		b.level = float64(perMinute)
		b.lastFill = now
		return
	}
	elapsed := now.Sub(b.lastFill).Minutes()
	if elapsed <= 0 {
		return
	}
	b.level += elapsed * float64(perMinute)
	if b.level > float64(perMinute) {
		b.level = float64(perMinute)
	}
	b.lastFill = now
}

// New builds a Limiter. now is injectable for tests.
func New(now func() time.Time) *Limiter {
	if now == nil {
		now = time.Now
	}
	return &Limiter{
		perKey:  map[string]*keyState{},
		now:     now,
		gcEvery: 10 * time.Minute,
		idleTTL: time.Hour,
	}
}

// Decision is the outcome of one admission attempt.
type Decision struct {
	// Release must be called when an admitted request finishes; nil on refusal.
	Release func()
	Reason  Reason
	// RetryAfter is how long the caller should wait before trying again. Only
	// meaningful on a refusal, and always a whole number of seconds when
	// rendered into the header, so it never advertises a wait shorter than it
	// really is.
	RetryAfter time.Duration
	// RemainingRequests is the rpm budget left after this admission, for the
	// rate-limit header. -1 when the request was refused.
	RemainingRequests int
}

// Acquire admits or refuses one request for the key. On admission it charges
// one request against the rpm bucket and takes a concurrency slot. rpm, tpm
// and conc are the key's resolved limits (already defaulted by the caller).
func (l *Limiter) Acquire(keyID string, rpm, tpm, conc int) Decision {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	l.gcLocked(now)

	st := l.perKey[keyID]
	if st == nil {
		st = &keyState{}
		l.perKey[keyID] = st
	}
	st.lastSeen = now
	st.rpm.refill(now, rpm)
	st.tpm.refill(now, tpm)

	if st.inFlight >= conc {
		// A slot frees when some in-flight request finishes, which no clock
		// predicts; a second is a courteous floor rather than a promise.
		return Decision{Reason: Concurrency, RetryAfter: time.Second, RemainingRequests: -1}
	}
	if st.rpm.level < 1 {
		return Decision{Reason: Rpm, RetryAfter: refillWait(st.rpm.level, 1, rpm), RemainingRequests: -1}
	}
	// The tpm bucket is charged after the fact, so admission only requires it
	// to be out of debt.
	if st.tpm.level < 0 {
		return Decision{Reason: Tpm, RetryAfter: refillWait(st.tpm.level, 0, tpm), RemainingRequests: -1}
	}

	st.rpm.level--
	st.inFlight++
	released := false
	return Decision{
		Release: func() {
			l.mu.Lock()
			defer l.mu.Unlock()
			if released {
				return
			}
			released = true
			if st.inFlight > 0 {
				st.inFlight--
			}
		},
		Reason:            OK,
		RemainingRequests: int(st.rpm.level),
	}
}

// refillWait is how long a bucket at level needs to reach target at perMinute.
func refillWait(level, target float64, perMinute int) time.Duration {
	if perMinute <= 0 || level >= target {
		return time.Second
	}
	minutes := (target - level) / float64(perMinute)
	d := time.Duration(minutes * float64(time.Minute))
	if d < time.Second {
		return time.Second
	}
	return d
}

// ChargeTokens records actual token usage against the key's tpm bucket once
// the response (or its failure) told us the count.
func (l *Limiter) ChargeTokens(keyID string, tpm, tokens int) {
	if tokens <= 0 {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	st := l.perKey[keyID]
	if st == nil {
		return
	}
	st.tpm.refill(l.now(), tpm)
	st.tpm.level -= float64(tokens)
}

// InFlight reports the key's current concurrent requests (for status surfaces).
func (l *Limiter) InFlight(keyID string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	if st := l.perKey[keyID]; st != nil {
		return st.inFlight
	}
	return 0
}

func (l *Limiter) gcLocked(now time.Time) {
	if now.Sub(l.lastGC) < l.gcEvery {
		return
	}
	l.lastGC = now
	for id, st := range l.perKey {
		if st.inFlight == 0 && now.Sub(st.lastSeen) > l.idleTTL {
			delete(l.perKey, id)
		}
	}
}

package limits

import (
	"testing"
	"time"
)

type clock struct{ t time.Time }

func (c *clock) now() time.Time          { return c.t }
func (c *clock) advance(d time.Duration) { c.t = c.t.Add(d) }
func newTestLimiter() (*Limiter, *clock) {
	c := &clock{t: time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)}
	return New(c.now), c
}

func TestRpmExhaustionAndRefill(t *testing.T) {
	l, c := newTestLimiter()
	for i := range 2 {
		d := l.Acquire("k", 2, 1000, 10)

		rel, reason := d.Release, d.Reason
		if reason != OK {
			t.Fatalf("request %d refused: %v", i, reason)
		}
		rel()
	}
	if reason := l.Acquire("k", 2, 1000, 10).Reason; reason != Rpm {
		t.Fatalf("third request within the minute: got %v, want Rpm", reason)
	}
	c.advance(30 * time.Second) // refills one request at 2/min
	d := l.Acquire("k", 2, 1000, 10)

	rel, reason := d.Release, d.Reason
	if reason != OK {
		t.Fatalf("after refill: got %v", reason)
	}
	rel()
}

func TestConcurrencyLimit(t *testing.T) {
	l, _ := newTestLimiter()
	d1 := l.Acquire("k", 100, 1000, 1)

	rel1, reason := d1.Release, d1.Reason
	if reason != OK {
		t.Fatal(reason)
	}
	if reason := l.Acquire("k", 100, 1000, 1).Reason; reason != Concurrency {
		t.Fatalf("got %v, want Concurrency", reason)
	}
	rel1()
	d2 := l.Acquire("k", 100, 1000, 1)

	rel2, reason := d2.Release, d2.Reason
	if reason != OK {
		t.Fatalf("after release: got %v", reason)
	}
	rel2()
	// A release function must be safe to call twice without freeing a slot
	// it no longer holds.
	rel1()
	d3 := l.Acquire("k", 100, 1000, 1)

	rel3, reason := d3.Release, d3.Reason
	if reason != OK {
		t.Fatal(reason)
	}
	if reason := l.Acquire("k", 100, 1000, 1).Reason; reason != Concurrency {
		t.Fatal("double release freed an extra concurrency slot")
	}
	rel3()
}

func TestTpmDebtBlocksUntilRefill(t *testing.T) {
	l, c := newTestLimiter()
	d := l.Acquire("k", 100, 600, 10)

	rel, reason := d.Release, d.Reason
	if reason != OK {
		t.Fatal(reason)
	}
	rel()
	// The response turned out to be huge: charge far beyond the bucket
	// (level 600 - 2400 = -1800).
	l.ChargeTokens("k", 600, 2400)
	if reason := l.Acquire("k", 100, 600, 10).Reason; reason != Tpm {
		t.Fatalf("got %v, want Tpm", reason)
	}
	c.advance(2 * time.Minute) // 1200 tokens refill; still 600 in debt
	if reason := l.Acquire("k", 100, 600, 10).Reason; reason != Tpm {
		t.Fatalf("still in debt: got %v, want Tpm", reason)
	}
	c.advance(3 * time.Minute)
	d = l.Acquire("k", 100, 600, 10)
	rel, reason = d.Release, d.Reason
	if reason != OK {
		t.Fatalf("after debt refilled: got %v", reason)
	}
	rel()
}

func TestKeysAreIndependent(t *testing.T) {
	l, _ := newTestLimiter()
	if reason := l.Acquire("a", 1, 1000, 10).Reason; reason != OK {
		t.Fatal(reason)
	}
	if reason := l.Acquire("a", 1, 1000, 10).Reason; reason != Rpm {
		t.Fatal("key a should be out of requests")
	}
	if reason := l.Acquire("b", 1, 1000, 10).Reason; reason != OK {
		t.Fatal("key b must not share key a's bucket")
	}
}

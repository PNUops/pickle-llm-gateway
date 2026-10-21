package ownership

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// mockAPI is a stand-in for the api's /internal/llm/objects calls. Each path
// answers from a scripted queue of statuses (empty means 200 with the default
// body), and every request is recorded.
type mockAPI struct {
	mu       sync.Mutex
	statuses map[string][]int
	owned    map[string][]string // keyId -> ids the lookup answers
	calls    map[string]int
	bodies   map[string][]map[string]any
	auth     []string
}

func newMockAPI(t *testing.T) (*mockAPI, *httptest.Server) {
	t.Helper()
	m := &mockAPI{
		statuses: map[string][]int{},
		owned:    map[string][]string{},
		calls:    map[string]int{},
		bodies:   map[string][]map[string]any{},
	}
	srv := httptest.NewServer(http.HandlerFunc(m.serve))
	t.Cleanup(srv.Close)
	return m, srv
}

func (m *mockAPI) script(path string, statuses ...int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.statuses[path] = append(m.statuses[path], statuses...)
}

func (m *mockAPI) count(path string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls[path]
}

func (m *mockAPI) serve(w http.ResponseWriter, r *http.Request) {
	var body map[string]any
	_ = json.NewDecoder(r.Body).Decode(&body)
	m.mu.Lock()
	path := r.URL.Path
	m.calls[path]++
	m.bodies[path] = append(m.bodies[path], body)
	m.auth = append(m.auth, r.Header.Get("Authorization"))
	status := http.StatusOK
	if q := m.statuses[path]; len(q) > 0 {
		status = q[0]
		m.statuses[path] = q[1:]
	}
	owned := m.owned[asString(body["keyId"])]
	m.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	if status != http.StatusOK && status != http.StatusCreated {
		w.WriteHeader(status)
		switch status {
		case http.StatusConflict:
			_, _ = w.Write([]byte(`{"code":"OBJECT_TAKEN"}`))
		case http.StatusNotFound:
			_, _ = w.Write([]byte(`{"code":"INTENT_UNKNOWN"}`))
		}
		return
	}
	switch path {
	case "/internal/llm/objects/intents":
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"intentId":"11111111-1111-1111-1111-111111111111"}`))
	case "/internal/llm/objects/bind":
		_, _ = w.Write([]byte(`{"state":"OWNED"}`))
	case "/internal/llm/objects/lookup":
		var asked []string
		for _, v := range body["objectIds"].([]any) {
			asked = append(asked, v.(string))
		}
		var answer []string
		for _, id := range owned {
			for _, a := range asked {
				if a == id {
					answer = append(answer, id)
				}
			}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"owned": answer})
	case "/internal/llm/objects/release":
		_, _ = w.Write([]byte(`{"released":true}`))
	}
}

func asString(v any) string {
	s, _ := v.(string)
	return s
}

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (f *fakeClock) now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.t
}

func (f *fakeClock) advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.t = f.t.Add(d)
}

func newTestClient(srv *httptest.Server, clock *fakeClock, slept *[]time.Duration) *Client {
	return New(srv.URL, "link-token", Options{
		Now: clock.now,
		Sleep: func(_ context.Context, d time.Duration) error {
			*slept = append(*slept, d)
			return nil
		},
	})
}

func TestIntentReturnsTheIntentAndSendsTheBearer(t *testing.T) {
	m, srv := newMockAPI(t)
	c := newTestClient(srv, &fakeClock{t: time.Unix(0, 0)}, new([]time.Duration))

	id, err := c.Intent(context.Background(), "key-1", KindFile, "evt-1")
	if err != nil {
		t.Fatal(err)
	}
	if id != "11111111-1111-1111-1111-111111111111" {
		t.Fatalf("intent id = %q", id)
	}
	body := m.bodies["/internal/llm/objects/intents"][0]
	if body["keyId"] != "key-1" || body["kind"] != "FILE" || body["eventId"] != "evt-1" {
		t.Fatalf("intent body = %v", body)
	}
	if m.auth[0] != "Bearer link-token" {
		t.Fatalf("authorization = %q", m.auth[0])
	}
}

func TestIntentIsNotRetriedAndAnUnreachableAPIIsUnavailable(t *testing.T) {
	m, srv := newMockAPI(t)
	m.script("/internal/llm/objects/intents", http.StatusServiceUnavailable)
	c := newTestClient(srv, &fakeClock{t: time.Unix(0, 0)}, new([]time.Duration))

	_, err := c.Intent(context.Background(), "key-1", KindFile, "evt-1")
	if !IsUnavailable(err) {
		t.Fatalf("err = %v, want unavailable", err)
	}
	if n := m.count("/internal/llm/objects/intents"); n != 1 {
		t.Fatalf("intent attempts = %d, want 1", n)
	}

	srv.Close()
	if _, err := c.Intent(context.Background(), "key-1", KindFile, "evt-1"); !IsUnavailable(err) {
		t.Fatalf("closed server: err = %v, want unavailable", err)
	}
}

func TestRefusalCarriesTheCodeAndIsNotUnavailable(t *testing.T) {
	m, srv := newMockAPI(t)
	m.script("/internal/llm/objects/bind", http.StatusConflict)
	var slept []time.Duration
	c := newTestClient(srv, &fakeClock{t: time.Unix(0, 0)}, &slept)

	err := c.Bind(context.Background(), "key-1", "intent-1", KindFile, "file-1", BindDetails{})
	if IsUnavailable(err) || RefusalCode(err) != CodeObjectTaken {
		t.Fatalf("err = %v, want refusal %s", err, CodeObjectTaken)
	}
	if len(slept) != 0 || m.count("/internal/llm/objects/bind") != 1 {
		t.Fatalf("a refusal was retried: slept %v, calls %d", slept, m.count("/internal/llm/objects/bind"))
	}
}

func TestBindRetriesAnUnavailableAPIThenCachesTheOwnership(t *testing.T) {
	m, srv := newMockAPI(t)
	m.script("/internal/llm/objects/bind", http.StatusBadGateway, http.StatusServiceUnavailable)
	var slept []time.Duration
	c := newTestClient(srv, &fakeClock{t: time.Unix(0, 0)}, &slept)
	size := int64(42)

	if err := c.Bind(context.Background(), "key-1", "intent-1", KindBatch, "batch-1", BindDetails{SizeBytes: &size}); err != nil {
		t.Fatal(err)
	}
	if got := m.count("/internal/llm/objects/bind"); got != 3 {
		t.Fatalf("bind attempts = %d, want 3", got)
	}
	if len(slept) != 2 || slept[0] != 200*time.Millisecond || slept[1] != 600*time.Millisecond {
		t.Fatalf("slept %v", slept)
	}
	if m.bodies["/internal/llm/objects/bind"][0]["sizeBytes"] != float64(42) {
		t.Fatalf("bind body = %v", m.bodies["/internal/llm/objects/bind"][0])
	}
	// The confirmed ownership answers without asking the api.
	owned, err := c.Owned(context.Background(), "key-1", KindBatch, []string{"batch-1"})
	if err != nil || !owned["batch-1"] {
		t.Fatalf("owned = %v, err %v", owned, err)
	}
	if m.count("/internal/llm/objects/lookup") != 0 {
		t.Fatal("a cached ownership asked the api")
	}
}

func TestBindGivesUpAfterTheBudget(t *testing.T) {
	m, srv := newMockAPI(t)
	for range 10 {
		m.script("/internal/llm/objects/bind", http.StatusServiceUnavailable)
	}
	var slept []time.Duration
	c := newTestClient(srv, &fakeClock{t: time.Unix(0, 0)}, &slept)

	err := c.Bind(context.Background(), "key-1", "intent-1", KindVideo, "video-1", BindDetails{})
	if !IsUnavailable(err) {
		t.Fatalf("err = %v, want unavailable", err)
	}
	if got := m.count("/internal/llm/objects/bind"); got != len(defaultBindDelays)+1 {
		t.Fatalf("bind attempts = %d, want %d", got, len(defaultBindDelays)+1)
	}
	var total time.Duration
	for _, d := range slept {
		total += d
	}
	if total != 9300*time.Millisecond {
		t.Fatalf("total pause = %v", total)
	}
	if c.cache.owned(cacheKey{"key-1", KindVideo, "video-1"}) {
		t.Fatal("an unbound object was cached")
	}
}

func TestOwnedCachesPositivesOnlyAndSplitsLongLists(t *testing.T) {
	m, srv := newMockAPI(t)
	m.owned["key-1"] = []string{"file-0", "file-150"}
	c := newTestClient(srv, &fakeClock{t: time.Unix(0, 0)}, new([]time.Duration))
	var ids []string
	for i := range 150 {
		ids = append(ids, "file-"+strconv.Itoa(i))
	}
	ids = append(ids, "file-150", "file-0") // duplicate of an earlier id

	owned, err := c.Owned(context.Background(), "key-1", KindFile, ids)
	if err != nil {
		t.Fatal(err)
	}
	if len(owned) != 2 || !owned["file-0"] || !owned["file-150"] {
		t.Fatalf("owned = %v", owned)
	}
	if got := m.count("/internal/llm/objects/lookup"); got != 2 {
		t.Fatalf("lookup calls = %d, want 2 (100 + 51)", got)
	}

	// Positives are served from the cache; the absent id is asked again.
	if _, err := c.Owned(context.Background(), "key-1", KindFile, []string{"file-0", "file-7"}); err != nil {
		t.Fatal(err)
	}
	asked := m.bodies["/internal/llm/objects/lookup"][2]["objectIds"].([]any)
	if len(asked) != 1 || asked[0] != "file-7" {
		t.Fatalf("second lookup asked %v, want only file-7", asked)
	}
}

func TestOwnedIgnoresIdsTheCallDidNotAsk(t *testing.T) {
	// An api that answers more than it was asked.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"owned":["file-a","file-other"]}`))
	}))
	t.Cleanup(srv.Close)
	c := newTestClient(srv, &fakeClock{t: time.Unix(0, 0)}, new([]time.Duration))

	owned, err := c.Owned(context.Background(), "key-1", KindFile, []string{"file-a"})
	if err != nil {
		t.Fatal(err)
	}
	if owned["file-other"] || c.cache.owned(cacheKey{"key-1", KindFile, "file-other"}) {
		t.Fatal("an id the call did not ask about was accepted")
	}
}

func TestOwnedFailsWholeWhenTheAPICannotBeAsked(t *testing.T) {
	m, srv := newMockAPI(t)
	m.script("/internal/llm/objects/lookup", http.StatusServiceUnavailable)
	c := newTestClient(srv, &fakeClock{t: time.Unix(0, 0)}, new([]time.Duration))
	c.Remember("key-1", KindFile, "file-cached")

	if _, err := c.Owned(context.Background(), "key-1", KindFile, []string{"file-cached", "file-new"}); !IsUnavailable(err) {
		t.Fatalf("err = %v, want unavailable", err)
	}
	// A request naming only cached ids still works while the api is down.
	owned, err := c.Owned(context.Background(), "key-1", KindFile, []string{"file-cached"})
	if err != nil || !owned["file-cached"] {
		t.Fatalf("cached-only lookup: %v, %v", owned, err)
	}
}

func TestCacheIsPerKeyAndExpires(t *testing.T) {
	m, srv := newMockAPI(t)
	clock := &fakeClock{t: time.Unix(0, 0)}
	c := newTestClient(srv, clock, new([]time.Duration))
	c.Remember("key-1", KindVideo, "video-1")

	owned, err := c.Owned(context.Background(), "key-2", KindVideo, []string{"video-1"})
	if err != nil || owned["video-1"] {
		t.Fatalf("another key saw a cached ownership: %v %v", owned, err)
	}
	if owned, _ := c.Owned(context.Background(), "key-1", KindFile, []string{"video-1"}); owned["video-1"] {
		t.Fatal("ownership crossed kinds")
	}

	clock.advance(DefaultCacheTTL)
	if c.cache.owned(cacheKey{"key-1", KindVideo, "video-1"}) {
		t.Fatal("entry outlived its lifetime")
	}
	if got := m.count("/internal/llm/objects/lookup"); got != 2 {
		t.Fatalf("lookup calls = %d, want 2 (other key, other kind)", got)
	}
}

func TestReleaseForgetsTheCachedOwnership(t *testing.T) {
	m, srv := newMockAPI(t)
	c := newTestClient(srv, &fakeClock{t: time.Unix(0, 0)}, new([]time.Duration))
	c.Remember("key-1", KindFile, "file-1")

	released, err := c.Release(context.Background(), "key-1", KindFile, "file-1", "", ReleaseDeleted)
	if err != nil || !released {
		t.Fatalf("release: %v %v", released, err)
	}
	if c.cache.owned(cacheKey{"key-1", KindFile, "file-1"}) {
		t.Fatal("released ownership still cached")
	}
	body := m.bodies["/internal/llm/objects/release"][0]
	if body["reason"] != "DELETED" || body["objectId"] != "file-1" || body["intentId"] != nil {
		t.Fatalf("release body = %v", body)
	}

	if _, err := c.Release(context.Background(), "key-1", KindBatch, "", "intent-9", ReleaseNotCreated); err != nil {
		t.Fatal(err)
	}
	body = m.bodies["/internal/llm/objects/release"][1]
	if body["intentId"] != "intent-9" || body["objectId"] != nil {
		t.Fatalf("intent release body = %v", body)
	}
}

func TestCacheEvictsExpiredThenOldest(t *testing.T) {
	clock := &fakeClock{t: time.Unix(0, 0)}
	c := newCache(time.Minute, 3, clock.now)
	c.put(cacheKey{"k", KindFile, "a"})
	clock.advance(30 * time.Second)
	c.put(cacheKey{"k", KindFile, "b"})
	c.put(cacheKey{"k", KindFile, "c"})
	clock.advance(31 * time.Second) // a has expired, b and c have not

	c.put(cacheKey{"k", KindFile, "d"})
	if c.owned(cacheKey{"k", KindFile, "a"}) || c.len() != 3 {
		t.Fatalf("expired entry kept or wrong size %d", c.len())
	}
	c.put(cacheKey{"k", KindFile, "b"}) // refresh moves b to the back
	c.put(cacheKey{"k", KindFile, "e"}) // full: the oldest, c, goes
	if c.owned(cacheKey{"k", KindFile, "c"}) {
		t.Fatal("oldest entry not evicted")
	}
	for _, id := range []string{"b", "d", "e"} {
		if !c.owned(cacheKey{"k", KindFile, id}) {
			t.Fatalf("entry %s missing", id)
		}
	}
}

func TestBindStopsAtItsBudgetWhenTheAPIHangs(t *testing.T) {
	// The handler never reads the body, so the server does not notice the
	// client giving up; release it before Close waits for it.
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	t.Cleanup(srv.Close)
	t.Cleanup(func() { close(release) })
	c := New(srv.URL, "link-token", Options{
		Timeout:    20 * time.Millisecond,
		BindBudget: 150 * time.Millisecond,
		BindDelays: []time.Duration{10 * time.Millisecond, 10 * time.Millisecond, 10 * time.Millisecond,
			10 * time.Millisecond, 10 * time.Millisecond, 10 * time.Millisecond, 10 * time.Millisecond,
			10 * time.Millisecond, 10 * time.Millisecond, 10 * time.Millisecond},
	})

	start := time.Now()
	err := c.Bind(context.Background(), "key-1", "intent-1", KindFile, "file-1", BindDetails{})
	if !IsUnavailable(err) {
		t.Fatalf("err = %v, want unavailable", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("bind took %v, past its budget", elapsed)
	}
}

func TestBindIgnoresTheCallersCancellation(t *testing.T) {
	m, srv := newMockAPI(t)
	m.script("/internal/llm/objects/bind", http.StatusServiceUnavailable)
	var slept []time.Duration
	c := newTestClient(srv, &fakeClock{t: time.Unix(0, 0)}, &slept)
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // the client hung up before the bind

	if err := c.Bind(ctx, "key-1", "intent-1", KindFile, "file-1", BindDetails{}); err != nil {
		t.Fatalf("bind after the client left: %v", err)
	}
	if got := m.count("/internal/llm/objects/bind"); got != 2 {
		t.Fatalf("bind attempts = %d, want 2", got)
	}
}

func TestAFourHundredWithoutACodeIsUnavailableNotARefusal(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/problem+json")
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"title":"인증이 필요합니다","status":401}`))
	}))
	t.Cleanup(srv.Close)
	c := newTestClient(srv, &fakeClock{t: time.Unix(0, 0)}, new([]time.Duration))

	_, err := c.Intent(context.Background(), "key-1", KindFile, "evt-1")
	if !IsUnavailable(err) || RefusalCode(err) != "" {
		t.Fatalf("err = %v, want unavailable without a code", err)
	}
	if !strings.Contains(err.Error(), "HTTP 401") {
		t.Fatalf("error lost the status: %v", err)
	}
}

func TestAnUnreadableAnswerIsUnavailable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"owned":`))
	}))
	t.Cleanup(srv.Close)
	c := newTestClient(srv, &fakeClock{t: time.Unix(0, 0)}, new([]time.Duration))

	if _, err := c.Owned(context.Background(), "key-1", KindFile, []string{"file-a"}); !IsUnavailable(err) {
		t.Fatalf("err = %v, want unavailable", err)
	}
}

func TestOwnedAnswersIdsThatCannotExistWithoutAsking(t *testing.T) {
	m, srv := newMockAPI(t)
	c := newTestClient(srv, &fakeClock{t: time.Unix(0, 0)}, new([]time.Duration))

	owned, err := c.Owned(context.Background(), "key-1", KindFile, []string{"", strings.Repeat("x", 256)})
	if err != nil || len(owned) != 0 {
		t.Fatalf("owned = %v, err %v", owned, err)
	}
	if m.count("/internal/llm/objects/lookup") != 0 {
		t.Fatal("ids that cannot be owned were sent to the api")
	}
}

func TestCacheOfOneKeepsTheNewest(t *testing.T) {
	clock := &fakeClock{t: time.Unix(0, 0)}
	c := newCache(time.Minute, 1, clock.now)
	c.put(cacheKey{"k", KindFile, "a"})
	c.put(cacheKey{"k", KindFile, "b"})
	if c.owned(cacheKey{"k", KindFile, "a"}) || !c.owned(cacheKey{"k", KindFile, "b"}) || c.len() != 1 {
		t.Fatal("a cache of one did not keep only the newest entry")
	}
}

package server

import (
	"bufio"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pnuops/pickle-llm-gateway/internal/config"
	"github.com/pnuops/pickle-llm-gateway/internal/spool"
)

// largeCfg opens the large-body pool in the harness: the ordinary bound is
// the harness's 1 MiB, the large one 4 MiB.
func largeCfg(slots, perKey int) func(*config.Config) {
	return func(c *config.Config) {
		c.ChatLargeBodyMaxBytes = 4 << 20
		c.ChatLargeResponseMaxBytes = 16 << 20
		c.ChatLargeBodySlots = slots
		c.ChatLargeBodySlotsPerKey = perKey
	}
}

func chatOfSize(n int) string {
	const head, tail = `{"model":"pickle-general","messages":[{"role":"user","content":"`, `"}]}`
	return head + strings.Repeat("x", n-len(head)-len(tail)) + tail
}

func TestLargeBodySlotsCountTotalAndPerKey(t *testing.T) {
	l := newLargeBodySlots(3, 2)
	a1, ok1 := l.acquire("a")
	_, ok2 := l.acquire("a")
	_, ok3 := l.acquire("a")
	_, ok4 := l.acquire("b")
	_, ok5 := l.acquire("c")
	if !ok1 || !ok2 || ok3 || !ok4 || ok5 {
		t.Fatalf("admission: %v %v %v %v %v", ok1, ok2, ok3, ok4, ok5)
	}
	a1()
	a1() // a second release must not free a slot it does not hold
	if l.inUse() != 2 {
		t.Fatalf("in use after release = %d", l.inUse())
	}
	if _, ok := l.acquire("c"); !ok {
		t.Fatal("a freed slot was not reusable")
	}
	if newLargeBodySlots(0, 2) != nil {
		t.Fatal("a zero-size pool should be off")
	}
	var off *largeBodySlots
	if _, ok := off.acquire("a"); ok || off.inUse() != 0 {
		t.Fatal("an off pool admitted a body")
	}
}

func TestLargeChatBodyIsAdmittedThroughThePool(t *testing.T) {
	h := newHarness(t, nil, largeCfg(2, 1))

	body := chatOfSize(3 << 20)
	if status, resp := h.chat(t, testToken, body); status != 200 {
		t.Fatalf("large body: %d %s", status, resp)
	}
	if _, _, _, raw := h.mock.lastRequest(); len(raw) < 3<<20-1024 {
		t.Fatalf("upstream got %d bytes", len(raw))
	}
	if h.srv.chatLarge.inUse() != 0 {
		t.Fatal("the slot was not released")
	}
	// Above the large bound: refused before the upstream is contacted.
	calls := h.mock.callCount()
	if status, resp := h.chat(t, testToken, chatOfSize(5<<20)); status != 400 || errCode(t, resp) != "request_too_large" {
		t.Fatalf("over the large bound: %d %s", status, resp)
	}
	if h.mock.callCount() != calls {
		t.Fatal("an oversized body reached the upstream")
	}
}

// With the pool off, a body above the ordinary bound is refused exactly as
// it always was.
func TestLargeChatBodyWithPoolOff(t *testing.T) {
	h := newHarness(t, nil, largeCfg(0, 0))
	if status, resp := h.chat(t, testToken, chatOfSize(2<<20)); status != 400 || errCode(t, resp) != "request_too_large" {
		t.Fatalf("got %d %s", status, resp)
	}
}

// A body that declares no length is read through the ordinary bound, so
// leaving the header off cannot reach the large one.
func TestUndeclaredLengthUsesTheOrdinaryBound(t *testing.T) {
	h := newHarness(t, nil, largeCfg(2, 2))
	body := chatOfSize(2 << 20)
	req, err := http.NewRequest(http.MethodPost, h.gw.URL+"/v1/chat/completions",
		io.MultiReader(strings.NewReader(body)))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+testToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	out, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 400 || errCode(t, out) != "request_too_large" {
		t.Fatalf("chunked large body: %d %s", resp.StatusCode, out)
	}
}

// The slot is held while the upstream has not answered, and a key past its
// share is refused before its body is read.
func TestLargeBodySlotIsHeldUntilHeaders(t *testing.T) {
	h := newHarness(t, nil, largeCfg(4, 1))
	parked := make(chan struct{})
	unpark := make(chan struct{})
	var once sync.Once
	h.srv.client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		once.Do(func() { close(parked) })
		<-unpark
		return &http.Response{StatusCode: 200, Header: make(http.Header), Request: r,
			Body: io.NopCloser(strings.NewReader(`{"model":"` + upstreamModel + `","choices":[],"usage":{"prompt_tokens":1,"completion_tokens":1}}`))}, nil
	})}
	done := make(chan int)
	go func() {
		status, _ := h.chatRaw(testToken, chatOfSize(2<<20))
		done <- status
	}()
	<-parked
	if h.srv.chatLarge.inUse() != 1 {
		t.Fatalf("in use while waiting for headers = %d", h.srv.chatLarge.inUse())
	}
	status, resp, hdr := h.chatHeaders(t, testToken, chatOfSize(2<<20))
	if status != 429 || errCode(t, resp) != "large_request_busy" || hdr.Get("Retry-After") != "5" {
		t.Fatalf("second large body from the same key: %d %s %q", status, resp, hdr.Get("Retry-After"))
	}
	close(unpark)
	if status := <-done; status != 200 {
		t.Fatalf("parked request: %d", status)
	}
	evs := h.spoolEvents(t)
	refused := 0
	for _, ev := range evs {
		if ev.ErrorType == "large_request_busy" && ev.Status == spool.StatusRateLimited {
			refused++
		}
	}
	if refused != 1 {
		t.Fatalf("refusal not recorded: %+v", evs)
	}
}

// Once the upstream has answered, the slot is free and the request body is
// unreachable, while the stream is still running.
func TestLargeBodyIsReleasedWhileStreaming(t *testing.T) {
	h := newHarness(t, nil, largeCfg(2, 2))
	pr, pw := io.Pipe()
	var sent *http.Request
	h.srv.client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		sent = r
		return &http.Response{StatusCode: 200, Header: make(http.Header), Request: r, Body: pr}, nil
	})}
	body := strings.Replace(chatOfSize(2<<20), `"messages"`, `"stream":true,"messages"`, 1)
	req, err := http.NewRequest(http.MethodPost, h.gw.URL+"/v1/chat/completions", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+testToken)
	type result struct {
		resp *http.Response
		err  error
	}
	got := make(chan result)
	go func() {
		resp, err := http.DefaultClient.Do(req)
		got <- result{resp, err}
	}()
	go func() {
		_, _ = io.WriteString(pw, `data: {"model":"`+upstreamModel+`","choices":[{"index":0,"delta":{"content":"hi"}}]}`+"\n\n")
	}()
	r := <-got
	if r.err != nil {
		t.Fatal(r.err)
	}
	defer r.resp.Body.Close()
	first, err := bufio.NewReader(r.resp.Body).ReadString('\n')
	if err != nil || !strings.HasPrefix(first, "data:") {
		t.Fatalf("first event: %q %v", first, err)
	}
	if n := h.srv.chatLarge.inUse(); n != 0 {
		t.Fatalf("the slot is held while streaming: %d", n)
	}
	if again, err := sent.GetBody(); err != nil {
		t.Fatal(err)
	} else if _, err := again.Read(make([]byte, 1)); !errors.Is(err, errUpstreamBodyReleased) {
		t.Fatalf("the request body is still reachable: %v", err)
	}
	_, _ = io.WriteString(pw, "data: [DONE]\n\n")
	pw.Close()
}

// An upload that stops partway is a transport fault, not invalid JSON.
func TestInterruptedUploadIsNotInvalidJSON(t *testing.T) {
	h := newHarness(t, nil, nil)
	addr := strings.TrimPrefix(h.gw.URL, "http://")
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.WriteString(conn, "POST /v1/chat/completions HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer "+testToken+
		"\r\nContent-Type: application/json\r\nContent-Length: 1000\r\n\r\n{\"model\":")
	conn.Close()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if evs := h.spoolEvents(t); len(evs) == 1 {
			if evs[0].ErrorType != "request_body_read_failed" || evs[0].Status != spool.StatusCanceled {
				t.Fatalf("recorded as %s/%s", evs[0].Status, evs[0].ErrorType)
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("no usage event for the interrupted upload")
}

// A non-streamed answer past the ordinary bound is read only with a slot.
func TestLargeNonStreamResponse(t *testing.T) {
	big := `{"model":"` + upstreamModel + `","choices":[{"index":0,"message":{"role":"assistant","content":"` +
		strings.Repeat("z", 9<<20) + `"}}],"usage":{"prompt_tokens":3,"completion_tokens":4}}`
	respond := func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: make(http.Header), Request: r,
			Body: io.NopCloser(strings.NewReader(big))}, nil
	}
	short := `{"model":"pickle-general","messages":[{"role":"user","content":"hi"}]}`

	h := newHarness(t, nil, largeCfg(2, 2))
	h.srv.client = &http.Client{Transport: roundTripFunc(respond)}
	status, body := h.chat(t, testToken, short)
	if status != 200 || len(body) < 9<<20 {
		t.Fatalf("with a slot: %d, %d bytes", status, len(body))
	}
	if h.srv.chatLarge.inUse() != 0 {
		t.Fatal("the response slot was not released")
	}

	off := newHarness(t, nil, largeCfg(0, 0))
	off.srv.client = &http.Client{Transport: roundTripFunc(respond)}
	if status, body := off.chat(t, testToken, short); status != 502 || errCode(t, body) != "upstream_response_too_large" {
		t.Fatalf("without a pool: %d %s", status, body)
	}
	evs := off.spoolEvents(t)
	if ev := evs[len(evs)-1]; ev.ErrorType != "upstream_response_too_large" || !ev.Estimated {
		t.Fatalf("recorded as %+v", ev)
	}

	small := newHarness(t, nil, func(c *config.Config) {
		largeCfg(2, 2)(c)
		c.ChatLargeResponseMaxBytes = 9 << 20
	})
	small.srv.client = &http.Client{Transport: roundTripFunc(respond)}
	if status, body := small.chat(t, testToken, short); status != 502 || errCode(t, body) != "upstream_response_too_large" {
		t.Fatalf("over the large bound: %d %s", status, body)
	}
}

// Empty data lines are charged too, so a stream of them cannot grow one event
// without bound.
func TestStreamOfEmptyDataLinesIsBounded(t *testing.T) {
	h := newHarness(t, nil, nil)
	stream := `data: {"model":"` + upstreamModel + `","choices":[{"index":0,"delta":{"content":"ok"}}]}` + "\n\n" +
		strings.Repeat("data:\n", 300_000) + "\n" + "data: [DONE]\n\n"
	h.srv.client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: make(http.Header), Request: r,
			Body: io.NopCloser(strings.NewReader(stream))}, nil
	})}
	_, body := h.chat(t, testToken, `{"model":"pickle-general","stream":true,"messages":[{"role":"user","content":"hi"}]}`)
	if !strings.Contains(string(body), "upstream_response_too_large") {
		t.Fatalf("empty data lines were not bounded: %s", body[max(0, len(body)-300):])
	}
}

// A broken upload on a live connection is answered, not left as an empty 200.
func TestBrokenUploadIsAnswered(t *testing.T) {
	h := newHarness(t, nil, nil)
	conn, err := net.Dial("tcp", strings.TrimPrefix(h.gw.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_, _ = io.WriteString(conn, "POST /v1/chat/completions HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer "+testToken+
		"\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\n\r\nzz\r\n")
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatal(err)
	}
	out, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 400 || errCode(t, out) != "request_body_incomplete" {
		t.Fatalf("got %d %s", resp.StatusCode, out)
	}
	if evs := h.spoolEvents(t); len(evs) != 1 || evs[0].ErrorType != "request_body_read_failed" {
		t.Fatalf("events: %+v", evs)
	}
}

// spyTransport records the request a real transport was given.
type spyTransport struct {
	base http.RoundTripper
	mu   sync.Mutex
	last *http.Request
}

func (s *spyTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	s.mu.Lock()
	s.last = r
	s.mu.Unlock()
	return s.base.RoundTrip(r)
}

// The release works through net/http's own transport, which is where the
// request would otherwise stay reachable for the whole response.
func TestLargeBodyReleaseThroughARealTransport(t *testing.T) {
	sent := make(chan struct{})
	finish := make(chan struct{})
	up := httptestServer(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, `data: {"model":"`+upstreamModel+`","choices":[{"index":0,"delta":{"content":"hi"}}]}`+"\n\n")
		w.(http.Flusher).Flush()
		close(sent)
		<-finish
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
	})
	h := newHarness(t, nil, func(c *config.Config) {
		largeCfg(2, 2)(c)
		c.Upstreams["mock"] = config.Upstream{Ref: "mock", BaseURL: up, APIKey: upstreamCred, CapField: "max_completion_tokens"}
	})
	spy := &spyTransport{base: &http.Transport{}}
	h.srv.client = &http.Client{Transport: spy}
	body := strings.Replace(chatOfSize(2<<20), `"messages"`, `"stream":true,"messages"`, 1)
	req, _ := http.NewRequest(http.MethodPost, h.gw.URL+"/v1/chat/completions", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+testToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if _, err := bufio.NewReader(resp.Body).ReadString('\n'); err != nil {
		t.Fatal(err)
	}
	<-sent
	spy.mu.Lock()
	last := spy.last
	spy.mu.Unlock()
	again, err := last.GetBody()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := again.Read(make([]byte, 1)); !errors.Is(err, errUpstreamBodyReleased) {
		t.Fatalf("the body is still reachable through the transport's request: %v", err)
	}
	if h.srv.chatLarge.inUse() != 0 {
		t.Fatal("the slot is held while streaming")
	}
	close(finish)
}

// httptestServer starts an upstream and returns its base URL.
func httptestServer(t *testing.T, h http.HandlerFunc) string {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return srv.URL
}

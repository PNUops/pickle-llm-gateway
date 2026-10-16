package server

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/pnuops/pickle-llm-gateway/internal/config"
	"github.com/pnuops/pickle-llm-gateway/internal/spool"
)

// gatedWriter holds back any write over 1 MiB until gate is closed, or fails
// or panics on it, and records the write deadlines it was given.
type gatedWriter struct {
	*httptest.ResponseRecorder
	gate      chan struct{}
	entered   chan struct{}
	fail      bool
	panics    bool
	deadlines []time.Time
}

func (g *gatedWriter) Write(b []byte) (int, error) {
	if len(b) > 1<<20 {
		switch {
		case g.panics:
			panic("client write")
		case g.fail:
			return 0, errors.New("client gone")
		}
		close(g.entered)
		<-g.gate
	}
	return g.ResponseRecorder.Write(b)
}

func (g *gatedWriter) SetWriteDeadline(t time.Time) error {
	g.deadlines = append(g.deadlines, t)
	return nil
}

func largeNamedStream(size int) string {
	return "event: content_block_delta\ndata: {\"t\":\"" + strings.Repeat("p", size) + "\"}\n\n" +
		"event: message_stop\ndata: " + msgStop + "\n\n"
}

func relayLarge(h *harness, w http.ResponseWriter, stream string) {
	resp := &http.Response{StatusCode: http.StatusOK, Header: make(http.Header),
		Body: io.NopCloser(strings.NewReader(stream))}
	var ev spool.Event
	h.srv.relayStream(w, resp, testNamedStream(), streamArgs{
		keyID:     "k-test",
		charge:    func(int) {},
		clientCtx: context.Background(),
		upCtx:     context.Background(),
	}, &ev, func() {})
}

// A large event keeps its slot until the client has taken it, under a write
// deadline that is cleared again for the rest of the stream.
func TestLargeStreamEventHoldsItsSlotThroughTheWrite(t *testing.T) {
	h := newHarness(t, nil, largeCfg(2))
	w := &gatedWriter{ResponseRecorder: httptest.NewRecorder(), gate: make(chan struct{}), entered: make(chan struct{})}
	done := make(chan struct{})
	go func() {
		defer close(done)
		relayLarge(h, w, largeNamedStream(9<<20))
	}()
	<-w.entered
	if n := h.srv.chatLarge.inUse(); n != 1 {
		t.Fatalf("slots in use while the client reads: %d", n)
	}
	close(w.gate)
	<-done
	if n := h.srv.chatLarge.inUse(); n != 0 {
		t.Fatalf("slots in use after the write: %d", n)
	}
	if len(w.deadlines) != 2 || w.deadlines[0].IsZero() || !w.deadlines[1].IsZero() {
		t.Fatalf("write deadlines: %v", w.deadlines)
	}
	if !strings.HasSuffix(w.Body.String(), "event: message_stop\ndata: "+msgStop+"\n\n") {
		t.Fatal("the stream did not finish")
	}
}

// A client that goes away, or a write that panics, gives the slot back.
func TestLargeStreamEventSlotReturnedOnFailedWrite(t *testing.T) {
	for _, panics := range []bool{false, true} {
		h := newHarness(t, nil, largeCfg(2))
		w := &gatedWriter{ResponseRecorder: httptest.NewRecorder(), fail: !panics, panics: panics}
		func() {
			defer func() { _ = recover() }()
			relayLarge(h, w, largeNamedStream(9<<20))
		}()
		if n := h.srv.chatLarge.inUse(); n != 0 {
			t.Fatalf("panics=%v: slots in use after the failed write: %d", panics, n)
		}
	}
}

// A non-streamed large answer keeps its slot while the client has not read
// it, and frees it once the client has.
func TestLargeNonStreamHoldsItsSlotUntilRead(t *testing.T) {
	const size = 32 << 20
	big := `{"model":"` + upstreamModel + `","choices":[{"index":0,"message":{"role":"assistant","content":"` +
		strings.Repeat("z", size) + `"}}],"usage":{"prompt_tokens":3,"completion_tokens":4}}`
	h := newHarness(t, nil, func(c *config.Config) {
		largeCfg(2)(c)
		c.ChatLargeResponseMaxBytes = 48 << 20
	})
	h.srv.client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: make(http.Header), Request: r,
			Body: io.NopCloser(strings.NewReader(big))}, nil
	})}
	req, _ := http.NewRequest(http.MethodPost, h.gw.URL+"/v1/chat/completions",
		strings.NewReader(`{"model":"pickle-general","messages":[{"role":"user","content":"hi"}]}`))
	req.Header.Set("Authorization", "Bearer "+testToken)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	// Headers are in and the body has not been read: a 32 MiB answer cannot
	// all sit in socket buffers, so the write is still running.
	if n := h.srv.chatLarge.inUse(); n != 1 {
		t.Fatalf("slots in use before the client reads: %d", n)
	}
	k, _ := io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if k < size {
		t.Fatalf("read %d bytes", k)
	}
	deadline := time.Now().Add(5 * time.Second)
	for h.srv.chatLarge.inUse() != 0 {
		if time.Now().After(deadline) {
			t.Fatal("the slot was not released after the client read the answer")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestLargeWriteBudget(t *testing.T) {
	if got := largeWriteBudget(128 << 20); got != 316*time.Second {
		t.Fatalf("128 MiB budget %s", got)
	}
	if got := largeWriteBudget(1); got != largeWriteFloor {
		t.Fatalf("floor %s", got)
	}
}

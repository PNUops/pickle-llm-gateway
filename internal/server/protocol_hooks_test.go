package server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/pnuops/pickle-llm-gateway/internal/snapshot"
	"github.com/pnuops/pickle-llm-gateway/internal/spool"
)

// The hooks a surface can set that chat leaves unset. Each test serves a copy
// of the chat surface with one hook changed, so what it shows is the hook and
// not some other difference between surfaces.

func chatVariant(change func(p *protocol)) *protocol {
	p := *protoChat
	change(&p)
	return &p
}

func serveVariant(t *testing.T, h *harness, p *protocol, hdr map[string]string, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if _, ok := hdr["Authorization"]; !ok {
		req.Header.Set("Authorization", "Bearer "+testToken)
	}
	for k, v := range hdr {
		if v == "" {
			req.Header.Del(k)
			continue
		}
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.srv.serveProtocol(p, rec, req)
	return rec
}

func lastEvent(t *testing.T, h *harness) spool.Event {
	t.Helper()
	evs := h.spoolEvents(t)
	if len(evs) == 0 {
		t.Fatal("no usage event")
	}
	return evs[len(evs)-1]
}

var errTestMoneyOnly = apiError{http.StatusBadRequest, "invalid_request_error", "test_money_only", "money only"}

func TestCreditOnlySurfaceRefusesSelfHostedModel(t *testing.T) {
	h := newHarness(t, creditDoc, nil)
	p := chatVariant(func(p *protocol) { p.creditOnly = &errTestMoneyOnly })

	rec := serveVariant(t, h, p, nil, chatBody)
	if rec.Code != 400 || errCode(t, rec.Body.Bytes()) != "test_money_only" {
		t.Fatalf("self-hosted model: %d %s", rec.Code, rec.Body)
	}
	if h.mock.callCount() != 0 {
		t.Fatal("a refused self-hosted request reached the upstream")
	}
	ev := lastEvent(t, h)
	if ev.Status != spool.StatusBadRequest || ev.ErrorType != "test_money_only" ||
		ev.BudgetAxis != snapshot.AxisToken {
		t.Fatalf("record: %+v", ev)
	}

	rec = serveVariant(t, h, p, nil, creditChatBody)
	if rec.Code != 200 {
		t.Fatalf("money-axis model on a money-only surface: %d %s", rec.Code, rec.Body)
	}
}

// The refusal is the surface's, ahead of the self-hosted field checks: a
// field the self-hosted side would refuse does not change which answer comes.
func TestCreditOnlyIsJudgedBeforeTokenAxisFields(t *testing.T) {
	h := newHarness(t, nil, nil)
	p := chatVariant(func(p *protocol) { p.creditOnly = &errTestMoneyOnly })
	body := `{"model":"pickle-general","reasoning_effort":"high","messages":[{"role":"user","content":"hi"}]}`
	rec := serveVariant(t, h, p, nil, body)
	if errCode(t, rec.Body.Bytes()) != "test_money_only" {
		t.Fatalf("got %d %s", rec.Code, rec.Body)
	}
}

func TestSurfaceTokenReader(t *testing.T) {
	h := newHarness(t, nil, nil)
	missing := apiError{http.StatusUnauthorized, "authentication_error", "missing_api_key", "send X-Test-Key"}
	p := chatVariant(func(p *protocol) {
		p.token = func(r *http.Request) (string, *apiError) {
			if v := r.Header.Get("X-Test-Key"); v != "" {
				return v, nil
			}
			return "", &missing
		}
	})

	rec := serveVariant(t, h, p, map[string]string{"Authorization": "", "X-Test-Key": testToken}, chatBody)
	if rec.Code != 200 {
		t.Fatalf("key in the surface's own header: %d %s", rec.Code, rec.Body)
	}
	// The surface's reader is the only one: a bearer it does not read is no key.
	rec = serveVariant(t, h, p, nil, chatBody)
	if rec.Code != 401 || errMessage(t, rec.Body.Bytes()) != "send X-Test-Key" {
		t.Fatalf("bearer on a surface that reads another header: %d %s", rec.Code, rec.Body)
	}
	rec = serveVariant(t, h, p, map[string]string{"Authorization": "", "X-Test-Key": "pickle-nope"}, chatBody)
	if rec.Code != 401 || errCode(t, rec.Body.Bytes()) != "invalid_api_key" {
		t.Fatalf("unknown key: %d %s", rec.Code, rec.Body)
	}
}

// A surface without a capture function records nothing, not an empty record,
// even for a key that opted in.
func TestNilCaptureMakesNoBodyRecord(t *testing.T) {
	h := newHarness(t, func(d *snapshot.Document) { d.Keys[0].RecordBodies = true }, nil)
	cb := h.withBodyCapture(t)
	off := chatVariant(func(p *protocol) { p.capture = nil })

	for _, stream := range []bool{false, true} {
		body := chatBody
		if stream {
			body = streamRequest
		}
		if rec := serveVariant(t, h, off, nil, body); rec.Code != 200 {
			t.Fatalf("stream=%v: %d %s", stream, rec.Code, rec.Body)
		}
	}
	// The chat surface itself still records, which is what tells the wait
	// below apart from a sink that delivers nothing.
	if rec := serveVariant(t, h, protoChat, nil, chatBody); rec.Code != 200 {
		t.Fatalf("chat: %d %s", rec.Code, rec.Body)
	}
	got := waitForRecords(t, cb, 1)
	if len(got) != 1 {
		t.Fatalf("records: %d, want only the chat one", len(got))
	}
	if n := h.srv.metrics.bodiesCaptured.Load(); n != 1 {
		t.Fatalf("bodiesCaptured %d", n)
	}
}

func TestUsageRequiredRefusesUnmeteredAnswer(t *testing.T) {
	h := newHarness(t, nil, nil)
	h.mock.set(func(o *mockOpts) { o.noUsage = true })
	p := chatVariant(func(p *protocol) { p.usageRequired = true })

	rec := serveVariant(t, h, p, nil, chatBody)
	if rec.Code != 502 || errCode(t, rec.Body.Bytes()) != "upstream_error" {
		t.Fatalf("answer without usage: %d %s", rec.Code, rec.Body)
	}
	if strings.Contains(rec.Body.String(), "안녕하세요") {
		t.Fatal("the unmetered answer was relayed")
	}
	ev := lastEvent(t, h)
	if ev.Status != spool.StatusUpstreamErr || ev.ErrorType != "upstream_usage_missing" ||
		ev.InputTokens != 0 || ev.OutputTokens != 0 || ev.Estimated {
		t.Fatalf("record: %+v", ev)
	}

	// With usage the surface serves as chat does.
	h.mock.set(func(o *mockOpts) { o.noUsage = false })
	rec = serveVariant(t, h, p, nil, chatBody)
	if rec.Code != 200 {
		t.Fatalf("answer with usage: %d %s", rec.Code, rec.Body)
	}
	ev = lastEvent(t, h)
	if ev.Status != spool.StatusOK || ev.InputTokens != 7 || ev.OutputTokens != 5 || ev.Estimated {
		t.Fatalf("record: %+v", ev)
	}
}

func TestUsageRequiredStreamWithoutUsageIsRecordedFailed(t *testing.T) {
	h := newHarness(t, nil, nil)
	h.mock.set(func(o *mockOpts) { o.noUsage = true })
	p := chatVariant(func(p *protocol) { p.usageRequired = true })

	rec := serveVariant(t, h, p, nil, streamRequest)
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), "data: [DONE]") {
		t.Fatalf("stream: %d %s", rec.Code, rec.Body)
	}
	ev := lastEvent(t, h)
	if ev.Status != spool.StatusUpstreamErr || ev.ErrorType != "upstream_usage_missing" ||
		ev.InputTokens != 0 || ev.OutputTokens != 0 || ev.Estimated {
		t.Fatalf("record: %+v", ev)
	}

	// A stream that failed for another reason keeps that reason.
	h.mock.set(func(o *mockOpts) { o.abortMid = true })
	_ = serveVariant(t, h, p, nil, streamRequest)
	if ev := lastEvent(t, h); ev.ErrorType != "upstream_stream_interrupted" {
		t.Fatalf("interrupted stream: %+v", ev)
	}
}

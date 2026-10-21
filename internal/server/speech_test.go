package server

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pnuops/pickle-llm-gateway/internal/snapshot"
	"github.com/pnuops/pickle-llm-gateway/internal/spool"
)

const speechBody = `{"model":"hexgrad/kokoro-82m","input":"안녕하세요","voice":"af_heart","response_format":"mp3"}`

// speechVendor answers the speech route with audio of the given media type,
// stamped with a generation id as the vendor does, and records what it got.
type speechVendor struct {
	mu          sync.Mutex
	path        string
	body        string
	contentType string
	audio       string
}

func (v *speechVendor) install(h *harness) {
	h.srv.passthroughClient = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		raw, _ := io.ReadAll(r.Body)
		v.mu.Lock()
		defer v.mu.Unlock()
		v.path, v.body = r.URL.Path, string(raw)
		hdr := make(http.Header)
		hdr.Set("Content-Type", v.contentType)
		hdr.Set(generationIDHeader, testGenerationID)
		return &http.Response{StatusCode: 200, Header: hdr, Request: r, ContentLength: int64(len(v.audio)),
			Body: io.NopCloser(strings.NewReader(v.audio))}, nil
	})}
}

func speechHarness(t *testing.T, endpoints ...string) (*harness, *speechVendor, *generationVendor) {
	t.Helper()
	h, g := generationHarnessFor(t, passthroughDoc(endpoints...), nil,
		[]time.Duration{time.Millisecond}, generationAnswer{200, generationRecordOK})
	v := &speechVendor{contentType: "audio/mpeg", audio: "\xff\xf3\x84\xc4 not really mp3"}
	v.install(h)
	return h, v, g
}

func postSpeech(t *testing.T, h *harness, body string) (int, http.Header, []byte) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, h.gw.URL+"/v1/audio/speech", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+testToken)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, resp.Header, out
}

// The audio comes back byte for byte under the vendor's media type. Nothing
// in it is usage, so the record has no counts, is not an estimate, and its
// price is read from the generation record.
func TestSpeechRelaysAudio(t *testing.T) {
	h, v, g := speechHarness(t, snapshot.EndpointSpeech)
	status, hdr, out := postSpeech(t, h, speechBody)
	if status != 200 || string(out) != v.audio || hdr.Get("Content-Type") != "audio/mpeg" ||
		hdr.Get(generationIDHeader) != testGenerationID {
		t.Fatalf("%d %v %q", status, hdr, out)
	}
	if v.path != "/audio/speech" || !strings.Contains(v.body, `"voice":"af_heart"`) {
		t.Fatalf("upstream %s %s", v.path, v.body)
	}
	h.srv.DrainCostLookups(context.Background())
	ev := lastEvent(t, h)
	if ev.Endpoint != spool.EndpointSpeech || ev.Status != spool.StatusOK || ev.InputTokens != 0 ||
		ev.OutputTokens != 0 || ev.Estimated || ev.CostUsd != "0.000023" || ev.PublicModelName != "hexgrad/kokoro-82m" {
		t.Fatalf("record: %+v", ev)
	}
	if g.readCount() != 1 {
		t.Fatalf("reads %d", g.readCount())
	}
}

// Raw PCM keeps the parameters that say how to play it.
func TestSpeechKeepsMediaTypeParameters(t *testing.T) {
	h, v, _ := speechHarness(t, snapshot.EndpointSpeech)
	v.contentType = "audio/pcm;rate=24000;channels=1"
	status, hdr, _ := postSpeech(t, h, `{"model":"hexgrad/kokoro-82m","input":"hi","voice":"af_heart"}`)
	ct := hdr.Get("Content-Type")
	if status != 200 || !strings.HasPrefix(ct, "audio/pcm") || !strings.Contains(ct, "rate=24000") ||
		!strings.Contains(ct, "channels=1") {
		t.Fatalf("%d %q", status, ct)
	}
}

// An answer that is not audio is not relayed: the gateway cannot say what it
// is, and it was billed as audio.
func TestSpeechRefusesAnAnswerThatIsNotAudio(t *testing.T) {
	for _, ct := range []string{"application/json", "text/html", "", "audio/"} {
		h, v, _ := speechHarness(t, snapshot.EndpointSpeech)
		v.contentType = ct
		status, _, out := postSpeech(t, h, speechBody)
		if status != http.StatusBadGateway || errCode(t, out) != "upstream_error" {
			t.Fatalf("%q: %d %s", ct, status, out)
		}
		h.srv.DrainCostLookups(context.Background())
		if ev := lastEvent(t, h); ev.Status != spool.StatusUpstreamErr || ev.ErrorType != "upstream_invalid_response" {
			t.Fatalf("%q: %+v", ct, ev)
		}
	}
}

// The OpenAI fields the vendor cannot honour are refused with the reason,
// before anything reaches the vendor.
func TestSpeechRefusesWhatTheVendorCannotHonour(t *testing.T) {
	h, v, _ := speechHarness(t, snapshot.EndpointSpeech)
	for _, tc := range []struct {
		name, field, value, code string
	}{
		{"wav", "response_format", `"wav"`, "unsupported_parameter"},
		{"opus", "response_format", `"opus"`, "unsupported_parameter"},
		{"aac", "response_format", `"aac"`, "unsupported_parameter"},
		{"flac", "response_format", `"flac"`, "unsupported_parameter"},
		{"format not a string", "response_format", `1`, "invalid_parameter_value"},
		{"sse", "stream_format", `"sse"`, "unsupported_parameter"},
		{"stream format not a string", "stream_format", `true`, "invalid_parameter_value"},
		{"voice cloning", "input_references", `[{"url":"https://example.com/a.wav"}]`, "unsupported_parameter"},
	} {
		body := `{"model":"hexgrad/kokoro-82m","input":"hi","voice":"af_heart","` + tc.field + `":` + tc.value + `}`
		status, _, out := postSpeech(t, h, body)
		if status != http.StatusBadRequest || errCode(t, out) != tc.code || !strings.Contains(errMessage(t, out), tc.field) {
			t.Fatalf("%s: %d %s", tc.name, status, out)
		}
	}
	if v.path != "" {
		t.Fatal("a refused request reached the vendor")
	}
	for _, body := range []string{
		`{"model":"hexgrad/kokoro-82m","input":"hi","voice":"af_heart","response_format":"pcm"}`,
		`{"model":"hexgrad/kokoro-82m","input":"hi","voice":"af_heart","stream_format":"audio"}`,
		`{"model":"hexgrad/kokoro-82m","input":"hi","voice":"af_heart","response_format":null,"input_references":null}`,
	} {
		if status, _, out := postSpeech(t, h, body); status != 200 {
			t.Fatalf("%s: %d %s", body, status, out)
		}
	}
}

func TestSpeechNeedsItsOwnGrant(t *testing.T) {
	h, v, _ := speechHarness(t, snapshot.EndpointImages, snapshot.EndpointRerank)
	status, _, out := postSpeech(t, h, speechBody)
	if status != http.StatusForbidden || !strings.Contains(errMessage(t, out), "TTS(speech)") {
		t.Fatalf("%d %s", status, out)
	}
	if v.path != "" {
		t.Fatal("a refused request reached the vendor")
	}
}

// Spoken output from chat is the speech grant's, judged from the field that
// asks for it alone. Audio input is not judged.
func TestChatAudioOutputNeedsSpeech(t *testing.T) {
	const audioOut = `{"model":"openai/gpt-audio","modalities":["text","audio"],"audio":{"voice":"alloy","format":"mp3"},` +
		`"messages":[{"role":"user","content":"hi"}]}`
	const audioIn = `{"model":"openai/gpt-audio","messages":[{"role":"user","content":[` +
		`{"type":"input_audio","input_audio":{"data":"AAAA","format":"wav"}}]}]}`
	h := newHarness(t, passthroughDoc(snapshot.EndpointImages), nil)
	status, body := h.chat(t, testToken, audioOut)
	if status != http.StatusForbidden || errCode(t, body) != "endpoint_not_allowed" ||
		!strings.Contains(errMessage(t, body), "TTS(speech)") {
		t.Fatalf("audio output without the grant: %d %s", status, body)
	}
	if status, body := h.chat(t, testToken, audioIn); status != 200 {
		t.Fatalf("audio input: %d %s", status, body)
	}
	h2 := newHarness(t, passthroughDoc(snapshot.EndpointSpeech), nil)
	if status, body := h2.chat(t, testToken, audioOut); status != 200 {
		t.Fatalf("audio output with the grant: %d %s", status, body)
	}
}

// A vendor router is refused here for every key, as on every passthrough
// route.
func TestSpeechRefusesRouterModels(t *testing.T) {
	h, v, _ := speechHarness(t, snapshot.EndpointSpeech)
	status, _, out := postSpeech(t, h, `{"model":"openrouter/auto","input":"hi","voice":"alloy"}`)
	if status != http.StatusBadRequest || errCode(t, out) != "router_not_supported" {
		t.Fatalf("%d %s", status, out)
	}
	if v.path != "" {
		t.Fatal("a router request reached the vendor")
	}
}

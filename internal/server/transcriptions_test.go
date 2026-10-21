package server

import (
	"bytes"
	"context"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pnuops/pickle-llm-gateway/internal/snapshot"
	"github.com/pnuops/pickle-llm-gateway/internal/spool"
)

// The two usage shapes measured on the vendor: a model billed by time reports
// seconds and a price, one billed by token reports its counts and a price.
const (
	sttTimeAnswer  = `{"text":" Hello from Pickle.","usage":{"seconds":1.55,"cost":0.0000051615}}`
	sttTokenAnswer = `{"text":"Hello from Pickle.","usage":{"total_tokens":51,"input_tokens":37,"output_tokens":14,"cost":0.00011625}}`
	sttJSONBody    = `{"model":"openai/whisper-large-v3-turbo","input_audio":{"data":"SUQz","format":"mp3"}}`
)

// sttVendor records what reached the vendor and answers with a JSON body.
type sttVendor struct {
	mu          sync.Mutex
	calls       int
	contentType string
	body        []byte
	answer      string
}

func (v *sttVendor) install(h *harness) {
	h.srv.passthroughClient = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		raw, _ := io.ReadAll(r.Body)
		v.mu.Lock()
		defer v.mu.Unlock()
		v.calls++
		v.contentType, v.body = r.Header.Get("Content-Type"), raw
		hdr := make(http.Header)
		hdr.Set("Content-Type", "application/json")
		hdr.Set(generationIDHeader, testGenerationID)
		return &http.Response{StatusCode: 200, Header: hdr, Request: r, ContentLength: int64(len(v.answer)),
			Body: io.NopCloser(strings.NewReader(v.answer))}, nil
	})}
}

func sttHarness(t *testing.T, endpoints ...string) (*harness, *sttVendor) {
	t.Helper()
	h := newHarness(t, passthroughDoc(endpoints...), nil)
	v := &sttVendor{answer: sttTimeAnswer}
	v.install(h)
	return h, v
}

// formPart is one part of a test upload.
type formPart struct {
	name, filename, contentType, value string
}

// form builds a multipart upload the way the OpenAI SDK does.
func form(t *testing.T, parts ...formPart) (string, []byte) {
	t.Helper()
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)
	for _, p := range parts {
		if p.filename == "" {
			if err := w.WriteField(p.name, p.value); err != nil {
				t.Fatal(err)
			}
			continue
		}
		h := make(map[string][]string)
		h["Content-Disposition"] = []string{`form-data; name="` + p.name + `"; filename="` + p.filename + `"`}
		if p.contentType != "" {
			h["Content-Type"] = []string{p.contentType}
		}
		fw, err := w.CreatePart(h)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = fw.Write([]byte(p.value))
	}
	_ = w.Close()
	return w.FormDataContentType(), buf.Bytes()
}

func postTranscription(t *testing.T, h *harness, contentType string, body []byte) (int, []byte) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, h.gw.URL+"/v1/audio/transcriptions", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+testToken)
	req.Header.Set("Content-Type", contentType)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, out
}

// readForm parses what reached the vendor.
func readForm(t *testing.T, contentType string, body []byte) (map[string][]string, map[string]*multipart.FileHeader) {
	t.Helper()
	_, params, err := mime.ParseMediaType(contentType)
	if err != nil {
		t.Fatalf("forwarded content type %q: %v", contentType, err)
	}
	f, err := multipart.NewReader(bytes.NewReader(body), params["boundary"]).ReadForm(1 << 20)
	if err != nil {
		t.Fatal(err)
	}
	files := map[string]*multipart.FileHeader{}
	for name, fhs := range f.File {
		files[name] = fhs[0]
	}
	return f.Value, files
}

// The JSON shape goes through like any passthrough body, and both usage
// shapes are recorded as the vendor reported them: a price with no counts for
// a model billed by time, counts and a price for one billed by token.
func TestTranscriptionsJSON(t *testing.T) {
	for _, tc := range []struct {
		answer  string
		in, out int
		cost    string
	}{
		{sttTimeAnswer, 0, 0, "0.0000051615"},
		{sttTokenAnswer, 37, 14, "0.00011625"},
	} {
		h, v := sttHarness(t, snapshot.EndpointTranscriptions)
		v.answer = tc.answer
		status, out := postTranscription(t, h, "application/json", []byte(sttJSONBody))
		if status != 200 || string(out) != tc.answer {
			t.Fatalf("%d %s", status, out)
		}
		if v.contentType != "application/json" || !strings.Contains(string(v.body), `"input_audio"`) {
			t.Fatalf("upstream %q %s", v.contentType, v.body)
		}
		ev := lastEvent(t, h)
		if ev.Endpoint != spool.EndpointTranscriptions || ev.Status != spool.StatusOK || ev.Estimated ||
			ev.InputTokens != tc.in || ev.OutputTokens != tc.out || string(ev.CostUsd) != tc.cost {
			t.Fatalf("record: %+v", ev)
		}
	}
}

// The OpenAI SDK's multipart upload is parsed and assembled again: the vendor
// gets the same fields and the same file bytes, in a form of the gateway's.
func TestTranscriptionsMultipart(t *testing.T) {
	h, v := sttHarness(t, snapshot.EndpointTranscriptions)
	audio := "ID3\x00\x01binary\xff\xfe"
	ct, body := form(t,
		formPart{name: "file", filename: "voice.mp3", contentType: "audio/mpeg", value: audio},
		formPart{name: "model", value: "openai/whisper-large-v3-turbo"},
		formPart{name: "response_format", value: "verbose_json"},
		formPart{name: "timestamp_granularities[]", value: "word"},
		formPart{name: "timestamp_granularities[]", value: "segment"},
		formPart{name: "language", value: "ko"},
	)
	status, out := postTranscription(t, h, ct, body)
	if status != 200 {
		t.Fatalf("%d %s", status, out)
	}
	values, files := readForm(t, v.contentType, v.body)
	if values["model"][0] != "openai/whisper-large-v3-turbo" || values["response_format"][0] != "verbose_json" ||
		values["language"][0] != "ko" || strings.Join(values["timestamp_granularities[]"], ",") != "word,segment" {
		t.Fatalf("forwarded fields %v", values)
	}
	fh := files["file"]
	if fh == nil || fh.Filename != "voice.mp3" || fh.Header.Get("Content-Type") != "audio/mpeg" {
		t.Fatalf("forwarded file %+v", fh)
	}
	f, _ := fh.Open()
	got, _ := io.ReadAll(f)
	if string(got) != audio {
		t.Fatalf("file bytes changed: %q", got)
	}
	ev := lastEvent(t, h)
	if ev.Status != spool.StatusOK || ev.PublicModelName != "openai/whisper-large-v3-turbo" || ev.CostUsd != "0.0000051615" {
		t.Fatalf("record: %+v", ev)
	}
}

// A form is refused, before anything reaches the vendor, when it carries a
// part this gateway does not forward, repeats one that cannot repeat, or asks
// for what the vendor cannot give.
func TestTranscriptionsFormRefusals(t *testing.T) {
	h, v := sttHarness(t, snapshot.EndpointTranscriptions)
	file := formPart{name: "file", filename: "a.mp3", contentType: "audio/mpeg", value: "ID3"}
	model := formPart{name: "model", value: "openai/whisper-large-v3-turbo"}
	for _, tc := range []struct {
		name  string
		parts []formPart
		code  string
	}{
		{"two models", []formPart{file, model, {name: "model", value: "openai/gpt-4o-mini-transcribe"}}, "invalid_parameter_value"},
		{"two files", []formPart{file, file, model}, "invalid_parameter_value"},
		{"unknown field", []formPart{file, model, {name: "include[]", value: "logprobs"}}, "unsupported_parameter"},
		{"models list", []formPart{file, model, {name: "models", value: `["x/y"]`}}, "unsupported_parameter"},
		{"text format", []formPart{file, model, {name: "response_format", value: "text"}}, "unsupported_parameter"},
		{"srt format", []formPart{file, model, {name: "response_format", value: "srt"}}, "unsupported_parameter"},
		{"stream", []formPart{file, model, {name: "stream", value: "true"}}, "streaming_not_supported"},
		{"stream not a boolean", []formPart{file, model, {name: "stream", value: "yes"}}, "invalid_parameter_value"},
		{"no model", []formPart{file}, "missing_parameter"},
	} {
		ct, body := form(t, tc.parts...)
		status, out := postTranscription(t, h, ct, body)
		if status != http.StatusBadRequest || errCode(t, out) != tc.code {
			t.Fatalf("%s: %d %s", tc.name, status, out)
		}
	}
	// A body that is not a form of named parts is refused by its shape.
	ct, whole := form(t, file, model)
	for name, tc := range map[string]struct {
		ct   string
		body []byte
	}{
		"no boundary":         {"multipart/form-data", whole},
		"cut before the end":  {ct, whole[:len(whole)-10]},
		"a part with no name": {ct, bytes.Replace(whole, []byte(`name="model"`), []byte(`x="model"`), 1)},
	} {
		status, out := postTranscription(t, h, tc.ct, tc.body)
		if status != http.StatusBadRequest || errCode(t, out) != "invalid_multipart_form" {
			t.Fatalf("%s: %d %s", name, status, out)
		}
	}
	// A text field that is not valid UTF-8 would be judged as one name and
	// sent as another.
	ct, bad := form(t, file, formPart{name: "model", value: "openai/whisper-\xff"})
	if status, out := postTranscription(t, h, ct, bad); status != http.StatusBadRequest || errCode(t, out) != "invalid_parameter_value" {
		t.Fatalf("invalid utf-8: %d %s", status, out)
	}
	if v.calls != 0 {
		t.Fatal("a refused request reached the vendor")
	}
}

// The JSON shape gets the same checks on the fields the vendor cannot honour,
// and an audio URL is passed on for the vendor to judge.
func TestTranscriptionsJSONRefusals(t *testing.T) {
	h, v := sttHarness(t, snapshot.EndpointTranscriptions)
	for _, tc := range []struct{ body, code string }{
		{`{"model":"openai/whisper-large-v3-turbo","input_audio":{"data":"SUQz","format":"mp3"},"response_format":"vtt"}`, "unsupported_parameter"},
		{`{"model":"openai/whisper-large-v3-turbo","input_audio":{"data":"SUQz","format":"mp3"},"chunking_strategy":"auto"}`, "unsupported_parameter"},
		{`{"model":"openai/whisper-large-v3-turbo","input_audio":{"data":"SUQz","format":"mp3"},"include":["logprobs"]}`, "unsupported_parameter"},
		{`{"model":"openai/whisper-large-v3-turbo","input_audio":{"data":"SUQz","format":"mp3"},"stream":true}`, "streaming_not_supported"},
	} {
		status, out := postTranscription(t, h, "application/json", []byte(tc.body))
		if status != http.StatusBadRequest || errCode(t, out) != tc.code {
			t.Fatalf("%s: %d %s", tc.body, status, out)
		}
	}
	if v.calls != 0 {
		t.Fatal("a refused request reached the vendor")
	}
	status, out := postTranscription(t, h, "application/json",
		[]byte(`{"model":"openai/whisper-large-v3-turbo","input_audio":{"url":"https://example.com/a.wav"},"response_format":"verbose_json"}`))
	if status != 200 || v.calls != 1 {
		t.Fatalf("url input: %d %s", status, out)
	}
}

// A file name is the client's text on its way into a header: what cannot be
// carried there is dropped, and the extension the vendor reads survives.
func TestTranscriptionsFilenameIsCleaned(t *testing.T) {
	h, v := sttHarness(t, snapshot.EndpointTranscriptions)
	ct, body := form(t,
		formPart{name: "file", filename: `a\"b.mp3`, contentType: "audio/mpeg", value: "ID3"},
		formPart{name: "model", value: "openai/whisper-large-v3-turbo"},
	)
	if status, out := postTranscription(t, h, ct, body); status != 200 {
		t.Fatalf("%d %s", status, out)
	}
	_, files := readForm(t, v.contentType, v.body)
	if fh := files["file"]; fh == nil || !strings.HasSuffix(fh.Filename, ".mp3") || strings.ContainsAny(fh.Filename, `"\`) {
		t.Fatalf("forwarded file %+v", files["file"])
	}
}

func TestTranscriptionsNeedsItsOwnGrant(t *testing.T) {
	h, v := sttHarness(t, snapshot.EndpointSpeech)
	status, out := postTranscription(t, h, "application/json", []byte(sttJSONBody))
	if status != http.StatusForbidden || !strings.Contains(errMessage(t, out), "STT(transcriptions)") {
		t.Fatalf("%d %s", status, out)
	}
	if v.calls != 0 {
		t.Fatal("a refused request reached the vendor")
	}
}

// The model fence runs on a form's model as on a JSON one, routers included.
func TestTranscriptionsFormModelIsFenced(t *testing.T) {
	h := newHarness(t, func(d *snapshot.Document) {
		passthroughDoc(snapshot.EndpointTranscriptions)(d)
		d.Keys[0].CreditAllowedModels = []string{"openai/whisper-*"}
	}, nil)
	v := &sttVendor{answer: sttTimeAnswer}
	v.install(h)
	file := formPart{name: "file", filename: "a.mp3", contentType: "audio/mpeg", value: "ID3"}
	for _, tc := range []struct{ model, code string }{
		{"openai/gpt-4o-mini-transcribe", "model_not_allowed"},
		{"openrouter/auto", "router_not_supported"},
	} {
		ct, body := form(t, file, formPart{name: "model", value: tc.model})
		if _, out := postTranscription(t, h, ct, body); errCode(t, out) != tc.code {
			t.Fatalf("%s: %s", tc.model, out)
		}
	}
	if v.calls != 0 {
		t.Fatal("a refused request reached the vendor")
	}
}

// Usage is optional on this route: an answer without it is recorded with no
// counts and no estimate, and an answer without a price has it read from the
// generation record.
func TestTranscriptionsWithoutUsageReadsTheCost(t *testing.T) {
	h, g := generationHarnessFor(t, passthroughDoc(snapshot.EndpointTranscriptions), nil,
		[]time.Duration{time.Millisecond}, generationAnswer{200, generationRecordOK})
	v := &sttVendor{answer: `{"text":"hi"}`}
	v.install(h)
	if status, out := postTranscription(t, h, "application/json", []byte(sttJSONBody)); status != 200 {
		t.Fatalf("%d %s", status, out)
	}
	h.srv.DrainCostLookups(context.Background())
	ev := lastEvent(t, h)
	if ev.Status != spool.StatusOK || ev.InputTokens != 0 || ev.Estimated || ev.CostUsd != "0.000023" || g.readCount() != 1 {
		t.Fatalf("record %+v after %d reads", ev, g.readCount())
	}
}

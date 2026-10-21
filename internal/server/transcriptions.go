// The transcription route's request side: the multipart upload the OpenAI SDK
// sends, and the OpenAI fields the vendor cannot honour.
package server

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"mime/multipart"
	"net/textproto"
	"strconv"
	"strings"
	"unicode/utf8"
)

// transcriptionFormFields are the multipart fields the route forwards, each
// with whether it may repeat. A form is assembled again from these alone, so
// a field outside the list is refused by name rather than dropped: dropping it
// would change the request without a word, and forwarding it unread would
// put a part on the wire this gateway never looked at.
//
// The list is the vendor's documented form plus OpenAI's own `prompt` and
// `stream`; `stream` is read so a streaming request is refused as on every
// passthrough route. The fields OpenAI documents that the vendor does not
// take (`chunking_strategy`, `include[]`, the known speaker lists) are not on
// it and are refused.
var transcriptionFormFields = map[string]bool{
	"model":                     false,
	"language":                  false,
	"prompt":                    false,
	"response_format":           false,
	"temperature":               false,
	"timestamp_granularities[]": true,
	"keyterms[]":                true,
	"diarize":                   false,
	"provider":                  false,
	"source_url":                false,
	"stream":                    false,
}

// transcriptionFileField is the part that carries the audio.
const transcriptionFileField = "file"

// Bounds on a form, apart from the body cap that already bounds the whole of
// it: a text field is a word or a short prompt, and a form has a handful of
// parts. Past either the form is refused rather than read further.
const (
	transcriptionFieldMaxBytes = 64 << 10
	transcriptionFormMaxParts  = 64
)

func isMultipartForm(contentType string) bool {
	mediaType, _, err := mime.ParseMediaType(contentType)
	return err == nil && mediaType == "multipart/form-data"
}

// readTranscriptionForm parses a multipart upload and assembles the form that
// is forwarded. It returns the fields as the JSON values the fences and the
// request checks read (each text field as a string, `stream` as a boolean
// when it reads as one), the assembled body and its media type.
//
// The body is held whole three times while this runs (the request, the parsed
// parts, the assembled form), the same multiple the JSON path costs: the
// parsed map and the re-serialized body beside the request.
func readTranscriptionForm(raw []byte, contentType string) (map[string]json.RawMessage, []byte, string, *apiError) {
	invalid := func(name string) (map[string]json.RawMessage, []byte, string, *apiError) {
		e := errInvalidParamValue(name)
		return nil, nil, "", &e
	}
	malformed := func() (map[string]json.RawMessage, []byte, string, *apiError) {
		e := errMalformedForm
		return nil, nil, "", &e
	}
	_, mtParams, err := mime.ParseMediaType(contentType)
	if err != nil || mtParams["boundary"] == "" {
		return malformed()
	}
	type filePart struct {
		filename, contentType string
		data                  []byte
	}
	var file *filePart
	fields := map[string][]string{}
	reader := multipart.NewReader(bytes.NewReader(raw), mtParams["boundary"])
	for parts := 0; ; parts++ {
		part, err := reader.NextRawPart()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil || parts >= transcriptionFormMaxParts {
			return malformed()
		}
		name := part.FormName()
		switch repeats, known := transcriptionFormFields[name]; {
		case name == transcriptionFileField:
			if file != nil {
				return invalid(name)
			}
			data, err := io.ReadAll(part)
			if err != nil {
				return malformed()
			}
			file = &filePart{filename: part.FileName(), contentType: part.Header.Get("Content-Type"), data: data}
		case name == "":
			return malformed()
		case !known:
			e := errUnsupportedParam(name)
			return nil, nil, "", &e
		default:
			if !repeats && len(fields[name]) > 0 {
				return invalid(name)
			}
			value, err := io.ReadAll(io.LimitReader(part, transcriptionFieldMaxBytes+1))
			// Valid UTF-8 only: the fences judge the value decoded as JSON,
			// where invalid bytes become U+FFFD, while the form would carry the
			// raw bytes on, so the name judged and the name sent would differ.
			if err != nil {
				return malformed()
			}
			if len(value) > transcriptionFieldMaxBytes || !utf8.Valid(value) {
				return invalid(name)
			}
			fields[name] = append(fields[name], string(value))
		}
	}

	params := map[string]json.RawMessage{}
	for name, values := range fields {
		var v []byte
		switch {
		case transcriptionFormFields[name]:
			v, _ = json.Marshal(values)
		case name == "stream" && (values[0] == "true" || values[0] == "false"):
			v = []byte(values[0])
		default:
			v, _ = json.Marshal(values[0])
		}
		params[name] = v
	}

	// Assembled in a fixed order, the model first as the OpenAI SDK sends it,
	// so the vendor reads the same form whatever order the client used.
	var out bytes.Buffer
	w := multipart.NewWriter(&out)
	names := []string{"model", "language", "prompt", "response_format", "temperature",
		"timestamp_granularities[]", "keyterms[]", "diarize", "provider", "source_url", "stream"}
	for _, name := range names {
		for _, value := range fields[name] {
			if w.WriteField(name, value) != nil {
				return invalid(name)
			}
		}
	}
	if file != nil {
		h := textproto.MIMEHeader{}
		h.Set("Content-Disposition", `form-data; name="file"; filename=`+strconv.Quote(cleanFilename(file.filename)))
		ct := "application/octet-stream"
		if mediaType, ctParams, err := mime.ParseMediaType(file.contentType); err == nil {
			if formatted := mime.FormatMediaType(mediaType, ctParams); formatted != "" {
				ct = formatted
			}
		}
		h.Set("Content-Type", ct)
		fw, err := w.CreatePart(h)
		if err != nil {
			return invalid("file")
		}
		if _, err := fw.Write(file.data); err != nil {
			return invalid("file")
		}
	}
	if w.Close() != nil {
		return invalid("file")
	}
	return params, out.Bytes(), w.FormDataContentType(), nil
}

// cleanFilename keeps a file name to printable characters a header can carry.
// The vendor reads the extension to tell the format; nothing else in the name
// matters, and it is the client's text on its way into a header.
func cleanFilename(name string) string {
	name = strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f || r == '"' || r == '\\' {
			return -1
		}
		return r
	}, name)
	if len(name) > 255 {
		name = name[len(name)-255:]
	}
	if name == "" {
		return "audio"
	}
	return name
}

// transcriptionFormats are the answer formats the vendor produces.
var transcriptionFormats = map[string]bool{"json": true, "verbose_json": true}

// refuseOpenAITranscriptionFields refuses the OpenAI transcription fields the
// vendor cannot honour, on either request shape, for the reason
// refuseOpenAIImageFields gives:
//
//   - response_format other than json or verbose_json: the vendor answers
//     only those; text, srt, vtt and diarized_json are refused by it with a
//     message a student cannot act on.
//   - chunking_strategy and include: not in the vendor's request, so they
//     would be dropped and the answer would lack what was asked for.
//
// A stream is refused by the check every passthrough route runs.
func refuseOpenAITranscriptionFields(params map[string]json.RawMessage) *apiError {
	present := func(name string) (json.RawMessage, bool) {
		raw, ok := params[name]
		if !ok || string(bytes.TrimSpace(raw)) == "null" {
			return nil, false
		}
		return raw, true
	}
	if raw, ok := present("response_format"); ok {
		var format string
		if json.Unmarshal(raw, &format) != nil {
			e := errInvalidParamValue("response_format")
			return &e
		}
		if !transcriptionFormats[format] {
			e := errUnsupportedValue("response_format",
				"이 경로는 json과 verbose_json만 돌려줍니다. response_format을 빼거나 json, verbose_json으로 보내주세요. 자막 형식은 verbose_json의 segments로 만들 수 있습니다.")
			return &e
		}
	}
	for _, name := range []string{"chunking_strategy", "include"} {
		if _, ok := present(name); ok {
			e := errUnsupportedParam(name)
			return &e
		}
	}
	return nil
}

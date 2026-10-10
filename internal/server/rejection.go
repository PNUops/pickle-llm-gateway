package server

import (
	"encoding/json"
	"path"
	"regexp"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

// An upstream 400 or 422 is the request's own fault, and the reason is the one
// thing the caller needs to fix it: a prompt over the context window looks
// exactly like a malformed parameter until somebody says which it was. So the
// refusal carries the upstream's message rather than a fixed sentence.
//
// Only the message, and only after it is cleaned. The rest of the body is the
// vendor's: OpenRouter nests the provider's raw answer and the provider's name
// under error.metadata, and neither is something this service relays. The
// message itself goes back to the same caller whose request produced it, so a
// quoted prompt is their own text; it still never reaches the log or the spool
// (upstreamReason keeps only the type and code there).

// rejectionReadLimit is how much of an error body is read. A provider quoting
// the offending input back can run well past a few kilobytes, and a truncated
// body no longer parses, which would lose the reason exactly when the input
// was large.
const rejectionReadLimit = 32 << 10

// rejectionMessageMax bounds the relayed reason. Context-window messages are a
// sentence or two; anything longer is quoting input back, and a refusal is no
// place to reflect a kilobyte of it.
const rejectionMessageMax = 400

var (
	rejectionURL = regexp.MustCompile(`(?i)\b[a-z][a-z0-9+.-]*://\S+`)
	// Addresses a server could name about itself: IPv4 with an optional port
	// (judged by octet below so a version string survives), IPv6 in either
	// compressed or full form, a host name with a port, and an absolute path
	// such as a served model's directory.
	rejectionIPv4     = regexp.MustCompile(`\b(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})(:\d+)?\b`)
	rejectionIPv6     = regexp.MustCompile(`(?i)\[?[0-9a-f]{0,4}(?::[0-9a-f]{0,4}){2,7}\]?(?::\d+)?`)
	rejectionHostPort = regexp.MustCompile(`(?i)\b[a-z0-9-]+(?:\.[a-z0-9-]+)*:\d{2,5}\b`)
	rejectionPath     = regexp.MustCompile(`(^|[\s'"(=,\[])/[^\s'"),\]]+`)
	rejectionEmail    = regexp.MustCompile(`[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)+`)
	// Credentials: bearer values, vendor key prefixes, and any long unbroken
	// token, which is what a key, a hash or a request id looks like. Model
	// names and ordinary words never reach 32 characters without punctuation
	// that breaks the run.
	rejectionBearer = regexp.MustCompile(`(?i)\bbearer\s+\S+`)
	rejectionKey    = regexp.MustCompile(`(?i)\b(?:sk|pk|rk)-[a-z0-9_-]{6,}`)
	rejectionToken  = regexp.MustCompile(`[A-Za-z0-9_+/=-]{32,}`)
)

// rejectionMessage extracts the message from an upstream error body, still
// raw. It returns "" when the body carries no message.
//
// Two shapes are read: the OpenAI envelope {"error": {"message": ...}}, which
// OpenRouter and current vLLM both use, and the older vLLM object
// {"object": "error", "message": ...}.
func rejectionMessage(detail []byte) string {
	var body struct {
		Error   json.RawMessage `json:"error"`
		Message string          `json:"message"`
	}
	if json.Unmarshal(detail, &body) != nil {
		return ""
	}
	msg := body.Message
	if len(body.Error) > 0 {
		var nested struct {
			Message string `json:"message"`
		}
		if json.Unmarshal(body.Error, &nested) == nil {
			msg = nested.Message
		}
	}
	return msg
}

// cleanRejection makes a raw message safe to relay. The order matters: the
// upstream model id is swapped first, while it is still whole, and the cut
// comes last, so neither redaction nor truncation can leave a fragment of the
// id that no longer matches.
func cleanRejection(msg, upstreamModel, publicModel string) string {
	msg = strings.ToValidUTF8(msg, "")
	msg = hideModelName(msg, upstreamModel, publicModel)
	msg = strings.Map(func(r rune) rune {
		switch {
		case unicode.IsControl(r) || unicode.IsSpace(r):
			return ' '
		case unicode.Is(unicode.Cf, r):
			// Format characters (bidi overrides, zero-width joiners) can make
			// the relayed text display as something it does not say.
			return -1
		}
		return r
	}, msg)
	msg = rejectionURL.ReplaceAllString(msg, "[URL]")
	msg = rejectionEmail.ReplaceAllString(msg, "[address]")
	msg = rejectionBearer.ReplaceAllString(msg, "[redacted]")
	msg = rejectionKey.ReplaceAllString(msg, "[redacted]")
	msg = rejectionPath.ReplaceAllString(msg, "${1}[path]")
	msg = rejectionIPv4.ReplaceAllStringFunc(msg, func(m string) string {
		parts := rejectionIPv4.FindStringSubmatch(m)
		for _, octet := range parts[1:5] {
			if n, err := strconv.Atoi(octet); err != nil || n > 255 {
				return m
			}
		}
		return "[address]"
	})
	msg = rejectionIPv6.ReplaceAllStringFunc(msg, func(m string) string {
		// Three colons, or a "::", is an address; two is a clock time.
		if strings.Count(m, ":") >= 3 || strings.Contains(m, "::") {
			return "[address]"
		}
		return m
	})
	msg = rejectionHostPort.ReplaceAllStringFunc(msg, func(m string) string {
		// A host has a letter in it; "1.5:30" is a number and a ratio.
		if strings.IndexFunc(m[:strings.LastIndex(m, ":")], unicode.IsLetter) < 0 {
			return m
		}
		return "[address]"
	})
	msg = rejectionToken.ReplaceAllString(msg, "[redacted]")
	msg = strings.Join(strings.Fields(msg), " ")
	if utf8.RuneCountInString(msg) > rejectionMessageMax {
		runes := []rune(msg)
		msg = string(runes[:rejectionMessageMax]) + "…"
	}
	return msg
}

// upstreamRejection is the refusal for an upstream 400 or 422. The public
// code stays upstream_rejected whatever the upstream called it: clients and
// the usage record already key on it, and the upstream's own type is not a
// stable vocabulary (vLLM sends Python exception names, OpenRouter a number).
// The status is the upstream's, because 400 and 422 are both the caller's
// fault and an SDK maps each to its own exception.
//
// The fixed sentence stays in front. OpenRouter answers many provider-side
// refusals with only "Provider returned error" in the message, and that alone
// would read as less help than the sentence it replaced.
func upstreamRejection(status int, detail []byte, upstreamModel, publicModel string) apiError {
	e := errUpstreamRejected
	e.status = status
	if msg := cleanRejection(rejectionMessage(detail), upstreamModel, publicModel); msg != "" {
		e.message += " 모델 서버 메시지: " + msg
	}
	return e
}

// hideModelName replaces the upstream model id with the public name. The
// public name exists to hide that id, and a self-hosted server names its
// model in exactly the messages that matter most ("This model's maximum
// context length is ...", or vLLM echoing the request body it validated).
// The match ignores case, and the id's last path segment is hidden too: a
// server that names its model by basename ("No endpoints found for X") would
// otherwise pass it straight through. A short basename is left alone, since
// it is as likely to be an ordinary word.
func hideModelName(msg, upstreamModel, publicModel string) string {
	if upstreamModel == "" || strings.EqualFold(upstreamModel, publicModel) {
		return msg
	}
	msg = regexp.MustCompile("(?i)"+regexp.QuoteMeta(upstreamModel)).ReplaceAllLiteralString(msg, publicModel)
	if base := path.Base(upstreamModel); base != upstreamModel && len(base) >= 8 &&
		!strings.EqualFold(base, publicModel) {
		msg = regexp.MustCompile("(?i)"+regexp.QuoteMeta(base)).ReplaceAllLiteralString(msg, publicModel)
	}
	return msg
}

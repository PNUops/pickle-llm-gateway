package server

import (
	"net/http"
	"regexp"
	"strconv"

	"github.com/pnuops/pickle-llm-gateway/internal/snapshot"
)

// chatHeaderAllowlist is which client headers reach the vendor on a money-axis
// chat request. Each one is a vendor feature for chat that changes only this
// request, paid for by the student's own vendor key; forwarding none of them
// was the gateway refusing something the vendor accepts.
//
//   - x-anthropic-beta: Anthropic beta opt-ins, such as strict tool use and
//     interleaved thinking, which the vendor otherwise strips.
//   - x-session-id: sticky provider routing for prompt caching. The body's
//     session_id already passes. Stickiness is tracked per account, so two
//     students choosing the same value share a provider choice and nothing
//     else.
//   - X-OpenRouter-Metadata: routing metadata in the response body, the
//     provider, the fallback order and BYOK use for this request. The same
//     object carries the organisation's guardrail verdicts, so it shows the
//     caller whatever guardrails the organisation runs. Opened when every
//     organisation was found to run none (2026-10-11); adding a guardrail is
//     the moment to decide again. The object is relayed as the vendor wrote
//     it, so for a catalogued money-axis alias it names the upstream model id
//     the public name otherwise stands in for; on the student's own paid
//     request that is accepted.
//
// Two groups are deliberately absent, each for a reason of its own.
//
//   - App attribution (HTTP-Referer, X-OpenRouter-Title, X-Title,
//     X-OpenRouter-Categories). The vendor publishes attributed traffic: its
//     rankings, each model's Apps tab, a public page per app and an
//     unauthenticated dataset show the app's requests, tokens and model mix.
//     An app is keyed by the referer URL rather than by account, so these
//     headers would let one student put the platform account's traffic on a
//     public page under any URL, somebody else's included. Hiding an app works
//     only when it is first created.
//   - Response caching (X-OpenRouter-Cache, -Cache-TTL, -Cache-Clear). A cache
//     hit is billed nothing and reports zero tokens while looking like a
//     normal success; only a response header tells it apart, and the usage
//     event has no field for it yet. Opening caching before the event can say
//     "this was a hit" would make every hit read as a free request nobody can
//     explain.
var chatHeaderAllowlist = []string{
	"X-Anthropic-Beta",
	"X-Session-Id",
	"X-OpenRouter-Metadata",
}

// forwardedHeaderValueMax bounds one forwarded value. Every header above
// carries a short token, a short list or a URL; anything longer is not a value
// these features take, and is dropped rather than cut, because a cut value is
// a different value.
const forwardedHeaderValueMax = 1024

// chatForwardedHeaders copies the allowlisted client headers, the first value
// of each only. Each takes one value (a comma-separated list for the beta
// opt-ins), so repeated lines could only pile bytes onto the vendor request.
func chatForwardedHeaders(h http.Header) http.Header {
	var out http.Header
	for _, name := range chatHeaderAllowlist {
		v := h.Get(name)
		if v == "" || len(v) > forwardedHeaderValueMax {
			continue
		}
		if out == nil {
			out = http.Header{}
		}
		out.Set(name, v)
	}
	return out
}

// Response headers. The vendor documents few, and which of them reach the
// student was chosen one by one against two questions: is it about this
// request alone, and does it say anything about the account the student's
// vendor key sits under.
//
//   - X-Generation-Id, on a successful money-axis answer: the vendor's id for
//     this one generation, the handle a support request or a usage question
//     can name. It is opaque, and looking it up needs an account key the
//     student does not hold.
//   - Retry-After, on a throttled (429) or unavailable (503) money-axis
//     answer: how long the vendor asked to wait, so an SDK backs off on the
//     vendor's schedule rather than guessing. With a fallback tried, the
//     longest wait any upstream asked for.
//
// Not relayed: X-RateLimit-*, which the vendor sends on its own 429s and
// which describe the account-wide platform limit rather than the student's
// key; Retry-After on a 402, which the vendor sends only when the account's
// in-flight budget is full; the response-cache headers, while caching stays
// closed and because Cache-Source-Id names another request's generation; and
// everything else, since nothing else is documented as meant for the caller.
const generationIDHeader = "X-Generation-Id"

// generationIDPattern accepts the vendor's id shape and nothing that could
// carry anything else.
var generationIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$`)

// relayGenerationID copies the vendor's generation id onto the response.
func relayGenerationID(w http.ResponseWriter, upstream http.Header) {
	if id := upstream.Get(generationIDHeader); generationIDPattern.MatchString(id) {
		w.Header().Set(generationIDHeader, id)
	}
}

// retryAfterMaxSeconds bounds a relayed wait. A longer one is not a backoff a
// client should sit through on our say-so.
const retryAfterMaxSeconds = 3600

// upstreamRetryAfter reads a vendor Retry-After given in seconds. The date
// form is not relayed: the vendor documents seconds.
func upstreamRetryAfter(h http.Header) string {
	v := h.Get("Retry-After")
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 || n > retryAfterMaxSeconds {
		return ""
	}
	return strconv.Itoa(n)
}

// relayRetryAfter sets the vendor's wait on a money-axis refusal. A gateway
// value already set (the per-key limits on the self-hosted side) is left
// alone, and so is a self-hosted upstream's.
func relayRetryAfter(w http.ResponseWriter, model *snapshot.Model, ae *attemptError) {
	if model.CreditAxis() && ae.retryAfter != "" && w.Header().Get("Retry-After") == "" {
		w.Header().Set("Retry-After", ae.retryAfter)
	}
}

// longerWait reports whether seconds a is a longer wait than b; an empty value
// is no wait at all.
func longerWait(a, b string) bool {
	na, errA := strconv.Atoi(a)
	if errA != nil {
		return false
	}
	nb, errB := strconv.Atoi(b)
	return errB != nil || na > nb
}

package server

import (
	"encoding/json"
	"net/http"

	"github.com/pnuops/pickle-llm-gateway/internal/config"
	"github.com/pnuops/pickle-llm-gateway/internal/snapshot"
	"github.com/pnuops/pickle-llm-gateway/internal/spool"
)

// protocol is what one client-facing surface changes about the shared
// pipeline. Everything not named here is the pipeline's and runs the same way
// on every surface, in the same order (see pipeline.go).
//
// A protocol is a value built once at package level. It must not capture a
// client, a logger or anything else off the Server: those are read at request
// time, so a test that swaps s.client still reaches every surface.
type protocol struct {
	endpoint string // usage event route name
	// capability is the key grant this surface needs, on both axes, checked
	// right after authentication.
	capability   string
	upstreamPath string // appended to the upstream base URL
	logLine      string // the request log's message

	// token reads the caller's key from the request, or returns the refusal
	// for a request that presented none. Surfaces differ in which headers a
	// client of theirs sends the key in, and the refusal names those headers.
	// nil reads Authorization: Bearer, as every OpenAI-shaped surface does.
	token func(r *http.Request) (string, *apiError)

	// limits are this surface's body bounds, so an agent surface can be given
	// its own without touching chat's.
	limits func(c *config.Config) bodyLimits

	// parse reads the fields the pipeline needs and refuses a malformed body.
	// The stream flag is stamped by the pipeline before parse runs, so a
	// refusal here still records whether a stream was asked for.
	parse func(params map[string]json.RawMessage) (parsedRequest, *refusal)
	// creditOnly, when set, is the refusal every self-hosted model gets on
	// this surface: the surface serves the money axis alone. nil serves both.
	// It is judged once the model has resolved and before tokenAxisParam,
	// which a surface closed to the self-hosted axis does not need to define.
	creditOnly *apiError
	// tokenAxisParam judges one top-level field on the self-hosted axis; nil
	// means the field may be forwarded.
	tokenAxisParam func(name string) *refusal
	// fences run in order on the money axis, after the model fence and before
	// the credential check. There is no default: a surface whose body has a
	// different shape needs fences that read that shape, and an inherited
	// list would read nothing and pass everything.
	fences []fence
	// requestHeaders picks the client headers forwarded on the money axis.
	requestHeaders func(h http.Header) http.Header
	// capReadFields are read, validated and removed; the smallest wins.
	// capWriteField receives the cap, or "" to use the upstream's CapField.
	capReadFields []string
	capWriteField string
	// shape renames the model and adds what metering needs.
	shape func(params map[string]json.RawMessage, req parsedRequest, model *snapshot.Model, streaming bool)
	// capture renders the request half of a body record from the parsed input.
	// nil turns body capture off on this surface: no record is made at all,
	// whatever the key opted into, so a surface whose record format nobody
	// has designed yet sends nothing rather than an empty record.
	capture func(input json.RawMessage) (json.RawMessage, bool)
	// usageRequired refuses to settle an answer without the upstream's usage.
	// A non-streamed answer that carries none is refused rather than metered
	// by estimate, and a stream that ends cleanly without one is recorded as
	// failed with no token counts. An estimate here would be a wrong number
	// that nothing marks as wrong once it is summed.
	usageRequired bool

	// readResponse reads a decoded non-streamed answer: it puts the public
	// name in place of the served one and pulls out what metering needs.
	readResponse func(m map[string]any, publicName string) responseResult
	newStream    func(s *Server, a streamArgs, ev *spool.Event) streamExtractor
	// writeError renders a refusal, with the Retry-After its code carries.
	// Every refusal on the surface goes through it, the ones inside the
	// response paths too; headers the pipeline set before the refusal stay.
	writeError func(w http.ResponseWriter, e apiError, requestID string)
}

// bodyLimits are one surface's request and response bounds.
type bodyLimits struct {
	// request is the ordinary request bound; largeRequest is reachable only
	// with a large-body slot, and largeResponse likewise for a non-streamed
	// answer. A zero large bound means the surface has none.
	request, largeRequest, largeResponse int64
}

// parsedRequest is what parse hands the pipeline.
type parsedRequest struct {
	publicModel string
	// fromCandidates marks a model named only through a fallback list.
	fromCandidates bool
	// input is the part of the body the size guard and body capture read. It
	// points into the request body, so the pipeline lets go of it with the
	// body (see the large-body slot).
	input      json.RawMessage
	wantsUsage bool
}

// refusal is an answer to the caller plus the name the usage record gets.
// errorType empty means the public code.
type refusal struct {
	e         apiError
	errorType string
}

// fence is one money-axis check over the request body. It returns nil to
// let the request on. The model is passed because a check may depend on what
// the model accepts, though none of chat's does yet.
type fence func(s *Server, doc *snapshot.Document, lookup func(string) *snapshot.Model,
	key *snapshot.Key, model *snapshot.Model, params map[string]json.RawMessage) *refusal

// responseResult is what a non-streamed answer yields for metering.
type responseResult struct {
	servedModel string // "" when the answer named none
	usage       usage
	haveUsage   bool
	answer      string // for the no-usage estimate and capture
	// upstreamError is the vendor's own error inside a 200 answer. The answer
	// is still relayed as it came, like the same error inside a stream, but
	// the request is not recorded as a success.
	upstreamError bool
}

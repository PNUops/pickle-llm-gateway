// Error responses in the OpenAI envelope: {"error": {message, type, code}}.
// Messages are Korean because students read them; type and code are stable
// machine-readable identifiers. Nothing here ever carries an upstream
// address, an upstream error body, or a credential. The one upstream text that
// does reach a caller is the cleaned message of a 400 or 422, never the body
// around it; rejection.go says what is removed and why.
package server

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
)

type apiError struct {
	status  int
	typ     string
	code    string
	message string
}

var (
	errMissingKey = apiError{http.StatusUnauthorized, "authentication_error", "missing_api_key",
		"API Key가 없습니다. Authorization: Bearer <API Key> 헤더를 보내주세요."}
	// The same refusal for an Anthropic client, which sends its key in
	// x-api-key or as a bearer depending on how it was configured; naming only
	// one would send half of them to change the wrong setting.
	errMissingKeyMessages = apiError{http.StatusUnauthorized, "authentication_error", "missing_api_key",
		"API Key가 없습니다. x-api-key: <API Key> 또는 Authorization: Bearer <API Key> 헤더를 보내주세요."}
	errInvalidKey = apiError{http.StatusUnauthorized, "authentication_error", "invalid_api_key",
		"API Key가 올바르지 않습니다. 콘솔에서 발급한 Key인지 확인해주세요."}
	errKeyExpired = apiError{http.StatusUnauthorized, "authentication_error", "api_key_expired",
		"API Key의 유효기간이 지났습니다. 콘솔에서 새 Key를 발급해주세요."}
	errKeyRevoked = apiError{http.StatusUnauthorized, "authentication_error", "api_key_revoked",
		"폐기된 API Key입니다. 콘솔에서 새 Key를 발급해주세요."}
	errKeySuspended = apiError{http.StatusForbidden, "permission_error", "account_suspended",
		"이용이 정지된 상태입니다. 관리자에게 문의해주세요."}
	errQuotaExhausted = apiError{http.StatusTooManyRequests, "rate_limit_error", "quota_exhausted",
		"사용량 한도를 모두 사용했습니다. 한도는 기간이 지나면 초기화되며, 증액이 필요하면 콘솔에서 신청해주세요."}
	errCreditUnavailable = apiError{http.StatusForbidden, "permission_error", "credit_unavailable",
		"이 API Key에는 유료 모델을 쓸 금액 한도가 없습니다. 콘솔에서 한도를 신청해주세요."}
	// 503 rather than 403 on purpose: nothing about the request is wrong and
	// the state ends on its own, so the honest answer is "not yet", which is
	// also the one OpenAI-compatible clients retry without being told. The
	// budget was granted; only its upstream key is still being created.
	errCreditPending = apiError{http.StatusServiceUnavailable, "server_error", "credit_pending",
		"승인된 금액 한도를 이 API Key에 적용하는 중입니다. 적용이 끝나면 자동으로 사용할 수 있으니 잠시 후 다시 시도해주세요."}
	errCreditExhausted = apiError{http.StatusTooManyRequests, "rate_limit_error", "credit_exhausted",
		"유료 모델에 부여된 금액 한도를 모두 사용했습니다. 증액이 필요하면 콘솔에서 신청해주세요."}
	errServiceDisabled = apiError{http.StatusServiceUnavailable, "server_error", "service_disabled",
		"LLM API 서비스가 점검 중입니다. 잠시 후 다시 시도해주세요."}
	// A self-hosted model on a route that serves paid models only. 400 rather
	// than 404: the model exists and is served elsewhere, and the caller can
	// act on that by naming a paid model or using the chat route.
	errModelNotOnMessages = apiError{http.StatusBadRequest, "invalid_request_error", "model_not_available_on_route",
		"자체 서빙 모델은 아직 /v1/messages에서 쓸 수 없습니다. 유료 모델 이름을 지정하거나 /v1/chat/completions를 사용해주세요."}
	errModelNotFound = apiError{http.StatusNotFound, "invalid_request_error", "model_not_found",
		"요청한 모델이 없습니다. GET /v1/models로 이용 가능한 모델을 확인해주세요."}
	// Two fences answer with this code, and they need different advice, so the
	// message differs while the code does not — a second public code would be
	// one more string every SDK and demo has to learn for a fact they already
	// handle. This one is the catalogue fence: those models ARE listed by
	// /v1/models, so the original advice is right here.
	errModelNotAllowed = apiError{http.StatusForbidden, "permission_error", "model_not_allowed",
		"이 API Key로는 요청한 모델을 사용할 수 없습니다. GET /v1/models로 이용 가능한 모델을 확인해주세요."}
	// The money fence. A commercial model reached through passthrough is never
	// in /v1/models, so pointing there would send the caller to a list that
	// cannot contain the answer; the console shows the key's own allow list.
	errCreditModelNotAllowed = apiError{http.StatusForbidden, "permission_error",
		"model_not_allowed",
		"이 API Key로는 요청한 유료 모델을 사용할 수 없습니다. 콘솔의 키 상세에서 허용된 모델을 확인해주세요."}
	errRateRequests = apiError{http.StatusTooManyRequests, "rate_limit_error", "rate_limit_requests",
		"자체 서빙 모델의 분당 요청 횟수 한도를 초과했습니다. 잠시 후 다시 시도해주세요."}
	errRateTokens = apiError{http.StatusTooManyRequests, "rate_limit_error", "rate_limit_tokens",
		"자체 서빙 모델의 분당 토큰 사용량 한도를 초과했습니다. 잠시 후 다시 시도해주세요."}
	errRateConcurrency = apiError{http.StatusTooManyRequests, "rate_limit_error", "rate_limit_concurrency",
		"자체 서빙 모델의 동시 요청 수 한도를 초과했습니다. 진행 중인 요청이 끝난 뒤 다시 시도해주세요."}
	// A body above the ordinary bound when every large-body slot of its
	// surface is taken, chat's or the passthrough's. 429 because waiting is
	// the remedy and the shortage is a limit rather than a fault.
	errLargeRequestBusy = apiError{http.StatusTooManyRequests, "rate_limit_error", "large_request_busy",
		"큰 요청(2 MiB 초과)을 처리할 자리가 지금은 없습니다. 잠시 후 다시 시도하거나, 파일과 이미지를 URL로 보내 요청을 줄여주세요."}
	// The chat response bound, said out loud: a non-streamed answer bigger than
	// this service relays, or one that found no large-body slot free.
	// Streaming helps only a long text answer, whose events are small; a
	// picture streams as one event under the same slot and bound, so the
	// remedy for an image is a smaller or fewer one.
	errChatResponseTooLarge = apiError{http.StatusBadGateway, "server_error", "upstream_response_too_large",
		"모델 서버 응답이 중계 가능한 크기를 넘었거나 지금 큰 응답을 처리할 자리가 없습니다. 긴 텍스트 응답이면 stream으로 받거나 최대 출력 길이(max_tokens)를 줄이고, 이미지 생성이면 해상도나 개수를 줄여주세요."}
	errServerBusy = apiError{http.StatusServiceUnavailable, "server_error", "server_busy",
		"요청이 많아 처리할 수 없습니다. 잠시 후 다시 시도해주세요."}
	errRequestTooLarge = apiError{http.StatusBadRequest, "invalid_request_error", "request_too_large",
		"요청 본문이 허용 크기를 초과했습니다."}
	errInputTooLong = apiError{http.StatusBadRequest, "invalid_request_error", "input_too_long",
		"입력이 모델의 최대 입력 길이를 초과했습니다."}
	// The upload stopped before the declared body arrived. Distinct from a
	// malformed body: nothing the caller sent was wrong, it was cut off.
	errRequestBodyUnreadable = apiError{http.StatusBadRequest, "invalid_request_error", "request_body_incomplete",
		"요청 본문을 끝까지 받지 못했습니다. 다시 보내주세요."}
	errBadJSON = apiError{http.StatusBadRequest, "invalid_request_error", "invalid_json",
		"요청 본문이 올바른 JSON이 아닙니다."}
	errUpstreamRejected = apiError{http.StatusBadRequest, "invalid_request_error", "upstream_rejected",
		"모델 서버가 요청을 거부했습니다. 요청 파라미터를 확인해주세요."}
	errUpstream = apiError{http.StatusBadGateway, "server_error", "upstream_error",
		"모델 서버 호출에 실패했습니다. 잠시 후 다시 시도해주세요."}
	errUpstreamTimeout = apiError{http.StatusGatewayTimeout, "server_error", "upstream_timeout",
		"모델 서버 응답이 제한 시간을 초과했습니다. 잠시 후 다시 시도해주세요."}
	// The response cap, said out loud. io.LimitReader truncates silently, so
	// without this the caller gets "the upstream sent something unparseable" —
	// a server fault they cannot act on — for a response that was simply
	// bigger than this service relays, or that found no large-body slot free.
	// The remedy is theirs, so it is named.
	errPassthroughResponseTooLarge = apiError{http.StatusBadGateway, "server_error",
		"upstream_response_too_large",
		"모델 서버 응답이 중계 가능한 크기를 넘었거나 지금 큰 응답을 처리할 자리가 없습니다. 한 번에 만드는 개수(n)나 요청한 크기를 줄여주세요."}
	errNotFound = apiError{http.StatusNotFound, "invalid_request_error", "unknown_endpoint",
		"지원하지 않는 경로입니다. 지원 범위는 GET /v1/models, POST /v1/chat/completions, POST /v1/messages, " +
			"POST /v1/images, POST /v1/images/generations, GET /v1/images/models, POST /v1/embeddings입니다."}
	errMethod = apiError{http.StatusMethodNotAllowed, "invalid_request_error", "method_not_allowed",
		"지원하지 않는 HTTP 메서드입니다."}
	errPassthroughNoStreaming = apiError{http.StatusBadRequest, "invalid_request_error",
		"streaming_not_supported",
		"이 경로는 스트리밍을 지원하지 않습니다. stream을 빼고 요청해주세요."}
)

// retryAfterSeconds is the Retry-After each error carries, keyed by the code
// above. Only a refusal that ends without anyone acting belongs here: waiting
// has to be the actual remedy, or the header sends a client back into a wall
// on a schedule. Everything absent sends no header, which is the honest
// answer for a refusal a person has to resolve.
//
// It lives beside the catalogue rather than inside apiError because the codes
// are already the stable machine identifiers and every other error would have
// to carry a zero it never uses.
var retryAfterSeconds = map[string]int{
	// Provisioning is attempted the moment the budget lands and normally
	// finishes well inside this. The sweep that retries a failed attempt runs
	// on a far longer period, so this fits the common case, not the degraded
	// one — a client that keeps getting 503 is seeing a real fault.
	"credit_pending": 10,
	// A large chat request holds its slot only until the upstream starts
	// answering, so request slots come free on the scale of a first token.
	// A large answer holds one until it is written, up to its write deadline
	// (largeWriteBudget), and a large passthrough request holds one through
	// its whole call (an image generation takes about half a minute), so a
	// full pool can take minutes to drain; five seconds is the retry pace,
	// not a promise that one is free by then.
	"large_request_busy": 5,
}

// Internal sentinels for upstream failure reasons. They never reach a client;
// the client sees the shaped envelope above.
var (
	errUnconfiguredUpstream = errors.New("model references an unconfigured upstream")
	errNoKeyCredential      = errors.New("key has no credential for this upstream")
	errUpstreamAuth         = errors.New("upstream rejected our credential")
	errUpstreamStatus       = errors.New("upstream returned an error status")
	errUpstreamThrottled    = errors.New("upstream is throttling this gateway")
)

// The same public code as an outright unknown field, because to an SDK the
// fact is identical: this request may not carry that field. What differs is
// what the caller can do about it, and that belongs in the message.
func errParamNeedsCreditModel(name string) apiError {
	return apiError{http.StatusBadRequest, "invalid_request_error", "unsupported_parameter",
		"자체 서빙 모델은 이 파라미터를 지원하지 않습니다: " + name +
			". 유료 모델에서만 사용할 수 있습니다."}
}

// A model named by one of the vendor's server tools. Same public code and same
// spool type as any other money-fence refusal — it is the same rule reaching a
// different field — but the advice has to say where the name was, because the
// caller did not put it in `model` and would not find it by looking there.
func errToolModelNotAllowed(name string) apiError {
	if name == "" || len(name) > maxPassthroughNameBytes {
		return apiError{http.StatusForbidden, "permission_error", "model_not_allowed",
			"이 API Key로는 tools가 지정한 모델을 사용할 수 없습니다. " +
				"콘솔의 키 상세에서 허용된 모델을 확인해주세요."}
	}
	return apiError{http.StatusForbidden, "permission_error", "model_not_allowed",
		"이 API Key로는 tools가 지정한 " + name + "을(를) 사용할 수 없습니다. " +
			"콘솔의 키 상세에서 허용된 모델을 확인해주세요."}
}

// A server tool left without a model on a fenced key. The vendor would pick one
// this gateway never sees, so the caller is asked to name it. Both arguments
// are constants from the fence's own table, never the request's spelling.
func errToolModelRequired(toolType, param string) apiError {
	return apiError{http.StatusForbidden, "permission_error", "model_not_allowed",
		"이 API Key에는 사용할 수 있는 모델이 정해져 있어 tools의 " + toolType +
			"에 " + param + "을(를) 직접 지정해야 합니다. " +
			"지정하지 않으면 요청 밖에서 모델이 정해져 허용 여부를 판단할 수 없기 때문입니다."}
}

// A model named by a `plugins` entry. Same rule and code as the tools refusal;
// the advice says where the name was for the same reason.
func errPluginModelNotAllowed(name string) apiError {
	if name == "" || len(name) > maxPassthroughNameBytes {
		return apiError{http.StatusForbidden, "permission_error", "model_not_allowed",
			"이 API Key로는 plugins가 지정한 모델을 사용할 수 없습니다. " +
				"콘솔의 키 상세에서 허용된 모델을 확인해주세요."}
	}
	return apiError{http.StatusForbidden, "permission_error", "model_not_allowed",
		"이 API Key로는 plugins가 지정한 " + name + "을(를) 사용할 수 없습니다. " +
			"콘솔의 키 상세에서 허용된 모델을 확인해주세요."}
}

// A router plugin on a fenced key. Like the router name, the remedy is to name
// a model. The id echoed is the canonical one, not the caller's spelling.
func errRouterPluginNotAllowed(id string) apiError {
	return apiError{http.StatusForbidden, "permission_error", "model_not_allowed",
		"이 API Key에는 사용할 수 있는 모델이 정해져 있어 plugins의 " + id + " 설정을 쓸 수 없습니다. " +
			"어떤 모델이 답할지 미리 알 수 없어 허용 여부를 판단할 수 없기 때문입니다. " +
			"모델 이름을 직접 지정해주세요."}
}

// The fusion plugin's preset or an empty panel on a fenced key: either leaves
// the panel to the vendor's side, out of the fence's sight.
var errFusionPluginNeedsModels = apiError{http.StatusForbidden, "permission_error", "model_not_allowed",
	"이 API Key에는 사용할 수 있는 모델이 정해져 있어 plugins의 fusion에 preset이나 빈 analysis_models를 쓸 수 없습니다. " +
		"model과 analysis_models를 직접 지정해주세요."}

// toolRefusalType names the tools refusal in the usage record. It reuses the
// existing types rather than adding one: the contract already enumerates these,
// and a refusal here is the same event as the same refusal one field over.
func toolRefusalType(e apiError) string {
	switch e.code {
	case "endpoint_not_allowed":
		return "endpoint_not_allowed"
	case "model_not_allowed":
		return "credit_model_not_allowed"
	default:
		return e.code
	}
}

// A preset. It stands in for the model and its fallback list, so it reaches
// past the money fence from outside the fields the fence reads, and it is
// refused for every key because presets live on the platform's own vendor
// account rather than the caller's.
var errPresetNotAllowed = apiError{http.StatusBadRequest, "invalid_request_error",
	"preset_not_supported",
	"이 게이트웨이는 preset을 지원하지 않습니다. 모델 이름을 직접 지정해주세요."}

// A router name on a key that carries a model fence. The generic refusal is
// wrong here: the caller did not ask for a model somebody excluded, they asked
// for a name that picks one later, and the remedy is to name a model rather
// than to go and read a list.
func errRouterModelNotAllowed(name string) apiError {
	return apiError{http.StatusForbidden, "permission_error", "model_not_allowed",
		"이 API Key에는 사용할 수 있는 모델이 정해져 있어 자동 선택 모델(" + name + ")을 쓸 수 없습니다. " +
			"어떤 모델이 답할지 미리 알 수 없어 허용 여부를 판단할 수 없기 때문입니다. " +
			"모델 이름을 직접 지정해주세요."}
}

// A fallback candidate the key may not be billed for. It names the entry
// because the caller sent a list and cannot otherwise tell which one is the
// problem, and the name is echoed back only when it is short enough to be a
// real model name — the field is client input, and a refusal is no place to
// reflect a kilobyte of it.
func errCandidateModelNotAllowed(name string) apiError {
	if name == "" || len(name) > maxPassthroughNameBytes {
		return apiError{http.StatusForbidden, "permission_error", "model_not_allowed",
			"이 API Key로는 대체 모델 목록(models)에 적힌 모델을 사용할 수 없습니다. " +
				"콘솔의 키 상세에서 허용된 모델을 확인해주세요."}
	}
	return apiError{http.StatusForbidden, "permission_error", "model_not_allowed",
		"이 API Key로는 대체 모델 목록(models)의 " + name + "을(를) 사용할 수 없습니다. " +
			"콘솔의 키 상세에서 허용된 모델을 확인해주세요."}
}

// The same refusal for the Messages surface's own fallback list, named the
// way the caller wrote it.
func errFallbackModelNotAllowed(name string) apiError {
	if name == "" || len(name) > maxPassthroughNameBytes {
		return apiError{http.StatusForbidden, "permission_error", "model_not_allowed",
			"이 API Key로는 대체 모델 목록(fallbacks)에 적힌 모델을 사용할 수 없습니다. " +
				"콘솔의 키 상세에서 허용된 모델을 확인해주세요."}
	}
	return apiError{http.StatusForbidden, "permission_error", "model_not_allowed",
		"이 API Key로는 대체 모델 목록(fallbacks)의 " + name + "을(를) 사용할 수 없습니다. " +
			"콘솔의 키 상세에서 허용된 모델을 확인해주세요."}
}

// errOutputTooLong names the field and the model's cap, which is the only
// thing a caller needs to fix the request: an agent client that sends its own
// default output length gets this on every request until the value is lowered.
func errOutputTooLong(field string, max int) apiError {
	return apiError{http.StatusBadRequest, "invalid_request_error", "output_limit_exceeded",
		"요청한 최대 출력 길이가 모델의 허용치를 초과했습니다. " + field + " 값을 " +
			strconv.Itoa(max) + " 이하로 지정해주세요."}
}

func errUnsupportedParam(name string) apiError {
	return apiError{http.StatusBadRequest, "invalid_request_error", "unsupported_parameter",
		"지원하지 않는 파라미터입니다: " + name + ". 지원 파라미터는 이용 안내 문서를 확인해주세요."}
}

// A field this route recognises but cannot honour, with the advice that fits
// it. Same public code as an unknown field: to an SDK the fact is the same.
func errUnsupportedValue(name, advice string) apiError {
	return apiError{http.StatusBadRequest, "invalid_request_error", "unsupported_parameter",
		"지원하지 않는 파라미터입니다: " + name + ". " + advice}
}

func errInvalidParamValue(name string) apiError {
	return apiError{http.StatusBadRequest, "invalid_request_error", "invalid_parameter_value",
		"파라미터 값이 올바르지 않습니다: " + name}
}

// The endpoint fence. 403 with a code of its own rather than a 404,
// deliberately: the passthrough paths are public documentation, so hiding
// their existence would buy nothing and would leave a student unable to tell
// "my key was not granted this" from "this service has no such path" — two
// states with completely different next actions.
//
// It names the capability rather than the path, because the grant is a
// capability and that is the word an approver's screen shows; a student
// quoting this refusal is then asking for the thing that can actually be
// given. And it says how to get it. This is the one fence on the surface a
// student cannot work around on their own — a refused model can be swapped
// for another, a refused capability only an approver can grant — so a message
// that only said no would leave them with nowhere to go.
func errEndpointNotAllowed(label string) apiError {
	return apiError{http.StatusForbidden, "permission_error", "endpoint_not_allowed",
		"이 API Key에는 " + label + " 기능이 허용되어 있지 않습니다. " +
			"콘솔에서 이 기능을 신청하면 승인 후 사용할 수 있습니다."}
}

func errMissingParam(name string) apiError {
	return apiError{http.StatusBadRequest, "invalid_request_error", "missing_parameter",
		"필수 파라미터가 없습니다: " + name}
}

func writeAPIError(w http.ResponseWriter, e apiError) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	if seconds := retryAfterSeconds[e.code]; seconds > 0 {
		w.Header().Set("Retry-After", strconv.Itoa(seconds))
	}
	w.WriteHeader(e.status)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]any{
			"message": e.message,
			"type":    e.typ,
			"code":    e.code,
		},
	})
}

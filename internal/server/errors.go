// Error responses in the OpenAI envelope: {"error": {message, type, code}}.
// Messages are Korean because students read them; type and code are stable
// machine-readable identifiers. Nothing here ever carries an upstream
// address, an upstream error body, or a credential.
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
	// The endpoint fence. 403 with a name of its own rather than a 404,
	// deliberately: the passthrough paths are public documentation, so hiding
	// their existence would buy nothing and would leave a student unable to
	// tell "my key was not granted this" from "this service has no such path"
	// — two states with completely different next actions.
	errEndpointNotAllowed = apiError{http.StatusForbidden, "permission_error", "endpoint_not_allowed",
		"이 API Key로는 이 경로를 사용할 수 없습니다. 콘솔의 키 상세에서 허용된 경로를 확인해주세요."}
	errRateRequests = apiError{http.StatusTooManyRequests, "rate_limit_error", "rate_limit_requests",
		"자체 서빙 모델의 분당 요청 횟수 한도를 초과했습니다. 잠시 후 다시 시도해주세요."}
	errRateTokens = apiError{http.StatusTooManyRequests, "rate_limit_error", "rate_limit_tokens",
		"자체 서빙 모델의 분당 토큰 사용량 한도를 초과했습니다. 잠시 후 다시 시도해주세요."}
	errRateConcurrency = apiError{http.StatusTooManyRequests, "rate_limit_error", "rate_limit_concurrency",
		"자체 서빙 모델의 동시 요청 수 한도를 초과했습니다. 진행 중인 요청이 끝난 뒤 다시 시도해주세요."}
	errServerBusy = apiError{http.StatusServiceUnavailable, "server_error", "server_busy",
		"요청이 많아 처리할 수 없습니다. 잠시 후 다시 시도해주세요."}
	errRequestTooLarge = apiError{http.StatusBadRequest, "invalid_request_error", "request_too_large",
		"요청 본문이 허용 크기를 초과했습니다."}
	errInputTooLong = apiError{http.StatusBadRequest, "invalid_request_error", "input_too_long",
		"입력이 모델의 최대 입력 길이를 초과했습니다."}
	errOutputTooLong = apiError{http.StatusBadRequest, "invalid_request_error", "output_limit_exceeded",
		"요청한 최대 출력 길이가 모델의 허용치를 초과했습니다."}
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
	// bigger than this service relays. The remedy is theirs, so it is named.
	errPassthroughResponseTooLarge = apiError{http.StatusBadGateway, "server_error",
		"upstream_response_too_large",
		"모델 서버 응답이 중계 가능한 크기를 넘었습니다. 한 번에 만드는 개수(n)나 요청한 크기를 줄여주세요."}
	errNotFound = apiError{http.StatusNotFound, "invalid_request_error", "unknown_endpoint",
		"지원하지 않는 경로입니다. 지원 범위는 GET /v1/models, POST /v1/chat/completions, " +
			"POST /v1/images, GET /v1/images/models, POST /v1/embeddings입니다."}
	errMethod = apiError{http.StatusMethodNotAllowed, "invalid_request_error", "method_not_allowed",
		"지원하지 않는 HTTP 메서드입니다."}
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

func errUnsupportedParam(name string) apiError {
	return apiError{http.StatusBadRequest, "invalid_request_error", "unsupported_parameter",
		"지원하지 않는 파라미터입니다: " + name + ". 지원 파라미터는 이용 안내 문서를 확인해주세요."}
}

func errInvalidParamValue(name string) apiError {
	return apiError{http.StatusBadRequest, "invalid_request_error", "invalid_parameter_value",
		"파라미터 값이 올바르지 않습니다: " + name}
}

// The passthrough surface reads only a bounded prefix of the request body, so
// a `model` sitting past that prefix is genuinely unreadable rather than
// absent. The message says which, because the fix is to move the field rather
// than to add it — a large base64 value ahead of `model` is the way to hit it.
func errPassthroughModelUnreadable(prefixBytes int) apiError {
	return apiError{http.StatusBadRequest, "invalid_request_error", "missing_parameter",
		"요청 본문 앞부분 " + strconv.Itoa(prefixBytes>>10) + " KiB 안에서 model 필드를 찾지 못했습니다. " +
			"model을 본문 앞쪽에 넣어주세요."}
}

// Refused here rather than left to the response cap. Past the cap the caller
// gets a 502 that describes a server fault, which is both wrong and unactionable;
// the limit is a fact about this service and saying it is the only useful answer.
func errPassthroughTooManyItems(max int) apiError {
	return apiError{http.StatusBadRequest, "invalid_request_error", "invalid_parameter_value",
		"한 번에 요청할 수 있는 개수(n)의 상한은 " + strconv.Itoa(max) + "입니다."}
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

// Error responses in the OpenAI envelope: {"error": {message, type, code}}.
// Messages are Korean because students read them; type and code are stable
// machine-readable identifiers. Nothing here ever carries an upstream
// address, an upstream error body, or a credential.
package server

import (
	"encoding/json"
	"net/http"
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
	errServiceDisabled = apiError{http.StatusServiceUnavailable, "service_unavailable", "service_disabled",
		"LLM API 서비스가 점검 중입니다. 잠시 후 다시 시도해주세요."}
	errModelNotFound = apiError{http.StatusNotFound, "invalid_request_error", "model_not_found",
		"요청한 모델이 없습니다. GET /v1/models로 이용 가능한 모델을 확인해주세요."}
	errModelNotAllowed = apiError{http.StatusForbidden, "permission_error", "model_not_allowed",
		"이 API Key로는 요청한 모델을 사용할 수 없습니다. GET /v1/models로 이용 가능한 모델을 확인해주세요."}
	errRateRequests = apiError{http.StatusTooManyRequests, "rate_limit_error", "rate_limit_requests",
		"분당 요청 횟수 한도를 초과했습니다. 잠시 후 다시 시도해주세요."}
	errRateTokens = apiError{http.StatusTooManyRequests, "rate_limit_error", "rate_limit_tokens",
		"분당 토큰 사용량 한도를 초과했습니다. 잠시 후 다시 시도해주세요."}
	errRateConcurrency = apiError{http.StatusTooManyRequests, "rate_limit_error", "rate_limit_concurrency",
		"동시 요청 수 한도를 초과했습니다. 진행 중인 요청이 끝난 뒤 다시 시도해주세요."}
	errServerBusy = apiError{http.StatusServiceUnavailable, "service_unavailable", "server_busy",
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
	errUpstream = apiError{http.StatusBadGateway, "upstream_error", "upstream_error",
		"모델 서버 호출에 실패했습니다. 잠시 후 다시 시도해주세요."}
	errUpstreamTimeout = apiError{http.StatusGatewayTimeout, "upstream_error", "upstream_timeout",
		"모델 서버 응답이 제한 시간을 초과했습니다. 잠시 후 다시 시도해주세요."}
	errNotFound = apiError{http.StatusNotFound, "invalid_request_error", "unknown_endpoint",
		"지원하지 않는 경로입니다. 지원 범위는 GET /v1/models, POST /v1/chat/completions입니다."}
	errMethod = apiError{http.StatusMethodNotAllowed, "invalid_request_error", "method_not_allowed",
		"지원하지 않는 HTTP 메서드입니다."}
)

func errUnsupportedParam(name string) apiError {
	return apiError{http.StatusBadRequest, "invalid_request_error", "unsupported_parameter",
		"지원하지 않는 파라미터입니다: " + name + ". 지원 파라미터는 이용 안내 문서를 확인해주세요."}
}

func errInvalidParamValue(name string) apiError {
	return apiError{http.StatusBadRequest, "invalid_request_error", "invalid_parameter_value",
		"파라미터 값이 올바르지 않습니다: " + name}
}

func errMissingParam(name string) apiError {
	return apiError{http.StatusBadRequest, "invalid_request_error", "missing_parameter",
		"필수 파라미터가 없습니다: " + name}
}

func writeAPIError(w http.ResponseWriter, e apiError) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(e.status)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]any{
			"message": e.message,
			"type":    e.typ,
			"code":    e.code,
		},
	})
}

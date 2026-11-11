package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/pnuops/pickle-llm-gateway/internal/snapshot"
	"github.com/pnuops/pickle-llm-gateway/internal/spool"
)

// The Anthropic-shaped batch route, /v1/messages/batches: what the Anthropic
// SDK's messages.batches calls. The vendor has no such route, but its own
// batch (/v1/batches here) can target /v1/messages and carries every result
// inline, so the translation keeps no state: the request's `params` become the
// vendor items' `body`, the batch id is the vendor's, and the status, counts
// and the results file are derived from the vendor's batch on every read.
//
// What does not translate is refused rather than imitated: a batch that mixes
// models (the vendor's batch has one), cancel (the vendor has none) and
// paging backwards (the vendor pages forward only). Ownership, fences and the
// price are the OpenRouter-shaped route's, so a batch made through either
// shape can be read through the other.

// anthropicCustomIDMax and the character set are the Anthropic API's rule
// for custom_id, enforced here so a request the SDK's own API would refuse is
// refused the same way rather than accepted by the vendor.
const anthropicCustomIDMax = 64

// anthropicBatchDeadline is the processing deadline the Anthropic batch
// object states, used when the vendor names none.
const anthropicBatchDeadline = 24 * time.Hour

// anthropicListMaxLimit is the Anthropic list's own bound on a page.
const anthropicListMaxLimit = 1000

var (
	errBatchMixedModels = apiError{http.StatusBadRequest, "invalid_request_error", "batch_mixed_models",
		"한 배치의 요청은 모두 같은 모델을 써야 해요. 모델마다 배치를 나눠 보내 주세요."}
	errBatchCancelUnsupported = apiError{http.StatusBadRequest, "invalid_request_error",
		"batch_cancel_unsupported",
		"배치 취소는 지원하지 않아요. 배치는 끝나거나 만료될 때까지 진행돼요."}
	errBatchBeforeUnsupported = apiError{http.StatusBadRequest, "invalid_request_error",
		"unsupported_parameter",
		"지원하지 않는 파라미터예요: before_id. 목록은 after_id로 다음 쪽만 읽을 수 있어요."}
)

func (s *Server) handleMessagesBatches(w http.ResponseWriter, r *http.Request) {
	const root = "/v1/messages/batches"
	if r.URL.Path == root {
		switch r.Method {
		case http.MethodPost:
			s.createMessagesBatch(w, r)
		case http.MethodGet:
			s.listMessagesBatches(w, r)
		default:
			writeAnthropicError(w, errMethod, "")
		}
		return
	}
	rest := strings.TrimPrefix(r.URL.Path, root+"/")
	id, sub, _ := strings.Cut(rest, "/")
	switch {
	case sub == "" && r.Method == http.MethodGet:
		s.readMessagesBatch(w, r, id, false)
	case sub == "" && r.Method == http.MethodDelete:
		s.deleteMessagesBatch(w, r, id)
	case sub == "results" && r.Method == http.MethodGet:
		s.readMessagesBatch(w, r, id, true)
	case sub == "cancel" && r.Method == http.MethodPost:
		c := s.beginAsyncAs(w, r, spool.EndpointBatchReads, snapshot.EndpointBatches, true)
		if c == nil {
			return
		}
		defer c.done()
		c.refuse(errBatchCancelUnsupported, spool.StatusBadRequest)
	case sub == "" || sub == "results" || sub == "cancel":
		writeAnthropicError(w, errMethod, "")
	default:
		writeAnthropicError(w, errNotFound, "")
	}
}

func (s *Server) createMessagesBatch(w http.ResponseWriter, r *http.Request) {
	c := s.beginAsyncAs(w, r, spool.EndpointBatches, snapshot.EndpointBatches, true)
	if c == nil {
		return
	}
	defer c.done()
	raw, ok := c.readRequest()
	if !ok {
		return
	}
	var top map[string]json.RawMessage
	if json.Unmarshal(raw, &top) != nil || top == nil {
		c.refuse(errBadJSON, spool.StatusBadRequest)
		return
	}
	for name := range top {
		if name != "requests" {
			c.refuse(errUnsupportedParam(boundedName(name)), spool.StatusBadRequest)
			return
		}
	}
	var requests []json.RawMessage
	if json.Unmarshal(top["requests"], &requests) != nil || len(requests) == 0 {
		c.refuse(errMissingParam("requests"), spool.StatusBadRequest)
		return
	}
	items := make([]batchItem, len(requests))
	model := ""
	for i, item := range requests {
		field := fmt.Sprintf("requests[%d]", i)
		var entry map[string]json.RawMessage
		if json.Unmarshal(item, &entry) != nil || entry == nil {
			c.refuse(errInvalidParamValue(field), spool.StatusBadRequest)
			return
		}
		for name := range entry {
			if name != "custom_id" && name != "params" {
				c.refuse(errUnsupportedParam(boundedName(field+"."+name)), spool.StatusBadRequest)
				return
			}
		}
		if json.Unmarshal(entry["custom_id"], &items[i].customID) != nil || items[i].customID == "" {
			c.refuse(errMissingParam(field+".custom_id"), spool.StatusBadRequest)
			return
		}
		if !validCustomID(items[i].customID) {
			c.refuse(errInvalidParamValue(field+".custom_id"), spool.StatusBadRequest)
			return
		}
		if json.Unmarshal(entry["params"], &items[i].body) != nil || items[i].body == nil {
			c.refuse(errMissingParam(field+".params"), spool.StatusBadRequest)
			return
		}
		var named string
		if json.Unmarshal(items[i].body["model"], &named) != nil || named == "" {
			c.refuse(errMissingParam(field+".params.model"), spool.StatusBadRequest)
			return
		}
		if model == "" {
			model = named
		} else if named != model {
			c.refuse(errBatchMixedModels, spool.StatusBadRequest)
			return
		}
	}
	answer, _, ok := c.createBatch("/v1/messages", model, map[string]json.RawMessage{}, items,
		"requests[%d].params")
	if !ok {
		return
	}
	out, err := anthropicBatchObject(answer, s.now())
	if err != nil {
		// The vendor accepted the batch; its answer goes back as it came
		// rather than as a 5xx an SDK would retry into a second batch.
		s.log.Error("batch submit answer cannot be translated", "keyId", c.key.KeyID, "error", err)
		c.writeJSON(http.StatusOK, answer)
		return
	}
	c.writeJSON(http.StatusOK, out)
}

func (s *Server) readMessagesBatch(w http.ResponseWriter, r *http.Request, id string, results bool) {
	c := s.beginAsyncAs(w, r, spool.EndpointBatchReads, snapshot.EndpointBatches, true)
	if c == nil {
		return
	}
	defer c.done()
	answer, ok := c.fetchBatch(id)
	if !ok {
		return
	}
	if !results {
		out, err := anthropicBatchObject(answer, s.now())
		if err != nil {
			c.refuseAs(errUpstream, spool.StatusUpstreamErr, "upstream_invalid_response")
			return
		}
		c.writeJSON(http.StatusOK, out)
		return
	}
	var batch vendorBatch
	if json.Unmarshal(answer, &batch) != nil {
		c.refuseAs(errUpstream, spool.StatusUpstreamErr, "upstream_invalid_response")
		return
	}
	if !asyncTerminal(batch.Status) {
		c.refuse(errBatchNotFinished, spool.StatusBadRequest)
		return
	}
	var out bytes.Buffer
	for _, item := range batch.Results {
		line, err := json.Marshal(anthropicResultLine(item, batch.Status))
		if err != nil {
			continue
		}
		out.Write(line)
		out.WriteByte('\n')
	}
	c.w.Header().Set("Content-Type", "application/x-jsonl")
	c.w.WriteHeader(http.StatusOK)
	var werr error
	body := out.Bytes()
	s.writeUnderDeadline(c.w, len(body), func() { _, werr = c.w.Write(body) })
	if werr != nil {
		c.ev.Status = spool.StatusCanceled
		c.ev.ErrorType = "client_disconnected"
	} else {
		c.ev.Status = spool.StatusOK
	}
	c.record()
}

func (s *Server) deleteMessagesBatch(w http.ResponseWriter, r *http.Request, id string) {
	c := s.beginAsyncAs(w, r, spool.EndpointBatchReads, snapshot.EndpointBatches, true)
	if c == nil {
		return
	}
	defer c.done()
	if _, ok := c.removeBatch(id); !ok {
		return
	}
	out, _ := json.Marshal(map[string]string{"id": id, "type": "message_batch_deleted"})
	c.writeJSON(http.StatusOK, out)
}

func (s *Server) listMessagesBatches(w http.ResponseWriter, r *http.Request) {
	c := s.beginAsyncAs(w, r, spool.EndpointBatchReads, snapshot.EndpointBatches, true)
	if c == nil {
		return
	}
	defer c.done()
	q := r.URL.Query()
	if q.Get("before_id") != "" {
		c.refuse(errBatchBeforeUnsupported, spool.StatusBadRequest)
		return
	}
	limit, ok := c.listLimit(q.Get("limit"), anthropicListMaxLimit)
	if !ok {
		return
	}
	page, ok := c.scanOwnedBatches(url.Values{}, q.Get("after_id"), "after_id", limit)
	if !ok {
		return
	}
	data := make([]json.RawMessage, 0, len(page.items))
	for _, item := range page.items {
		converted, err := anthropicBatchObject(item, s.now())
		if err != nil {
			c.refuseAs(errUpstream, spool.StatusUpstreamErr, "upstream_invalid_response")
			return
		}
		data = append(data, converted)
	}
	out := struct {
		Data      []json.RawMessage `json:"data"`
		FirstID   *string           `json:"first_id"`
		LastID    *string           `json:"last_id"`
		HasMore   bool              `json:"has_more"`
		Truncated bool              `json:"truncated,omitempty"`
	}{Data: data, HasMore: page.hasMore(), Truncated: page.truncated}
	out.FirstID, out.LastID = page.firstID(), page.lastID()
	body, err := json.Marshal(out)
	if err != nil {
		c.refuseAs(errUpstream, spool.StatusUpstreamErr, "upstream_invalid_response")
		return
	}
	c.writeJSON(http.StatusOK, body)
}

// validCustomID is the Anthropic API's custom_id rule.
func validCustomID(id string) bool {
	if id == "" || len(id) > anthropicCustomIDMax {
		return false
	}
	for i := 0; i < len(id); i++ {
		b := id[i]
		if !(b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z' || b >= '0' && b <= '9' || b == '-' || b == '_') {
			return false
		}
	}
	return true
}

// vendorBatch is the part of the vendor's batch object the translation reads.
// Every member is optional and read leniently: a field of a surprising type
// is treated as absent rather than failing the owner's read.
type vendorBatch struct {
	ID             string
	Status         string
	CreatedAt      int64
	ExpiresAt      int64
	FinalizedAt    int64
	Total          int
	Completed      int
	Failed         int
	Results        []vendorBatchResult
	resultsPresent bool
}

type vendorBatchResult struct {
	CustomID  string
	Status    int
	RequestID *string
	Body      json.RawMessage
	HasResp   bool
	ErrType   string
	ErrMsg    string
	HasErr    bool
}

func (b *vendorBatch) UnmarshalJSON(raw []byte) error {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		return err
	}
	_ = json.Unmarshal(m["id"], &b.ID)
	_ = json.Unmarshal(m["status"], &b.Status)
	b.CreatedAt = jsonInt(m["created_at"])
	b.ExpiresAt = jsonInt(m["expires_at"])
	b.FinalizedAt = jsonInt(m["finalized_at"])
	var counts map[string]json.RawMessage
	if json.Unmarshal(m["request_counts"], &counts) == nil {
		b.Total = int(jsonInt(counts["total"]))
		b.Completed = int(jsonInt(counts["completed"]))
		b.Failed = int(jsonInt(counts["failed"]))
	}
	var results []json.RawMessage
	if json.Unmarshal(m["results"], &results) == nil && results != nil {
		b.resultsPresent = true
		for _, raw := range results {
			var item map[string]json.RawMessage
			if json.Unmarshal(raw, &item) != nil || item == nil {
				continue
			}
			var r vendorBatchResult
			_ = json.Unmarshal(item["custom_id"], &r.CustomID)
			var resp map[string]json.RawMessage
			if json.Unmarshal(item["response"], &resp) == nil && resp != nil {
				r.HasResp = true
				r.Status = int(jsonInt(resp["status_code"]))
				var rid string
				if json.Unmarshal(resp["request_id"], &rid) == nil && rid != "" {
					r.RequestID = &rid
				}
				if body := bytes.TrimSpace(resp["body"]); len(body) > 0 && string(body) != "null" {
					r.Body = body
				}
			}
			var e map[string]json.RawMessage
			if json.Unmarshal(item["error"], &e) == nil && e != nil {
				r.HasErr = true
				_ = json.Unmarshal(e["type"], &r.ErrType)
				_ = json.Unmarshal(e["message"], &r.ErrMsg)
			}
			b.Results = append(b.Results, r)
		}
	}
	return nil
}

// jsonInt reads a JSON number as an integer, or 0 for anything else.
func jsonInt(raw json.RawMessage) int64 {
	var f float64
	if json.Unmarshal(raw, &f) != nil || f < 0 || f > 1<<53 {
		return 0
	}
	return int64(f)
}

// succeeded reports whether one vendor result is a message.
func (r vendorBatchResult) succeeded() bool {
	return r.HasResp && !r.HasErr && r.Status >= 200 && r.Status < 300 && len(r.Body) > 0
}

// anthropicBatchObject renders the vendor's batch as the Anthropic
// message_batch object. now stands in for a creation time the vendor did not
// state.
func anthropicBatchObject(answer []byte, now time.Time) ([]byte, error) {
	var b vendorBatch
	if err := json.Unmarshal(answer, &b); err != nil {
		return nil, err
	}
	if b.ID == "" {
		return nil, fmt.Errorf("batch has no id")
	}
	created := now.UTC()
	if b.CreatedAt > 0 {
		created = time.Unix(b.CreatedAt, 0).UTC()
	}
	expires := created.Add(anthropicBatchDeadline)
	if b.ExpiresAt > 0 {
		expires = time.Unix(b.ExpiresAt, 0).UTC()
	}
	type counts struct {
		Processing int `json:"processing"`
		Succeeded  int `json:"succeeded"`
		Errored    int `json:"errored"`
		Canceled   int `json:"canceled"`
		Expired    int `json:"expired"`
	}
	out := struct {
		ID                string  `json:"id"`
		Type              string  `json:"type"`
		ProcessingStatus  string  `json:"processing_status"`
		RequestCounts     counts  `json:"request_counts"`
		CreatedAt         string  `json:"created_at"`
		ExpiresAt         string  `json:"expires_at"`
		EndedAt           *string `json:"ended_at"`
		ArchivedAt        *string `json:"archived_at"`
		CancelInitiatedAt *string `json:"cancel_initiated_at"`
		ResultsURL        *string `json:"results_url"`
	}{
		ID:        b.ID,
		Type:      "message_batch",
		CreatedAt: created.Format(time.RFC3339),
		ExpiresAt: expires.Format(time.RFC3339),
	}
	switch {
	case asyncTerminal(b.Status):
		out.ProcessingStatus = "ended"
		ended := now.UTC()
		if b.FinalizedAt > 0 {
			ended = time.Unix(b.FinalizedAt, 0).UTC()
		}
		endedAt := ended.Format(time.RFC3339)
		out.EndedAt = &endedAt
		// Root-relative: both Anthropic SDKs join it to their base URL,
		// including one with a path prefix.
		results := "/v1/messages/batches/" + url.PathEscape(b.ID) + "/results"
		out.ResultsURL = &results
		n := counts{}
		if b.resultsPresent {
			for _, r := range b.Results {
				switch anthropicResultType(r, b.Status) {
				case "succeeded":
					n.Succeeded++
				case "expired":
					n.Expired++
				case "canceled":
					n.Canceled++
				default:
					n.Errored++
				}
			}
		} else {
			// A metadata-only answer (a list entry): the vendor's counts.
			n.Succeeded = b.Completed
			n.Errored = b.Failed
		}
		// Requests the vendor never resolved count as the batch ended them.
		if rest := b.Total - (n.Succeeded + n.Errored + n.Expired + n.Canceled); rest > 0 {
			switch b.Status {
			case "expired":
				n.Expired += rest
			case "cancelled":
				n.Canceled += rest
			default:
				n.Errored += rest
			}
		}
		out.RequestCounts = n
	case b.Status == "cancelling":
		out.ProcessingStatus = "canceling"
		out.RequestCounts = counts{Processing: max(b.Total-b.Completed-b.Failed, 0),
			Succeeded: b.Completed, Errored: b.Failed}
	default:
		out.ProcessingStatus = "in_progress"
		out.RequestCounts = counts{Processing: max(b.Total-b.Completed-b.Failed, 0),
			Succeeded: b.Completed, Errored: b.Failed}
	}
	return json.Marshal(out)
}

// anthropicResultType classifies one vendor result in Anthropic's terms. A
// result with neither a message nor an error is one the batch ended: expired
// or cancelled with it, errored otherwise.
func anthropicResultType(r vendorBatchResult, batchStatus string) string {
	if r.succeeded() {
		return "succeeded"
	}
	if !r.HasResp && !r.HasErr {
		switch batchStatus {
		case "expired":
			return "expired"
		case "cancelled":
			return "canceled"
		}
	}
	return "errored"
}

// anthropicResultLine renders one vendor result as a line of the Anthropic
// results file.
func anthropicResultLine(r vendorBatchResult, batchStatus string) map[string]any {
	result := map[string]any{}
	switch t := anthropicResultType(r, batchStatus); t {
	case "succeeded":
		result["type"] = "succeeded"
		result["message"] = r.Body
	case "expired", "canceled":
		result["type"] = t
	default:
		typ, msg := r.ErrType, r.ErrMsg
		if r.HasResp && len(r.Body) > 0 && (typ == "" || msg == "") {
			var envelope struct {
				Error struct {
					Type    string `json:"type"`
					Message string `json:"message"`
				} `json:"error"`
			}
			if json.Unmarshal(r.Body, &envelope) == nil {
				if typ == "" {
					typ = envelope.Error.Type
				}
				if msg == "" {
					msg = envelope.Error.Message
				}
			}
		}
		if !anthropicErrorTypes[typ] {
			typ = anthropicErrorTypeForStatus(r.Status)
		}
		if msg == "" {
			msg = "요청을 처리하지 못했어요."
		}
		var requestID any
		if r.RequestID != nil {
			requestID = *r.RequestID
		}
		result["type"] = "errored"
		result["error"] = map[string]any{
			"type":       "error",
			"error":      map[string]string{"type": typ, "message": msg},
			"request_id": requestID,
		}
	}
	return map[string]any{"custom_id": r.CustomID, "result": result}
}

// anthropicErrorTypes is the Anthropic API's error type vocabulary.
var anthropicErrorTypes = map[string]bool{
	"invalid_request_error": true, "authentication_error": true, "billing_error": true,
	"permission_error": true, "not_found_error": true, "rate_limit_error": true,
	"timeout_error": true, "api_error": true, "overloaded_error": true,
}

func anthropicErrorTypeForStatus(status int) string {
	switch status {
	case http.StatusBadRequest, http.StatusUnprocessableEntity, http.StatusRequestEntityTooLarge:
		return "invalid_request_error"
	case http.StatusUnauthorized:
		return "authentication_error"
	case http.StatusPaymentRequired:
		return "billing_error"
	case http.StatusForbidden:
		return "permission_error"
	case http.StatusNotFound:
		return "not_found_error"
	case http.StatusTooManyRequests:
		return "rate_limit_error"
	case http.StatusGatewayTimeout, http.StatusRequestTimeout:
		return "timeout_error"
	case 529:
		return "overloaded_error"
	}
	return "api_error"
}

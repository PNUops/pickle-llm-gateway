package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/pnuops/pickle-llm-gateway/internal/ownership"
	"github.com/pnuops/pickle-llm-gateway/internal/snapshot"
	"github.com/pnuops/pickle-llm-gateway/internal/spool"
)

// The batch route, /v1/batches. The vendor's batch carries its requests
// inline (`requests[]` of `{custom_id, body}`) and returns its results inline
// on the batch object, so it involves no file upload. One batch has one
// target endpoint and one model.
//
// A batch is fenced like the requests it carries: the key needs the batches
// grant and the grant of the target endpoint, the top-level model goes
// through the money-axis model fence, and every request body goes through
// the fences of its endpoint, exactly as it would on its own. A body that
// names a model names the batch's model or is refused.

// batchRetention is how long the vendor keeps a batch: it is deleted 30 days
// after submission and answers 410 from then on. The ledger row carries it as
// the object's expiry, which is the end of retention and not the vendor's
// processing deadline (the batch object's own `expires_at`, about a day).
const batchRetention = 30 * 24 * time.Hour

// batchListMaxPages bounds the vendor pages one list request reads while
// looking for this key's batches among the workspace's.
const batchListMaxPages = 5

// batchListMaxLimit bounds a list page, and with it one ownership lookup.
const batchListMaxLimit = 100

// batchTarget is one endpoint a batch may target: the grant it needs and the
// surface whose fences judge each body. A nil surface is the embeddings
// passthrough, fenced as that route fences.
type batchTarget struct {
	capability string
	proto      *protocol
}

var batchTargets = map[string]batchTarget{
	"/v1/chat/completions": {snapshot.EndpointChat, protoChat},
	"/v1/responses":        {snapshot.EndpointChat, protoResponses},
	"/v1/messages":         {snapshot.EndpointChat, protoMessages},
	"/v1/embeddings":       {snapshot.EndpointEmbeddings, nil},
}

var (
	// Every 2xx is an acceptance: anything else would count as a refusal,
	// release the intent of an object that exists, and invite a retry.
	batchSubmitPass = successStatuses()
	batchReadPass   = map[int]bool{http.StatusNotFound: true, http.StatusGone: true}
	batchDeletePass = map[int]bool{http.StatusNotFound: true, http.StatusConflict: true,
		http.StatusGone: true}
)

func (s *Server) handleBatches(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/v1/batches" {
		switch r.Method {
		case http.MethodPost:
			s.submitBatch(w, r)
		case http.MethodGet:
			s.listBatches(w, r)
		default:
			writeAPIError(w, errMethod)
		}
		return
	}
	id := strings.TrimPrefix(r.URL.Path, "/v1/batches/")
	// The vendor documents no route under a batch (there is no cancel), so a
	// deeper path is not one this service serves.
	if strings.Contains(id, "/") {
		writeAPIError(w, errNotFound)
		return
	}
	switch r.Method {
	case http.MethodGet:
		s.readBatch(w, r, id)
	case http.MethodDelete:
		s.deleteBatch(w, r, id)
	default:
		writeAPIError(w, errMethod)
	}
}

func (s *Server) submitBatch(w http.ResponseWriter, r *http.Request) {
	c := s.beginAsync(w, r, spool.EndpointBatches, snapshot.EndpointBatches)
	if c == nil {
		return
	}
	defer c.done()
	raw, ok := c.readRequest()
	if !ok {
		return
	}
	var params map[string]json.RawMessage
	if json.Unmarshal(raw, &params) != nil || params == nil {
		c.refuse(errBadJSON, spool.StatusBadRequest)
		return
	}
	var endpoint, publicModel string
	if v, ok := params["endpoint"]; !ok || json.Unmarshal(v, &endpoint) != nil || endpoint == "" {
		c.refuse(errMissingParam("endpoint"), spool.StatusBadRequest)
		return
	}
	if v, ok := params["model"]; !ok || json.Unmarshal(v, &publicModel) != nil || publicModel == "" {
		c.refuse(errMissingParam("model"), spool.StatusBadRequest)
		return
	}
	var requests []json.RawMessage
	if v, ok := params["requests"]; !ok || json.Unmarshal(v, &requests) != nil || len(requests) == 0 {
		c.refuse(errMissingParam("requests"), spool.StatusBadRequest)
		return
	}
	// The vendor would post the result to an address the gateway never sees,
	// and the owner reads the same result here with GET /v1/batches/{id}.
	if v, ok := params["callback_url"]; ok && string(v) != "null" {
		c.refuse(errUnsupportedValue("callback_url",
			"배치가 끝났는지는 GET /v1/batches/{id}로 확인해 주세요."), spool.StatusBadRequest)
		return
	}
	items := make([]batchItem, len(requests))
	for i, item := range requests {
		field := fmt.Sprintf("requests[%d]", i)
		var entry map[string]json.RawMessage
		if json.Unmarshal(item, &entry) != nil || entry == nil {
			c.refuse(errInvalidParamValue(field), spool.StatusBadRequest)
			return
		}
		// Only the two documented members travel: an unread member such as a
		// per-item model or url could name what the fences never judged.
		for name := range entry {
			if name != "custom_id" && name != "body" {
				c.refuse(errUnsupportedParam(boundedName(field+"."+name)), spool.StatusBadRequest)
				return
			}
		}
		if v, ok := entry["custom_id"]; !ok || json.Unmarshal(v, &items[i].customID) != nil ||
			items[i].customID == "" {
			c.refuse(errMissingParam(field+".custom_id"), spool.StatusBadRequest)
			return
		}
		if v, ok := entry["body"]; !ok || json.Unmarshal(v, &items[i].body) != nil || items[i].body == nil {
			c.refuse(errMissingParam(field+".body"), spool.StatusBadRequest)
			return
		}
	}
	answer, status, ok := c.createBatch(endpoint, publicModel, params, items, "requests[%d].body")
	if !ok {
		return
	}
	c.writeJSON(status, answer)
}

// batchItem is one request of a batch: its id and the body judged and sent.
type batchItem struct {
	customID string
	body     map[string]json.RawMessage
}

// batchTopMembers are the vendor's top-level batch members this gateway
// forwards. Anything else is refused rather than forwarded unread.
var batchTopMembers = map[string]bool{
	"endpoint": true, "model": true, "requests": true, "completion_window": true,
	"provider": true, "callback_url": true,
}

// createBatch fences a batch and submits it. params carries the top-level
// members besides endpoint, model and requests (it may be empty);
// bodyField formats the field name a refusal of item i names. On success it
// returns the vendor's answer and status for the caller to relay or
// translate; otherwise it has answered and recorded.
//
// After the vendor accepted, nothing here answers a 5xx except
// OBJECT_TAKEN: an SDK retries a 5xx, and every retry would be a second
// billed batch, since a batch cannot be cancelled.
func (c *asyncCall) createBatch(endpoint, publicModel string, params map[string]json.RawMessage,
	items []batchItem, bodyField string) ([]byte, int, bool) {
	target, ok := batchTargets[endpoint]
	if !ok {
		c.refuse(errInvalidParamValue("endpoint"), spool.StatusBadRequest)
		return nil, 0, false
	}
	for name := range params {
		if !batchTopMembers[name] {
			c.refuse(errUnsupportedParam(boundedName(name)), spool.StatusBadRequest)
			return nil, 0, false
		}
	}
	if !c.key.AllowsEndpoint(target.capability) {
		c.refuse(errEndpointNotAllowed(endpointLabel(target.capability)), spool.StatusAuthRejected)
		return nil, 0, false
	}
	model := c.fenceModel(publicModel, params, target.proto != nil)
	if model == nil {
		return nil, 0, false
	}
	// A batch goes to the vendor under the name it was sent with. A
	// catalogue model served under another name or by another upstream
	// cannot be batched there.
	if !strings.EqualFold(model.UpstreamRef, c.doc.PassthroughRef) || model.UpstreamModel != publicModel {
		c.refuseAs(errModelNotOnBatches, spool.StatusBadRequest, "model_not_available_on_route")
		return nil, 0, false
	}
	forwarded := make([]json.RawMessage, len(items))
	for i, item := range items {
		body, ok := c.fenceBatchBody(fmt.Sprintf(bodyField, i), item.body, target, model, publicModel)
		if !ok {
			return nil, 0, false
		}
		entry, err := json.Marshal(struct {
			CustomID string          `json:"custom_id"`
			Body     json.RawMessage `json:"body"`
		}{item.customID, body})
		if err != nil {
			c.refuse(errBadJSON, spool.StatusBadRequest)
			return nil, 0, false
		}
		forwarded[i] = entry
	}
	// The vendor refuses a body whose `requests` comes before the other
	// members, so the body is written in that order rather than as a map.
	var buf bytes.Buffer
	buf.WriteByte('{')
	head := map[string]json.RawMessage{}
	for k, v := range params {
		if k != "requests" && k != "endpoint" && k != "model" {
			head[k] = v
		}
	}
	head["endpoint"], _ = json.Marshal(endpoint)
	head["model"], _ = json.Marshal(publicModel)
	for k, v := range head {
		name, _ := json.Marshal(k)
		buf.Write(name)
		buf.WriteByte(':')
		buf.Write(v)
		buf.WriteByte(',')
	}
	list, err := json.Marshal(forwarded)
	if err != nil {
		c.refuse(errBadJSON, spool.StatusBadRequest)
		return nil, 0, false
	}
	buf.WriteString(`"requests":`)
	buf.Write(list)
	buf.WriteByte('}')
	body := buf.Bytes()
	if !json.Valid(body) {
		c.refuse(errBadJSON, spool.StatusBadRequest)
		return nil, 0, false
	}

	s := c.s
	r := c.r
	// The intent, before anything is spent: a ledger that cannot be written
	// to means the vendor is not called.
	intentID, err := s.ownership.Intent(r.Context(), c.key.KeyID, ownership.KindBatch, c.ev.EventUUID)
	if err != nil {
		c.answerIntentFailure(err)
		return nil, 0, false
	}
	upCtx, cancel := context.WithTimeout(r.Context(), s.cfg.RequestMaxDuration)
	defer cancel()
	resp, ae := c.call(upCtx, http.MethodPost, "/batches", body, batchSubmitPass)
	if ae != nil && ae.status == http.StatusNotFound {
		// The vendor has no batch provider it may bill for this model: the
		// model has no batch variant on the platform, or only on a provider
		// key the business account does not hold.
		c.releaseIntent(ownership.KindBatch, intentID)
		c.refuse(errBatchModelUnavailable, spool.StatusBadRequest)
		return nil, 0, false
	}
	if ae != nil {
		if submitRefused(ae) {
			c.releaseIntent(ownership.KindBatch, intentID)
		} else {
			c.noRetry()
		}
		c.answerFailure(upCtx, ae)
		return nil, 0, false
	}
	defer resp.Body.Close()
	c.noRetry()
	answer, ok := c.readAnswer(resp)
	if !ok {
		return nil, 0, false
	}
	c.w.Header().Del("X-Should-Retry")
	id := objectIDOf(answer)
	if id == "" {
		// The vendor accepted a batch this gateway cannot name. Its answer
		// goes back as it came, since a 5xx would be retried into a second
		// batch; the intent stays pending, which is what reconciliation reads.
		s.log.Error("batch submit answer carries no usable id", "keyId", c.key.KeyID,
			"intentId", intentID)
		return answer, resp.StatusCode, true
	}
	expires := s.now().Add(batchRetention).UTC()
	c.ev.Object = &spool.ObjectRef{Kind: string(ownership.KindBatch), IntentID: intentID,
		ObjectID: id, ExpiresAt: expires.Format(time.RFC3339)}
	if err := s.ownership.Bind(r.Context(), c.key.KeyID, intentID, ownership.KindBatch, id,
		ownership.BindDetails{ExpiresAt: &expires}); err != nil {
		code := ownership.RefusalCode(err)
		if code == ownership.CodeObjectTaken {
			// The batch id is another key's row. It must not be undone and
			// must not be handed to this key, so the answer is the
			// undecidable one.
			s.log.Error("batch bind refused: object taken", "keyId", c.key.KeyID,
				"intentId", intentID, "batchId", id)
			c.noRetry()
			c.refuse(errOwnershipUnavailable, spool.StatusUpstreamErr)
			return nil, 0, false
		}
		// A batch cannot be undone. The submit succeeds, this instance
		// remembers the owner, and the usage event carries the late bind.
		s.log.Warn("batch bind did not land; answering success and binding late",
			"keyId", c.key.KeyID, "intentId", intentID, "batchId", id, "code", code, "error", err)
		s.ownership.Remember(c.key.KeyID, ownership.KindBatch, id)
	}
	return answer, resp.StatusCode, true
}

// fenceBatchBody judges one request body and returns it as it is forwarded:
// re-serialized from what was judged, so the vendor reads the same members
// the fences did. field names the body in a refusal.
func (c *asyncCall) fenceBatchBody(field string, body map[string]json.RawMessage, target batchTarget,
	model *snapshot.Model, publicModel string) (json.RawMessage, bool) {
	if v, ok := body["model"]; ok {
		var named string
		if json.Unmarshal(v, &named) != nil || named != publicModel {
			c.refuse(errInvalidParamValue(field+".model"), spool.StatusBadRequest)
			return nil, false
		}
	}
	// A result arrives on the batch object, never as a stream.
	if v, ok := body["stream"]; ok {
		var streaming bool
		if json.Unmarshal(v, &streaming) != nil {
			c.refuse(errInvalidParamValue(field+".stream"), spool.StatusBadRequest)
			return nil, false
		}
		if streaming {
			c.refuse(errPassthroughNoStreaming, spool.StatusBadRequest)
			return nil, false
		}
	}
	// The fences judge a copy that names the model, as the request would on
	// its own route; what is forwarded is the body as sent, with the
	// model's output bound added below when it names none.
	judged := make(map[string]json.RawMessage, len(body)+1)
	for k, v := range body {
		judged[k] = v
	}
	if _, ok := judged["model"]; !ok {
		named, _ := json.Marshal(publicModel)
		judged["model"] = named
	}
	lookup := c.lookup()
	if target.proto != nil && model.MaxOutputTokens > 0 && len(target.proto.capReadFields) > 0 {
		// The model's output bound, as chat applies it: a larger cap is
		// refused rather than forwarded to be billed, and a body naming none
		// gets the bound written in.
		capped := false
		for _, f := range target.proto.capReadFields {
			if raw, ok := body[f]; ok && string(bytes.TrimSpace(raw)) != "null" {
				capped = true
			}
		}
		if !capped {
			field := target.proto.capWriteField
			if field == "" {
				field = "max_tokens"
			}
			body[field], _ = json.Marshal(model.MaxOutputTokens)
		}
		for _, f := range target.proto.capReadFields {
			raw, ok := body[f]
			if !ok || string(bytes.TrimSpace(raw)) == "null" {
				continue
			}
			var n int
			if json.Unmarshal(raw, &n) != nil || n <= 0 {
				c.refuse(errInvalidParamValue(field+"."+f), spool.StatusBadRequest)
				return nil, false
			}
			if n > model.MaxOutputTokens {
				c.refuse(errOutputTooLong(field+"."+f, model.MaxOutputTokens), spool.StatusBadRequest)
				return nil, false
			}
		}
	}
	if target.proto != nil {
		if _, rf := target.proto.parse(judged); rf != nil {
			c.refuseWith(rf, spool.StatusBadRequest)
			return nil, false
		}
		for _, f := range target.proto.fences {
			if rf := f(c.s, &c.doc, lookup, c.key, model, judged); rf != nil {
				c.refuseWith(rf, spool.StatusBadRequest)
				return nil, false
			}
		}
	} else {
		if _, present := judged[snapshot.PresetField]; present {
			c.refuseAs(errPresetNotAllowed, spool.StatusBadRequest, "preset_not_allowed")
			return nil, false
		}
		if bad, ok := fenceCandidateModels(&c.doc, lookup, c.key, judged); !ok {
			c.refuseAs(errCandidateModelNotAllowed(bad), spool.StatusBadRequest,
				"credit_model_not_allowed")
			return nil, false
		}
		if e := c.s.fenceServerTools(&c.doc, lookup, c.key, judged); e != nil {
			c.refuseAs(*e, spool.StatusBadRequest, toolRefusalType(*e))
			return nil, false
		}
		if e := c.s.fencePlugins(&c.doc, lookup, c.key, judged); e != nil {
			c.refuseAs(*e, spool.StatusBadRequest, toolRefusalType(*e))
			return nil, false
		}
	}
	rebuilt, err := json.Marshal(body)
	if err != nil {
		c.refuse(errBadJSON, spool.StatusBadRequest)
		return nil, false
	}
	return rebuilt, true
}

func (s *Server) readBatch(w http.ResponseWriter, r *http.Request, id string) {
	c := s.beginAsync(w, r, spool.EndpointBatchReads, snapshot.EndpointBatches)
	if c == nil {
		return
	}
	defer c.done()
	answer, ok := c.fetchBatch(id)
	if !ok {
		return
	}
	c.writeJSON(http.StatusOK, answer)
}

// fetchBatch reads one of this key's batches from the vendor and records its
// price once it is final. On anything but a readable batch it has answered
// and recorded, and returns false.
func (c *asyncCall) fetchBatch(id string) ([]byte, bool) {
	if !validObjectID(id) {
		c.refuse(errObjectNotFound, spool.StatusBadRequest)
		return nil, false
	}
	if !c.owns(ownership.KindBatch, id) {
		return nil, false
	}
	upCtx, cancel := context.WithTimeout(c.r.Context(), c.s.cfg.RequestMaxDuration)
	defer cancel()
	resp, ae := c.call(upCtx, http.MethodGet, "/batches/"+url.PathEscape(id), nil, batchReadPass)
	if ae != nil {
		c.answerFailure(upCtx, ae)
		return nil, false
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusGone {
		c.vendorGone(resp, ownership.KindBatch, id)
		return nil, false
	}
	answer, ok := c.readAnswer(resp)
	if !ok {
		return nil, false
	}
	// Only the members the price needs are read, each on its own, so a
	// member of a surprising type never costs the owner the answer.
	var head map[string]json.RawMessage
	if json.Unmarshal(answer, &head) != nil || head == nil {
		c.refuseAs(errUpstream, spool.StatusUpstreamErr, "upstream_invalid_response")
		return nil, false
	}
	c.settleBatchHead(id, head)
	return answer, true
}

// settleBatchEntry records the price of a batch read as a list entry.
func (c *asyncCall) settleBatchEntry(id string, item json.RawMessage) {
	var head map[string]json.RawMessage
	if json.Unmarshal(item, &head) == nil && head != nil {
		c.settleBatchHead(id, head)
	}
}

// settleBatchHead records a batch's price once it is over. The vendor prices
// a batch on the batch itself; an expired or failed batch can have billed
// part of its work. A read of any kind settles, so a batch listed or read
// once after it ends has its price recorded.
func (c *asyncCall) settleBatchHead(id string, head map[string]json.RawMessage) {
	var status, model string
	_ = json.Unmarshal(head["status"], &status)
	_ = json.Unmarshal(head["model"], &model)
	if asyncTerminal(status) {
		c.settle(spool.EndpointBatchCost, ownership.KindBatch, id, model, head["usage"])
	}
}

func (s *Server) deleteBatch(w http.ResponseWriter, r *http.Request, id string) {
	c := s.beginAsync(w, r, spool.EndpointBatchReads, snapshot.EndpointBatches)
	if c == nil {
		return
	}
	defer c.done()
	answer, ok := c.removeBatch(id)
	if !ok {
		return
	}
	c.writeJSON(http.StatusOK, answer)
}

// removeBatch deletes one of this key's finished batches at the vendor and
// releases it in the ledger. On anything but a deletion it has answered and
// recorded, and returns false.
func (c *asyncCall) removeBatch(id string) ([]byte, bool) {
	if !validObjectID(id) {
		c.refuse(errObjectNotFound, spool.StatusBadRequest)
		return nil, false
	}
	if !c.owns(ownership.KindBatch, id) {
		return nil, false
	}
	upCtx, cancel := context.WithTimeout(c.r.Context(), c.s.cfg.RequestMaxDuration)
	defer cancel()
	resp, ae := c.call(upCtx, http.MethodDelete, "/batches/"+url.PathEscape(id), nil, batchDeletePass)
	if ae != nil {
		c.answerFailure(upCtx, ae)
		return nil, false
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusNotFound, http.StatusGone:
		c.vendorGone(resp, ownership.KindBatch, id)
		return nil, false
	case http.StatusConflict:
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, rejectionReadLimit))
		c.refuse(errBatchNotFinished, spool.StatusBadRequest)
		return nil, false
	}
	answer, ok := c.readAnswer(resp)
	if !ok {
		return nil, false
	}
	c.release(ownership.KindBatch, id, ownership.ReleaseDeleted)
	return answer, true
}

// listBatches answers the vendor's list filtered to this key's batches (see
// scanOwnedBatches).
func (s *Server) listBatches(w http.ResponseWriter, r *http.Request) {
	c := s.beginAsync(w, r, spool.EndpointBatchReads, snapshot.EndpointBatches)
	if c == nil {
		return
	}
	defer c.done()
	q := r.URL.Query()
	limit, ok := c.listLimit(q.Get("limit"), batchListMaxLimit)
	if !ok {
		return
	}
	vendor := url.Values{}
	// Only the parameters whose meaning this route knows go on; the rest are
	// dropped rather than forwarded unread.
	for _, name := range []string{"status", "created_after", "created_before"} {
		v := q.Get(name)
		if v == "" {
			continue
		}
		if !validListFilter(v) {
			c.refuse(errInvalidParamValue(name), spool.StatusBadRequest)
			return
		}
		vendor.Set(name, v)
	}
	page, ok := c.scanOwnedBatches(vendor, q.Get("after"), "after", limit)
	if !ok {
		return
	}
	out := struct {
		Object    string            `json:"object"`
		Data      []json.RawMessage `json:"data"`
		FirstID   *string           `json:"first_id"`
		LastID    *string           `json:"last_id"`
		HasMore   bool              `json:"has_more"`
		Truncated bool              `json:"truncated,omitempty"`
	}{Object: "list", Data: page.items, HasMore: page.hasMore(), Truncated: page.truncated}
	out.FirstID, out.LastID = page.firstID(), page.lastID()
	body, err := json.Marshal(out)
	if err != nil {
		c.refuseAs(errUpstream, spool.StatusUpstreamErr, "upstream_invalid_response")
		return
	}
	c.writeJSON(http.StatusOK, body)
}

// listLimit reads a page size: default 20, at least 1, clamped to max. On a
// bad value it has answered and returns false.
func (c *asyncCall) listLimit(v string, max int) (int, bool) {
	if v == "" {
		return 20, true
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 {
		c.refuse(errInvalidParamValue("limit"), spool.StatusBadRequest)
		return 0, false
	}
	return min(n, max), true
}

// validListFilter accepts a status name or a time bound (Unix seconds or an
// ISO 8601 date or date-time): letters, digits and the punctuation of a
// timestamp, bounded.
func validListFilter(v string) bool {
	if v == "" || len(v) > 64 {
		return false
	}
	for i := 0; i < len(v); i++ {
		b := v[i]
		if !(b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z' || b >= '0' && b <= '9' ||
			b == '-' || b == '_' || b == ':' || b == '.' || b == '+') {
			return false
		}
	}
	return true
}

// ownedBatchPage is one page of this key's batches out of the workspace's.
type ownedBatchPage struct {
	items []json.RawMessage
	ids   []string
	// more: this key may have further batches past the last one kept.
	more bool
	// truncated: the scan stopped at its page bound while the vendor still
	// had entries, so batches of this key may follow even where the page
	// cannot offer a cursor to reach them (nothing was kept).
	truncated bool
}

func (p ownedBatchPage) hasMore() bool { return (p.more || p.truncated) && len(p.ids) > 0 }

func (p ownedBatchPage) firstID() *string {
	if len(p.ids) == 0 {
		return nil
	}
	return &p.ids[0]
}

func (p ownedBatchPage) lastID() *string {
	if len(p.ids) == 0 {
		return nil
	}
	return &p.ids[len(p.ids)-1]
}

// scanOwnedBatches reads the vendor's list, which covers the whole workspace
// (every key of the business account), asks the ledger which ids are this
// key's and keeps only those, reading further vendor pages up to
// batchListMaxPages to fill the page. The cursor a caller passes (after,
// named cursorName in a refusal) must be one of this key's batches and is
// refused otherwise before the vendor sees it: how the vendor answers another
// key's id would tell whether it exists. The cursor handed back is always the
// key's own last id. On a failure it has answered and returns false.
func (c *asyncCall) scanOwnedBatches(vendor url.Values, after, cursorName string, limit int) (ownedBatchPage, bool) {
	var page ownedBatchPage
	if after != "" {
		owned := false
		if validObjectID(after) {
			got, err := c.s.ownership.Owned(c.r.Context(), c.key.KeyID, ownership.KindBatch, []string{after})
			if err != nil {
				c.refuse(errOwnershipUnavailable, spool.StatusUpstreamErr)
				return page, false
			}
			owned = got[after]
		}
		if !owned {
			c.refuse(errInvalidParamValue(cursorName), spool.StatusBadRequest)
			return page, false
		}
		vendor.Set("after", after)
	}
	// The vendor page is always the largest, whatever the client asked
	// for: this key's batches are a subset, and a small page would make the
	// page bound reach them later.
	vendor.Set("limit", strconv.Itoa(batchListMaxLimit))
	upCtx, cancel := context.WithTimeout(c.r.Context(), c.s.cfg.RequestMaxDuration)
	defer cancel()
	for scanned := 0; ; scanned++ {
		if scanned == batchListMaxPages {
			page.truncated = true
			return page, true
		}
		resp, ae := c.call(upCtx, http.MethodGet, "/batches?"+vendor.Encode(), nil, nil)
		if ae != nil {
			c.answerFailure(upCtx, ae)
			return page, false
		}
		answer, ok := c.readAnswer(resp)
		resp.Body.Close()
		if !ok {
			return page, false
		}
		var listing struct {
			Data    []json.RawMessage `json:"data"`
			HasMore bool              `json:"has_more"`
		}
		if json.Unmarshal(answer, &listing) != nil {
			c.refuseAs(errUpstream, spool.StatusUpstreamErr, "upstream_invalid_response")
			return page, false
		}
		ids := make([]string, len(listing.Data))
		var ask []string
		for i, item := range listing.Data {
			ids[i] = objectIDOf(item)
			if ids[i] != "" {
				ask = append(ask, ids[i])
			}
		}
		owned := map[string]bool{}
		if len(ask) > 0 {
			got, err := c.s.ownership.Owned(c.r.Context(), c.key.KeyID, ownership.KindBatch, ask)
			if err != nil {
				c.refuse(errOwnershipUnavailable, spool.StatusUpstreamErr)
				return page, false
			}
			owned = got
		}
		for i, item := range listing.Data {
			if ids[i] == "" || !owned[ids[i]] {
				continue
			}
			if len(page.ids) == limit {
				page.more = true
				return page, true
			}
			page.items = append(page.items, item)
			page.ids = append(page.ids, ids[i])
			c.settleBatchEntry(ids[i], item)
		}
		if !listing.HasMore || len(listing.Data) == 0 {
			return page, true
		}
		if ids[len(ids)-1] == "" {
			// The vendor's own cursor is unreadable, so reading on is not
			// possible; say so rather than end the list silently.
			page.truncated = true
			return page, true
		}
		if len(page.ids) == limit {
			// Full exactly at a vendor page boundary; whether more of this
			// key's batches follow is not known without reading on.
			page.more = true
			return page, true
		}
		// Reading on from the vendor's own cursor is internal; it never
		// reaches the client.
		vendor.Set("after", ids[len(ids)-1])
	}
}

// submitRefused reports whether the vendor definitely did not create the
// object: a refusal it answered, not a timeout, a dropped connection or a
// server error, after which the object may exist.
func submitRefused(ae *attemptError) bool {
	// A 408 is a timeout on the way, not the vendor declining to create.
	if ae.timeout || errors.Is(ae.err, context.DeadlineExceeded) || ae.status == http.StatusRequestTimeout {
		return false
	}
	if ae.refusal != nil || ae.throttled {
		return true
	}
	switch ae.kind {
	case failureCredential, failureKeyCredential, failureVendorRejected:
		return true
	}
	return false
}

// answerIntentFailure maps a failed intent: a key the api no longer counts as
// active, a key without a business account, or a ledger that cannot be asked.
// The vendor has not been called in any of them.
func (c *asyncCall) answerIntentFailure(err error) {
	switch ownership.RefusalCode(err) {
	case ownership.CodeKeyNotActive:
		c.refuse(errKeyRevoked, spool.StatusAuthRejected)
	case ownership.CodeNoAccount:
		c.refuse(errCreditUnavailable, spool.StatusAuthRejected)
	default:
		if c.r.Context().Err() != nil {
			c.ev.Status = spool.StatusCanceled
			c.ev.ErrorType = "client_disconnected"
			c.record()
			return
		}
		c.s.log.Warn("ownership intent failed", "keyId", c.key.KeyID, "error", err)
		c.refuse(errOwnershipUnavailable, spool.StatusUpstreamErr)
	}
}

// vendorGone answers an owned object the vendor no longer has, and releases
// it so the ledger stops calling it owned.
func (c *asyncCall) vendorGone(resp *http.Response, kind ownership.Kind, id string) {
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, rejectionReadLimit))
	c.release(kind, id, ownership.ReleaseVendorGone)
	c.refuse(errObjectNotFound, spool.StatusBadRequest)
}

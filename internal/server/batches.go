package server

import (
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
	batchSubmitPass = map[int]bool{http.StatusCreated: true, http.StatusAccepted: true}
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
	if json.Unmarshal(raw, &params) != nil {
		c.refuse(errBadJSON, spool.StatusBadRequest)
		return
	}
	var endpoint, publicModel string
	if v, ok := params["endpoint"]; !ok || json.Unmarshal(v, &endpoint) != nil || endpoint == "" {
		c.refuse(errMissingParam("endpoint"), spool.StatusBadRequest)
		return
	}
	target, ok := batchTargets[endpoint]
	if !ok {
		c.refuse(errInvalidParamValue("endpoint"), spool.StatusBadRequest)
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
	if !c.key.AllowsEndpoint(target.capability) {
		c.refuse(errEndpointNotAllowed(endpointLabel(target.capability)), spool.StatusAuthRejected)
		return
	}
	model := c.fenceModel(publicModel, params)
	if model == nil {
		return
	}
	forwarded := make([]json.RawMessage, len(requests))
	for i, item := range requests {
		next, ok := c.fenceBatchRequest(i, item, target, model, publicModel)
		if !ok {
			return
		}
		forwarded[i] = next
	}
	list, err := json.Marshal(forwarded)
	if err != nil {
		c.refuse(errBadJSON, spool.StatusBadRequest)
		return
	}
	params["requests"] = list
	body, err := json.Marshal(params)
	if err != nil {
		c.refuse(errBadJSON, spool.StatusBadRequest)
		return
	}

	// The intent, before anything is spent: a ledger that cannot be written
	// to means the vendor is not called.
	intentID, err := s.ownership.Intent(r.Context(), c.key.KeyID, ownership.KindBatch, c.ev.EventUUID)
	if err != nil {
		c.answerIntentFailure(err)
		return
	}
	upCtx, cancel := context.WithTimeout(r.Context(), s.cfg.RequestMaxDuration)
	defer cancel()
	resp, ae := c.call(upCtx, http.MethodPost, "/batches", body, batchSubmitPass)
	if ae != nil {
		if submitRefused(ae) {
			c.releaseIntent(ownership.KindBatch, intentID)
		}
		c.answerFailure(upCtx, ae)
		return
	}
	defer resp.Body.Close()
	answer, ok := c.readAnswer(resp)
	if !ok {
		return
	}
	id := objectIDOf(answer)
	if id == "" {
		// The vendor may have created a batch this gateway cannot name. The
		// intent stays pending, which is what reconciliation reads.
		s.log.Error("batch submit answer carries no usable id", "keyId", c.key.KeyID,
			"intentId", intentID)
		c.refuseAs(errUpstream, spool.StatusUpstreamErr, "upstream_invalid_response")
		return
	}
	expires := s.now().Add(batchRetention).UTC()
	c.ev.Object = &spool.ObjectRef{Kind: string(ownership.KindBatch), IntentID: intentID,
		ObjectID: id, ExpiresAt: expires.Format(time.RFC3339)}
	if err := s.ownership.Bind(r.Context(), c.key.KeyID, intentID, ownership.KindBatch, id,
		ownership.BindDetails{ExpiresAt: &expires}); err != nil {
		if code := ownership.RefusalCode(err); code != "" {
			// OBJECT_TAKEN above all must not be undone: the batch is another
			// key's. Nothing here can say who owns it, so the answer is the
			// undecidable one.
			s.log.Error("batch bind refused", "keyId", c.key.KeyID, "intentId", intentID,
				"batchId", id, "code", code)
			c.refuse(errOwnershipUnavailable, spool.StatusUpstreamErr)
			return
		}
		// The vendor offers no cancel, so a batch cannot be undone. Telling
		// the client it failed would buy a second billed batch per SDK
		// retry; the submit succeeds, this instance remembers the owner, and
		// the usage event carries the late bind.
		s.log.Warn("batch bind did not land; answering success and binding late",
			"keyId", c.key.KeyID, "intentId", intentID, "batchId", id, "error", err)
		s.ownership.Remember(c.key.KeyID, ownership.KindBatch, id)
	}
	c.writeJSON(resp.StatusCode, answer)
}

// fenceBatchRequest judges one `requests[]` item and returns it as it is
// forwarded: re-serialized from what was judged, so the vendor reads the same
// members the fences did.
func (c *asyncCall) fenceBatchRequest(i int, item json.RawMessage, target batchTarget,
	model *snapshot.Model, publicModel string) (json.RawMessage, bool) {
	field := fmt.Sprintf("requests[%d]", i)
	var entry map[string]json.RawMessage
	if json.Unmarshal(item, &entry) != nil || entry == nil {
		c.refuse(errInvalidParamValue(field), spool.StatusBadRequest)
		return nil, false
	}
	var body map[string]json.RawMessage
	if v, ok := entry["body"]; !ok || json.Unmarshal(v, &body) != nil || body == nil {
		c.refuse(errMissingParam(field+".body"), spool.StatusBadRequest)
		return nil, false
	}
	if v, ok := body["model"]; ok {
		var named string
		if json.Unmarshal(v, &named) != nil || named != publicModel {
			c.refuse(errInvalidParamValue(field+".body.model"), spool.StatusBadRequest)
			return nil, false
		}
	}
	// A result arrives on the batch object, never as a stream.
	if v, ok := body["stream"]; ok {
		var streaming bool
		if json.Unmarshal(v, &streaming) != nil {
			c.refuse(errInvalidParamValue(field+".body.stream"), spool.StatusBadRequest)
			return nil, false
		}
		if streaming {
			c.refuse(errPassthroughNoStreaming, spool.StatusBadRequest)
			return nil, false
		}
	}
	// The fences judge a copy that names the model, as the request would on
	// its own route; what is forwarded is the body as sent.
	judged := make(map[string]json.RawMessage, len(body)+1)
	for k, v := range body {
		judged[k] = v
	}
	if _, ok := judged["model"]; !ok {
		named, _ := json.Marshal(publicModel)
		judged["model"] = named
	}
	noLookup := func(string) *snapshot.Model { return nil }
	if target.proto != nil {
		if _, rf := target.proto.parse(judged); rf != nil {
			c.refuseWith(rf, spool.StatusBadRequest)
			return nil, false
		}
		for _, f := range target.proto.fences {
			if rf := f(c.s, &c.doc, noLookup, c.key, model, judged); rf != nil {
				c.refuseWith(rf, spool.StatusBadRequest)
				return nil, false
			}
		}
	} else {
		if _, present := judged[snapshot.PresetField]; present {
			c.refuseAs(errPresetNotAllowed, spool.StatusBadRequest, "preset_not_allowed")
			return nil, false
		}
		if bad, ok := fenceCandidateModels(&c.doc, noLookup, c.key, judged); !ok {
			c.refuseAs(errCandidateModelNotAllowed(bad), spool.StatusBadRequest,
				"credit_model_not_allowed")
			return nil, false
		}
		if e := c.s.fenceServerTools(&c.doc, noLookup, c.key, judged); e != nil {
			c.refuseAs(*e, spool.StatusBadRequest, toolRefusalType(*e))
			return nil, false
		}
		if e := c.s.fencePlugins(&c.doc, noLookup, c.key, judged); e != nil {
			c.refuseAs(*e, spool.StatusBadRequest, toolRefusalType(*e))
			return nil, false
		}
	}
	rebuilt, err := json.Marshal(body)
	if err != nil {
		c.refuse(errBadJSON, spool.StatusBadRequest)
		return nil, false
	}
	entry["body"] = rebuilt
	out, err := json.Marshal(entry)
	if err != nil {
		c.refuse(errBadJSON, spool.StatusBadRequest)
		return nil, false
	}
	return out, true
}

func (s *Server) readBatch(w http.ResponseWriter, r *http.Request, id string) {
	c := s.beginAsync(w, r, spool.EndpointBatchReads, snapshot.EndpointBatches)
	if c == nil {
		return
	}
	defer c.done()
	if !validObjectID(id) {
		c.refuse(errObjectNotFound, spool.StatusBadRequest)
		return
	}
	if !c.owns(ownership.KindBatch, id) {
		return
	}
	upCtx, cancel := context.WithTimeout(r.Context(), s.cfg.RequestMaxDuration)
	defer cancel()
	resp, ae := c.call(upCtx, http.MethodGet, "/batches/"+url.PathEscape(id), nil, batchReadPass)
	if ae != nil {
		c.answerFailure(upCtx, ae)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusGone {
		c.vendorGone(resp, ownership.KindBatch, id)
		return
	}
	answer, ok := c.readAnswer(resp)
	if !ok {
		return
	}
	var head struct {
		Status string `json:"status"`
		Model  string `json:"model"`
		Usage  *usage `json:"usage"`
	}
	if json.Unmarshal(answer, &head) != nil {
		c.refuseAs(errUpstream, spool.StatusUpstreamErr, "upstream_invalid_response")
		return
	}
	// The vendor prices a batch once it is done, on the batch itself.
	if head.Status == "completed" && head.Usage != nil {
		c.settle(spool.EndpointBatchCost, ownership.KindBatch, id, head.Model, *head.Usage)
	}
	c.writeJSON(http.StatusOK, answer)
}

func (s *Server) deleteBatch(w http.ResponseWriter, r *http.Request, id string) {
	c := s.beginAsync(w, r, spool.EndpointBatchReads, snapshot.EndpointBatches)
	if c == nil {
		return
	}
	defer c.done()
	if !validObjectID(id) {
		c.refuse(errObjectNotFound, spool.StatusBadRequest)
		return
	}
	if !c.owns(ownership.KindBatch, id) {
		return
	}
	upCtx, cancel := context.WithTimeout(r.Context(), s.cfg.RequestMaxDuration)
	defer cancel()
	resp, ae := c.call(upCtx, http.MethodDelete, "/batches/"+url.PathEscape(id), nil, batchDeletePass)
	if ae != nil {
		c.answerFailure(upCtx, ae)
		return
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusNotFound, http.StatusGone:
		c.vendorGone(resp, ownership.KindBatch, id)
		return
	case http.StatusConflict:
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, rejectionReadLimit))
		c.refuse(errBatchNotFinished, spool.StatusBadRequest)
		return
	}
	answer, ok := c.readAnswer(resp)
	if !ok {
		return
	}
	c.release(ownership.KindBatch, id, ownership.ReleaseDeleted)
	c.writeJSON(http.StatusOK, answer)
}

// listBatches answers the vendor's list filtered to this key's batches. The
// vendor lists the whole workspace, every key of the business account, so a
// page is read, its ids asked of the ledger, and only the owned entries kept.
// Further vendor pages are read, up to batchListMaxPages, to fill the page.
// The cursor handed back is always one of this key's own ids: the vendor's
// cursor is a batch id, and passing it on would name another key's batch.
func (s *Server) listBatches(w http.ResponseWriter, r *http.Request) {
	c := s.beginAsync(w, r, spool.EndpointBatchReads, snapshot.EndpointBatches)
	if c == nil {
		return
	}
	defer c.done()
	q := r.URL.Query()
	limit := 20
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			c.refuse(errInvalidParamValue("limit"), spool.StatusBadRequest)
			return
		}
		limit = min(n, batchListMaxLimit)
	}
	vendor := url.Values{}
	vendor.Set("limit", strconv.Itoa(limit))
	// Only the parameters whose meaning this route knows go on; the rest are
	// dropped rather than forwarded unread.
	for _, name := range []string{"status", "created_after", "created_before"} {
		v := q.Get(name)
		if v == "" {
			continue
		}
		if !validObjectID(v) {
			c.refuse(errInvalidParamValue(name), spool.StatusBadRequest)
			return
		}
		vendor.Set(name, v)
	}
	if after := q.Get("after"); after != "" {
		// A cursor this key does not own is refused before the vendor sees
		// it: how the vendor answers another key's id would tell whether it
		// exists. Owned and absent are refused alike.
		owned := false
		if validObjectID(after) {
			got, err := s.ownership.Owned(r.Context(), c.key.KeyID, ownership.KindBatch, []string{after})
			if err != nil {
				c.refuse(errOwnershipUnavailable, spool.StatusUpstreamErr)
				return
			}
			owned = got[after]
		}
		if !owned {
			c.refuse(errInvalidParamValue("after"), spool.StatusBadRequest)
			return
		}
		vendor.Set("after", after)
	}
	upCtx, cancel := context.WithTimeout(r.Context(), s.cfg.RequestMaxDuration)
	defer cancel()
	kept := make([]json.RawMessage, 0, limit)
	var keptIDs []string
	more := false
	for page := 0; page < batchListMaxPages; page++ {
		resp, ae := c.call(upCtx, http.MethodGet, "/batches?"+vendor.Encode(), nil, nil)
		if ae != nil {
			c.answerFailure(upCtx, ae)
			return
		}
		answer, ok := c.readAnswer(resp)
		resp.Body.Close()
		if !ok {
			return
		}
		var listing struct {
			Data    []json.RawMessage `json:"data"`
			HasMore bool              `json:"has_more"`
		}
		if json.Unmarshal(answer, &listing) != nil {
			c.refuseAs(errUpstream, spool.StatusUpstreamErr, "upstream_invalid_response")
			return
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
			got, err := s.ownership.Owned(r.Context(), c.key.KeyID, ownership.KindBatch, ask)
			if err != nil {
				c.refuse(errOwnershipUnavailable, spool.StatusUpstreamErr)
				return
			}
			owned = got
		}
		full := false
		for i, item := range listing.Data {
			if ids[i] == "" || !owned[ids[i]] {
				continue
			}
			if len(kept) == limit {
				full = true
				break
			}
			kept = append(kept, item)
			keptIDs = append(keptIDs, ids[i])
		}
		if full {
			more = true
			break
		}
		if !listing.HasMore || len(listing.Data) == 0 || ids[len(ids)-1] == "" {
			break
		}
		if len(kept) == limit {
			// The page is full exactly at a vendor page boundary; whether more
			// of this key's batches follow is not known without reading on.
			more = true
			break
		}
		// Reading on from the vendor's own cursor is internal; it never
		// reaches the client.
		vendor.Set("after", ids[len(ids)-1])
		if page == batchListMaxPages-1 {
			more = len(kept) > 0
		}
	}
	out := struct {
		Object  string            `json:"object"`
		Data    []json.RawMessage `json:"data"`
		FirstID *string           `json:"first_id"`
		LastID  *string           `json:"last_id"`
		HasMore bool              `json:"has_more"`
	}{Object: "list", Data: kept}
	if len(keptIDs) > 0 {
		out.FirstID, out.LastID = &keptIDs[0], &keptIDs[len(keptIDs)-1]
	}
	// A next page needs a cursor of this key's own, so with nothing kept
	// there is none to offer.
	out.HasMore = more && len(keptIDs) > 0
	body, err := json.Marshal(out)
	if err != nil {
		c.refuseAs(errUpstream, spool.StatusUpstreamErr, "upstream_invalid_response")
		return
	}
	c.writeJSON(http.StatusOK, body)
}

// submitRefused reports whether the vendor definitely did not create the
// object: a refusal it answered, not a timeout, a dropped connection or a
// server error, after which the object may exist.
func submitRefused(ae *attemptError) bool {
	if ae.timeout || errors.Is(ae.err, context.DeadlineExceeded) {
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

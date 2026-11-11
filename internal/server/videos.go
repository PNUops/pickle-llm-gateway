package server

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/pnuops/pickle-llm-gateway/internal/ownership"
	"github.com/pnuops/pickle-llm-gateway/internal/snapshot"
	"github.com/pnuops/pickle-llm-gateway/internal/spool"
)

// The video route, /v1/videos, in the vendor's own shape. Neither the OpenAI
// API (its Videos API was shut down on 2026-09-24) nor the Anthropic API
// offers video generation, so there is no SDK shape to serve; the vendor's
// shape is passed through with this service's fences.
//
// A video job is fenced like an image request (the videos grant, the money
// fence on its model) and owned like a batch: an intent before the vendor
// call, a bind after it, and every later read asks the ledger first. The
// vendor has no cancel and no delete, so a submit cannot be undone: a bind
// that does not land answers the vendor's success and binds late. The vendor
// prices a job once it is over, on the job; that price is recorded once.
//
// The vendor's answers name its own addresses: a polling URL and download
// URLs that need the business account's vendor key. They are rewritten to this
// service's paths, and the signed keyless download links are dropped, so every
// read of a job's output goes through the ownership check.

var (
	videoSubmitPass  = successStatuses()
	videoReadPass    = map[int]bool{http.StatusNotFound: true, http.StatusGone: true}
	videoContentPass = map[int]bool{http.StatusNotFound: true, http.StatusGone: true,
		http.StatusConflict: true}

	errVideoNotReady = apiError{http.StatusConflict, "invalid_request_error", "video_not_ready",
		"영상이 아직 준비되지 않았어요. GET /v1/videos/{id}로 상태가 completed인지 확인한 뒤 다시 받아 주세요."}
)

func (s *Server) handleVideos(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/v1/videos" {
		if r.Method != http.MethodPost {
			writeAPIError(w, errMethod)
			return
		}
		s.submitVideo(w, r)
		return
	}
	rest := strings.TrimPrefix(r.URL.Path, "/v1/videos/")
	id, sub, _ := strings.Cut(rest, "/")
	if r.Method != http.MethodGet {
		writeAPIError(w, errMethod)
		return
	}
	switch sub {
	case "":
		s.readVideo(w, r, id)
	case "content":
		s.readVideoContent(w, r, id)
	default:
		writeAPIError(w, errNotFound)
	}
}

func (s *Server) submitVideo(w http.ResponseWriter, r *http.Request) {
	c := s.beginAsync(w, r, spool.EndpointVideos, snapshot.EndpointVideos)
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
	// Only the documented members travel. One this route does not read could
	// name another job or another model past the checks below.
	for name := range params {
		if !videoMembers[name] {
			c.refuse(errUnsupportedParam(boundedName(name)), spool.StatusBadRequest)
			return
		}
	}
	var publicModel string
	if json.Unmarshal(params["model"], &publicModel) != nil || publicModel == "" {
		c.refuse(errMissingParam("model"), spool.StatusBadRequest)
		return
	}
	// The vendor would post the result to an address this service never
	// sees, and with it the price; the owner polls here instead.
	if v, ok := params["callback_url"]; ok && string(v) != "null" {
		c.refuse(errUnsupportedValue("callback_url",
			"영상이 끝났는지는 GET /v1/videos/{id}로 확인해 주세요."), spool.StatusBadRequest)
		return
	}
	// Editing or extending a job reads that job's output, so it must be one
	// of this key's own; another key's job and no job answer alike.
	if v, ok := params["previous_job_id"]; ok && string(v) != "null" {
		var previous string
		if json.Unmarshal(v, &previous) != nil || !validObjectID(previous) {
			c.refuse(errInvalidParamValue("previous_job_id"), spool.StatusBadRequest)
			return
		}
		if !c.owns(ownership.KindVideo, previous) {
			return
		}
	}
	if c.fenceModel(publicModel, params, false) == nil {
		return
	}
	body, err := json.Marshal(params)
	if err != nil {
		c.refuse(errBadJSON, spool.StatusBadRequest)
		return
	}

	intentID, err := s.ownership.Intent(r.Context(), c.key.KeyID, ownership.KindVideo, c.ev.EventUUID)
	if err != nil {
		c.answerIntentFailure(err)
		return
	}
	upCtx, cancel := context.WithTimeout(r.Context(), s.cfg.RequestMaxDuration)
	defer cancel()
	resp, ae := c.call(upCtx, http.MethodPost, "/videos", body, videoSubmitPass)
	if ae != nil {
		if submitRefused(ae) {
			c.releaseIntent(ownership.KindVideo, intentID)
		} else {
			c.noRetry()
		}
		c.answerFailure(upCtx, ae)
		return
	}
	defer resp.Body.Close()
	c.noRetry()
	answer, ok := c.readAnswer(resp)
	if !ok {
		return
	}
	c.w.Header().Del("X-Should-Retry")
	id := objectIDOf(answer)
	if id == "" {
		// Accepted but unnamed: relayed as it came, since a 5xx would be
		// retried into a second billed job. The intent stays pending for
		// reconciliation.
		s.log.Error("video submit answer carries no usable id", "keyId", c.key.KeyID,
			"intentId", intentID)
		c.writeJSON(resp.StatusCode, answer)
		return
	}
	c.ev.Object = &spool.ObjectRef{Kind: string(ownership.KindVideo), IntentID: intentID, ObjectID: id}
	if err := s.ownership.Bind(r.Context(), c.key.KeyID, intentID, ownership.KindVideo, id,
		ownership.BindDetails{}); err != nil {
		code := ownership.RefusalCode(err)
		if code == ownership.CodeObjectTaken {
			s.log.Error("video bind refused: object taken", "keyId", c.key.KeyID,
				"intentId", intentID, "videoId", id)
			c.noRetry()
			c.refuse(errOwnershipUnavailable, spool.StatusUpstreamErr)
			return
		}
		s.log.Warn("video bind did not land; answering success and binding late",
			"keyId", c.key.KeyID, "intentId", intentID, "videoId", id, "code", code, "error", err)
		s.ownership.Remember(c.key.KeyID, ownership.KindVideo, id)
	}
	c.writeJSON(resp.StatusCode, rewriteVideoJob(answer, id))
}

func (s *Server) readVideo(w http.ResponseWriter, r *http.Request, id string) {
	c := s.beginAsync(w, r, spool.EndpointVideoReads, snapshot.EndpointVideos)
	if c == nil {
		return
	}
	defer c.done()
	if !validObjectID(id) {
		c.refuse(errObjectNotFound, spool.StatusBadRequest)
		return
	}
	if !c.owns(ownership.KindVideo, id) {
		return
	}
	upCtx, cancel := context.WithTimeout(r.Context(), s.cfg.RequestMaxDuration)
	defer cancel()
	resp, ae := c.call(upCtx, http.MethodGet, "/videos/"+url.PathEscape(id), nil, videoReadPass)
	if ae != nil {
		c.answerFailure(upCtx, ae)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusGone {
		c.vendorGone(resp, ownership.KindVideo, id)
		return
	}
	answer, ok := c.readAnswer(resp)
	if !ok {
		return
	}
	var head map[string]json.RawMessage
	if json.Unmarshal(answer, &head) != nil || head == nil {
		c.refuseAs(errUpstream, spool.StatusUpstreamErr, "upstream_invalid_response")
		return
	}
	var status string
	_ = json.Unmarshal(head["status"], &status)
	// The job's answer names no model, so the price is recorded against the
	// key alone; the submit's own event carries the model.
	if asyncTerminal(status) {
		c.settle(spool.EndpointVideoCost, ownership.KindVideo, id, "", head["usage"])
	}
	c.writeJSON(http.StatusOK, rewriteVideoJob(answer, id))
}

// readVideoContent relays a finished job's video bytes, streamed rather than
// held, under the passthrough response bound.
func (s *Server) readVideoContent(w http.ResponseWriter, r *http.Request, id string) {
	c := s.beginAsync(w, r, spool.EndpointVideoReads, snapshot.EndpointVideos)
	if c == nil {
		return
	}
	defer c.done()
	if !validObjectID(id) {
		c.refuse(errObjectNotFound, spool.StatusBadRequest)
		return
	}
	path := "/videos/" + url.PathEscape(id) + "/content"
	if v := r.URL.Query().Get("index"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 || n > 64 {
			c.refuse(errInvalidParamValue("index"), spool.StatusBadRequest)
			return
		}
		path += "?index=" + strconv.Itoa(n)
	}
	if !c.owns(ownership.KindVideo, id) {
		return
	}
	upCtx, cancel := context.WithTimeout(r.Context(), s.cfg.RequestMaxDuration)
	defer cancel()
	resp, ae := c.call(upCtx, http.MethodGet, path, nil, videoContentPass)
	if ae != nil {
		c.answerFailure(upCtx, ae)
		return
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusNotFound, http.StatusGone:
		// Not released: a job whose output is gone may still answer its
		// status, and the status read is where the ledger heals.
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, rejectionReadLimit))
		c.refuse(errObjectNotFound, spool.StatusBadRequest)
		return
	case http.StatusConflict:
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, rejectionReadLimit))
		c.refuse(errVideoNotReady, spool.StatusBadRequest)
		return
	}
	limit := s.cfg.PassthroughResponseMaxBytes
	if resp.ContentLength > limit {
		c.refuse(errPassthroughResponseTooLarge, spool.StatusUpstreamErr)
		return
	}
	contentType := resp.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "video/mp4"
	}
	w.Header().Set("Content-Type", contentType)
	if resp.ContentLength >= 0 {
		w.Header().Set("Content-Length", strconv.FormatInt(resp.ContentLength, 10))
	}
	w.WriteHeader(http.StatusOK)
	size := resp.ContentLength
	if size < 0 {
		size = limit
	}
	var copied int64
	var cerr error
	s.writeUnderDeadline(w, int(min(size, int64(^uint(0)>>1))), func() {
		copied, cerr = io.Copy(w, io.LimitReader(resp.Body, limit+1))
	})
	switch {
	case cerr != nil && r.Context().Err() != nil:
		c.ev.Status = spool.StatusCanceled
		c.ev.ErrorType = "client_disconnected"
	case cerr != nil:
		c.ev.Status = spool.StatusUpstreamErr
		c.ev.ErrorType = "upstream_read_failed"
	case copied > limit:
		// The headers are gone; a cut-short body is the only signal left.
		c.ev.Status = spool.StatusUpstreamErr
		c.ev.ErrorType = "response_too_large"
	default:
		c.ev.Status = spool.StatusOK
	}
	c.record()
}

// videoMembers are the vendor's documented submit members.
var videoMembers = map[string]bool{
	"model": true, "prompt": true, "duration": true, "resolution": true, "aspect_ratio": true,
	"size": true, "frame_images": true, "input_references": true, "generate_audio": true,
	"seed": true, "previous_job_id": true, "provider": true, "callback_url": true,
	"session_id": true, "user": true, "trace": true,
}

// videoJobMembers are the job members relayed to the client, read from the
// vendor's documented job object; anything else, a signed link included, is
// left out.
var videoJobMembers = []string{"id", "generation_id", "status", "error", "expires_at", "usage"}

// rewriteVideoJob rebuilds a job answer from its documented members and
// points its addresses at this service: the polling URL at GET
// /v1/videos/{id} and each download at its content path with the same index.
// The vendor's signed keyless links (`urls`) are left out, since they would
// read the output without the ownership check. An answer that is not an
// object is returned as it came.
func rewriteVideoJob(answer []byte, id string) []byte {
	var job map[string]json.RawMessage
	if json.Unmarshal(answer, &job) != nil || job == nil {
		return answer
	}
	out := make(map[string]json.RawMessage, len(videoJobMembers)+2)
	for _, name := range videoJobMembers {
		if v, ok := job[name]; ok {
			out[name] = v
		}
	}
	base := "/v1/videos/" + url.PathEscape(id)
	out["polling_url"], _ = json.Marshal(base)
	var unsigned []json.RawMessage
	if json.Unmarshal(job["unsigned_urls"], &unsigned) == nil && unsigned != nil {
		links := make([]string, len(unsigned))
		for i := range unsigned {
			links[i] = base + "/content?index=" + strconv.Itoa(i)
		}
		out["unsigned_urls"], _ = json.Marshal(links)
	}
	rewritten, err := json.Marshal(out)
	if err != nil {
		return answer
	}
	return rewritten
}

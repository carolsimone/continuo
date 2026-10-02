package http

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/carolsimone/continuo/release-controller/service/handlers"
)

// ReceiveVerificationRequest is the JSON body of POST /verification-runs. It
// maps one to one onto handlers.ReceiveVerificationInput; the application
// layer never sees the wire field names.
type ReceiveVerificationRequest struct {
	RunID             string `json:"run_id"`
	Service           string `json:"service"`
	ImageTag          string `json:"image_tag"`
	Kind              string `json:"kind"`
	VerifiesReleaseID string `json:"verifies_release_id"`
	Attempt           int    `json:"attempt"`
	SourceOverlayURI  string `json:"source_overlay_uri"`
}

func (b ReceiveVerificationRequest) toInput() handlers.ReceiveVerificationInput {
	return handlers.ReceiveVerificationInput{
		RunID:             b.RunID,
		Service:           b.Service,
		ImageTag:          b.ImageTag,
		Kind:              b.Kind,
		VerifiesReleaseID: b.VerifiesReleaseID,
		Attempt:           b.Attempt,
		SourceOverlayURI:  b.SourceOverlayURI,
	}
}

func (s *Server) handleReceiveVerification(w http.ResponseWriter, r *http.Request) {
	var body ReceiveVerificationRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}
	in := body.toInput()
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	if err := handlers.ReceiveVerification(ctx, s.deps, in); err != nil {
		switch {
		case errors.Is(err, handlers.ErrInvalidInput):
			http.Error(w, err.Error(), http.StatusBadRequest)
		case errors.Is(err, handlers.ErrRunKindConflict):
			http.Error(w, err.Error(), http.StatusConflict)
		default:
			s.log.Error("receive verification failed", "run_id", in.RunID, "error", err)
			http.Error(w, "internal error", http.StatusInternalServerError)
		}
		return
	}
	if err := handlers.AdvanceQueue(ctx, s.deps); err != nil {
		s.log.Warn("advance queue after verification receive", "error", err)
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(map[string]string{"run_id": in.RunID, "status": "received"})
}

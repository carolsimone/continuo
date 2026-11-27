package http

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/carolsimone/continuo/release-controller/service/handlers"
)

// ReceiveFixRequest is the JSON body of POST /release-checks. It
// maps one to one onto handlers.ReceiveFixInput; the application
// layer never sees the wire field names.
type ReceiveFixRequest struct {
	CheckID           string `json:"check_id"`
	Project           string `json:"project"`
	ImageTag          string `json:"image_tag"`
	Kind              string `json:"kind"`
	VerifiesReleaseID string `json:"verifies_release_id"`
	RemediationRound  int    `json:"remediation_round"`
	Try               int    `json:"try"`
	SourceOverlayURI  string `json:"source_overlay_uri"`
}

func (b ReceiveFixRequest) toInput() handlers.ReceiveFixInput {
	return handlers.ReceiveFixInput{
		CheckID:           b.CheckID,
		Project:           b.Project,
		ImageTag:          b.ImageTag,
		Kind:              b.Kind,
		VerifiesReleaseID: b.VerifiesReleaseID,
		Round:             b.RemediationRound,
		Try:               b.Try,
		SourceOverlayURI:  b.SourceOverlayURI,
	}
}

func (s *Server) handleReceiveFix(w http.ResponseWriter, r *http.Request) {
	var body ReceiveFixRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}
	in := body.toInput()
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	if err := handlers.ReceiveFix(ctx, s.deps, in); err != nil {
		switch {
		case errors.Is(err, handlers.ErrInvalidInput):
			http.Error(w, err.Error(), http.StatusBadRequest)
		case errors.Is(err, handlers.ErrCheckKindConflict):
			http.Error(w, err.Error(), http.StatusConflict)
		default:
			s.log.Error("receive fix's release check failed", "check_id", in.CheckID, "error", err)
			http.Error(w, "internal error", http.StatusInternalServerError)
		}
		return
	}
	if err := handlers.AdvanceQueue(ctx, s.deps); err != nil {
		s.log.Warn("advance queue after receiving a fix's release check", "error", err)
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(map[string]string{"check_id": in.CheckID, "status": "received"})
}

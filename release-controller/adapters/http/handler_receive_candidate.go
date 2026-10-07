package http

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/carolsimone/continuo/pkg/maintenance"
	"github.com/carolsimone/continuo/release-controller/service/handlers"
)

// ReceiveCandidateRequest is the JSON body of POST /releases. It maps one to
// one onto handlers.ReceiveCandidateInput; the application layer never sees
// the wire field names.
type ReceiveCandidateRequest struct {
	Service           string `json:"service"`
	ReleaseID         string `json:"release_id"`
	ImageTag          string `json:"image_tag"`
	Bootstrap         bool   `json:"bootstrap"`
	Repo              string `json:"repo"`
	CommitSHA         string `json:"commit_sha"`
	Kind              string `json:"kind"`
	Shadow            bool   `json:"shadow"`
	SourceOverlayURI  string `json:"source_overlay_uri"`
	VerifiesReleaseID string `json:"verifies_release_id"`
}

func (b ReceiveCandidateRequest) toInput() handlers.ReceiveCandidateInput {
	return handlers.ReceiveCandidateInput{
		Service:           b.Service,
		ReleaseID:         b.ReleaseID,
		ImageTag:          b.ImageTag,
		Bootstrap:         b.Bootstrap,
		Repo:              b.Repo,
		CommitSHA:         b.CommitSHA,
		Kind:              b.Kind,
		Shadow:            b.Shadow,
		SourceOverlayURI:  b.SourceOverlayURI,
		VerifiesReleaseID: b.VerifiesReleaseID,
	}
}

func (s *Server) handleReceiveCandidate(w http.ResponseWriter, r *http.Request) {
	var body ReceiveCandidateRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}
	in := body.toInput()
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	if err := handlers.ReceiveCandidate(ctx, s.deps, in); err != nil {
		if errors.Is(err, maintenance.ErrActive) {
			writeMaintenance(w)
			return
		}
		if errors.Is(err, handlers.ErrRunKindConflict) || errors.Is(err, handlers.ErrReleaseIDConflict) {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		if errors.Is(err, handlers.ErrInvalidCandidate) {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		// A storage or timeout failure is not the caller's fault and its text
		// names internals, so it is logged here and answered generically.
		s.log.Error("receive candidate", "release_id", in.ReleaseID, "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	// Attempt to advance the queue immediately. This is a safe no-op when a
	// release is already active or the queue remains empty.
	if err := handlers.AdvanceQueue(ctx, s.deps); err != nil {
		s.log.Warn("advance queue after receive", "error", err)
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(map[string]string{"release_id": in.ReleaseID, "status": "received"})
}

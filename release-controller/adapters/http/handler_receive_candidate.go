package http

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/carolsimone/continuo/release-controller/service/handlers"
)

func (s *Server) handleReceiveCandidate(w http.ResponseWriter, r *http.Request) {
	var in handlers.ReceiveCandidateInput
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	if err := handlers.ReceiveCandidate(ctx, s.deps, in); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
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

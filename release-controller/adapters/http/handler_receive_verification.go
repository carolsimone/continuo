package http

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/carolsimone/continuo/release-controller/service/handlers"
)

func (s *Server) handleReceiveVerification(w http.ResponseWriter, r *http.Request) {
	var in handlers.ReceiveVerificationInput
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}
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

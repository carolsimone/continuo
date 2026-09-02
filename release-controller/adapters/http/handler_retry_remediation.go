package http

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/carolsimone/continuo/release-controller/domain/pipeline"
	"github.com/carolsimone/continuo/release-controller/service/handlers"
)

// handleRetryRemediation starts another remediation round on a rejected
// release. A refusal answers 409 with a machine-readable reason so the UI can
// say what to do instead — open the proposal, wait for the round already in
// flight, push a new commit — rather than retry. A failed read of remediation
// proposals answers 502; any other failure answers 500.
func (s *Server) handleRetryRemediation(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	res, err := handlers.RetryRemediation(ctx, s.deps, r.PathValue("id"))
	w.Header().Set("Content-Type", "application/json")
	switch {
	case err == nil:
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]any{"release_id": res.ReleaseID, "remediation_round": res.RemediationRound})
	case errors.Is(err, handlers.ErrReleaseNotFound):
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "not_found"})
	case errors.Is(err, pipeline.ErrNotRejected):
		writeRefusal(w, "not_rejected", nil)
	case errors.Is(err, handlers.ErrNotHealable):
		writeRefusal(w, "not_healable", nil)
	case errors.Is(err, handlers.ErrNotRetryable):
		writeRefusal(w, "not_retryable", nil)
	case errors.Is(err, pipeline.ErrRoundsExhausted):
		writeRefusal(w, "rounds_exhausted", nil)
	case errors.Is(err, handlers.ErrRetryInProgress):
		writeRefusal(w, "retry_in_progress", nil)
	case errors.Is(err, handlers.ErrProposalReaderUnavailable):
		s.log.Warn("retry remediation", "release", r.PathValue("id"), "error", err)
		w.WriteHeader(http.StatusBadGateway)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "proposal_reader_unavailable"})
	default:
		var open handlers.ErrProposalOpen
		if errors.As(err, &open) {
			writeRefusal(w, "proposal_open", map[string]string{"proposal_id": open.ProposalID, "pr_url": open.PRURL})
			return
		}
		s.log.Warn("retry remediation", "release", r.PathValue("id"), "error", err)
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "internal"})
	}
}

func writeRefusal(w http.ResponseWriter, reason string, extra map[string]string) {
	body := map[string]string{"error": reason}
	for k, v := range extra {
		if v != "" {
			body[k] = v
		}
	}
	w.WriteHeader(http.StatusConflict)
	_ = json.NewEncoder(w).Encode(body)
}

package http

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/carolsimone/continuo/release-controller/domain/pipeline"
	"github.com/carolsimone/continuo/release-controller/domain/repository"
)

type verificationRunSummary struct {
	RunID       string `json:"run_id"`
	Status      string `json:"status"`
	Service     string `json:"service"`
	Attempt     int    `json:"attempt"`
	CreatedAt   string `json:"created_at"`
	ActivatedAt string `json:"activated_at"`
	FinishedAt  string `json:"finished_at"`
	FailReason  string `json:"fail_reason,omitempty"`
}

// handleListVerificationRuns lists the verification runs that verify one
// release, newest first. `verifies` is required: the listing exists for the
// release page, and a global list of verification runs is not a read anyone
// needs. Unpaginated: bounded by rounds × attempts × edited services.
func (s *Server) handleListVerificationRuns(w http.ResponseWriter, r *http.Request) {
	verifies := r.URL.Query().Get("verifies")
	if verifies == "" {
		http.Error(w, "verifies query param is required", http.StatusBadRequest)
		return
	}
	kind := pipeline.KindVerification
	u := s.deps.NewUoW()
	items, _, err := u.RunRepo().List(r.Context(), repository.ListFilter{Kind: &kind, VerifiesReleaseID: &verifies, Limit: 100})
	if err != nil {
		s.log.Error("list verification runs failed", "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	out := make([]verificationRunSummary, 0, len(items))
	for _, run := range items {
		activated, aok := run.ActivatedAt()
		finished, fok := run.FinishedAt()
		out = append(out, verificationRunSummary{
			RunID: run.ID(), Status: string(run.Status()), Service: run.ChangedService(), Attempt: run.Attempt(),
			CreatedAt: run.CreatedAt().UTC().Format(time.RFC3339), ActivatedAt: rfc3339OrEmpty(activated, aok),
			FinishedAt: rfc3339OrEmpty(finished, fok), FailReason: run.FailReason(),
		})
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"runs": out})
}

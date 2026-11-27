package http

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/carolsimone/continuo/release-controller/domain/releasecheck"
	"github.com/carolsimone/continuo/release-controller/domain/repository"
)

type releaseCheckSummary struct {
	CheckID     string `json:"check_id"`
	Status      string `json:"status"`
	Project     string `json:"project"`
	Round       int    `json:"round"`
	Try         int    `json:"try"`
	CreatedAt   string `json:"created_at"`
	ActivatedAt string `json:"activated_at"`
	FinishedAt  string `json:"finished_at"`
	FailReason  string `json:"fail_reason,omitempty"`
}

// handleListReleaseChecks lists the release checks of the fixes that verify
// one release, newest first. `verifies` is required: the listing exists for
// the release page, and a global list of the release checks of fixes is not a
// read anyone needs. Unpaginated: bounded by rounds × tries × edited projects.
func (s *Server) handleListReleaseChecks(w http.ResponseWriter, r *http.Request) {
	verifies := r.URL.Query().Get("verifies")
	if verifies == "" {
		http.Error(w, "verifies query param is required", http.StatusBadRequest)
		return
	}
	kind := releasecheck.KindFix
	u := s.deps.NewUoW()
	items, _, err := u.ReleaseCheckRepo().List(r.Context(), repository.ListFilter{Kind: &kind, VerifiesReleaseID: &verifies, Limit: 100})
	if err != nil {
		s.log.Error("list release checks failed", "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	out := make([]releaseCheckSummary, 0, len(items))
	for _, check := range items {
		activated, aok := check.ActivatedAt()
		finished, fok := check.FinishedAt()
		out = append(out, releaseCheckSummary{
			CheckID: check.ID(), Status: string(check.Status()), Project: check.ChangedProject(),
			Round: check.Round(), Try: check.Try(),
			CreatedAt: check.CreatedAt().UTC().Format(time.RFC3339), ActivatedAt: rfc3339OrEmpty(activated, aok),
			FinishedAt: rfc3339OrEmpty(finished, fok), FailReason: check.FailReason(),
		})
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"checks": out})
}

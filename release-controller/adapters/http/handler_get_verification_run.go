package http

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/carolsimone/continuo/release-controller/domain/pipeline"
)

// rfc3339OrEmpty formats t as RFC3339 when ok, or "" when the timestamp has
// not happened yet (a run that has not activated or has not finished).
func rfc3339OrEmpty(t time.Time, ok bool) string {
	if !ok {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}

// verificationRunResponse builds the JSON object returned by
// GET /verification-runs/{id}. per_node_results keeps the release shape so
// the UI renders both with one table.
func verificationRunResponse(r *pipeline.Run) map[string]any {
	activated, aok := r.ActivatedAt()
	finished, fok := r.FinishedAt()
	return map[string]any{
		"run_id":              r.ID(),
		"status":              string(r.Status()),
		"changed_service":     r.ChangedService(),
		"verifies_release_id": r.VerifiesReleaseID(),
		"attempt":             r.Attempt(),
		"created_at":          r.CreatedAt().UTC().Format(time.RFC3339),
		"activated_at":        rfc3339OrEmpty(activated, aok),
		"finished_at":         rfc3339OrEmpty(finished, fok),
		"transitions":         r.Transitions(),
		"validation_node_ids": r.ValidationNodeIDs(),
		"failing_nodes":       r.FailingNodes(),
		"fail_reason":         r.FailReason(),
		"fail_detail":         r.FailDetail(),
		"per_node_results":    r.PerNodeResults(),
		"image_tags":          r.ImageTags(),
		"manifest_kind":       string(r.ManifestKind()),
	}
}

func (s *Server) handleGetVerificationRun(w http.ResponseWriter, r *http.Request) {
	u := s.deps.NewUoW()
	run, err := u.RunRepo().Get(r.Context(), r.PathValue("id"))
	if err != nil || run == nil || run.Kind() != pipeline.KindVerification {
		http.Error(w, "verification run not found", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(verificationRunResponse(run))
}

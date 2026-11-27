package http

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/carolsimone/continuo/release-controller/adapters/serialization"
	"github.com/carolsimone/continuo/release-controller/domain/releasecheck"
)

// rfc3339OrEmpty formats t as RFC3339 when ok, or "" when the timestamp has
// not happened yet (a check that has not activated or has not finished).
func rfc3339OrEmpty(t time.Time, ok bool) string {
	if !ok {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}

// releaseCheckResponse builds the JSON object returned by
// GET /release-checks/{id}. per_node_results keeps the release shape so
// the UI renders both with one table, and the parse timings are named as a
// release names them.
func releaseCheckResponse(r *releasecheck.Check) map[string]any {
	activated, aok := r.ActivatedAt()
	finished, fok := r.FinishedAt()
	body := map[string]any{
		"check_id":            r.ID(),
		"status":              string(r.Status()),
		"changed_project":     r.ChangedProject(),
		"verifies_release_id": r.VerifiesReleaseID(),
		"remediation_round":   r.RemediationRound(),
		"try":                 r.Try(),
		"created_at":          r.CreatedAt().UTC().Format(time.RFC3339),
		"activated_at":        rfc3339OrEmpty(activated, aok),
		"finished_at":         rfc3339OrEmpty(finished, fok),
		"transitions":         serialization.TransitionsFromDomain(r.Transitions()),
		"validation_node_ids": r.ValidationNodeIDs(),
		"failing_nodes":       r.FailingNodes(),
		"fail_reason":         r.FailReason(),
		"fail_detail":         r.FailDetail(),
		"per_node_results":    serialization.NodeValidationResultsFromDomain(r.PerNodeResults()),
		"image_tags":          r.ImageTags(),
		"manifest_kind":       string(r.ManifestKind()),
	}
	addParseTimings(body, r.ParseTimings())
	return body
}

// handleGetReleaseCheck answers GET /release-checks/{id} with a fix's release
// check. A candidate release's check is read at GET /releases/{id}, so an id
// of a candidate's check answers 404 here.
func (s *Server) handleGetReleaseCheck(w http.ResponseWriter, r *http.Request) {
	u := s.deps.NewUoW()
	check, err := u.ReleaseCheckRepo().Get(r.Context(), r.PathValue("id"))
	if err != nil || check == nil || check.Kind() != releasecheck.KindFix {
		http.Error(w, "no fix's release check has this id (a candidate release's check is at GET /releases/{id})", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(releaseCheckResponse(check))
}

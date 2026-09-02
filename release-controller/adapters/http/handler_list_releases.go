package http

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/carolsimone/continuo/release-controller/domain/pipeline"
	"github.com/carolsimone/continuo/release-controller/domain/repository"
)

type releaseListItem struct {
	ReleaseID    string  `json:"release_id"`
	Status       string  `json:"status"`
	CreatedAt    string  `json:"created_at"`
	ResolvedAt   *string `json:"resolved_at"`
	NodeCount    int     `json:"node_count"`
	Bootstrap    bool    `json:"bootstrap"`
	Shadow       bool    `json:"shadow"`
	RejectReason string  `json:"reject_reason,omitempty"`
	Repo         string  `json:"repo"`
	CommitSHA    string  `json:"commit_sha"`
}

// handleListReleases returns paginated release history, newest-first.
// Query params: status (optional), limit (optional), cursor (optional).
func (s *Server) handleListReleases(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	var statusPtr *string
	if st := q.Get("status"); st != "" {
		statusPtr = &st
	}
	// limit is best-effort: an unparseable, non-positive, or out-of-range value
	// falls back to the default page size, which the repository clamps (see
	// RunRepository.List). Only the cursor is strictly validated, since a
	// malformed cursor is an unambiguous client error.
	limit := 0
	if n, err := strconv.Atoi(q.Get("limit")); err == nil {
		limit = n
	}
	cursor, err := decodeCursor(q.Get("cursor"))
	if err != nil {
		http.Error(w, "invalid cursor", http.StatusBadRequest)
		return
	}

	u := s.deps.NewUoW()
	items, next, err := u.RunRepo().List(r.Context(), repository.ListFilter{
		Status: statusPtr, Limit: limit, Cursor: cursor,
	})
	if err != nil {
		s.log.Error("list releases failed", "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	out := make([]releaseListItem, 0, len(items))
	for _, rel := range items {
		out = append(out, toReleaseListItem(rel))
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"releases":    out,
		"next_cursor": encodeCursor(next),
	})
}

// toReleaseListItem projects a release aggregate onto the JSON list row. repo
// and commit_sha are carried so the UI can resolve the commit author for the
// Releases tab without a per-release detail fetch.
func toReleaseListItem(rel *pipeline.Run) releaseListItem {
	var resolved *string
	if t := resolvedAt(rel); t != nil {
		resolvedStr := t.UTC().Format(time.RFC3339)
		resolved = &resolvedStr
	}
	return releaseListItem{
		ReleaseID:    rel.ID(),
		Status:       string(rel.Status()),
		CreatedAt:    rel.CreatedAt().UTC().Format(time.RFC3339),
		ResolvedAt:   resolved,
		NodeCount:    len(rel.CandidateTopology()),
		Bootstrap:    rel.IsBootstrap(),
		Shadow:       rel.Kind() == pipeline.KindVerification,
		RejectReason: rel.FailReason(),
		Repo:         rel.Repo(),
		CommitSHA:    rel.CommitSHA(),
	}
}

// resolvedAt returns the timestamp of the terminal transition, or nil if the
// run has not resolved. The resolved_at column is not persisted, so it is
// derived from the transition history.
func resolvedAt(rel *pipeline.Run) *time.Time {
	ts := rel.Transitions()
	for i := len(ts) - 1; i >= 0; i-- {
		if ts[i].To.IsTerminal() {
			at := ts[i].At
			return &at
		}
	}
	return nil
}

package http

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/domain/repository"
)

type releaseListItem struct {
	ReleaseID    string  `json:"release_id"`
	Status       string  `json:"status"`
	CreatedAt    string  `json:"created_at"`
	ResolvedAt   *string `json:"resolved_at"`
	NodeCount    int     `json:"node_count"`
	Bootstrap    bool    `json:"bootstrap"`
	RejectReason string  `json:"reject_reason,omitempty"`
}

// handleListReleases returns paginated release history, newest-first.
// Query params: status (optional), limit (optional), cursor (optional).
func (s *Server) handleListReleases(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	var statusPtr *string
	if st := q.Get("status"); st != "" {
		statusPtr = &st
	}
	limit := 20
	if l := q.Get("limit"); l != "" {
		n, err := strconv.Atoi(l)
		if err != nil || n <= 0 {
			http.Error(w, "invalid limit", http.StatusBadRequest)
			return
		}
		limit = n
	}
	cursor, err := decodeCursor(q.Get("cursor"))
	if err != nil {
		http.Error(w, "invalid cursor", http.StatusBadRequest)
		return
	}

	u := s.deps.NewUoW()
	items, next, err := u.ReleaseRepo().List(r.Context(), repository.ListFilter{
		Status: statusPtr, Limit: limit, Cursor: cursor,
	})
	if err != nil {
		s.log.Error("list releases failed", "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	out := make([]releaseListItem, 0, len(items))
	for _, rel := range items {
		var resolved *string
		if t := resolvedAt(rel); t != nil {
			resolvedStr := t.UTC().Format(time.RFC3339)
			resolved = &resolvedStr
		}
		out = append(out, releaseListItem{
			ReleaseID:    rel.ID(),
			Status:       string(rel.Status()),
			CreatedAt:    rel.CreatedAt().UTC().Format(time.RFC3339),
			ResolvedAt:   resolved,
			NodeCount:    len(rel.CandidateTopology()),
			Bootstrap:    rel.IsBootstrap(),
			RejectReason: rel.RejectReason(),
		})
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"releases":    out,
		"next_cursor": encodeCursor(next),
	})
}

// resolvedAt returns the timestamp of the terminal transition, or nil if the
// release has not resolved. The resolved_at column is not persisted, so it is
// derived from the transition history.
func resolvedAt(rel *release.Release) *time.Time {
	ts := rel.Transitions()
	for i := len(ts) - 1; i >= 0; i-- {
		switch ts[i].To {
		case release.StatusPromoted, release.StatusRejected, release.StatusSuperseded:
			at := ts[i].At
			return &at
		}
	}
	return nil
}

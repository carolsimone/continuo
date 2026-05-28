package http

import (
	"encoding/json"
	"net/http"
)

func (s *Server) handleGetRelease(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	// ReleaseRepo() on the UoW uses the connection pool when no tx is active,
	// making this a clean read-only path without an explicit transaction.
	rel, err := s.deps.UoW.ReleaseRepo().Get(r.Context(), id)
	if err != nil || rel == nil {
		http.Error(w, "release not found", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"release_id":          rel.ID(),
		"status":              string(rel.Status()),
		"transitions":         rel.Transitions(),
		"validation_node_ids": rel.ValidationNodeIDs(),
		"reject_reason":       rel.RejectReason(),
		"failing_nodes":       rel.FailingNodes(),
	})
}

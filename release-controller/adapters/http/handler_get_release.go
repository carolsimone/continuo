package http

import (
	"encoding/json"
	"net/http"
)

func (s *Server) handleGetRelease(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	// Each request gets its own UoW; no Begin is called — the repo uses the
	// connection pool directly for this read-only path.
	u := s.deps.NewUoW()
	rel, err := u.ReleaseRepo().Get(r.Context(), id)
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
		"per_node_results":    rel.PerNodeResults(),
		"image_tags":          rel.ImageTags(),
		"manifests_uri":       rel.ManifestsURI(),
		"bootstrap":           rel.IsBootstrap(),
	})
}

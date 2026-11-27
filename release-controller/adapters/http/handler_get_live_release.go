package http

import (
	"encoding/json"
	"net/http"
)

func (s *Server) handleGetLiveRelease(w http.ResponseWriter, r *http.Request) {
	// Each request gets its own UoW; no Begin is called — the repo uses the
	// connection pool directly for this read-only path.
	u := s.deps.NewUoW()
	cp, err := u.LiveReleaseRepo().Get(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"live_release_id": cp.ReleaseID(),
		"node_count":      cp.Topology().NodeCount,
		"updated_at":      cp.UpdatedAt(),
	})
}

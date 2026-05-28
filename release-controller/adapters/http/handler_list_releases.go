package http

import (
	"encoding/json"
	"net/http"
)

// handleListReleases returns the active release (if any) and the oldest queued
// release (if any). Full history and pagination are deferred until UI needs them.
func (s *Server) handleListReleases(w http.ResponseWriter, r *http.Request) {
	active, _ := s.deps.UoW.ReleaseRepo().ActiveRelease(r.Context())
	next, _ := s.deps.UoW.ReleaseRepo().NextQueuedRelease(r.Context())

	out := map[string]any{"active": nil, "next_queued": nil}
	if active != nil {
		out["active"] = map[string]string{"release_id": active.ID(), "status": string(active.Status())}
	}
	if next != nil {
		out["next_queued"] = map[string]string{"release_id": next.ID(), "status": string(next.Status())}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}

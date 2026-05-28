package http

import (
	"encoding/json"
	"net/http"
)

func (s *Server) handleGetCurrentProd(w http.ResponseWriter, r *http.Request) {
	// Each request gets its own UoW; no Begin is called — the repo uses the
	// connection pool directly for this read-only path.
	u := s.deps.NewUoW()
	cp, err := u.CurrentProdRepo().Get(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"current_prod_release_id": cp.ReleaseID(),
		"node_count":              len(cp.TopologySnapshot()),
		"updated_at":              cp.UpdatedAt(),
	})
}

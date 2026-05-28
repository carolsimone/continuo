package http

import (
	"encoding/json"
	"net/http"
)

func (s *Server) handleGetCurrentProd(w http.ResponseWriter, r *http.Request) {
	cp, err := s.deps.UoW.CurrentProdRepo().Get(r.Context())
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

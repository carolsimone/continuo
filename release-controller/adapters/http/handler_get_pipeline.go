package http

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/carolsimone/continuo/release-controller/domain/pipeline"
	"github.com/carolsimone/continuo/release-controller/service/handlers"
)

func (s *Server) handleGetPipeline(w http.ResponseWriter, r *http.Request) {
	p, err := handlers.GetPipeline(r.Context(), s.deps)
	if err != nil {
		s.log.Error("get pipeline failed", "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	var active any
	if p.Active != nil {
		a := map[string]any{
			"run_id":   p.Active.RunID,
			"run_kind": string(p.Active.Kind),
			"status":   string(p.Active.Status),
			"service":  p.Active.Service,
			"since":    p.Active.Since.UTC().Format(time.RFC3339),
		}
		if p.Active.Kind == pipeline.KindVerification {
			a["verifies_release_id"] = p.Active.VerifiesReleaseID
			a["attempt"] = p.Active.Attempt
		}
		active = a
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"active": active})
}

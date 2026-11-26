package http

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/carolsimone/continuo/release-controller/domain/releasecheck"
	"github.com/carolsimone/continuo/release-controller/service/handlers"
)

func (s *Server) handleGetReleaseQueue(w http.ResponseWriter, r *http.Request) {
	p, err := handlers.GetReleaseQueue(r.Context(), s.deps)
	if err != nil {
		s.log.Error("get release queue failed", "error", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	var active any
	if p.Active != nil {
		a := map[string]any{
			"check_id":   p.Active.CheckID,
			"check_kind": string(p.Active.Kind),
			"status":     string(p.Active.Status),
			"project":    p.Active.Project,
			"since":      p.Active.Since.UTC().Format(time.RFC3339),
		}
		if p.Active.Kind == releasecheck.KindFix {
			a["verifies_release_id"] = p.Active.VerifiesReleaseID
			a["attempt"] = p.Active.Attempt
		}
		active = a
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"active": active})
}

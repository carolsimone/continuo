package handlers

import "github.com/carolsimone/continuo/release-controller/domain/release"

// NodeResult is the inbound wire shape of one per-node result carried by the
// compile and seed-build aggregate events (validation.completed:v1 no longer
// carries per-node content — it is projected via validation.node.result:v1). It
// is kept separate from the domain value object release.NodeValidationResult so
// the transport shape stays decoupled from the domain; the handlers map
// NodeResult → release.NodeValidationResult before recording it.
type NodeResult struct {
	NodeID        string `json:"node_id"`
	Status        string `json:"status"` // "ok" or "failed"
	DBTLogURI     string `json:"dbt_log_uri,omitempty"`
	RunResultsURI string `json:"run_results_uri,omitempty"`
	DurationMS    int64  `json:"duration_ms,omitempty"`
}

// stageResults converts the inbound per-node wire results of a compile or
// seed-build leg into the domain value objects recorded on the release, and
// derives the failing-node set in the same pass. The validation leg keeps its
// own conversion because it additionally carries DurationMS.
func stageResults(perNode []NodeResult) (results []release.NodeValidationResult, failing []string) {
	results = make([]release.NodeValidationResult, len(perNode))
	for i, n := range perNode {
		results[i] = release.NodeValidationResult{
			NodeID:        n.NodeID,
			Status:        n.Status,
			DBTLogURI:     n.DBTLogURI,
			RunResultsURI: n.RunResultsURI,
		}
		if n.Status != "ok" {
			failing = append(failing, n.NodeID)
		}
	}
	return results, failing
}

package redis

import "github.com/carolsimone/continuo/release-controller/service/handlers"

// stageNodeResultDTO is the JSON shape of one per_node entry on the aggregate
// events of the compile and seed-build legs (compile.completed:v1 and
// seed.build.completed:v1). The validation leg instead streams per-node
// content as kind:"node" messages on validation.result:v1 and has its own DTO.
type stageNodeResultDTO struct {
	NodeID        string `json:"node_id"`
	Status        string `json:"status"` // "ok" or "failed"
	DBTLogURI     string `json:"dbt_log_uri,omitempty"`
	RunResultsURI string `json:"run_results_uri,omitempty"`
	DurationMS    int64  `json:"duration_ms,omitempty"`

	// FailedContainer attributes a compile-leg failure to the pod container
	// that failed (compile | parse-prod | parse-candidate | upload). Empty for
	// successes and for non-compile legs.
	FailedContainer string `json:"failed_container,omitempty"`
}

// stageNodeResultsToInput maps the decoded per-node wire entries to the
// handler's input values, preserving the nil vs non-nil-empty distinction: an
// absent per_node key decodes to a nil slice, and an explicit empty array
// decodes to a non-nil empty slice.
func stageNodeResultsToInput(in []stageNodeResultDTO) []handlers.NodeResult {
	if in == nil {
		return nil
	}
	out := make([]handlers.NodeResult, len(in))
	for i, n := range in {
		out[i] = handlers.NodeResult{
			NodeID:          n.NodeID,
			Status:          n.Status,
			DBTLogURI:       n.DBTLogURI,
			RunResultsURI:   n.RunResultsURI,
			DurationMS:      n.DurationMS,
			FailedContainer: n.FailedContainer,
		}
	}
	return out
}

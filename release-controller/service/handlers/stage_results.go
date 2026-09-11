package handlers

import "github.com/carolsimone/continuo/release-controller/domain/pipeline"

// NodeResult is one per-node result carried by the compile and seed-build
// aggregate events (the validation leg instead streams per-node content as
// kind:"node" messages on the unified validation.result:v1 stream). The Redis
// adapter decodes the wire entries into it; it is kept separate from the
// domain value object pipeline.NodeValidationResult so the transport shape
// stays decoupled from the domain, and the handlers map NodeResult →
// pipeline.NodeValidationResult before recording it.
type NodeResult struct {
	NodeID        string
	Status        string // "ok" or "failed"
	DBTLogURI     string
	RunResultsURI string
	DurationMS    int64

	// FailedContainer attributes a compile-leg failure to the pod container
	// that failed (compile | parse-prod | parse-candidate | upload). Empty
	// for successes and for non-compile legs.
	FailedContainer string
}

// stageResults converts the inbound per-node wire results of a compile or
// seed-build leg into the domain value objects recorded on the release, and
// derives the failing-node set in the same pass. The validation leg keeps its
// own conversion because it additionally carries DurationMS.
func stageResults(perNode []NodeResult) (results []pipeline.NodeValidationResult, failing []string) {
	results = make([]pipeline.NodeValidationResult, len(perNode))
	for i, n := range perNode {
		results[i] = pipeline.NodeValidationResult{
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

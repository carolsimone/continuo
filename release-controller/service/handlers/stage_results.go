package handlers

import "github.com/carolsimone/continuo/release-controller/domain/release"

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

package handlers

import (
	"github.com/carolsimone/continuo/pkg/seedreload"
	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/domain/releasecheck"
)

// NodeResult is one per-node result carried by the compile and seed-build
// aggregate events (the validation leg instead streams per-node content as
// kind:"node" messages on the unified validation.result:v1 stream). The Redis
// adapter decodes the wire entries into it; it is kept separate from the
// domain value object releasecheck.NodeValidationResult so the transport shape
// stays decoupled from the domain, and the handlers map NodeResult →
// releasecheck.NodeValidationResult before recording it.
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

	// DependentsCheck is what the seed's report dependents check found in the warehouse.
	// Nil for any node whose result carries no findings.
	DependentsCheck *seedreload.Findings
}

// stageResults converts the inbound per-node wire results of a compile or
// seed-build leg into the domain value objects recorded on the release, and
// derives the failing-node set in the same pass. Each result keeps the time its
// Job ran (DurationMS), which is 0 for a node that never started.
func stageResults(perNode []NodeResult) (results []releasecheck.NodeValidationResult, failing []string) {
	results = make([]releasecheck.NodeValidationResult, len(perNode))
	for i, n := range perNode {
		results[i] = releasecheck.NodeValidationResult{
			NodeID:        n.NodeID,
			Status:        n.Status,
			DBTLogURI:     n.DBTLogURI,
			RunResultsURI: n.RunResultsURI,
			DurationMS:    n.DurationMS,
		}
		if n.Status != release.NodeStatusOK {
			failing = append(failing, n.NodeID)
		}
	}
	return results, failing
}

// failedNodes pairs each failing node id with the project projectOf names for
// it, the input of the operator-facing failure summaries. projectOf may be nil
// where no candidate topology exists yet, as in the compile leg.
func failedNodes(ids []string, projectOf func(id string) string) []release.FailedNode {
	nodes := make([]release.FailedNode, len(ids))
	for i, id := range ids {
		nodes[i].NodeID = id
		if projectOf != nil {
			nodes[i].Project = projectOf(id)
		}
	}
	return nodes
}

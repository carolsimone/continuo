package release

import (
	"slices"
	"sort"

	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
)

// The statuses a validation node reports besides a failure: NodeStatusOK when
// it passed, NodeStatusSkipped when it never ran because a node upstream of it
// failed. Any other status means the node itself failed.
const (
	NodeStatusOK      = "ok"
	NodeStatusSkipped = "skipped"
)

// NodeOutcome is one validation node's reported status.
type NodeOutcome struct {
	NodeID string
	Status string
}

// splitValidationFailures sorts the nodes that failed validation by how the
// validation leg prepared them. rebuilt holds the nodes built from their own
// candidate code; every other validation node is an unchanged upstream cloned
// empty from its production table.
//
// A built node that fails points at the release's code. A cloned node that
// fails points at its production table, which no code change can repair: the
// table may never have been built, may have been dropped, or may not be
// readable. A node that passed or was skipped is in neither list. Both lists
// are sorted by id.
func splitValidationFailures(results []NodeOutcome, rebuilt map[string]bool) (built, cloned []string) {
	for _, r := range results {
		if r.Status == NodeStatusOK || r.Status == NodeStatusSkipped {
			continue
		}
		if rebuilt[r.NodeID] {
			built = append(built, r.NodeID)
		} else {
			cloned = append(cloned, r.NodeID)
		}
	}
	sort.Strings(built)
	sort.Strings(cloned)
	return built, cloned
}

// ValidationRejection is how a failed validation rejects its release: the
// reason, the operator-facing detail and the nodes it names as failing.
type ValidationRejection struct {
	Reason       pkg_model.RejectReason
	Detail       string
	FailingNodes []string
}

// DecideValidationRejection decides how a failed validation rejects its
// release. results holds every validation node's stored outcome, rebuilt the
// nodes built from their own candidate code (every other validation node is
// copied empty from its production table), failing the nodes that did not
// pass, skipped ones included, and summary the explanation of the failure
// (SummarizeValidationFailure).
//
// A failed copy failed on its production table, not on the release's code:
//   - Only copies failed: the reason is upstream_clone_failed, the failing
//     nodes are the copies, and the detail asks the operator to run them
//     (SummarizeCloneFailure).
//   - A built node failed too: the reason stays validation_failed, the
//     failing nodes are every node of failing but the copies, and the detail
//     is the summary followed by ". " and the sentence about the copies.
//   - No copy failed: the reason is validation_failed, the failing nodes are
//     failing, and the detail is the summary.
//
// The failing nodes come out sorted by id, whatever order failing is in.
func DecideValidationRejection(results []NodeOutcome, rebuilt map[string]bool, failing []string, summary string) ValidationRejection {
	built, cloned := splitValidationFailures(results, rebuilt)
	switch {
	case len(cloned) > 0 && len(built) == 0:
		return ValidationRejection{
			Reason:       pkg_model.RejectReasonUpstreamCloneFailed,
			Detail:       SummarizeCloneFailure(cloned),
			FailingNodes: cloned,
		}
	case len(cloned) > 0:
		isCloned := make(map[string]bool, len(cloned))
		for _, id := range cloned {
			isCloned[id] = true
		}
		var notCloned []string
		for _, id := range failing {
			if !isCloned[id] {
				notCloned = append(notCloned, id)
			}
		}
		sort.Strings(notCloned)
		return ValidationRejection{
			Reason:       pkg_model.RejectReasonValidationFailed,
			Detail:       summary + ". " + SummarizeCloneFailure(cloned),
			FailingNodes: notCloned,
		}
	}
	return ValidationRejection{
		Reason:       pkg_model.RejectReasonValidationFailed,
		Detail:       summary,
		FailingNodes: slices.Sorted(slices.Values(failing)),
	}
}

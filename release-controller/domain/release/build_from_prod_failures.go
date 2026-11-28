package release

import (
	"slices"
	"sort"

	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
)

// The statuses a node of a leg (compile, seed build or validation)
// reports besides a failure: NodeStatusOK when it passed, NodeStatusSkipped
// when it never ran because a node upstream of it failed (the validation leg
// reports it). Any other status means the node itself failed.
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
// candidate code; every other validation node is an unchanged upstream built
// empty from its production table.
//
// A node built from its code that fails points at the release's code. A node
// built from production that fails points at its production table, which no
// code change can repair: the table may never have been built, may have been
// dropped, or may not be readable. A node that passed or was skipped is in
// neither list. Both lists are sorted by id.
func splitValidationFailures(results []NodeOutcome, rebuilt map[string]bool) (built, fromProd []string) {
	for _, r := range results {
		if r.Status == NodeStatusOK || r.Status == NodeStatusSkipped {
			continue
		}
		if rebuilt[r.NodeID] {
			built = append(built, r.NodeID)
		} else {
			fromProd = append(fromProd, r.NodeID)
		}
	}
	sort.Strings(built)
	sort.Strings(fromProd)
	return built, fromProd
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
// built empty from its production table), failing the nodes that did not
// pass, skipped ones included, and summary the explanation of the failure
// (SummarizeValidationFailure).
//
// A node built from production that failed failed on its production table,
// not on the release's code:
//   - Only nodes built from production failed: the reason is
//     upstream_build_from_prod_failed, the failing nodes are those nodes, and
//     the detail asks the operator to run them (SummarizeBuildFromProdFailure).
//   - A node built from its code failed too: the reason stays
//     validation_failed, the failing nodes are every node of failing but the
//     nodes built from production, and the detail is the summary followed by
//     ". " and the sentence about the nodes built from production.
//   - No node built from production failed: the reason is validation_failed,
//     the failing nodes are failing, and the detail is the summary.
//
// The failing nodes come out sorted by id, whatever order failing is in.
func DecideValidationRejection(results []NodeOutcome, rebuilt map[string]bool, failing []string, summary string) ValidationRejection {
	built, fromProd := splitValidationFailures(results, rebuilt)
	switch {
	case len(fromProd) > 0 && len(built) == 0:
		return ValidationRejection{
			Reason:       pkg_model.RejectReasonUpstreamBuildFromProdFailed,
			Detail:       SummarizeBuildFromProdFailure(fromProd),
			FailingNodes: fromProd,
		}
	case len(fromProd) > 0:
		isFromProd := make(map[string]bool, len(fromProd))
		for _, id := range fromProd {
			isFromProd[id] = true
		}
		var notFromProd []string
		for _, id := range failing {
			if !isFromProd[id] {
				notFromProd = append(notFromProd, id)
			}
		}
		sort.Strings(notFromProd)
		return ValidationRejection{
			Reason:       pkg_model.RejectReasonValidationFailed,
			Detail:       summary + ". " + SummarizeBuildFromProdFailure(fromProd),
			FailingNodes: notFromProd,
		}
	}
	return ValidationRejection{
		Reason:       pkg_model.RejectReasonValidationFailed,
		Detail:       summary,
		FailingNodes: slices.Sorted(slices.Values(failing)),
	}
}

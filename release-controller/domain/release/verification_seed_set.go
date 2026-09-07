package release

import "sort"

// VerificationBuildSets partitions the nodes a verification run rebuilds from
// its candidate (rather than clones empty from production) into two roles that
// the run must keep apart:
//
//   - scope: the fix's own delta — nodes of the verification's topology that
//     differ from current_prod AND from the rejected release's candidate
//     (touched by the fix, or absent from that candidate). These seed the
//     downstream validation closure: the run measures the fix and everything
//     below it.
//
//   - context: nodes the rejected release changed and validated ok — they
//     differ from current_prod, the fix did not touch them, and the rejected
//     release did not record them in failing_nodes. They are rebuilt from the
//     candidate wherever the scope closure needs them as ancestors, so the
//     fix's node sees the shape that release produced; but they must NOT expand
//     the closure to their OTHER descendants. A sibling failure downstream of a
//     shared changed ancestor, still unfixed and named in failing_nodes, would
//     otherwise be pulled back in through that ancestor and re-fail this run on
//     a fault its fix was never about — so context nodes never seed the closure,
//     and that sibling is left to its own verification.
//
// A node the rejected release recorded as not ok (failed or skipped) that the
// fix did not touch is in neither set: cloned from production. A node another
// release promoted since the rejection matches current_prod and is in neither
// set. Both slices are sorted and deduplicated; nil when empty.
func VerificationBuildSets(verification, prod, rejected Topology, rejectedFailing []string) (scope, context []string) {
	touchedByFix := make(map[string]bool)
	for _, id := range DerivedChangedNodeIDs(verification, rejected) {
		touchedByFix[id] = true
	}
	unproven := make(map[string]bool, len(rejectedFailing))
	for _, id := range rejectedFailing {
		unproven[id] = true
	}
	scopeSeen := make(map[string]bool)
	contextSeen := make(map[string]bool)
	for _, id := range DerivedChangedNodeIDs(verification, prod) {
		switch {
		case touchedByFix[id]:
			if !scopeSeen[id] {
				scopeSeen[id] = true
				scope = append(scope, id)
			}
		case !unproven[id]:
			if !contextSeen[id] {
				contextSeen[id] = true
				context = append(context, id)
			}
		}
	}
	sort.Strings(scope)
	sort.Strings(context)
	return scope, context
}

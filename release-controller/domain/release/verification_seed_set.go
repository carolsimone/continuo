package release

import "sort"

// VerificationSeedSet returns the node ids a verification run rebuilds from its
// candidate rather than clones empty from production: every node of the
// verification's topology that differs from production and is either touched
// by the fix (differs from the rejected candidate, or is absent from it) or was
// not recorded as failing by the rejected release.
//
// rejectedFailing is the rejected release's failing_nodes, which already lists
// every node its validation left not ok — failed and skipped alike. A node in
// it that the fix did not touch is a still-unfixed failure that a separate
// verification answers for (a fix spanning two services submits one run per
// edited service), so it is left out here and cloned from production instead
// of failing this run on a fault this run's fix was never about. A node NOT in
// it that differs from production is a change the rejected release already
// validated ok; rebuilding it from the candidate gives its descendants the
// shape that release actually produced, which is what the fix must be
// measured against.
//
// Output is sorted and deduplicated; nil when nothing is to be rebuilt.
func VerificationSeedSet(verification, prod, rejected Topology, rejectedFailing []string) []string {
	touchedByFix := make(map[string]bool)
	for _, id := range DerivedChangedNodeIDs(verification, rejected) {
		touchedByFix[id] = true
	}
	unproven := make(map[string]bool, len(rejectedFailing))
	for _, id := range rejectedFailing {
		unproven[id] = true
	}
	seen := make(map[string]bool)
	var out []string
	for _, id := range DerivedChangedNodeIDs(verification, prod) {
		if seen[id] {
			continue
		}
		if touchedByFix[id] || !unproven[id] {
			seen[id] = true
			out = append(out, id)
		}
	}
	sort.Strings(out)
	return out
}

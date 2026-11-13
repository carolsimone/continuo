package release

import "sort"

// The statuses a validation node reports besides a failure: ok when it passed,
// skipped when it never ran because a node upstream of it failed. Any other
// status means the node itself failed.
const (
	nodeOK      = "ok"
	nodeSkipped = "skipped"
)

// NodeOutcome is one validation node's reported status.
type NodeOutcome struct {
	NodeID string
	Status string
}

// SplitValidationFailures sorts the nodes that failed validation by how the
// validation leg prepared them. rebuilt holds the nodes built from their own
// candidate code; every other validation node is an unchanged upstream cloned
// empty from its production table.
//
// A built node that fails points at the release's code. A cloned node that
// fails points at its production table, which no code change can repair: the
// table may never have been built, may have been dropped, or may not be
// readable. A node that passed or was skipped is in neither list. Both lists
// are sorted by id.
func SplitValidationFailures(results []NodeOutcome, rebuilt map[string]bool) (built, cloned []string) {
	for _, r := range results {
		if r.Status == nodeOK || r.Status == nodeSkipped {
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

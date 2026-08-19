package ports

import "context"

// ContractPackager packages a directory of python-node contract yaml files
// into the merged, hash-folded wire contract that the team's release CI
// produces from the same source after a merge to main. A python-node fix
// proposal is packaged by running the identical tool, so the release
// artifact's hashes verify against manifest-controller for the same reason
// any other release's do: nothing downstream is reimplementing the fold.
type ContractPackager interface {
	// Merge runs `continuo-runtime merge` against contractDir, resolving the
	// node scripts and their in-repo import closure against repoRoot,
	// labeling the wire contract with service, and rendering declared reads
	// under dialect. It returns the merged contract.yaml bytes exactly as the
	// CLI wrote them; every node entry carries the four hash fields
	// (source_hash, shared_code_hash, config_hash, content_hash) as a side
	// effect of the merge step — there is no separate hash call.
	Merge(ctx context.Context, contractDir, repoRoot, service, dialect string) ([]byte, error)
}

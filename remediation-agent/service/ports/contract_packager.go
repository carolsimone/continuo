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
	//
	// repoRoot is the SERVICE's own directory — the parent of contractDir,
	// and the same directory the service's runtime image uses as its
	// application root — never the enclosing monorepo root. Every script path
	// a contract declares is relative to it, and the shared-code hashes fold
	// the import closure discovered beneath it, so passing the monorepo root
	// resolves those paths against the wrong base and folds a different set of
	// files: the hashes then disagree with the ones the team's own CI produced
	// for the identical source, and the release is rejected for a mismatch
	// that has nothing to do with the fix.
	Merge(ctx context.Context, contractDir, repoRoot, service, dialect string) ([]byte, error)
}

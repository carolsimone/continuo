package ports

import (
	"context"
	"errors"
)

// ErrContractRejected reports that the packaging tool ran, read the contract it
// was given, and refused it. The distinction the sentinel carries is whether
// retrying could ever produce a different answer.
//
// It could not. The tool is deterministic on its input, and the input is
// rebuilt identically on a redelivery — the model's answer is served from the
// trigger-keyed idempotency cache, so the same refused contract is reassembled
// every time. Treating the refusal as transient therefore repeats the refusal
// on every redelivery until the trigger is dead-lettered once the consumer's
// delivery limit is reached, leaving the try in flight with nothing that
// will ever finish it. A caller records a terminal failure instead, keeping the
// tool's own complaint as the evidence.
//
// Everything else a packaging call can fail on — a missing binary, a context
// deadline, a permission error — is left unwrapped, because a later try at
// the same contract genuinely can succeed.
var ErrContractRejected = errors.New("the packaging tool refused the contract")

// ContractPackager packs the ODCS contracts of a project's contract directory
// into the odcs-bundle.json a Python release is submitted with, by running
// the same tool the team's CI runs for a release. A python fix is packed by
// that tool, so its bundle is read by topology-controller for the same reason
// any other release's is: nothing downstream reimplements the packing.
type ContractPackager interface {
	// Pack runs `continuo-runtime pack` against contractDir, resolving the
	// node scripts and their in-repo import closure against repoRoot,
	// labelling the bundle with project, checking declared reads under
	// dialect and recording imagePackages (package name to version) as the
	// package list of the image the project runs. It returns the bundle's
	// bytes exactly as the CLI wrote them.
	//
	// repoRoot is the project's own directory — the parent of contractDir,
	// and the same directory the project's runtime image uses as its
	// application root — never the enclosing monorepo root. Every script path
	// a contract declares is relative to it, and the shared-code hashes fold
	// the import closure discovered beneath it, so passing the monorepo root
	// resolves those paths against the wrong base and folds a different set of
	// files: the hashes then disagree with the ones the team's own CI produced
	// for the identical source, and the release is rejected for a mismatch
	// that has nothing to do with the fix.
	Pack(ctx context.Context, contractDir, repoRoot, project, dialect string, imagePackages map[string]string) ([]byte, error)
}

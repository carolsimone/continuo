package fixer

import (
	"context"
	"errors"
	"fmt"

	"github.com/carolsimone/continuo/remediation-agent/domain/prompt"
	"github.com/carolsimone/continuo/remediation-agent/service/ports"
)

// csvReadKey is the name a python-csv node's contract entry gives its one
// read: the "reads: {csv: <uri>}" key the runtime loads the file from. It is
// the single read a python-csv node is allowed to have, and the only one this
// lane's guard is concerned with — unlike a python-model node, whose script
// may declare any number of reads under any names.
const csvReadKey = "csv"

// csvValidationFixer handles a python-csv node's validation rejection. A
// python-csv node is contract-only: an entry in a contract yaml declaring its
// schema and table, a single "csv" read naming the file the runtime loads, and
// the output_columns it promises that file to carry. There is no script at
// all — nothing runs to produce the relation — so validation instead fetches
// the file's header line and rejects the release when a declared output
// column is missing from it.
//
// That is a different failure shape from a python-model node's (whose script
// the runtime actually executes, and whose fix must therefore preserve
// whatever reads that script performs), which is why this is its own lane
// rather than a case inside pythonValidationFixer: the fix here corrects the
// contract to match a file that already exists and is the source of truth,
// the model is told a different set of rules, and the one read this lane must
// never lose has one fixed name rather than however many the failing node's
// own reads section holds.
//
// Locating the failing node's contract file, verifying no sibling in the same
// directory also failed (so the shadow release this fix ends in can actually
// pass), and everything from the model call through submitting and recording
// the shadow release is identical in shape to the python-model lane, so those
// steps are shared: this type differs from pythonValidationFixer only in what
// evidence it shows the model (buildCsvProposeRequest) and what "the fix
// preserved the node's declaration" means for a contract-only node
// (csvDeclarationBreach).
type csvValidationFixer struct{}

func (csvValidationFixer) Propose(ctx context.Context, svc Services, in Input) (Result, error) {
	schema, table, ok := splitNodeID(in.NodeID)
	if !ok {
		return skipPython(svc, in, fmt.Sprintf(
			"node id %q does not name a schema and a table, so the contract file declaring it cannot be found", in.NodeID))
	}
	if in.Service == "" {
		return skipPython(svc, in, "the trigger names no service, so the fix has no release to be verified under")
	}

	root, cleanup, err := svc.Archive.Fetch(ctx, in.Repo, in.CommitSHA)
	if errors.Is(err, ports.ErrSourceNotFound) {
		return skipPython(svc, in, fmt.Sprintf("repository %s at commit %s is not available", in.Repo, in.CommitSHA))
	}
	if err != nil {
		return Result{}, fmt.Errorf("fetch repo %s@%s: %w", in.Repo, in.CommitSHA, err)
	}
	defer cleanup()

	located, err := svc.ContractLocator.Locate(root, schema, table)
	if errors.Is(err, ports.ErrNodeNotDeclared) || errors.Is(err, ports.ErrAmbiguousDeclaration) {
		return skipPython(svc, in, err.Error())
	}
	if err != nil {
		return Result{}, fmt.Errorf("locate contract for %s: %w", in.NodeID, err)
	}

	sibling, err := siblingFailure(ctx, svc, in, located, root)
	if err != nil {
		return Result{}, err
	}
	if sibling != "" {
		return skipPython(svc, in, fmt.Sprintf(
			"%s also failed validation in release %s and is declared in the same contract directory (%s) as %s. "+
				"A fix is packaged from that whole directory, so any shadow release verifying it would re-run both nodes "+
				"and be rejected by %s. Fix both nodes together.",
			sibling, in.ReleaseID, located.ContractDir, in.NodeID, sibling))
	}

	return proposeContractFixViaShadow(ctx, svc, in, schema, table, root, located,
		buildCsvProposeRequest, csvDeclarationBreach)
}

// buildCsvProposeRequest assembles the csv-specific evidence and turns it into
// the one LLM call's request. The evidence a csv node's fix needs — the
// failure text, the runner log carrying the header-check message, the
// bundle's contract entry, upstream diffs, precedent, and prior attempts — is
// exactly what pythonEvidence already assembles: a csv node runs on the same
// python-runtime image and its code-bundle entry is read the same way, so the
// evidence is gathered once there. CsvEvidence mirrors PythonEvidence field
// for field for exactly this reason, so the result converts directly rather
// than being rebuilt field by field.
func buildCsvProposeRequest(ctx context.Context, svc Services, in Input, located ports.Located) (prompt.ProposeRequest, error) {
	ev, err := pythonEvidence(ctx, svc, in, located)
	if err != nil {
		return prompt.ProposeRequest{}, err // transient read: the driver redelivers
	}
	return prompt.AssembleCsvContractFix(prompt.CsvEvidence(ev)), nil
}

// csvDeclarationBreach is the post-apply guard for a python-csv fix. Every
// node the answer's own files declared before the edit must still be
// declared, under an unchanged identity — the same rule declarationBreach
// enforces for a python-model fix, because it holds regardless of what a fix
// is otherwise allowed to touch.
//
// What differs is the read check. A python-model node's script may perform
// any number of reads under any names, so that guard refuses dropping ANY of
// them. A python-csv node has exactly one read, always named "csv", naming the
// file the runtime loads — and that is the one thing whose VALUE this fix is
// expected to change (a stale uri is a legitimate repair). So this guard does
// not apply the blanket "no read may be dropped" rule at all; it enforces only
// that the "csv" key itself survives, for every node (in the answer's own
// files) that had one before the edit. A node that never declared a "csv" read
// is not this lane's concern and is left to whatever else, if anything, judges
// it.
func csvDeclarationBreach(svc Services, files []ports.ProposedFile, originals map[string]string) string {
	before, after, breach := buildDeclarationMaps(svc, files, originals)
	if breach != "" {
		return breach
	}
	if breach := identityBreach(before, after); breach != "" {
		return breach
	}
	for _, k := range sortedDeclarationKeys(before) {
		was, now := before[k], after[k]
		if !hasReadKey(was.ReadKeys, csvReadKey) {
			continue
		}
		if !hasReadKey(now.ReadKeys, csvReadKey) {
			return fmt.Sprintf(
				"the model's answer no longer declares the %q read of %s. A python-csv node's single read names the "+
					"file its runtime loads; that read's uri may be corrected, but the %q key itself may never be "+
					"removed or renamed, or the node would have nothing left for the runtime to read the file from",
				csvReadKey, k, csvReadKey)
		}
	}
	return ""
}

// hasReadKey reports whether name appears among keys, compared verbatim: a
// contract's read keys are exactly how the runtime is told to read them, so a
// re-spelling is a different key, not the same one under another name.
func hasReadKey(keys []string, name string) bool {
	for _, k := range keys {
		if k == name {
			return true
		}
	}
	return false
}

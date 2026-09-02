package fixer

import (
	"context"
	"fmt"

	"github.com/carolsimone/continuo/agent-remediation/domain/prompt"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
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
// directory also failed (so the verification run the driver later submits
// can actually pass), and everything from the model call through packaging the
// merged contract is not merely similar in shape to the python-model lane's —
// it is the same code, shared through locateContractForFix and
// proposeContractFixViaVerification. This type differs from pythonValidationFixer
// only in the two seams passed into proposeContractFixViaVerification: what
// evidence it shows the model (buildCsvProposeRequest) and what "the fix
// preserved the node's declaration" means for a contract-only node
// (csvDeclarationBreach).
type csvValidationFixer struct{}

func (csvValidationFixer) Propose(ctx context.Context, svc Services, in Input) (Result, error) {
	schema, table, root, located, cleanup, skip, err := locateContractForFix(ctx, svc, in)
	if cleanup != nil {
		defer cleanup()
	}
	if err != nil {
		return Result{}, err
	}
	if skip != nil {
		return *skip, nil
	}

	return proposeContractFixViaVerification(ctx, svc, in, schema, table, root, located,
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
// What differs from declarationBreach is the extra, csv-specific message
// applied to a node that declared a "csv" read before the edit: dropping that
// key gets a rationale naming the csv read by name, because it is the file
// the runtime loads — and that read's VALUE (the uri) is the one thing this
// fix is expected to be allowed to change (a stale uri is a legitimate
// repair). That message does not replace the blanket rule; it fires first,
// as a more specific error for the same condition.
//
// Every node, csv or not, is still held to the blanket "no read may be
// dropped" rule via droppedReadBreach — the same rule declarationBreach
// applies to every node it checks — run unconditionally after the csv-key
// check. For the failing python-csv node itself, which declares exactly one
// read, the two checks catch the identical breach; the csv-specific message
// only gets to say so first. The blanket rule is what protects any sibling in
// the same contract directory — most commonly a python-model node, whose
// script this fix never touches and whose reads it therefore may never
// subtract.
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
		if hasReadKey(was.ReadKeys, csvReadKey) && !hasReadKey(now.ReadKeys, csvReadKey) {
			return fmt.Sprintf(
				"the model's answer no longer declares the %q read of %s. A python-csv node's single read names the "+
					"file its runtime loads; that read's uri may be corrected, but the %q key itself may never be "+
					"removed or renamed, or the node would have nothing left for the runtime to read the file from",
				csvReadKey, k, csvReadKey)
		}
		// Whatever the "csv" check above did not already refuse — a sibling
		// that never declared a "csv" read, or the csv node's own read of it
		// surviving — is still held to the blanket rule: a read present before
		// the edit and absent after it is a breach, whoever's read it is.
		if breach := droppedReadBreach(k, was, now); breach != "" {
			return breach
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

package release

import pkg_model "github.com/carolsimone/continuo/pkg/domain/model"

// RebuiltFromCandidateSet is the set of validation nodes built from their own
// candidate code rather than copied empty from production: the scope's
// descendant closure, plus any verification context rebuild that lands in the
// validation set as an ancestor of the scope. A context node outside the
// validation set is irrelevant to the run and is not built. The parse,
// seed-build and validation-result legs all derive a node's validation op from
// this same set, so they never disagree on whether a node is built from the
// candidate.
func RebuiltFromCandidateSet(changedClosure, contextRebuilds, validationIDs []string) map[string]bool {
	set := make(map[string]bool, len(changedClosure)+len(contextRebuilds))
	for _, id := range changedClosure {
		set[id] = true
	}
	if len(contextRebuilds) > 0 {
		inValidation := make(map[string]bool, len(validationIDs))
		for _, id := range validationIDs {
			inValidation[id] = true
		}
		for _, id := range contextRebuilds {
			if inValidation[id] {
				set[id] = true
			}
		}
	}
	return set
}

// ValidationOpFor returns how validation prepares node n, and the production
// schema it copies n from (empty unless the op is clone_from_prod). A node in
// rebuiltFromCandidate is built from its own candidate code: a python node
// from its validation spec of declared reads and output columns
// (build_from_columns), a dbt test by EXPLAINing its compiled SQL against the
// candidate schema (check_binds), which builds nothing, and any other node
// from its compiled SQL (build_from_sql). Every other node of the validation
// set is an unchanged upstream with no candidate artifact, so it is copied
// empty from its production table whatever its type (clone_from_prod).
func ValidationOpFor(n Node, rebuiltFromCandidate map[string]bool) (op pkg_model.ValidationOp, prodSchema string) {
	if rebuiltFromCandidate[n.UniqueID] {
		switch {
		case pkg_model.NodeType(n.NodeType).IsPython():
			return pkg_model.ValidationOpBuildFromColumns, ""
		case n.NodeType == string(pkg_model.NodeTypeDbtTest):
			// A test is a leaf, so it is never here as an unchanged upstream.
			return pkg_model.ValidationOpCheckBinds, ""
		}
		return pkg_model.ValidationOpBuildFromSQL, ""
	}
	return pkg_model.ValidationOpCloneFromProd, n.SchemaName
}

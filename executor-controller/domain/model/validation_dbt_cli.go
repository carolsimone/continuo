// Package model contains ValidationCommand, the container-command builder for
// the mode=validation dispatch path. It deliberately does not extend
// pkg/domain/model.NodeType.Command(): that method's contract is the prod
// path and must stay byte-stable.
package model

import (
	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
)

// ValidationCommand returns the container command for a single validation node.
//
// Every node type — model, snapshot, and seed — runs validation_runner.py. The
// runner dispatches on the VALIDATION_OP env var (build_from_sql | clone_from_prod):
// build_from_sql reads the node's compiled SQL from the local file at
// CANDIDATE_SQL_PATH (placed there by the fetch init container) and runs
// CREATE TABLE <candidate>.<table> AS (<sql>) WITH NO DATA; clone_from_prod
// clones an existing prod table's shape empty. Seeds are no longer validated with
// `dbt seed --empty`; they are pre-built / cloned (see release-controller), so they
// need no distinct command here.
func ValidationCommand(nt pkg_model.NodeType, tableName string) []string {
	return []string{"python", "/validation_runner.py"}
}

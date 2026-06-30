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
// build_from_sql fetches the node's compiled SQL from S3 (CANDIDATE_SQL_URI)
// and runs CREATE TABLE <candidate>.<table> AS (<sql>) WITH NO DATA;
// clone_from_prod
// clones an existing prod table's shape empty. Seeds need no distinct command
// here: they are pre-built (new/changed) or cloned from prod (unchanged) by the
// release-controller, never validated via dbt.
func ValidationCommand(nt pkg_model.NodeType, tableName string) []string {
	return []string{"python", "/validation_runner.py"}
}

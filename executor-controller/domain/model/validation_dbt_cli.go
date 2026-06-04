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
// Seeds have no SELECT to rewrite: dbt builds an empty seed table in the
// candidate schema (DBT_TARGET_SCHEMA) from the CSV's column definitions, so they
// keep `dbt seed --select <table> --empty`.
//
// Models and snapshots are built by validation_runner.py, which executes
// `CREATE TABLE <candidate>.<table> AS (<CANDIDATE_SQL>) WITH NO DATA`. CANDIDATE_SQL
// is the node's compiled SQL with every schema-qualified reference already
// rewritten to the candidate schema, so raw cross-service refs resolve against the
// empty upstream tables built earlier in dependency order — no model edits, no dbt
// recompile from production-schema refs.
func ValidationCommand(nt pkg_model.NodeType, tableName string) []string {
	if nt == pkg_model.NodeTypeDbtSeed {
		args := append([]string{}, nt.Command(tableName)...) // dbt seed --select <table>
		return append(args, "--empty")
	}
	return []string{"python", "/validation_runner.py"}
}

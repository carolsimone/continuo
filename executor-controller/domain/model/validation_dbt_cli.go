// Package model contains validation_dbt_cli, the dbt CLI builder for the
// mode=validation dispatch path. It deliberately does not extend
// pkg/domain/model.NodeType.Command(): that method's contract is the prod
// path and must stay byte-stable.
package model

import (
	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
)

// ValidationDbtCommand returns the dbt CLI args for a single validation node.
// candidateSchema is the destination schema for the dry-run materialization;
// deferStateURI may be empty (bootstrap release) in which case --defer/--state
// are omitted — the dbt run executes without deferring to a prior manifest.
func ValidationDbtCommand(nt pkg_model.NodeType, tableName, candidateSchema, deferStateURI string) []string {
	base := nt.Command(tableName) // shares the prod verb mapping
	args := append([]string{}, base...)
	args = append(args, "--empty", "--target-schema", candidateSchema)
	if deferStateURI != "" {
		args = append(args, "--defer", "--state", deferStateURI)
	}
	return args
}

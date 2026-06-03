// Package model contains validation_dbt_cli, the dbt CLI builder for the
// mode=validation dispatch path. It deliberately does not extend
// pkg/domain/model.NodeType.Command(): that method's contract is the prod
// path and must stay byte-stable.
package model

import (
	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
)

// ValidationDbtCommand returns the dbt CLI args for a single validation node.
// The dry-run materialization always uses --empty (zero input rows) into the
// candidate schema (delivered out-of-band via the DBT_TARGET_SCHEMA env var and
// the generate_schema_name macro). Intra-service ref()s resolve inside the
// candidate schema — upstream candidate tables are built first by continuo's
// topologically-gated dispatch — so dbt is never asked to --defer to a prior
// manifest. Cross-service refs read from {{ env_var('DBT_UPSTREAM_SCHEMA', target.schema) }}, which the executor
// sets to the candidate schema via DBT_UPSTREAM_SCHEMA, so they too resolve
// against the in-flight candidate build.
func ValidationDbtCommand(nt pkg_model.NodeType, tableName string) []string {
	base := nt.Command(tableName) // shares the prod verb mapping
	args := append([]string{}, base...)
	args = append(args, "--empty")
	return args
}

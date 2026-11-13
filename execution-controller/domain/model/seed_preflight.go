package model

import (
	"fmt"
	"strings"

	pkgmodel "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/seedreload"
)

// SeedPreflightContainer is the name of the init container that runs a seed's
// preflight against the warehouse, after every other init container and
// before dbt loads the seed.
const SeedPreflightContainer = "seed-preflight"

// SeedRelation splits a seed's relation on its first dot into its schema and
// table; ok is false unless both are non-empty.
func SeedRelation(relation string) (schema, table string, ok bool) {
	schema, table, found := strings.Cut(relation, ".")
	return schema, table, found && schema != "" && table != ""
}

// seedPreflightProblem returns what stops p from running before the load of a
// node of type nodeType, "" when nothing does: a preflight runs only on a
// seed, in check or report mode, for a warehouse family the runner knows,
// against a relation that names a schema and a table.
func seedPreflightProblem(nodeType pkgmodel.NodeType, p *seedreload.Preflight) string {
	switch {
	case nodeType != pkgmodel.NodeTypeDbtSeed:
		return fmt.Sprintf("seed_preflight on node type %q, which is not %q", nodeType, pkgmodel.NodeTypeDbtSeed)
	case p.Mode != seedreload.PreflightCheck && p.Mode != seedreload.PreflightReport:
		return fmt.Sprintf("seed_preflight mode %q is not %q or %q", p.Mode, seedreload.PreflightCheck, seedreload.PreflightReport)
	case !p.Family.IsValid():
		return fmt.Sprintf("seed_preflight engine_family %q is not a warehouse family", p.Family)
	}
	if _, _, ok := SeedRelation(p.Relation); !ok {
		return fmt.Sprintf("seed_preflight relation %q is not <schema>.<table>", p.Relation)
	}
	return ""
}

// FullRefreshUndefined is the reason and message of a full refresh of a node
// of type nodeType whose service is governed by the dbt-commands block block
// ("default" or "services.<name>"), which defines no command for it: a seed
// lacks seed_full_refresh, a model full_refresh.
func FullRefreshUndefined(nodeType pkgmodel.NodeType, block string) (pkgmodel.FailureReason, string) {
	if nodeType == pkgmodel.NodeTypeDbtSeed {
		return pkgmodel.FailureReasonSeedFullRefreshUndefined,
			fmt.Sprintf("dbt-commands %s defines no seed_full_refresh command", block)
	}
	return pkgmodel.FailureReasonFullRefreshUndefined,
		fmt.Sprintf("dbt-commands %s defines no full_refresh command", block)
}

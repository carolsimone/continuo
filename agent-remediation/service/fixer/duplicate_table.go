package fixer

import (
	"context"

	"github.com/carolsimone/continuo/agent-remediation/domain/prompt"
	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
)

// duplicateTableFixer resolves a naming collision: two models in the release
// produce the same warehouse relation. It reads only the claimant the release
// changed — the file whose change introduced the collision — and asks for a
// rename. The competing producer's source is deliberately not read: its service
// and path are enough for the model to choose a distinguishing name, and the
// operator sees every claimant on the release page and can rename a different
// one instead.
//
// Reading only the changed claimant is also the only thing that works. Each team
// ships from its own repository, so the trigger's repo and commit describe the
// changed service alone. When no claimant belongs to it — a bootstrap release,
// or two services colliding while a third is released — the target's source is
// in another team's repository, the read returns ErrSourceNotFound, and this
// Fixer skips rather than proposing a change to a file it cannot see.
//
// A python target is skipped outright, before any read is attempted. A python
// node's schema and table are declared in its service's contract.yaml, not in
// the file named by FilePath (that path is the contract's script entry, a
// program that produces the relation but does not name it) — and this system
// carries no repository path for contract.yaml at all, only for the object it
// reaches topology-controller through. Reading and renaming the script would
// therefore change nothing about which relation the node claims, so the
// rejection would recur on the next release; the operator resolves it by hand
// from the release page instead, which names every claimant.
//
// A dbt-seed target is skipped the same way, for a different reason: a seed's
// relation name comes from its CSV filename or the project's seed config,
// never from the CSV's own contents, so reading and renaming the seed file
// would not change which relation it claims either. Worse than a no-op,
// singleFileInterpret would accept ANY edited CSV as a valid rename proposal —
// for a seed, editing the file body means altering the data it loads, so a
// naive "propose whatever came back" would silently corrupt the seed's
// content instead of fixing the collision.
type duplicateTableFixer struct{}

func (duplicateTableFixer) Propose(ctx context.Context, svc Services, in Input) (Result, error) {
	return sourceFileFix{
		gather:    duplicateTableGather,
		build:     duplicateTableBuild,
		interpret: singleFileInterpret,
	}.Propose(ctx, svc, in)
}

func duplicateTableGather(ctx context.Context, svc Services, in Input) (Gathered, string, error) {
	if pkg_model.NodeType(in.NodeType).IsPython() {
		return Gathered{}, "the claimant is a python node, whose relation is declared in its service's " +
			"contract.yaml rather than in the file the trigger names; rename it in the contract by hand", nil
	}
	if in.NodeType == string(pkg_model.NodeTypeDbtSeed) {
		return Gathered{}, "the claimant is a dbt seed, whose relation name comes from the csv filename or " +
			"project config, never from the csv's contents; an edited csv would alter the seed's data, " +
			"not its name, so rename the seed by hand", nil
	}
	if in.FilePath == "" || in.Service == "" {
		return Gathered{}, "the trigger names no source file or service for the claimant, so there is no file to fix", nil
	}
	// The competing claimant's source is never read: the trigger's
	// OtherService/OtherFilePath are all the prompt needs to name it.
	return readOffendingFile(ctx, svc, in, in.Service, in.FilePath)
}

// duplicateTableBuild ignores the dbt log: a duplicate-relation rejection
// happens at parse time, before any Job runs, so there is none.
func duplicateTableBuild(svc Services, g Gathered, in Input, _ string, precedents []prompt.Precedent) prompt.ProposeRequest {
	return prompt.AssembleDuplicateTableFix(
		prompt.NamedFile{Path: g.Primary, Content: svc.Sanitizer.Sanitize(g.Files[g.Primary])},
		in.RelationID,
		in.OtherService,
		in.OtherFilePath,
		precedents,
	)
}

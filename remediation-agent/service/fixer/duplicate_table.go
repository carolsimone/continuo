package fixer

import (
	"context"
	"errors"
	"path"

	"github.com/carolsimone/continuo/remediation-agent/domain/prompt"
	"github.com/carolsimone/continuo/remediation-agent/service/ports"
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
type duplicateTableFixer struct{}

func (duplicateTableFixer) Propose(ctx context.Context, svc Services, in Input) (Result, error) {
	return singleShot{
		gather:    duplicateTableGather,
		build:     duplicateTableBuild,
		interpret: singleFileInterpret,
	}.Propose(ctx, svc, in)
}

func duplicateTableGather(ctx context.Context, svc Services, in Input) (Gathered, bool, error) {
	if in.FilePath == "" || in.Service == "" {
		svc.Logger.Info("duplicate-table fix: trigger carries no source location; skipping",
			"node", in.NodeID)
		return Gathered{}, true, nil
	}
	prefix, ok := svc.ServiceRepoPaths[in.Service]
	if !ok {
		svc.Logger.Warn("duplicate-table fix: no repo path mapping for service; skipping",
			"node", in.NodeID, "service", in.Service)
		return Gathered{}, true, nil
	}
	offending := path.Join(prefix, in.FilePath)
	content, err := svc.Source.ReadFile(ctx, in.Repo, in.CommitSHA, offending)
	if err != nil {
		if errors.Is(err, ports.ErrSourceNotFound) {
			svc.Logger.Warn("duplicate-table fix: offending file not found; skipping", "path", offending)
			return Gathered{}, true, nil
		}
		return Gathered{}, false, err // transient: redeliver
	}
	return Gathered{
		Files:   map[string]string{offending: content},
		Order:   []string{offending},
		Primary: offending,
	}, false, nil
}

// duplicateTableBuild ignores the dbt log: a duplicate-relation rejection
// happens at parse time, before any Job runs, so there is none.
func duplicateTableBuild(svc Services, g Gathered, in Input, _ string) prompt.ProposeRequest {
	return prompt.AssembleDuplicateTableFix(
		prompt.NamedFile{Path: g.Primary, Content: svc.Sanitizer.Sanitize(g.Files[g.Primary])},
		in.NodeID,
		in.OtherService,
		in.OtherFilePath,
	)
}

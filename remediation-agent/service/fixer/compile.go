package fixer

import (
	"context"
	"errors"
	"path"
	"strings"

	"github.com/carolsimone/continuo/remediation-agent/domain/prompt"
	"github.com/carolsimone/continuo/remediation-agent/domain/proposal"
	"github.com/carolsimone/continuo/remediation-agent/service/ports"
)

// compileFixer fixes a dbt compile failure. It reads the offending file (the
// classifier-extracted FilePath, service resolved from NodeID) and, when that
// file is a .sql, also its co-located schema.yml siblings and the service's
// dbt_project.yml. The model chooses which shown file to change.
type compileFixer struct{}

func (compileFixer) Propose(ctx context.Context, svc Services, in Input) (Result, error) {
	return singleShot{gather: compileGather, build: compileBuild, interpret: compileInterpret}.Propose(ctx, svc, in)
}

func compileGather(ctx context.Context, svc Services, in Input) (Gathered, bool, error) {
	if in.FilePath == "" { // project-level error: no models/ path in the log
		svc.Logger.Info("compile fix: no file path in log; skipping", "node", in.NodeID)
		return Gathered{}, true, nil
	}
	// For compile the NodeID IS the service discriminator (a synthetic id).
	prefix, ok := svc.ServiceRepoPaths[in.NodeID]
	if !ok {
		svc.Logger.Warn("compile fix: no repo path mapping for service; skipping", "service", in.NodeID)
		return Gathered{}, true, nil
	}
	offending := path.Join(prefix, in.FilePath)
	content, err := svc.Source.ReadFile(ctx, in.Repo, in.CommitSHA, offending)
	if err != nil {
		if errors.Is(err, ports.ErrSourceNotFound) {
			svc.Logger.Warn("compile fix: offending file not found; skipping", "path", offending)
			return Gathered{}, true, nil
		}
		return Gathered{}, false, err // transient: redeliver
	}
	g := Gathered{Files: map[string]string{offending: content}, Order: []string{offending}, Primary: offending}

	// Best-effort extra context, only when the offending file is a .sql.
	if strings.HasSuffix(offending, ".sql") {
		dir := path.Dir(offending)
		if paths, derr := svc.Source.ListDir(ctx, in.Repo, in.CommitSHA, dir); derr == nil {
			for _, p := range paths {
				if p == offending {
					continue
				}
				if strings.HasSuffix(p, ".yml") || strings.HasSuffix(p, ".yaml") {
					addFile(ctx, svc, in, &g, p)
				}
			}
		}
		addFile(ctx, svc, in, &g, path.Join(prefix, "dbt_project.yml"))
	}
	return g, false, nil
}

// addFile reads one best-effort context file, ignoring a not-found result.
func addFile(ctx context.Context, svc Services, in Input, g *Gathered, p string) {
	if _, seen := g.Files[p]; seen {
		return
	}
	c, err := svc.Source.ReadFile(ctx, in.Repo, in.CommitSHA, p)
	if err != nil {
		return // context is optional; skip on any error
	}
	g.Files[p] = c
	g.Order = append(g.Order, p)
}

func compileBuild(g Gathered, in Input) prompt.ProposeRequest {
	files := make([]prompt.NamedFile, 0, len(g.Order))
	for _, p := range g.Order {
		files = append(files, prompt.NamedFile{Path: p, Content: g.Files[p]})
	}
	return prompt.AssembleCompileFix(files, in.DBTLog, in.NodeID)
}

func compileInterpret(res ports.ProposeResult, g Gathered, in Input) Outcome {
	target := res.TargetFile
	if target == "" {
		target = g.Primary // default to the offending file
	}
	if _, ok := g.Files[target]; !ok {
		return Outcome{Status: proposal.StatusSkipped} // model named a file we never showed
	}
	if res.ProposedContent == "" || res.ProposedContent == g.Files[target] {
		return Outcome{Status: proposal.StatusFailed} // no-op
	}
	return Outcome{
		Status:           proposal.StatusProposed,
		TargetFile:       target,
		CorrectedContent: res.ProposedContent,
		Confidence:       res.Confidence,
		Rationale:        res.Rationale,
		Model:            res.Model,
		SuspectedRoot:    res.SuspectedRootCauseNode,
	}
}

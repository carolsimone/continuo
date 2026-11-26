package rename

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path"
	"strings"

	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/domain/workflow"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
)

// ModelRename is the context step `model_rename`: it plans the move of the
// model whose relation another node already builds to a new name in the same
// schema.
type ModelRename struct {
	archive   ports.RepoArchive
	finder    ports.ModelReferenceFinder
	nodes     ports.NodeLocator
	repoPaths map[string]string
	logger    *slog.Logger
}

var _ step.ContextSource = (*ModelRename)(nil)

// NewModelRename builds the model_rename step. nodes answers whether a node of
// any project already builds a table (catalog's live topology); repoPaths maps
// a project to its dbt project's directory in the repository.
func NewModelRename(archive ports.RepoArchive, finder ports.ModelReferenceFinder, nodes ports.NodeLocator, repoPaths map[string]string, logger *slog.Logger) *ModelRename {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &ModelRename{archive: archive, finder: finder, nodes: nodes, repoPaths: repoPaths, logger: logger}
}

// Meta describes the step.
func (*ModelRename) Meta() step.Meta {
	return step.Meta{
		Name: "model_rename",
		Kind: step.KindContext,
		Description: "Plans the move of a dbt model whose relation another node already builds to <name>_v2 (the next free version) in the same schema, " +
			"and gathers every file of its project that names it. Declines when the model declares no continuo.entity or continuo.grain while its project's other models do.",
		Needs:    []remediation.EvidenceKey{remediation.KeySourceFiles},
		Provides: []remediation.EvidenceKey{remediation.KeyModelRename},
		Stages:   []workflow.Stage{workflow.StageContext},
	}
}

// Gather reads the model's project at the trigger's commit, picks the first
// free name of name_v2, name_v3, … (free in the model's project and in the
// live topology of every project), and provides the plan with every file that
// names the model. When the model's alias makes it write the table in dispute,
// the plan changes the alias instead of moving the file. The model's file is the failure's source file. A model the
// project's other models out-declare, by naming what their table is
// (continuo.entity or continuo.grain) when it names nothing, is declined: a
// new name passes the duplicate check and still fails the ontology rule, and
// what a table is is its owner's to say.
func (s *ModelRename) Gather(ctx context.Context, in step.ContextInput) (step.ContextOutput, error) {
	f := in.Failure
	files, ok := remediation.Get[remediation.SourceFiles](in.Evidence, remediation.KeySourceFiles)
	if !ok {
		return step.ContextOutput{}, errors.New("model_rename: the evidence holds no source files")
	}
	oldPath := files.Primary
	if !strings.HasSuffix(oldPath, ".sql") {
		return s.decline(f, fmt.Sprintf("%s is not a SQL model, so it cannot be renamed", oldPath)), nil
	}
	project := f.Target.Project
	projectDir, ok := s.repoPaths[project]
	if !ok || projectDir == "" {
		return s.decline(f, fmt.Sprintf("no repository directory is configured for project %q", project)), nil
	}
	root, cleanup, err := s.archive.Fetch(ctx, f.Trigger.Repo, f.Trigger.CommitSHA)
	if errors.Is(err, ports.ErrSourceNotFound) {
		return s.decline(f, fmt.Sprintf("repository %s at commit %s is not available", f.Trigger.Repo, f.Trigger.CommitSHA)), nil
	}
	if err != nil {
		return step.ContextOutput{}, fmt.Errorf("fetch repo %s@%s: %w", f.Trigger.Repo, f.Trigger.CommitSHA, err)
	}
	defer cleanup()

	model := strings.TrimSuffix(path.Base(oldPath), ".sql")
	refs, err := s.finder.Find(root, projectDir, model)
	if err != nil {
		return step.ContextOutput{}, fmt.Errorf("model_rename: %w", err)
	}
	plan := remediation.ModelRename{
		Project:       project,
		Relation:      relationOf(f.Target),
		OtherProject:  f.Target.OtherProject,
		OtherFilePath: f.Target.OtherFilePath,
		OldName:       model,
		OldPath:       oldPath,
		References:    refs.Files,
		Order:         refs.Order,
	}
	// The model writes a table of another name than its own when its alias
	// sets it: the alias is what clashes, so the alias is what changes.
	if table := remediation.RelationTable(plan.Relation); table != model {
		plan.ChangesAlias, plan.OldName = true, table
	}
	schema := remediation.RelationSchema(plan.Relation)
	plan.NewName = remediation.NextModelName(plan.OldName, func(n string) bool {
		return refs.ModelNames[n] || s.builtElsewhere(ctx, schema, n)
	})
	plan.NewPath = oldPath
	if !plan.ChangesAlias {
		plan.NewPath = path.Join(path.Dir(oldPath), plan.NewName+".sql")
	}
	if !refs.DeclaresKind && refs.OthersDeclareKind {
		return s.decline(f, fmt.Sprintf(
			"Renaming it to %s fixes the duplicate, but the model has no metadata: add continuo.entity or continuo.grain to its schema.yml entry.",
			plan.NewName)), nil
	}
	return step.ContextOutput{Evidence: map[remediation.EvidenceKey]any{remediation.KeyModelRename: plan}}, nil
}

// builtElsewhere reports whether a node of the live topology, of any project,
// already builds schema.name, so a new name never trades one clash for
// another. A failed read is logged and read as no such node: the
// verification run still refuses a name that clashes.
func (s *ModelRename) builtElsewhere(ctx context.Context, schema, name string) bool {
	if s.nodes == nil || schema == "" {
		return false
	}
	filePath, project, err := s.nodes.Locate(ctx, schema+"."+name)
	if err != nil {
		s.logger.Warn("model rename: could not ask the catalog whether a name is taken", "name", schema+"."+name, "error", err)
		return false
	}
	return filePath != "" || project != ""
}

// relationOf is the relation in dispute: the trigger's RelationID, or the
// node's own id when the trigger names none.
func relationOf(n remediation.TriggerNode) string {
	if n.RelationID != "" {
		return n.RelationID
	}
	return n.NodeID
}

func (s *ModelRename) decline(f remediation.Failure, reason string) step.ContextOutput {
	s.logger.Info("model rename declined", "node", f.Target.NodeID, "reason", reason)
	return step.ContextOutput{Declined: true, Decline: reason}
}

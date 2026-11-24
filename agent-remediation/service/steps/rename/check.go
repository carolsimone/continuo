package rename

import (
	"context"
	"errors"
	"fmt"

	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/domain/workflow"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
)

// MatchesPlan is the check `rename_matches_plan`: a rename draft moves the
// model's file to the planned path and renames the model in every file that
// names it, and edits no other file.
type MatchesPlan struct {
	finder    ports.ModelReferenceFinder
	repoPaths map[string]string
}

var _ step.Check = (*MatchesPlan)(nil)

// NewMatchesPlan builds the rename_matches_plan step. finder reads the
// patched checkout the same way the model_rename step read the repository, so
// both judge "names the model" by one rule; repoPaths maps a service to its
// dbt project's directory.
func NewMatchesPlan(finder ports.ModelReferenceFinder, repoPaths map[string]string) *MatchesPlan {
	return &MatchesPlan{finder: finder, repoPaths: repoPaths}
}

// Meta describes the step.
func (*MatchesPlan) Meta() step.Meta {
	return step.Meta{
		Name: "rename_matches_plan",
		Kind: step.KindCheck,
		Description: "Fails a rename draft that does not move the model's file to the planned path, that edits a file the plan does not name, " +
			"or that leaves a file of the project still naming the model by its old name.",
		Needs:  []remediation.EvidenceKey{remediation.KeyDraft, remediation.KeyModelRename},
		Stages: []workflow.Stage{workflow.StagePreflight},
	}
}

// Check finds, in this order: rename_missing when no edit moves the old path
// to the new one; edit_outside_rename for each edit at a path that is neither
// the new path nor one of the plan's references; and reference_left for each
// file of the project that, with the draft written over a checkout, still
// names the model by its old name, read by the same finder model_rename used.
// A draft with a path outside the checkout is not scanned: its
// edit_outside_rename finding already refuses it.
func (c *MatchesPlan) Check(ctx context.Context, in step.CheckInput) (step.CheckOutput, error) {
	plan, ok := remediation.Get[remediation.ModelRename](in.Evidence, remediation.KeyModelRename)
	if !ok {
		return step.CheckOutput{}, errors.New("rename_matches_plan: the evidence holds no model rename")
	}
	res := remediation.CheckResult{Check: string(c.Meta().Name)}
	moved := false
	for _, e := range in.Draft.Edits {
		if e.Path == plan.NewPath && e.RenamedFrom == plan.OldPath {
			moved = true
		}
	}
	if !moved {
		res.Findings = append(res.Findings, remediation.Finding{
			Code:    "rename_missing",
			Message: fmt.Sprintf("the fix must move %s to %s, renaming the model %s to %s", plan.OldPath, plan.NewPath, plan.OldName, plan.NewName),
			File:    plan.OldPath,
		})
	}
	for _, e := range in.Draft.Edits {
		if e.Path == plan.NewPath {
			continue
		}
		if _, ok := plan.References[e.Path]; !ok {
			res.Findings = append(res.Findings, remediation.Finding{
				Code:    "edit_outside_rename",
				Message: fmt.Sprintf("%s is not one of the files the rename changes: the model's file at %s and the files that name the model", e.Path, plan.NewPath),
				File:    e.Path,
			})
		}
	}
	left, err := c.stillNaming(ctx, in, plan)
	if err != nil {
		return step.CheckOutput{}, err
	}
	for _, p := range left {
		res.Findings = append(res.Findings, remediation.Finding{
			Code:    "reference_left",
			Message: fmt.Sprintf("%s still names the model %s, which the fix renames to %s", p, plan.OldName, plan.NewName),
			File:    p,
		})
	}
	return step.CheckOutput{Result: res}, nil
}

// stillNaming writes the draft over a checkout and returns, sorted, every file
// of the model's project that still names the model by its old name.
func (c *MatchesPlan) stillNaming(ctx context.Context, in step.CheckInput, plan remediation.ModelRename) ([]string, error) {
	if in.Scratch == nil {
		return nil, errors.New("rename_matches_plan: the check was given no checkout")
	}
	projectDir, ok := c.repoPaths[plan.Service]
	if !ok || projectDir == "" {
		return nil, fmt.Errorf("rename_matches_plan: no repository directory is configured for service %q", plan.Service)
	}
	files := make([]ports.ProposedFile, 0, len(in.Draft.Edits))
	for _, e := range in.Draft.Edits {
		files = append(files, ports.ProposedFile{Path: e.Path, Content: e.Content})
	}
	root, _, err := in.Scratch.PatchedCheckout(ctx, files)
	if errors.Is(err, step.ErrPathOutsideCheckout) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("rename_matches_plan: %w", err)
	}
	refs, err := c.finder.Find(root, projectDir, plan.OldName)
	if err != nil {
		return nil, fmt.Errorf("rename_matches_plan: %w", err)
	}
	return refs.Order, nil
}

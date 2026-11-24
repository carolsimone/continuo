package rename

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strings"

	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/domain/workflow"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
)

// MatchesPlan is the check `rename_matches_plan`: a rename draft moves the
// model's file to the planned path and renames the model in every file that
// names it, and edits no other file.
type MatchesPlan struct{}

var _ step.Check = (*MatchesPlan)(nil)

// NewMatchesPlan builds the rename_matches_plan step.
func NewMatchesPlan() *MatchesPlan { return &MatchesPlan{} }

// Meta describes the step.
func (MatchesPlan) Meta() step.Meta {
	return step.Meta{
		Name: "rename_matches_plan",
		Kind: step.KindCheck,
		Description: "Fails a rename draft that does not move the model's file to the planned path, that edits a file the plan does not name, " +
			"or that leaves a file still naming the model by its old name.",
		Needs:  []remediation.EvidenceKey{remediation.KeyDraft, remediation.KeyModelRename},
		Stages: []workflow.Stage{workflow.StagePreflight},
	}
}

// Check finds, in this order: rename_missing when no edit moves the old path
// to the new one; edit_outside_rename for each edit at a path that is neither
// the new path nor one of the plan's references; reference_left for each
// reference the draft leaves naming the old model, unedited or edited.
func (MatchesPlan) Check(_ context.Context, in step.CheckInput) (step.CheckOutput, error) {
	plan, ok := remediation.Get[remediation.ModelRename](in.Evidence, remediation.KeyModelRename)
	if !ok {
		return step.CheckOutput{}, errors.New("rename_matches_plan: the evidence holds no model rename")
	}
	res := remediation.CheckResult{Check: string(MatchesPlan{}.Meta().Name)}
	edited := map[string]remediation.Edit{}
	moved := false
	for _, e := range in.Draft.Edits {
		edited[e.Path] = e
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
	for _, p := range plan.Order {
		content := plan.References[p]
		if e, ok := edited[p]; ok {
			content = e.Content
		}
		if namesOldModel(p, content, plan.OldName) {
			res.Findings = append(res.Findings, remediation.Finding{
				Code:    "reference_left",
				Message: fmt.Sprintf("%s still names the model %s, which the fix renames to %s", p, plan.OldName, plan.NewName),
				File:    p,
			})
		}
	}
	return step.CheckOutput{Result: res}, nil
}

// namesOldModel reports whether a file of the project still names the model
// by its old name: a properties file by an entry of that name, a SQL file by
// a ref.
func namesOldModel(p, content, name string) bool {
	switch strings.ToLower(path.Ext(p)) {
	case ".yml", ".yaml":
		return remediation.NamesModelEntry(content, name)
	case ".sql":
		return remediation.RefsModel(content, name)
	}
	return false
}

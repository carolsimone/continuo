package rename

import (
	"context"
	"fmt"
	"strings"

	"github.com/carolsimone/continuo/agent-remediation/domain/prompt"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
)

// Prompt is the prompt step `duplicate-table`: the model applies the planned
// rename to the model's file and to every file that names the model.
type Prompt struct{}

var _ step.Prompt = (*Prompt)(nil)

// NewPrompt builds the duplicate-table prompt step.
func NewPrompt() *Prompt { return &Prompt{} }

// Meta describes the step.
func (*Prompt) Meta() step.Meta {
	return step.Meta{
		Name: "duplicate-table",
		Kind: step.KindPrompt,
		Description: "Shows the model the planned rename of a dbt model whose relation another node builds, the model's file and every file that names it, " +
			"and reads back the model's file at its new path and each other file with the model renamed. The first-round request carries the earlier tries at the failure, when there are any.",
		Needs: []remediation.EvidenceKey{remediation.KeyModelRename, remediation.KeySourceFiles, remediation.KeyFailurePrecedents},
	}
}

// Propose asks the model to apply the plan and reads its answer into a draft.
// A round after the first revises the last round's request with its answer and
// findings (step.RevisionRequest).
func (p *Prompt) Propose(ctx context.Context, in step.PromptInput, m step.Model) (step.PromptOutput, error) {
	plan, ok := remediation.Get[remediation.ModelRename](in.Evidence, remediation.KeyModelRename)
	if !ok {
		// A cluster that finished gathering before its type listed model_rename
		// has no plan, and a cluster past gathering never gathers again.
		return step.PromptOutput{Declined: true, Decline: noPlanReason}, nil
	}
	files, ok := remediation.Get[remediation.SourceFiles](in.Evidence, remediation.KeySourceFiles)
	if !ok {
		return step.PromptOutput{}, fmt.Errorf("duplicate-table: the evidence holds no source files")
	}
	req, revising, err := step.RevisionRequest(in)
	if err != nil {
		return step.PromptOutput{}, fmt.Errorf("duplicate-table: %w", err)
	}
	if !revising {
		req = request(in, plan, files)
	}
	res, err := m.Propose(ctx, req)
	if err != nil {
		return step.PromptOutput{}, fmt.Errorf("llm propose: %w", err)
	}
	return step.PromptOutput{Draft: draft(in.Failure, plan, files, res)}, nil
}

// request builds the first round's request, with the earlier tries at the
// failure (step.WithPreviousTries).
func request(in step.PromptInput, plan remediation.ModelRename, files remediation.SourceFiles) prompt.ProposeRequest {
	references := make([]prompt.NamedFile, 0, len(plan.Order))
	for _, p := range plan.Order {
		references = append(references, prompt.NamedFile{Path: p, Content: plan.References[p]})
	}
	precedents, _ := remediation.Get[[]prompt.Precedent](in.Evidence, remediation.KeyFailurePrecedents)
	model := prompt.NamedFile{Path: plan.OldPath, Content: files.Files[plan.OldPath]}
	req := prompt.AssembleModelRename(plan.Relation, plan.OtherProject, plan.OtherFilePath, plan.OldName, plan.NewName,
		model, plan.NewPath, references, precedents)
	if plan.ChangesAlias {
		req = prompt.AssembleAliasChange(plan.Relation, plan.OtherProject, plan.OtherFilePath, plan.OldName, plan.NewName,
			model, references, precedents)
	}
	return step.WithPreviousTries(req, in.Evidence)
}

// draft reads the model's answer into a draft. The model's file answered at
// its new path is an edit that moves it from its old path, unless the plan
// changes the alias, where the new path is the old one and the file stays;
// any other answered file is an edit in place, whose original is the content
// the plan read, when the plan read it. The draft names the model's old file as its target and
// opens its rationale with the plan's account of the rename, which the pull
// request shows.
func draft(f remediation.Failure, plan remediation.ModelRename, files remediation.SourceFiles, res ports.ProposeResult) remediation.Draft {
	d := remediation.Draft{
		NamedTarget: plan.OldPath,
		TargetShown: true,
		Rationale:   strings.TrimSpace(plan.Explanation() + " " + res.Rationale),
		Confidence:  res.Confidence,
		Model:       res.Model,
	}
	for i, answered := range res.Files {
		contentKey, diffKey := editKeys(f, i)
		e := remediation.Edit{
			Path:         answered.Path,
			Content:      answered.Content,
			TargetNodeID: f.Target.NodeID,
			ContentKey:   contentKey,
			DiffKey:      diffKey,
		}
		switch answered.Path {
		case plan.NewPath:
			if !plan.ChangesAlias {
				e.RenamedFrom = plan.OldPath
			}
			e.Original = files.Files[plan.OldPath]
		case plan.OldPath:
			e.Original = files.Files[plan.OldPath]
		default:
			e.Original = plan.References[answered.Path]
		}
		d.Edits = append(d.Edits, e)
	}
	d.SourceResolved = len(d.Edits) > 0
	return d
}

// noPlanReason is why a cluster with no rename plan is handed over: it was
// gathered before its type planned renames, and a new round gathers again.
const noPlanReason = "This fix was gathered before continuo planned the rename of a duplicate model; use Try again to start it afresh."

// editKeys are the keys under which the runner stores the content and diff of
// the draft's i-th edit.
func editKeys(f remediation.Failure, i int) (contentKey, diffKey string) {
	base := fmt.Sprintf("%s/edit-%d", f.ArtifactPrefix(f.Target.NodeID), i)
	return base + ".content", base + ".diff"
}

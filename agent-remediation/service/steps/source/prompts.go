package source

import (
	"context"
	"fmt"

	"github.com/carolsimone/continuo/agent-remediation/domain/prompt"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/domain/workflow"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
)

// builder turns the evidence of one cluster into the model's request. Each
// source-file prompt differs only in the request it builds, in whether it reads
// the dbt log, and in how it picks the file the model's answer replaces.
type builder func(f remediation.Failure, files remediation.SourceFiles, dbtLog string, precedents []prompt.Precedent) prompt.ProposeRequest

// picker chooses the file an answer replaces from the shown files.
type picker func(res ports.ProposeResult, files remediation.SourceFiles) (target string, shown bool)

// sourcePrompt is a prompt step whose fix is one edited source file: it builds
// one request, makes one model call and reads the answer into a one-edit draft.
type sourcePrompt struct {
	name        workflow.StepName
	description string
	readsLog    bool
	// suspectsRoot reports whether the answer may name the node it suspects of
	// being the root cause.
	suspectsRoot bool
	build        builder
	pick         picker
}

var _ step.Prompt = (*sourcePrompt)(nil)

// NewDBTCompile builds the dbt-compile prompt: a dbt compile failure, where the
// model picks which shown file to change.
func NewDBTCompile() step.Prompt {
	return &sourcePrompt{
		name:         "dbt-compile",
		description:  "Shows the model the files around a failed dbt compile with the dbt log, and reads back the corrected content of the one file it chooses. The first-draft request carries the earlier tries at the failure, when there are any.",
		readsLog:     true,
		suspectsRoot: true,
		pick:         pickNamed,
		build: func(f remediation.Failure, files remediation.SourceFiles, dbtLog string, precedents []prompt.Precedent) prompt.ProposeRequest {
			return prompt.AssembleCompileFix(shownFiles(files), dbtLog, f.Target.NodeID, precedents)
		},
	}
}

// NewDBTParse builds the dbt-parse prompt: a node whose compiled SQL the parser
// rejected, where the model picks which shown file to change and is shown the
// parser's error text in place of a dbt log.
func NewDBTParse() step.Prompt {
	return &sourcePrompt{
		name:         "dbt-parse",
		description:  "Shows the model the files around a node whose SQL the parser rejected with the parser's error, and reads back the corrected content of the one file it chooses. The first-draft request carries the earlier tries at the failure, when there are any.",
		suspectsRoot: true,
		pick:         pickNamed,
		build: func(f remediation.Failure, files remediation.SourceFiles, _ string, precedents []prompt.Precedent) prompt.ProposeRequest {
			return prompt.AssembleParseFix(shownFiles(files), f.Target.ErrorExcerpt, f.Target.Project, f.Target.NodeID, precedents)
		},
	}
}

// NewSeed builds the seed prompt: a dbt seed whose csv failed to load. The file
// to change is always the seed's own csv.
func NewSeed() step.Prompt {
	return &sourcePrompt{
		name:        "seed",
		description: "Shows the model a seed's csv with the dbt log, and reads back the corrected csv. The first-draft request carries the earlier tries at the failure, when there are any.",
		readsLog:    true,
		pick: func(_ ports.ProposeResult, files remediation.SourceFiles) (string, bool) {
			return files.Primary, true
		},
		build: func(f remediation.Failure, files remediation.SourceFiles, dbtLog string, precedents []prompt.Precedent) prompt.ProposeRequest {
			return prompt.AssembleSeedFix(files.Primary, files.Files[files.Primary], dbtLog, f.Target.NodeID, precedents)
		},
	}
}

// Meta describes the step.
func (p *sourcePrompt) Meta() step.Meta {
	needs := []remediation.EvidenceKey{remediation.KeyFailure, remediation.KeySourceFiles}
	if p.readsLog {
		needs = append(needs, remediation.KeyDBTLog)
	}
	return step.Meta{
		Name:        p.name,
		Kind:        step.KindPrompt,
		Description: p.description,
		Needs:       append(needs, remediation.KeyFailurePrecedents),
	}
}

// Propose builds the request from the cluster's evidence, calls the model once
// and reads its answer into a draft. In a draft after the first the request is
// the revision of the last draft's request with its answer and the findings
// (step.RevisionRequest), and the answer is read the same way. The draft names
// the file the model chose. When that file is one the model was shown, the draft
// holds one edit of it, whose content may still be empty or unchanged for the
// checks to refuse; when it is not, the draft holds no edit and says the target
// was not shown.
func (p *sourcePrompt) Propose(ctx context.Context, in step.PromptInput, m step.Model) (step.PromptOutput, error) {
	files, ok := remediation.Get[remediation.SourceFiles](in.Evidence, remediation.KeySourceFiles)
	if !ok {
		return step.PromptOutput{}, fmt.Errorf("%s: the evidence holds no source files", p.name)
	}
	req, revising, err := step.RevisionRequest(in)
	if err != nil {
		return step.PromptOutput{}, fmt.Errorf("%s: %w", p.name, err)
	}
	if !revising {
		req = p.request(in, files)
	}

	res, err := m.Propose(ctx, req)
	if err != nil {
		return step.PromptOutput{}, fmt.Errorf("llm propose: %w", err)
	}
	return step.PromptOutput{Draft: p.draft(in.Failure, files, res)}, nil
}

// request builds the first draft's request from the cluster's evidence, with
// the earlier tries at the failure (step.WithPreviousTries).
func (p *sourcePrompt) request(in step.PromptInput, files remediation.SourceFiles) prompt.ProposeRequest {
	var dbtLog string
	if p.readsLog {
		log, _ := remediation.Get[remediation.Text](in.Evidence, remediation.KeyDBTLog)
		dbtLog = string(log)
	}
	precedents, _ := remediation.Get[[]prompt.Precedent](in.Evidence, remediation.KeyFailurePrecedents)
	return step.WithPreviousTries(p.build(in.Failure, files, dbtLog, precedents), in.Evidence)
}

// draft reads the model's answer into a draft. The edit's original is the file
// as the evidence holds it.
func (p *sourcePrompt) draft(f remediation.Failure, files remediation.SourceFiles, res ports.ProposeResult) remediation.Draft {
	target, shown := p.pick(res, files)
	d := remediation.Draft{
		NamedTarget: res.TargetFile,
		TargetShown: shown,
		Rationale:   res.Rationale,
		Confidence:  res.Confidence,
		Model:       res.Model,
	}
	if p.suspectsRoot {
		d.SuspectedRoot = res.SuspectedRootCauseNode
	}
	if !shown {
		return d
	}
	contentKey, diffKey := sourceArtifactKeys(f)
	d.SourceResolved = true
	d.Edits = []remediation.Edit{{
		Path:         target,
		Original:     files.Files[target],
		Content:      res.ProposedContent,
		TargetNodeID: f.Target.NodeID,
		ContentKey:   contentKey,
		DiffKey:      diffKey,
	}}
	return d
}

// pickNamed resolves the target_file the model named against the shown files.
func pickNamed(res ports.ProposeResult, files remediation.SourceFiles) (string, bool) {
	return resolveTarget(res.TargetFile, files)
}

// shownFiles lists the shown files in order.
func shownFiles(files remediation.SourceFiles) []prompt.NamedFile {
	out := make([]prompt.NamedFile, 0, len(files.Order))
	for _, p := range files.Order {
		out = append(out, prompt.NamedFile{Path: p, Content: files.Files[p]})
	}
	return out
}

// sourceArtifactKeys are the keys under which the runner stores the corrected
// source and its diff for the failure's try.
func sourceArtifactKeys(f remediation.Failure) (contentKey, diffKey string) {
	base := f.ArtifactPrefix(f.Target.NodeID) + ".source"
	return base + ".sql", base + ".diff"
}

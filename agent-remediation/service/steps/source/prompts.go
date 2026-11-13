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
type builder func(sanitizer ports.LogSanitizer, f remediation.Failure, files remediation.SourceFiles, dbtLog string, precedents []prompt.Precedent) prompt.ProposeRequest

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
	sanitizer    ports.LogSanitizer
}

var _ step.Prompt = (*sourcePrompt)(nil)

// NewDBTCompile builds the dbt-compile prompt: a dbt compile failure, where the
// model picks which shown file to change.
func NewDBTCompile(sanitizer ports.LogSanitizer) step.Prompt {
	return &sourcePrompt{
		name:         "dbt-compile",
		description:  "Shows the model the files around a failed dbt compile with the dbt log, and reads back the corrected content of the one file it chooses.",
		readsLog:     true,
		suspectsRoot: true,
		sanitizer:    sanitizer,
		pick:         pickNamed,
		build: func(san ports.LogSanitizer, f remediation.Failure, files remediation.SourceFiles, dbtLog string, precedents []prompt.Precedent) prompt.ProposeRequest {
			return prompt.AssembleCompileFix(sanitizedFiles(san, files), dbtLog, f.Target.NodeID, precedents)
		},
	}
}

// NewDBTParse builds the dbt-parse prompt: a node whose compiled SQL the parser
// rejected, where the model picks which shown file to change and is shown the
// parser's error text in place of a dbt log.
func NewDBTParse(sanitizer ports.LogSanitizer) step.Prompt {
	return &sourcePrompt{
		name:         "dbt-parse",
		description:  "Shows the model the files around a node whose SQL the parser rejected with the parser's error, and reads back the corrected content of the one file it chooses.",
		suspectsRoot: true,
		sanitizer:    sanitizer,
		pick:         pickNamed,
		build: func(san ports.LogSanitizer, f remediation.Failure, files remediation.SourceFiles, _ string, precedents []prompt.Precedent) prompt.ProposeRequest {
			return prompt.AssembleParseFix(sanitizedFiles(san, files), san.Sanitize(f.Target.ErrorExcerpt), f.Target.Service, f.Target.NodeID, precedents)
		},
	}
}

// NewSeed builds the seed prompt: a dbt seed whose csv failed to load. The file
// to change is always the seed's own csv.
func NewSeed(sanitizer ports.LogSanitizer) step.Prompt {
	return &sourcePrompt{
		name:        "seed",
		description: "Shows the model a seed's csv with the dbt log, and reads back the corrected csv.",
		readsLog:    true,
		sanitizer:   sanitizer,
		pick: func(_ ports.ProposeResult, files remediation.SourceFiles) (string, bool) {
			return files.Primary, true
		},
		build: func(san ports.LogSanitizer, f remediation.Failure, files remediation.SourceFiles, dbtLog string, precedents []prompt.Precedent) prompt.ProposeRequest {
			return prompt.AssembleSeedFix(files.Primary, san.Sanitize(files.Files[files.Primary]), dbtLog, f.Target.NodeID, precedents)
		},
	}
}

// NewDuplicateTable builds the duplicate-table prompt: two models that produce
// the same warehouse relation, where the model renames the changed claimant.
func NewDuplicateTable(sanitizer ports.LogSanitizer) step.Prompt {
	return &sourcePrompt{
		name:         "duplicate-table",
		description:  "Shows the model the changed claimant of a relation two models produce, names the other producer, and reads back the corrected content of the claimant's file.",
		suspectsRoot: true,
		sanitizer:    sanitizer,
		pick:         pickNamed,
		build: func(san ports.LogSanitizer, f remediation.Failure, files remediation.SourceFiles, _ string, precedents []prompt.Precedent) prompt.ProposeRequest {
			n := f.Target
			return prompt.AssembleDuplicateTableFix(
				prompt.NamedFile{Path: files.Primary, Content: san.Sanitize(files.Files[files.Primary])},
				n.RelationID, n.OtherService, n.OtherFilePath, precedents)
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
// and reads its answer into a draft. The draft names the file the model chose.
// When that file is one the model was shown, the draft holds one edit of it,
// whose content may still be empty or unchanged for the checks to refuse; when
// it is not, the draft holds no edit and says the target was not shown.
func (p *sourcePrompt) Propose(ctx context.Context, in step.PromptInput, m step.Model) (step.PromptOutput, error) {
	files, ok := remediation.Get[remediation.SourceFiles](in.Evidence, remediation.KeySourceFiles)
	if !ok {
		return step.PromptOutput{}, fmt.Errorf("%s: the evidence holds no source files", p.name)
	}
	var dbtLog string
	if p.readsLog {
		log, _ := remediation.Get[remediation.Text](in.Evidence, remediation.KeyDBTLog)
		dbtLog = string(log)
	}
	precedents, _ := remediation.Get[[]prompt.Precedent](in.Evidence, remediation.KeyFailurePrecedents)

	res, err := m.Propose(ctx, p.build(p.sanitizer, in.Failure, files, dbtLog, precedents))
	if err != nil {
		return step.PromptOutput{}, fmt.Errorf("llm propose: %w", err)
	}
	return step.PromptOutput{Draft: p.draft(in.Failure, files, res)}, nil
}

// draft reads the model's answer into a draft. The edit's original is the file
// as the repository holds it, not the sanitized copy the model saw, because the
// fix is applied to the real file.
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

// sanitizedFiles lists the shown files in order with each one sanitized, since
// they leave for an external model. The raw content stays in files for the diff
// and the no-op check.
func sanitizedFiles(san ports.LogSanitizer, files remediation.SourceFiles) []prompt.NamedFile {
	out := make([]prompt.NamedFile, 0, len(files.Order))
	for _, p := range files.Order {
		out = append(out, prompt.NamedFile{Path: p, Content: san.Sanitize(files.Files[p])})
	}
	return out
}

// sourceArtifactKeys are the keys under which the runner stores the corrected
// source and its diff for the failure's attempt.
func sourceArtifactKeys(f remediation.Failure) (contentKey, diffKey string) {
	base := fmt.Sprintf("proposed-fix/%s/%s/attempt-%d.source", f.Trigger.ReleaseID, f.Target.NodeID, f.Attempt)
	return base + ".sql", base + ".diff"
}

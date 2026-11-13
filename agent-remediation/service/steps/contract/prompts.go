package contract

import (
	"context"
	"fmt"

	"github.com/carolsimone/continuo/agent-remediation/domain/prompt"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/domain/workflow"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
	pkgmodel "github.com/carolsimone/continuo/pkg/domain/model"
)

// assembler turns the python evidence into the lane's request.
type assembler func(prompt.PythonEvidence) prompt.ProposeRequest

// contractPrompt is a prompt step whose fix is made in the contract yaml that
// declares the node: it builds one request from the cluster's evidence, makes
// one model call and reads the answer into a draft with one edit per answered
// file. The three prompts differ only in the request they build.
type contractPrompt struct {
	name        workflow.StepName
	description string
	assemble    assembler
	sanitizer   ports.LogSanitizer
}

var _ step.Prompt = (*contractPrompt)(nil)

// NewPythonContract builds the python-contract prompt: a python node whose
// output failed validation against its contract.
func NewPythonContract(sanitizer ports.LogSanitizer) step.Prompt {
	return &contractPrompt{
		name: "python-contract",
		description: "Shows the model a python node's contract yaml with the validation error, runner log, the contract entry the release recorded, " +
			"upstream changes, precedent and earlier attempts, and reads back the complete new content of every contract file it changes.",
		assemble:  prompt.AssemblePythonContractFix,
		sanitizer: sanitizer,
	}
}

// NewPythonParse builds the python-parse prompt: a python node one of whose
// reads has SQL the release's parser rejected.
func NewPythonParse(sanitizer ports.LogSanitizer) step.Prompt {
	return &contractPrompt{
		name: "python-parse",
		description: "Shows the model a python node's contract yaml with the SQL parser's error, upstream changes, precedent and earlier attempts, " +
			"and reads back the complete new content of every contract file it changes.",
		assemble:  prompt.AssemblePythonParseFix,
		sanitizer: sanitizer,
	}
}

// NewCSVContract builds the csv-contract prompt: a python-csv node whose
// declared columns the csv file's header does not carry.
func NewCSVContract(sanitizer ports.LogSanitizer) step.Prompt {
	return &contractPrompt{
		name: "csv-contract",
		description: "Shows the model a python-csv node's contract yaml with the validation error, the header check's runner log, the contract entry the release recorded, " +
			"upstream changes, precedent and earlier attempts, and reads back the complete new content of every contract file it changes.",
		assemble: func(ev prompt.PythonEvidence) prompt.ProposeRequest {
			return prompt.AssembleCsvContractFix(prompt.CsvEvidence(ev))
		},
		sanitizer: sanitizer,
	}
}

// Meta describes the step.
func (p *contractPrompt) Meta() step.Meta {
	return step.Meta{
		Name:        p.name,
		Kind:        step.KindPrompt,
		Description: p.description,
		Needs: []remediation.EvidenceKey{
			remediation.KeyFailure,
			remediation.KeyContractFile,
			remediation.KeyContractEntry,
			remediation.KeyDBTLog,
			remediation.KeyUpstreamChanges,
			remediation.KeyFailurePrecedents,
			remediation.KeyPriorAttempts,
		},
	}
}

// Propose assembles what the model is shown. The failure text and the
// declaring file are required; the runner log, the bundle's contract entry,
// upstream changes, precedent and prior attempts each degrade to their own
// absence, because a missing evidence section costs the model context while a
// blocked heal costs the operator the fix entirely. The error excerpt and the
// yaml text are sanitized here, since they leave for an external model, and the
// other sections arrive sanitized from the steps that gathered them.
//
// The draft holds one edit for each file the model answered with, naming the
// path it answered and the content it gave. Each edit's original is left empty:
// the checks read it from a checkout of the repository. A draft with no edit
// is what an answer with no file gives.
func (p *contractPrompt) Propose(ctx context.Context, in step.PromptInput, m step.Model) (step.PromptOutput, error) {
	file, ok := remediation.Get[remediation.ContractFile](in.Evidence, remediation.KeyContractFile)
	if !ok {
		return step.PromptOutput{}, fmt.Errorf("%s: the evidence holds no contract file", p.name)
	}
	entry, _ := remediation.Get[remediation.Text](in.Evidence, remediation.KeyContractEntry)
	dbtLog, _ := remediation.Get[remediation.Text](in.Evidence, remediation.KeyDBTLog)
	upstream, _ := remediation.Get[[]prompt.UpstreamChange](in.Evidence, remediation.KeyUpstreamChanges)
	precedents, _ := remediation.Get[[]prompt.Precedent](in.Evidence, remediation.KeyFailurePrecedents)
	attempts, _ := remediation.Get[[]prompt.PriorAttempt](in.Evidence, remediation.KeyPriorAttempts)

	f := in.Failure
	res, err := m.Propose(ctx, p.assemble(prompt.PythonEvidence{
		NodeID:          f.Target.NodeID,
		NoReads:         f.Target.NodeType == string(pkgmodel.NodeTypePythonApi),
		ErrorExcerpt:    p.sanitizer.Sanitize(f.Target.ErrorExcerpt),
		RunnerLog:       string(dbtLog),
		ContractEntry:   string(entry),
		YAMLPath:        file.YAMLPath,
		YAMLText:        p.sanitizer.Sanitize(file.YAMLText),
		UpstreamChanges: upstream,
		Precedents:      precedents,
		PriorAttempts:   attempts,
	}))
	if err != nil {
		return step.PromptOutput{}, fmt.Errorf("llm propose: %w", err)
	}

	draft := remediation.Draft{
		Rationale:  res.Rationale,
		Confidence: res.Confidence,
		Model:      res.Model,
	}
	for i, answered := range res.Files {
		contentKey, diffKey := editKeys(f, i)
		draft.Edits = append(draft.Edits, remediation.Edit{
			Path:         answered.Path,
			Content:      answered.Content,
			TargetNodeID: f.Target.NodeID,
			ContentKey:   contentKey,
			DiffKey:      diffKey,
		})
	}
	// The edits name real files of the repository at the trigger's commit.
	draft.SourceResolved = len(draft.Edits) > 0
	return step.PromptOutput{Draft: draft}, nil
}

// editKeys are the keys under which the runner stores the content and the diff
// of the attempt's i-th edit. The edits of one attempt share a directory and
// are numbered within it, so two of them never write the same key.
func editKeys(f remediation.Failure, i int) (contentKey, diffKey string) {
	base := fmt.Sprintf("proposed-fix/%s/%s/attempt-%d/edit-%d", f.Trigger.ReleaseID, f.Target.NodeID, f.Attempt, i)
	return base + ".content", base + ".diff"
}

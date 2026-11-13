package contract_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/domain/prompt"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
	"github.com/carolsimone/continuo/agent-remediation/service/steps/contract"
)

var (
	upstreamChanges = []prompt.UpstreamChange{{NodeID: "model.svc.up", Depth: 1, CodeDiff: "<code>"}}
	precedentList   = []prompt.Precedent{{ReleaseID: "old", NodeID: "n0", ErrorExcerpt: "same error"}}
	priorAttempts   = []prompt.PriorAttempt{{Attempt: 1, VerifyError: "<still bad>", Diffs: []prompt.AttemptDiff{{Path: "svc/contracts/fx.yaml", Diff: "<-a>"}}}}
)

// promptEvidence is what the context steps leave in a cluster's evidence.
func promptEvidence() remediation.Evidence {
	return contractFileEvidence().
		With(remediation.KeyContractEntry, remediation.Text(`<{"table":"rates"}>`)).
		With(remediation.KeyDBTLog, remediation.Text("<runner log>")).
		With(remediation.KeyUpstreamChanges, upstreamChanges).
		With(remediation.KeyFailurePrecedents, precedentList).
		With(remediation.KeyPriorAttempts, priorAttempts)
}

// laneEvidence is the evidence the lane's pythonEvidence builds for the same
// inputs: the error excerpt and the declaring file's text are sanitized there,
// and the rest arrives already sanitized.
func laneEvidence(noReads bool) prompt.PythonEvidence {
	return prompt.PythonEvidence{
		NodeID:          nodeID,
		NoReads:         noReads,
		ErrorExcerpt:    "<column amount missing>",
		RunnerLog:       "<runner log>",
		ContractEntry:   `<{"table":"rates"}>`,
		YAMLPath:        "svc/contracts/fx.yaml",
		YAMLText:        "<nodes:\n  - table: rates\n>",
		UpstreamChanges: upstreamChanges,
		Precedents:      precedentList,
		PriorAttempts:   priorAttempts,
	}
}

type promptCase struct {
	name       string
	build      func() step.Prompt
	wantStep   string
	nodeType   string
	laneResult func(noReads bool) prompt.ProposeRequest
}

func promptCases() []promptCase {
	return []promptCase{
		{
			name:     "python-contract",
			build:    func() step.Prompt { return contract.NewPythonContract(markSanitizer{}) },
			wantStep: "python-contract",
			nodeType: "python-node",
			laneResult: func(noReads bool) prompt.ProposeRequest {
				return prompt.AssemblePythonContractFix(laneEvidence(noReads))
			},
		},
		{
			name:     "python-parse",
			build:    func() step.Prompt { return contract.NewPythonParse(markSanitizer{}) },
			wantStep: "python-parse",
			nodeType: "python-node",
			laneResult: func(noReads bool) prompt.ProposeRequest {
				return prompt.AssemblePythonParseFix(laneEvidence(noReads))
			},
		},
		{
			name:     "csv-contract",
			build:    func() step.Prompt { return contract.NewCSVContract(markSanitizer{}) },
			wantStep: "csv-contract",
			nodeType: "python-csv",
			laneResult: func(noReads bool) prompt.ProposeRequest {
				return prompt.AssembleCsvContractFix(prompt.CsvEvidence(laneEvidence(noReads)))
			},
		},
	}
}

func proposeWith(t *testing.T, p step.Prompt, f remediation.Failure, ev remediation.Evidence, m *fakeModel) step.PromptOutput {
	t.Helper()
	out, err := p.Propose(context.Background(), step.PromptInput{Failure: f, Evidence: ev}, m)
	require.NoError(t, err)
	require.False(t, out.Declined)
	return out
}

func answer(files ...ports.ProposedFile) ports.ProposeResult {
	return ports.ProposeResult{
		Files:                  files,
		Rationale:              "the column is named amount_eur",
		Confidence:             "high",
		Model:                  "model-1",
		SuspectedRootCauseNode: "py.fx.root",
	}
}

func TestPrompts_MetaNamesTheStepAndWhatItReads(t *testing.T) {
	for _, tc := range promptCases() {
		t.Run(tc.name, func(t *testing.T) {
			m := tc.build().Meta()
			assert.Equal(t, tc.wantStep, string(m.Name))
			assert.Equal(t, step.KindPrompt, m.Kind)
			assert.NotEmpty(t, m.Description)
			assert.Equal(t, []remediation.EvidenceKey{
				remediation.KeyFailure, remediation.KeyContractFile, remediation.KeyContractEntry, remediation.KeyDBTLog,
				remediation.KeyUpstreamChanges, remediation.KeyFailurePrecedents, remediation.KeyPriorAttempts,
			}, m.Needs)
			assert.Empty(t, m.Provides)
		})
	}
}

func TestPrompts_MakeTheLanesRequestWithOneModelCall(t *testing.T) {
	for _, tc := range promptCases() {
		t.Run(tc.name, func(t *testing.T) {
			f := failure(func(f *remediation.Failure) { f.Target.NodeType = tc.nodeType })
			model := answers(answer(ports.ProposedFile{Path: "svc/contracts/fx.yaml", Content: "fixed"}))

			proposeWith(t, tc.build(), f, promptEvidence(), model)

			require.Len(t, model.reqs, 1)
			assert.Equal(t, tc.laneResult(false), model.reqs[0])
		})
	}
}

func TestPrompts_APythonAPINodeIsToldItDeclaresNoReadsInTheValidationPrompt(t *testing.T) {
	f := failure(func(f *remediation.Failure) { f.Target.NodeType = "python-api" })
	model := answers(answer())

	proposeWith(t, contract.NewPythonContract(markSanitizer{}), f, promptEvidence(), model)

	require.Len(t, model.reqs, 1)
	assert.Equal(t, prompt.AssemblePythonContractFix(laneEvidence(true)), model.reqs[0])
	assert.NotEqual(t, prompt.AssemblePythonContractFix(laneEvidence(false)), model.reqs[0])
}

func TestPrompts_EvidenceOtherThanTheContractFileMayBeAbsent(t *testing.T) {
	for _, tc := range promptCases() {
		t.Run(tc.name, func(t *testing.T) {
			model := answers(answer())

			proposeWith(t, tc.build(), failure(nil), contractFileEvidence(), model)

			ev := laneEvidence(false)
			ev.RunnerLog, ev.ContractEntry, ev.UpstreamChanges, ev.Precedents, ev.PriorAttempts = "", "", nil, nil, nil
			want := map[string]prompt.ProposeRequest{
				"python-contract": prompt.AssemblePythonContractFix(ev),
				"python-parse":    prompt.AssemblePythonParseFix(ev),
				"csv-contract":    prompt.AssembleCsvContractFix(prompt.CsvEvidence(ev)),
			}[tc.name]
			require.Len(t, model.reqs, 1)
			assert.Equal(t, want, model.reqs[0])
		})
	}
}

func TestPrompts_TheDraftHoldsOneEditPerAnsweredFile(t *testing.T) {
	for _, tc := range promptCases() {
		t.Run(tc.name, func(t *testing.T) {
			model := answers(answer(
				ports.ProposedFile{Path: "svc/contracts/fx.yaml", Content: "fixed fx"},
				ports.ProposedFile{Path: "svc/contracts/shared.yaml", Content: "fixed shared"},
			))

			out := proposeWith(t, tc.build(), failure(nil), promptEvidence(), model)

			assert.Equal(t, remediation.Draft{
				Edits: []remediation.Edit{
					{
						Path:         "svc/contracts/fx.yaml",
						Content:      "fixed fx",
						TargetNodeID: nodeID,
						ContentKey:   "proposed-fix/rel-1/py.fx.rates/attempt-2/edit-0.content",
						DiffKey:      "proposed-fix/rel-1/py.fx.rates/attempt-2/edit-0.diff",
					},
					{
						Path:         "svc/contracts/shared.yaml",
						Content:      "fixed shared",
						TargetNodeID: nodeID,
						ContentKey:   "proposed-fix/rel-1/py.fx.rates/attempt-2/edit-1.content",
						DiffKey:      "proposed-fix/rel-1/py.fx.rates/attempt-2/edit-1.diff",
					},
				},
				Rationale:      "the column is named amount_eur",
				Confidence:     "high",
				Model:          "model-1",
				SourceResolved: true,
			}, out.Draft)
		})
	}
}

func TestPrompts_AnAnswerWithNoFilesGivesADraftWithNoEdit(t *testing.T) {
	for _, tc := range promptCases() {
		t.Run(tc.name, func(t *testing.T) {
			out := proposeWith(t, tc.build(), failure(nil), promptEvidence(), answers(answer()))

			assert.Empty(t, out.Draft.Edits)
			assert.False(t, out.Draft.SourceResolved)
			assert.Equal(t, "high", out.Draft.Confidence)
			assert.Equal(t, "model-1", out.Draft.Model)
		})
	}
}

func TestPrompts_NeedTheContractFileAndReturnTheModelError(t *testing.T) {
	for _, tc := range promptCases() {
		t.Run(tc.name, func(t *testing.T) {
			model := answers(answer())
			_, err := tc.build().Propose(context.Background(),
				step.PromptInput{Failure: failure(nil), Evidence: remediation.NewEvidence()}, model)
			require.Error(t, err)
			assert.Empty(t, model.reqs)

			failing := &fakeModel{answers: []modelAnswer{{err: errBoom}}}
			_, err = tc.build().Propose(context.Background(),
				step.PromptInput{Failure: failure(nil), Evidence: promptEvidence()}, failing)
			require.ErrorIs(t, err, errBoom)
			assert.EqualError(t, err, "llm propose: boom")
		})
	}
}

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
	earlierTries    = []remediation.TryRecord{{
		Round: 1, Try: 1, Attempt: 1,
		Clusters: []remediation.ClusterRecord{{Members: []string{nodeID}, Edits: []remediation.EditRecord{{Path: "svc/contracts/fx.yaml", Diff: "<-a>"}}}},
		Gate:     []remediation.Finding{{Code: "release_check_failed", Message: "<still bad>"}},
	}}
)

// promptEvidence is what the context steps leave in a cluster's evidence.
func promptEvidence() remediation.Evidence {
	return contractFileEvidence().
		With(remediation.KeyContractEntry, remediation.Text(`<{"table":"rates"}>`)).
		With(remediation.KeyCurrentContract, remediation.Text("<id: fx.rates\nversion: \"3\"\n>")).
		With(remediation.KeyDBTLog, remediation.Text("<runner log>")).
		With(remediation.KeyUpstreamChanges, upstreamChanges).
		With(remediation.KeyFailurePrecedents, precedentList).
		With(remediation.KeyPreviousTries, earlierTries)
}

// expectedEvidence is the evidence the contract prompt builds for the same
// inputs.
func expectedEvidence(noReads bool) prompt.PythonEvidence {
	return prompt.PythonEvidence{
		NodeID:          nodeID,
		NoReads:         noReads,
		ErrorExcerpt:    "column amount missing",
		RunnerLog:       "<runner log>",
		ContractEntry:   `<{"table":"rates"}>`,
		YAMLPath:        "svc/contracts/fx.yaml",
		YAMLText:        "nodes:\n  - table: rates\n",
		CurrentContract: "<id: fx.rates\nversion: \"3\"\n>",
		UpstreamChanges: upstreamChanges,
		Precedents:      precedentList,
	}
}

// withTries is req with the section on the earlier tries the prompt evidence
// holds.
func withTries(req prompt.ProposeRequest) prompt.ProposeRequest {
	return step.WithPreviousTries(req, promptEvidence())
}

type promptCase struct {
	name        string
	build       func() step.Prompt
	wantStep    string
	nodeType    string
	wantRequest func(noReads bool) prompt.ProposeRequest
}

func promptCases() []promptCase {
	return []promptCase{
		{
			name:     "python-contract",
			build:    func() step.Prompt { return contract.NewPythonContract() },
			wantStep: "python-contract",
			nodeType: "python-node",
			wantRequest: func(noReads bool) prompt.ProposeRequest {
				return withTries(prompt.AssemblePythonContractFix(expectedEvidence(noReads)))
			},
		},
		{
			name:     "python-parse",
			build:    func() step.Prompt { return contract.NewPythonParse() },
			wantStep: "python-parse",
			nodeType: "python-node",
			wantRequest: func(noReads bool) prompt.ProposeRequest {
				return withTries(prompt.AssemblePythonParseFix(expectedEvidence(noReads)))
			},
		},
		{
			name:     "csv-contract",
			build:    func() step.Prompt { return contract.NewCSVContract() },
			wantStep: "csv-contract",
			nodeType: "python-csv",
			wantRequest: func(noReads bool) prompt.ProposeRequest {
				return withTries(prompt.AssembleCsvContractFix(prompt.CsvEvidence(expectedEvidence(noReads))))
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
				remediation.KeyFailure, remediation.KeyContractFile, remediation.KeyContractEntry, remediation.KeyCurrentContract,
				remediation.KeyDBTLog, remediation.KeyUpstreamChanges, remediation.KeyFailurePrecedents,
			}, m.Needs)
			assert.Empty(t, m.Provides)
		})
	}
}

func TestPrompts_AssembleTheirRequestAndMakeOneModelCall(t *testing.T) {
	for _, tc := range promptCases() {
		t.Run(tc.name, func(t *testing.T) {
			f := failure(func(f *remediation.Failure) { f.Target.NodeType = tc.nodeType })
			model := answers(answer(ports.ProposedFile{Path: "svc/contracts/fx.yaml", Content: "fixed"}))

			proposeWith(t, tc.build(), f, promptEvidence(), model)

			require.Len(t, model.reqs, 1)
			assert.Equal(t, tc.wantRequest(false), model.reqs[0])
			assert.Contains(t, model.reqs[0].User, "## Earlier tries at this failure\n", "the first round's request shows the earlier tries")
			assert.Contains(t, model.reqs[0].User, "Why the gate failed: <still bad>")
			assert.Contains(t, model.reqs[0].User, "Current contract (the node's contract as continuo last promoted it, for reference only: do not copy its version, or its provenance and reads properties, into the file):\n```yaml\n<id: fx.rates",
				"the first round's request shows the node's current contract")
		})
	}
}

func TestPrompts_APythonAPINodeIsToldItDeclaresNoReadsInTheValidationPrompt(t *testing.T) {
	f := failure(func(f *remediation.Failure) { f.Target.NodeType = "python-api" })
	model := answers(answer())

	proposeWith(t, contract.NewPythonContract(), f, promptEvidence(), model)

	require.Len(t, model.reqs, 1)
	assert.Equal(t, withTries(prompt.AssemblePythonContractFix(expectedEvidence(true))), model.reqs[0])
	assert.NotEqual(t, withTries(prompt.AssemblePythonContractFix(expectedEvidence(false))), model.reqs[0])
}

func TestPrompts_EvidenceOtherThanTheContractFileMayBeAbsent(t *testing.T) {
	for _, tc := range promptCases() {
		t.Run(tc.name, func(t *testing.T) {
			model := answers(answer())

			proposeWith(t, tc.build(), failure(nil), contractFileEvidence(), model)

			ev := expectedEvidence(false)
			ev.RunnerLog, ev.ContractEntry, ev.CurrentContract, ev.UpstreamChanges, ev.Precedents = "", "", "", nil, nil
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

// revisesTheLastCall runs the step's first round, then a second round that
// carries the first one's draft, last call and findings. It asserts the second
// round makes one model call, the revision of the first round's request with
// its answer and the findings, and that the draft it returns is the one the
// first round builds from the same answer.
func revisesTheLastCall(t *testing.T, tc promptCase) {
	t.Helper()
	f := failure(func(f *remediation.Failure) { f.Target.NodeType = tc.nodeType })
	first := answer(ports.ProposedFile{Path: "svc/contracts/fx.yaml", Content: "first try"})
	second := answer(
		ports.ProposedFile{Path: "svc/contracts/fx.yaml", Content: "second try"},
		ports.ProposedFile{Path: "svc/contracts/shared.yaml", Content: "second shared"},
	)
	m := answers(first, second)
	round1 := proposeWith(t, tc.build(), f, promptEvidence(), m)
	findings := []remediation.Finding{
		{Code: "contract_rejected", Message: "the packager refused the contract: column amount is not declared", File: "svc/contracts/fx.yaml"},
		{Code: "content_unchanged", Message: "the model returned the file unchanged, which is not a fix"},
	}

	out, err := tc.build().Propose(context.Background(), step.PromptInput{
		Failure: f, Evidence: promptEvidence(), Round: 1,
		Previous: &round1.Draft, PreviousRequest: &m.reqs[0], PreviousAnswer: &first, Findings: findings,
	}, m)

	require.NoError(t, err)
	require.Len(t, m.reqs, 2, "a later round makes one model call")
	assert.Equal(t, prompt.Revise(m.reqs[0], step.AnswerJSON(first), step.FindingLines(findings)), m.reqs[1])
	assert.NotEqual(t, m.reqs[0], m.reqs[1])
	again := proposeWith(t, tc.build(), f, promptEvidence(), answers(second))
	assert.Equal(t, again.Draft, out.Draft, "the answer is read as the first round's answer is")
}

func TestPythonContract_RevisesTheLastCallWithTheFindings(t *testing.T) {
	revisesTheLastCall(t, promptCases()[0])
}

func TestPythonParse_RevisesTheLastCallWithTheFindings(t *testing.T) {
	revisesTheLastCall(t, promptCases()[1])
}

func TestCSVContract_RevisesTheLastCallWithTheFindings(t *testing.T) {
	revisesTheLastCall(t, promptCases()[2])
}

func TestPrompts_ALaterRoundWithoutThePreviousCallIsAnErrorAndCallsNoModel(t *testing.T) {
	req, res := prompt.ProposeRequest{User: "u"}, answer()
	cases := map[string]step.PromptInput{
		"no previous call":    {Round: 1},
		"no previous request": {Round: 1, PreviousAnswer: &res},
		"no previous answer":  {Round: 1, PreviousRequest: &req},
	}
	for _, tc := range promptCases() {
		for name, in := range cases {
			t.Run(tc.name+" "+name, func(t *testing.T) {
				in.Failure, in.Evidence = failure(nil), promptEvidence()
				m := answers(answer())

				_, err := tc.build().Propose(context.Background(), in, m)

				require.Error(t, err)
				assert.ErrorContains(t, err, tc.wantStep)
				assert.Empty(t, m.reqs)
			})
		}
	}
}

package validation_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/domain/prompt"
	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
	"github.com/carolsimone/continuo/agent-remediation/service/steps/validation"
)

const (
	candidateSQL  = "select bad from t"
	modelSource   = "select  bad  from t -- real source"
	fixedSQL      = "select good from t"
	fixedSource   = "select  good  from t -- real source"
	repoFilePath  = "services/svc/models/a.sql"
	diagnosisNote = "the column is named good"
	editNote      = "renamed the column in the model"
)

var (
	upstreamChanges = []prompt.UpstreamChange{{NodeID: "model.svc.up", Depth: 1, CodeDiff: "<code>"}}
	precedentList   = []prompt.Precedent{{ReleaseID: "old", NodeID: "n0", ErrorExcerpt: "same error"}}
	ancestorList    = []prompt.ReleaseUpstreamChange{{NodeID: "model.svc.up", Service: "svc", Depth: 1, Diff: "<diff>"}}
)

func validationEvidence(bundle remediation.CandidateSource) remediation.Evidence {
	return remediation.NewEvidence().
		With(remediation.KeyCandidateSQL, remediation.Text(candidateSQL)).
		With(remediation.KeyCodeBundleSource, bundle).
		With(remediation.KeyDBTLog, remediation.Text("sanitized log")).
		With(remediation.KeyOwnChangeDiff, remediation.Text("<own diff>")).
		With(remediation.KeyUpstreamChanges, upstreamChanges).
		With(remediation.KeyFailurePrecedents, precedentList).
		With(remediation.KeyChangedAncestorDiffs, ancestorList)
}

func resolvedBundle() remediation.CandidateSource {
	return remediation.CandidateSource{Source: modelSource, Origin: "bundle", FilePath: "models/a.sql", Service: "svc"}
}

func stepOne(sql string) ports.ProposeResult {
	return ports.ProposeResult{
		ProposedSQL:            sql,
		Rationale:              diagnosisNote,
		Confidence:             "high",
		Model:                  "model-1",
		SuspectedRootCauseNode: "model.svc.root",
	}
}

func stepTwo(source, confidence string) ports.ProposeResult {
	return ports.ProposeResult{ProposedSQL: source, Rationale: editNote, Confidence: confidence, Model: "model-2"}
}

// firstRequest is the diagnosis request the validation lane builds from the
// inputs the step is given.
func firstRequest(ancestorsKnown bool) prompt.ProposeRequest {
	return prompt.Assemble(prompt.Evidence{
		NodeID:                 "model.svc.a",
		ErrorSignature:         "sig-1",
		CandidateSQL:           candidateSQL,
		DBTLog:                 "sanitized log",
		OwnChangeDiff:          "<own diff>",
		UpstreamChanges:        upstreamChanges,
		Precedents:             precedentList,
		ReleaseUpstreamChanges: ancestorList,
		ReleaseAncestorsKnown:  ancestorsKnown,
	})
}

// secondRequest is the source-fix request the lane builds: the sanitized source
// and the first answer's rationale.
func secondRequest() prompt.ProposeRequest {
	return prompt.AssembleSourceFix("<"+modelSource+">", "model.svc.a", diagnosisNote)
}

func newValidationPrompt(w *fakeArtifacts) *validation.DBTValidation {
	return validation.NewDBTValidation(w, markSanitizer{}, repoPaths, discardLogger())
}

func proposeValidation(t *testing.T, w *fakeArtifacts, f remediation.Failure, ev remediation.Evidence, m *fakeModel) step.PromptOutput {
	t.Helper()
	out, err := newValidationPrompt(w).Propose(context.Background(), step.PromptInput{Failure: f, Evidence: ev}, m)
	require.NoError(t, err)
	require.False(t, out.Declined)
	return out
}

func TestDBTValidation_MetaNamesTheStepAndWhatItReads(t *testing.T) {
	m := validation.NewDBTValidation(nil, nil, nil, nil).Meta()
	assert.Equal(t, "dbt-validation", string(m.Name))
	assert.Equal(t, step.KindPrompt, m.Kind)
	assert.NotEmpty(t, m.Description)
	assert.Equal(t, []remediation.EvidenceKey{
		remediation.KeyFailure, remediation.KeyCandidateSQL, remediation.KeyCodeBundleSource, remediation.KeyDBTLog,
		remediation.KeyOwnChangeDiff, remediation.KeyUpstreamChanges, remediation.KeyFailurePrecedents, remediation.KeyChangedAncestorDiffs,
	}, m.Needs)
	assert.Empty(t, m.Provides)
}

func TestDBTValidation_MakesTheLanesTwoRequests(t *testing.T) {
	model := answers(stepOne(fixedSQL), stepTwo(fixedSource, "high"))

	proposeValidation(t, &fakeArtifacts{}, failure(nil), validationEvidence(resolvedBundle()), model)

	require.Len(t, model.reqs, 2)
	assert.Equal(t, firstRequest(true), model.reqs[0])
	assert.Equal(t, secondRequest(), model.reqs[1])
}

func TestDBTValidation_AncestorsAreKnownOnlyForAValidationRejection(t *testing.T) {
	for _, source := range []string{"validation", "seed_build", "compile", "duplicate_table", ""} {
		t.Run("source "+source, func(t *testing.T) {
			model := answers(stepOne(""))

			proposeValidation(t, &fakeArtifacts{}, failure(func(f *remediation.Failure) { f.Trigger.Source = source }),
				validationEvidence(resolvedBundle()), model)

			assert.Equal(t, firstRequest(source == "validation"), model.reqs[0])
		})
	}
}

func TestDBTValidation_WritesTheCandidateArtifactsAndOffersASourceEdit(t *testing.T) {
	w := &fakeArtifacts{}

	out := proposeValidation(t, w, failure(nil), validationEvidence(resolvedBundle()),
		answers(stepOne(fixedSQL), stepTwo(fixedSource, "medium")))

	assert.Equal(t, []artifactWrite{
		{"proposed-fix/rel-1/model.svc.a/attempt-2.sql", fixedSQL, "text/plain"},
		{"proposed-fix/rel-1/model.svc.a/attempt-2.diff", proposal.ComputeUnifiedDiff(candidateSQL, fixedSQL, "model.svc.a"), "text/plain"},
	}, w.writes)
	assert.Equal(t, remediation.Draft{
		Edits: []remediation.Edit{{
			Path:         repoFilePath,
			Original:     modelSource,
			Content:      fixedSource,
			TargetNodeID: "model.svc.a",
			ContentKey:   "proposed-fix/rel-1/model.svc.a/attempt-2.source.sql",
			DiffKey:      "proposed-fix/rel-1/model.svc.a/attempt-2.source.diff",
		}},
		Rationale:           editNote,
		Confidence:          "high",
		Model:               "model-1",
		SuspectedRoot:       "model.svc.root",
		CandidateFixSQLURI:  "s3://artifacts/proposed-fix/rel-1/model.svc.a/attempt-2.sql",
		CandidateFixDiffURI: "s3://artifacts/proposed-fix/rel-1/model.svc.a/attempt-2.diff",
		SourceResolved:      true,
	}, out.Draft)
}

func TestDBTValidation_EveryDegradedSecondStepGivesADraftWithNoEdit(t *testing.T) {
	cases := []struct {
		name   string
		bundle remediation.CandidateSource
		two    *modelAnswer
		calls  int
	}{
		{"a low-confidence answer", resolvedBundle(), &modelAnswer{res: stepTwo(fixedSource, "low")}, 2},
		{"a low-confidence answer in any case", resolvedBundle(), &modelAnswer{res: stepTwo(fixedSource, " Low ")}, 2},
		{"an empty answer", resolvedBundle(), &modelAnswer{res: stepTwo("", "high")}, 2},
		{"an answer equal to the source", resolvedBundle(), &modelAnswer{res: stepTwo(modelSource, "high")}, 2},
		{"a failed call", resolvedBundle(), &modelAnswer{err: errBoom}, 2},
		{"no source", remediation.CandidateSource{FilePath: "models/a.sql", Service: "svc"}, nil, 1},
		{"no file path", remediation.CandidateSource{Source: modelSource, Service: "svc"}, nil, 1},
		{"no service", remediation.CandidateSource{Source: modelSource, FilePath: "models/a.sql"}, nil, 1},
		{"a service with no repository path", remediation.CandidateSource{Source: modelSource, FilePath: "models/a.sql", Service: "ghost"}, nil, 1},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			model := answers(stepOne(fixedSQL))
			if c.two != nil {
				model.answers = append(model.answers, *c.two)
			}
			w := &fakeArtifacts{}

			out := proposeValidation(t, w, failure(nil), validationEvidence(c.bundle), model)

			assert.Len(t, model.reqs, c.calls)
			assert.Empty(t, out.Draft.Edits)
			assert.False(t, out.Draft.SourceResolved)
			assert.Equal(t, diagnosisNote, out.Draft.Rationale, "the candidate fix keeps the first call's note")
			assert.Equal(t, "high", out.Draft.Confidence)
			assert.Equal(t, "model-1", out.Draft.Model)
			assert.NotEmpty(t, out.Draft.CandidateFixSQLURI)
			assert.NotEmpty(t, out.Draft.CandidateFixDiffURI)
			assert.Len(t, w.writes, 2, "the candidate artifacts are written whatever the second step does")
		})
	}
}

func TestDBTValidation_TheDraftsConfidenceIsTheFirstAnswers(t *testing.T) {
	first := stepOne(fixedSQL)
	first.Confidence = "medium"

	out := proposeValidation(t, &fakeArtifacts{}, failure(nil), validationEvidence(resolvedBundle()),
		answers(first, stepTwo(fixedSource, "high")))

	assert.Equal(t, "medium", out.Draft.Confidence)
	assert.Len(t, out.Draft.Edits, 1)
}

func TestDBTValidation_AFirstAnswerWithNoSQLWritesNothingAndOffersNothing(t *testing.T) {
	w := &fakeArtifacts{}
	model := answers(stepOne(""))

	out := proposeValidation(t, w, failure(nil), validationEvidence(resolvedBundle()), model)

	assert.Len(t, model.reqs, 1)
	assert.Empty(t, w.writes)
	assert.Empty(t, out.Draft.Edits)
	assert.Empty(t, out.Draft.CandidateFixSQLURI)
	assert.Empty(t, out.Draft.CandidateFixDiffURI)
	assert.Equal(t, "high", out.Draft.Confidence)
}

func TestDBTValidation_AFailedFirstCallIsReturned(t *testing.T) {
	model := &fakeModel{answers: []modelAnswer{{err: errBoom}}}

	_, err := newValidationPrompt(&fakeArtifacts{}).Propose(context.Background(),
		step.PromptInput{Failure: failure(nil), Evidence: validationEvidence(resolvedBundle())}, model)

	require.ErrorIs(t, err, errBoom)
	assert.Contains(t, err.Error(), "llm propose")
}

func TestDBTValidation_AFailedArtifactWriteIsReturned(t *testing.T) {
	cases := []struct{ name, failOn, want string }{
		{"the sql", "proposed-fix/rel-1/model.svc.a/attempt-2.sql", "write candidate sql"},
		{"the diff", "proposed-fix/rel-1/model.svc.a/attempt-2.diff", "write candidate diff"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			model := answers(stepOne(fixedSQL))

			_, err := newValidationPrompt(&fakeArtifacts{failOn: c.failOn}).Propose(context.Background(),
				step.PromptInput{Failure: failure(nil), Evidence: validationEvidence(resolvedBundle())}, model)

			require.ErrorIs(t, err, errBoom)
			assert.Contains(t, err.Error(), c.want)
			assert.Len(t, model.reqs, 1, "no second call after a failed write")
		})
	}
}

func TestDBTValidation_RefusesEvidenceMissingWhatItMustHave(t *testing.T) {
	for _, drop := range []remediation.EvidenceKey{remediation.KeyCandidateSQL, remediation.KeyCodeBundleSource} {
		t.Run(string(drop), func(t *testing.T) {
			ev := remediation.NewEvidence()
			full := validationEvidence(resolvedBundle())
			for _, k := range full.Keys() {
				if k == drop {
					continue
				}
				v, _ := remediation.Get[any](full, k)
				ev = ev.With(k, v)
			}
			model := answers()

			_, err := newValidationPrompt(&fakeArtifacts{}).Propose(context.Background(),
				step.PromptInput{Failure: failure(nil), Evidence: ev}, model)

			require.Error(t, err)
			assert.Empty(t, model.reqs)
		})
	}
}

func TestDBTValidation_OptionalEvidenceMayBeAbsent(t *testing.T) {
	ev := remediation.NewEvidence().
		With(remediation.KeyCandidateSQL, remediation.Text(candidateSQL)).
		With(remediation.KeyCodeBundleSource, remediation.CandidateSource{})
	model := answers(stepOne(""))

	proposeValidation(t, &fakeArtifacts{}, failure(nil), ev, model)

	assert.Equal(t, prompt.Assemble(prompt.Evidence{
		NodeID: "model.svc.a", ErrorSignature: "sig-1", CandidateSQL: candidateSQL, ReleaseAncestorsKnown: true,
	}), model.reqs[0])
}

// revisionInput is the input of a later round: the previous round's draft and
// last call, and the findings the checks made in the draft.
func revisionInput(prev remediation.Draft, req prompt.ProposeRequest, ans ports.ProposeResult, findings []remediation.Finding, ev remediation.Evidence) step.PromptInput {
	return step.PromptInput{Failure: failure(nil), Evidence: ev, Round: 1, Previous: &prev, PreviousRequest: &req, PreviousAnswer: &ans, Findings: findings}
}

var revisionFindings = []remediation.Finding{
	{Code: "content_unchanged", Message: "the model returned the file unchanged, which is not a fix", File: repoFilePath},
	{Code: "sql_parse", Message: "unexpected token", File: repoFilePath, Line: 3},
}

func TestDBTValidation_RevisesTheLastCallWithTheFindings(t *testing.T) {
	first := stepOne(fixedSQL)
	first.Confidence = "medium"
	second := stepTwo(fixedSource, "medium")
	round1w := &fakeArtifacts{}
	m := answers(first, second)
	round1 := proposeValidation(t, round1w, failure(nil), validationEvidence(resolvedBundle()), m)
	require.Len(t, m.reqs, 2)

	revised := stepTwo("select  better  from t -- real source", "high")
	revised.Rationale = "qualified the column"
	m.answers = append(m.answers, modelAnswer{res: revised})
	w := &fakeArtifacts{}
	out, err := newValidationPrompt(w).Propose(context.Background(),
		revisionInput(round1.Draft, m.reqs[1], second, revisionFindings, validationEvidence(resolvedBundle())), m)

	require.NoError(t, err)
	require.Len(t, m.reqs, 3, "a later round makes one model call, the source fix revised, and does not diagnose again")
	assert.Equal(t, prompt.Revise(m.reqs[1], step.AnswerJSON(second), step.FindingLines(revisionFindings)), m.reqs[2])
	assert.NotEqual(t, m.reqs[1], m.reqs[2])
	assert.Empty(t, w.writes, "the candidate artifacts are the first round's")
	assert.Equal(t, remediation.Draft{
		Edits: []remediation.Edit{{
			Path:         repoFilePath,
			Original:     modelSource,
			Content:      "select  better  from t -- real source",
			TargetNodeID: "model.svc.a",
			ContentKey:   "proposed-fix/rel-1/model.svc.a/attempt-2.source.sql",
			DiffKey:      "proposed-fix/rel-1/model.svc.a/attempt-2.source.diff",
		}},
		Rationale:           "qualified the column",
		Confidence:          "medium",
		Model:               "model-1",
		SuspectedRoot:       "model.svc.root",
		CandidateFixSQLURI:  "s3://artifacts/proposed-fix/rel-1/model.svc.a/attempt-2.sql",
		CandidateFixDiffURI: "s3://artifacts/proposed-fix/rel-1/model.svc.a/attempt-2.diff",
		SourceResolved:      true,
	}, out.Draft, "the first call's parts come from the previous draft, the edit from the revised answer")
}

func TestDBTValidation_ADegradedRevisionOfTheSourceFixKeepsThePreviousDraftsCandidateFix(t *testing.T) {
	cases := map[string]ports.ProposeResult{
		"a low-confidence answer":       stepTwo(fixedSource, "low"),
		"an empty answer":               stepTwo("", "high"),
		"an answer equal to the source": stepTwo(modelSource, "high"),
	}
	for name, revised := range cases {
		t.Run(name, func(t *testing.T) {
			first := stepOne(fixedSQL)
			second := stepTwo(fixedSource, "high")
			m := answers(first, second)
			round1 := proposeValidation(t, &fakeArtifacts{}, failure(nil), validationEvidence(resolvedBundle()), m)
			m.answers = append(m.answers, modelAnswer{res: revised})

			out, err := newValidationPrompt(&fakeArtifacts{}).Propose(context.Background(),
				revisionInput(round1.Draft, m.reqs[1], second, revisionFindings, validationEvidence(resolvedBundle())), m)

			require.NoError(t, err)
			require.Len(t, m.reqs, 3)
			assert.Empty(t, out.Draft.Edits)
			assert.False(t, out.Draft.SourceResolved)
			assert.Equal(t, "high", out.Draft.Confidence)
			assert.Equal(t, "model-1", out.Draft.Model)
			assert.Equal(t, "model.svc.root", out.Draft.SuspectedRoot)
			assert.Equal(t, round1.Draft.CandidateFixSQLURI, out.Draft.CandidateFixSQLURI)
			assert.Equal(t, round1.Draft.CandidateFixDiffURI, out.Draft.CandidateFixDiffURI)
			assert.NotEmpty(t, out.Draft.CandidateFixSQLURI)
		})
	}
}

// A first round whose diagnosis held no SQL made one call and no source fix. A
// later round revises that diagnosis and, once it holds SQL, applies it to the
// source as the first round does.
func TestDBTValidation_ALaterRoundAfterADiagnosisWithNoSQLRevisesTheDiagnosisThenAppliesIt(t *testing.T) {
	noSQL := stepOne("")
	m := answers(noSQL)
	round1 := proposeValidation(t, &fakeArtifacts{}, failure(nil), validationEvidence(resolvedBundle()), m)
	require.Len(t, m.reqs, 1)
	m.answers = append(m.answers, modelAnswer{res: stepOne(fixedSQL)}, modelAnswer{res: stepTwo(fixedSource, "high")})
	w := &fakeArtifacts{}

	out, err := newValidationPrompt(w).Propose(context.Background(),
		revisionInput(round1.Draft, m.reqs[0], noSQL, revisionFindings, validationEvidence(resolvedBundle())), m)

	require.NoError(t, err)
	require.Len(t, m.reqs, 3)
	assert.Equal(t, prompt.Revise(firstRequest(true), step.AnswerJSON(noSQL), step.FindingLines(revisionFindings)), m.reqs[1], "the diagnosis is revised")
	assert.Equal(t, secondRequest(), m.reqs[2], "and its answer is applied to the source as in the first round")
	assert.Len(t, w.writes, 2)
	fresh := proposeValidation(t, &fakeArtifacts{}, failure(nil), validationEvidence(resolvedBundle()),
		answers(stepOne(fixedSQL), stepTwo(fixedSource, "high")))
	assert.Equal(t, fresh.Draft, out.Draft, "the draft is the one the first round builds from the same answers")
}

func TestDBTValidation_ARevisedDiagnosisWithNoSQLMakesOneCallAndOffersNothing(t *testing.T) {
	noSQL := stepOne("")
	m := answers(noSQL)
	round1 := proposeValidation(t, &fakeArtifacts{}, failure(nil), validationEvidence(resolvedBundle()), m)
	m.answers = append(m.answers, modelAnswer{res: noSQL})
	w := &fakeArtifacts{}

	out, err := newValidationPrompt(w).Propose(context.Background(),
		revisionInput(round1.Draft, m.reqs[0], noSQL, revisionFindings, validationEvidence(resolvedBundle())), m)

	require.NoError(t, err)
	require.Len(t, m.reqs, 2)
	assert.Empty(t, w.writes)
	assert.Empty(t, out.Draft.Edits)
	assert.Empty(t, out.Draft.CandidateFixSQLURI)
}

func TestDBTValidation_ALaterRoundWithoutThePreviousCallIsAnErrorAndCallsNoModel(t *testing.T) {
	req, res := secondRequest(), stepTwo(fixedSource, "high")
	prev := remediation.Draft{Rationale: diagnosisNote}
	cases := map[string]step.PromptInput{
		"no previous call":    {Round: 1, Previous: &prev},
		"no previous request": {Round: 1, Previous: &prev, PreviousAnswer: &res},
		"no previous answer":  {Round: 1, Previous: &prev, PreviousRequest: &req},
		"no previous draft":   {Round: 1, PreviousRequest: &req, PreviousAnswer: &res},
	}
	for name, in := range cases {
		t.Run(name, func(t *testing.T) {
			in.Failure, in.Evidence = failure(nil), validationEvidence(resolvedBundle())
			m := answers(stepTwo(fixedSource, "high"))

			_, err := newValidationPrompt(&fakeArtifacts{}).Propose(context.Background(), in, m)

			require.Error(t, err)
			assert.ErrorContains(t, err, "dbt-validation")
			assert.Empty(t, m.reqs)
		})
	}
}

package source_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/domain/prompt"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
	"github.com/carolsimone/continuo/agent-remediation/service/steps/source"
)

const (
	sqlPath  = "services/svc/models/x.sql"
	ymlPath  = "services/svc/models/schema.yml"
	projPath = "services/svc/dbt_project.yml"
)

var shownFiles = remediation.SourceFiles{
	Files:   map[string]string{sqlPath: "select 1", ymlPath: "version: 2", projPath: "name: svc"},
	Order:   []string{sqlPath, ymlPath, projPath},
	Primary: sqlPath,
	Service: "svc",
}

var precedents = []prompt.Precedent{{ReleaseID: "old", NodeID: "n0", ErrorExcerpt: "same error"}}

func evidenceFor(files remediation.SourceFiles) remediation.Evidence {
	return remediation.NewEvidence().
		With(remediation.KeySourceFiles, files).
		With(remediation.KeyDBTLog, remediation.Text("sanitized log")).
		With(remediation.KeyFailurePrecedents, precedents)
}

func propose(t *testing.T, p step.Prompt, f remediation.Failure, ev remediation.Evidence, m *fakeModel) step.PromptOutput {
	t.Helper()
	out, err := p.Propose(context.Background(), step.PromptInput{Failure: f, Evidence: ev}, m)
	require.NoError(t, err)
	require.Len(t, m.reqs, 1, "one model call")
	require.Empty(t, out.Decline)
	return out
}

func answer(target, content string) ports.ProposeResult {
	return ports.ProposeResult{
		TargetFile:             target,
		ProposedContent:        content,
		Rationale:              "because",
		Confidence:             "high",
		Model:                  "model-1",
		SuspectedRootCauseNode: "model.svc.root",
	}
}

func sanitizedAll(files remediation.SourceFiles) []prompt.NamedFile {
	var out []prompt.NamedFile
	for _, p := range files.Order {
		out = append(out, prompt.NamedFile{Path: p, Content: markSanitizer{}.Sanitize(files.Files[p])})
	}
	return out
}

func TestPrompts_MetaNamesTheStepAndWhatItReads(t *testing.T) {
	cases := []struct {
		p     step.Prompt
		name  string
		needs []remediation.EvidenceKey
	}{
		{source.NewDBTCompile(markSanitizer{}), "dbt-compile", []remediation.EvidenceKey{remediation.KeyFailure, remediation.KeySourceFiles, remediation.KeyDBTLog, remediation.KeyFailurePrecedents}},
		{source.NewDBTParse(markSanitizer{}), "dbt-parse", []remediation.EvidenceKey{remediation.KeyFailure, remediation.KeySourceFiles, remediation.KeyFailurePrecedents}},
		{source.NewSeed(markSanitizer{}), "seed", []remediation.EvidenceKey{remediation.KeyFailure, remediation.KeySourceFiles, remediation.KeyDBTLog, remediation.KeyFailurePrecedents}},
		{source.NewDuplicateTable(markSanitizer{}), "duplicate-table", []remediation.EvidenceKey{remediation.KeyFailure, remediation.KeySourceFiles, remediation.KeyFailurePrecedents}},
	}
	for _, c := range cases {
		m := c.p.Meta()
		assert.Equal(t, c.name, string(m.Name))
		assert.Equal(t, step.KindPrompt, m.Kind, c.name)
		assert.NotEmpty(t, m.Description, c.name)
		assert.Equal(t, c.needs, m.Needs, c.name)
		assert.Empty(t, m.Provides, c.name)
	}
}

func TestDBTCompile_BuildsTheLanesRequestAndOneEditDraft(t *testing.T) {
	f := failure("compile", remediation.TriggerNode{NodeID: "svc", FilePath: "models/x.sql"})
	m := &fakeModel{results: []ports.ProposeResult{answer("models/x.sql", "select 2")}}

	out := propose(t, source.NewDBTCompile(markSanitizer{}), f, evidenceFor(shownFiles), m)

	want := prompt.AssembleCompileFix(sanitizedAll(shownFiles), "sanitized log", "svc", precedents)
	assert.Equal(t, want, m.reqs[0])
	assert.Equal(t, remediation.Draft{
		Edits: []remediation.Edit{{
			Path:         sqlPath,
			Original:     "select 1",
			Content:      "select 2",
			TargetNodeID: "svc",
			ContentKey:   "proposed-fix/rel-1/svc/attempt-2.source.sql",
			DiffKey:      "proposed-fix/rel-1/svc/attempt-2.source.diff",
		}},
		NamedTarget:    "models/x.sql",
		TargetShown:    true,
		Rationale:      "because",
		Confidence:     "high",
		Model:          "model-1",
		SuspectedRoot:  "model.svc.root",
		SourceResolved: true,
	}, out.Draft)
}

func TestDBTCompile_ADraftThatNamesAFileItWasNotShownHasNoEdit(t *testing.T) {
	f := failure("compile", remediation.TriggerNode{NodeID: "svc", FilePath: "models/x.sql"})
	m := &fakeModel{results: []ports.ProposeResult{answer("models/other.sql", "select 2")}}

	out := propose(t, source.NewDBTCompile(markSanitizer{}), f, evidenceFor(shownFiles), m)

	assert.False(t, out.Draft.TargetShown)
	assert.Equal(t, "models/other.sql", out.Draft.NamedTarget)
	assert.Empty(t, out.Draft.Edits)
	assert.Equal(t, "because", out.Draft.Rationale)
	assert.Equal(t, "model-1", out.Draft.Model)
}

func TestDBTCompile_AnEmptyTargetNeedsASoleShownFile(t *testing.T) {
	f := failure("compile", remediation.TriggerNode{NodeID: "svc", FilePath: "models/x.sql"})

	several := propose(t, source.NewDBTCompile(markSanitizer{}), f, evidenceFor(shownFiles),
		&fakeModel{results: []ports.ProposeResult{answer("", "select 2")}})
	assert.False(t, several.Draft.TargetShown)

	one := remediation.SourceFiles{Files: map[string]string{sqlPath: "select 1"}, Order: []string{sqlPath}, Primary: sqlPath}
	sole := propose(t, source.NewDBTCompile(markSanitizer{}), f, evidenceFor(one),
		&fakeModel{results: []ports.ProposeResult{answer("", "select 2")}})
	assert.True(t, sole.Draft.TargetShown)
	require.Len(t, sole.Draft.Edits, 1)
	assert.Equal(t, sqlPath, sole.Draft.Edits[0].Path)
}

func TestDBTCompile_AnUnchangedAnswerStillBecomesAnEditForTheChecksToRefuse(t *testing.T) {
	f := failure("compile", remediation.TriggerNode{NodeID: "svc", FilePath: "models/x.sql"})
	m := &fakeModel{results: []ports.ProposeResult{answer("models/x.sql", "select 1")}}

	out := propose(t, source.NewDBTCompile(markSanitizer{}), f, evidenceFor(shownFiles), m)

	require.Len(t, out.Draft.Edits, 1)
	assert.Equal(t, out.Draft.Edits[0].Original, out.Draft.Edits[0].Content)
}

func TestDBTParse_BuildsTheLanesRequest(t *testing.T) {
	f := failure("parse", remediation.TriggerNode{NodeID: "model.svc.x", Service: "svc", FilePath: "models/x.sql", ErrorExcerpt: "parse error near from"})
	m := &fakeModel{results: []ports.ProposeResult{answer("models/x.sql", "select 2")}}

	out := propose(t, source.NewDBTParse(markSanitizer{}), f, evidenceFor(shownFiles), m)

	want := prompt.AssembleParseFix(sanitizedAll(shownFiles), "<parse error near from>", "svc", "model.svc.x", precedents)
	assert.Equal(t, want, m.reqs[0])
	require.Len(t, out.Draft.Edits, 1)
	assert.Equal(t, "proposed-fix/rel-1/model.svc.x/attempt-2.source.sql", out.Draft.Edits[0].ContentKey)
	assert.Equal(t, "model.svc.x", out.Draft.Edits[0].TargetNodeID)
	assert.Equal(t, "model.svc.root", out.Draft.SuspectedRoot)
}

func TestSeed_BuildsTheLanesRequestAndAlwaysTargetsTheCSV(t *testing.T) {
	csv := remediation.SourceFiles{Files: map[string]string{"services/svc/seeds/a.csv": "a,b\n1"}, Order: []string{"services/svc/seeds/a.csv"}, Primary: "services/svc/seeds/a.csv", Service: "svc"}
	f := failure("seed_build", remediation.TriggerNode{NodeID: "seed.svc.a", Service: "svc", FilePath: "seeds/a.csv"})
	// The model names some other file, or none: the seed's target is its csv.
	for _, target := range []string{"", "seeds/a.csv", "elsewhere.csv"} {
		m := &fakeModel{results: []ports.ProposeResult{answer(target, "a,b\n1,2")}}

		out := propose(t, source.NewSeed(markSanitizer{}), f, evidenceFor(csv), m)

		want := prompt.AssembleSeedFix("services/svc/seeds/a.csv", "<a,b\n1>", "sanitized log", "seed.svc.a", precedents)
		assert.Equal(t, want, m.reqs[0])
		assert.True(t, out.Draft.TargetShown, target)
		require.Len(t, out.Draft.Edits, 1, target)
		e := out.Draft.Edits[0]
		assert.Equal(t, "services/svc/seeds/a.csv", e.Path)
		assert.Equal(t, "a,b\n1", e.Original)
		assert.Equal(t, "a,b\n1,2", e.Content)
		assert.Equal(t, "proposed-fix/rel-1/seed.svc.a/attempt-2.source.sql", e.ContentKey)
		assert.Equal(t, "proposed-fix/rel-1/seed.svc.a/attempt-2.source.diff", e.DiffKey)
		assert.Empty(t, out.Draft.SuspectedRoot, "the seed lane reads no suspected root")
	}
}

func TestDuplicateTable_BuildsTheLanesRequest(t *testing.T) {
	one := remediation.SourceFiles{Files: map[string]string{sqlPath: "select 1"}, Order: []string{sqlPath}, Primary: sqlPath, Service: "svc"}
	f := failure("duplicate_table", remediation.TriggerNode{
		NodeID: "model.svc.x", RelationID: "analytics.orders", Service: "svc", FilePath: "models/x.sql",
		OtherService: "billing", OtherFilePath: "models/orders.sql",
	})
	m := &fakeModel{results: []ports.ProposeResult{answer("", "select 1 as renamed")}}

	out := propose(t, source.NewDuplicateTable(markSanitizer{}), f, evidenceFor(one), m)

	want := prompt.AssembleDuplicateTableFix(prompt.NamedFile{Path: sqlPath, Content: "<select 1>"},
		"analytics.orders", "billing", "models/orders.sql", precedents)
	assert.Equal(t, want, m.reqs[0])
	assert.True(t, out.Draft.TargetShown)
	require.Len(t, out.Draft.Edits, 1)
	assert.Equal(t, "select 1 as renamed", out.Draft.Edits[0].Content)
	assert.Equal(t, "model.svc.root", out.Draft.SuspectedRoot)
}

func TestPrompts_ReadMissingLogAndPrecedentsAsNone(t *testing.T) {
	ev := remediation.NewEvidence().With(remediation.KeySourceFiles, shownFiles)
	f := failure("compile", remediation.TriggerNode{NodeID: "svc", FilePath: "models/x.sql"})
	m := &fakeModel{results: []ports.ProposeResult{answer("models/x.sql", "select 2")}}

	propose(t, source.NewDBTCompile(markSanitizer{}), f, ev, m)

	assert.Equal(t, prompt.AssembleCompileFix(sanitizedAll(shownFiles), "", "svc", nil), m.reqs[0])
}

func TestPrompts_ModelErrorIsReturned(t *testing.T) {
	f := failure("compile", remediation.TriggerNode{NodeID: "svc", FilePath: "models/x.sql"})
	_, err := source.NewDBTCompile(markSanitizer{}).Propose(context.Background(),
		step.PromptInput{Failure: f, Evidence: evidenceFor(shownFiles)}, &fakeModel{err: errBoom})
	assert.ErrorIs(t, err, errBoom)
	assert.ErrorContains(t, err, "llm propose")
}

func TestPrompts_WithoutSourceFilesAreAnErrorAndCallNoModel(t *testing.T) {
	m := &fakeModel{}
	_, err := source.NewDBTCompile(markSanitizer{}).Propose(context.Background(),
		step.PromptInput{Evidence: remediation.NewEvidence()}, m)
	assert.ErrorContains(t, err, "dbt-compile")
	assert.Empty(t, m.reqs)
}

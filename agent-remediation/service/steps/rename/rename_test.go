package rename_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/domain/prompt"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
	"github.com/carolsimone/continuo/agent-remediation/service/steps/rename"
)

const (
	oldPath = "services/marketing/models/table_b.sql"
	newPath = "services/marketing/models/table_b_v2.sql"
	ymlPath = "services/marketing/models/schema.yml"
	refPath = "services/marketing/models/uses_b.sql"
)

// fakeArchive hands out a fixed root, or err.
type fakeArchive struct {
	err      error
	fetches  int
	cleanups int
}

func (f *fakeArchive) Fetch(context.Context, string, string) (string, func(), error) {
	f.fetches++
	if f.err != nil {
		return "", nil, f.err
	}
	return "/checkout", func() { f.cleanups++ }, nil
}

// fakeFinder answers refs and records what it was asked.
type fakeFinder struct {
	refs             ports.ModelReferences
	root, dir, model string
}

func (f *fakeFinder) Find(root, dir, model string) (ports.ModelReferences, error) {
	f.root, f.dir, f.model = root, dir, model
	return f.refs, nil
}

// fakeModel answers with res and records the request.
type fakeModel struct {
	res  ports.ProposeResult
	reqs []prompt.ProposeRequest
}

func (m *fakeModel) Propose(_ context.Context, req prompt.ProposeRequest) (ports.ProposeResult, error) {
	m.reqs = append(m.reqs, req)
	return m.res, nil
}

func failure() remediation.Failure {
	return remediation.Failure{
		Trigger: remediation.Trigger{ReleaseID: "rel-1", Repo: "owner/demo", CommitSHA: "abc"},
		Attempt: 2,
		Target: remediation.TriggerNode{
			NodeID: "analytics.table_b", RelationID: "analytics.table_b", Service: "marketing",
			FilePath: "models/table_b.sql", OtherService: "service-1", OtherFilePath: "services/service-1/models/table_b.sql",
		},
	}
}

func sourceFiles() remediation.SourceFiles {
	return remediation.SourceFiles{
		Files: map[string]string{oldPath: "select 1 as id\n"}, Order: []string{oldPath}, Primary: oldPath, Service: "marketing",
	}
}

func references() ports.ModelReferences {
	return ports.ModelReferences{
		Files: map[string]string{
			ymlPath: "models:\n  - name: table_b\n    meta: {continuo: {grain: [id]}}\n",
			refPath: "select * from {{ ref('table_b') }}\n",
		},
		Order:        []string{ymlPath, refPath},
		ModelNames:   map[string]bool{"table_b": true, "uses_b": true},
		DeclaresKind: true,
	}
}

func plan() remediation.ModelRename {
	refs := references()
	return remediation.ModelRename{
		Service: "marketing", Relation: "analytics.table_b",
		OtherService: "service-1", OtherFilePath: "services/service-1/models/table_b.sql",
		OldName: "table_b", NewName: "table_b_v2", OldPath: oldPath, NewPath: newPath,
		References: refs.Files, Order: refs.Order,
	}
}

func gather(t *testing.T, s *rename.ModelRename) step.ContextOutput {
	t.Helper()
	out, err := s.Gather(context.Background(), step.ContextInput{
		Failure:  failure(),
		Evidence: remediation.NewEvidence().With(remediation.KeySourceFiles, sourceFiles()),
	})
	require.NoError(t, err)
	return out
}

func TestModelRename_PlansTheMoveToTheNextFreeName(t *testing.T) {
	archive := &fakeArchive{}
	refs := references()
	refs.ModelNames["table_b_v2"] = true
	finder := &fakeFinder{refs: refs}
	s := rename.NewModelRename(archive, finder, map[string]string{"marketing": "services/marketing"}, nil)

	out := gather(t, s)

	require.False(t, out.Declined, out.Decline)
	assert.Equal(t, "/checkout", finder.root)
	assert.Equal(t, "services/marketing", finder.dir)
	assert.Equal(t, "table_b", finder.model)
	assert.Equal(t, 1, archive.cleanups, "the checkout is removed")
	want := plan()
	want.NewName, want.NewPath = "table_b_v3", "services/marketing/models/table_b_v3.sql"
	assert.Equal(t, want, out.Evidence[remediation.KeyModelRename])
}

func TestModelRename_DeclinesAModelWithNoMetadataWhenItsProjectDeclaresIt(t *testing.T) {
	refs := references()
	refs.DeclaresKind, refs.OthersDeclareKind = false, true
	s := rename.NewModelRename(&fakeArchive{}, &fakeFinder{refs: refs}, map[string]string{"marketing": "services/marketing"}, nil)

	out := gather(t, s)

	assert.True(t, out.Declined)
	assert.Equal(t,
		"Renaming it to table_b_v2 fixes the duplicate, but the model has no metadata: add continuo.entity or continuo.grain to its schema.yml entry.",
		out.Decline)
}

func TestModelRename_PlansAModelWithNoMetadataWhenItsProjectDeclaresNone(t *testing.T) {
	refs := references()
	refs.DeclaresKind, refs.OthersDeclareKind = false, false
	s := rename.NewModelRename(&fakeArchive{}, &fakeFinder{refs: refs}, map[string]string{"marketing": "services/marketing"}, nil)

	assert.False(t, gather(t, s).Declined)
}

func TestModelRename_Declines(t *testing.T) {
	cases := map[string]struct {
		archive *fakeArchive
		paths   map[string]string
		want    string
	}{
		"no project directory for the service": {&fakeArchive{}, map[string]string{}, `no repository directory is configured for service "marketing"`},
		"the repository is not available":      {&fakeArchive{err: ports.ErrSourceNotFound}, map[string]string{"marketing": "services/marketing"}, "repository owner/demo at commit abc is not available"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			out := gather(t, rename.NewModelRename(tc.archive, &fakeFinder{refs: references()}, tc.paths, nil))
			assert.True(t, out.Declined)
			assert.Equal(t, tc.want, out.Decline)
		})
	}
}

func TestModelRename_AnotherFetchErrorIsTheStepsError(t *testing.T) {
	s := rename.NewModelRename(&fakeArchive{err: errors.New("timeout")}, &fakeFinder{}, map[string]string{"marketing": "services/marketing"}, nil)
	_, err := s.Gather(context.Background(), step.ContextInput{
		Failure:  failure(),
		Evidence: remediation.NewEvidence().With(remediation.KeySourceFiles, sourceFiles()),
	})
	assert.ErrorContains(t, err, "timeout")
}

func TestPrompt_ShowsThePlanAndReadsTheMoveAndTheReferences(t *testing.T) {
	m := &fakeModel{res: ports.ProposeResult{
		Files: []ports.ProposedFile{
			{Path: newPath, Content: "select 1 as id\n"},
			{Path: ymlPath, Content: "models:\n  - name: table_b_v2\n    meta: {continuo: {grain: [id]}}\n"},
			{Path: refPath, Content: "select * from {{ ref('table_b_v2') }}\n"},
		},
		Rationale: "renamed the model", Confidence: "high", Model: "claude-test",
	}}
	ev := remediation.NewEvidence().With(remediation.KeySourceFiles, sourceFiles()).With(remediation.KeyModelRename, plan())

	out, err := rename.NewPrompt().Propose(context.Background(), step.PromptInput{Failure: failure(), Evidence: ev}, m)
	require.NoError(t, err)

	require.Len(t, m.reqs, 1)
	assert.Contains(t, m.reqs[0].User, "Rename the model table_b to table_b_v2.")
	assert.Contains(t, m.reqs[0].User, "The model's file "+oldPath+" moves to "+newPath)
	assert.Contains(t, m.reqs[0].User, "File "+ymlPath+" names the model")
	d := out.Draft
	assert.Equal(t, oldPath, d.NamedTarget)
	assert.True(t, d.TargetShown)
	assert.True(t, d.SourceResolved)
	assert.Equal(t, plan().Explanation()+" renamed the model", d.Rationale)
	require.Len(t, d.Edits, 3)
	assert.Equal(t, remediation.Edit{
		Path: newPath, RenamedFrom: oldPath, Original: "select 1 as id\n", Content: "select 1 as id\n",
		TargetNodeID: "analytics.table_b",
		ContentKey:   "proposed-fix/rel-1/analytics.table_b/attempt-2/edit-0.content",
		DiffKey:      "proposed-fix/rel-1/analytics.table_b/attempt-2/edit-0.diff",
	}, d.Edits[0])
	assert.Empty(t, d.Edits[1].RenamedFrom)
	assert.Equal(t, references().Files[ymlPath], d.Edits[1].Original)
	assert.Equal(t, references().Files[refPath], d.Edits[2].Original)
}

func check(t *testing.T, d remediation.Draft) []remediation.Finding {
	t.Helper()
	out, err := rename.NewMatchesPlan().Check(context.Background(), step.CheckInput{
		Draft:    d,
		Evidence: remediation.NewEvidence().With(remediation.KeyModelRename, plan()),
	})
	require.NoError(t, err)
	return out.Result.Findings
}

func TestMatchesPlan_PassesTheWholeRename(t *testing.T) {
	assert.Empty(t, check(t, remediation.Draft{Edits: []remediation.Edit{
		{Path: newPath, RenamedFrom: oldPath, Content: "select 1 as id\n"},
		{Path: ymlPath, Content: "models:\n  - name: table_b_v2\n"},
		{Path: refPath, Content: "select * from {{ ref('table_b_v2') }}\n"},
	}}))
}

func TestMatchesPlan_RefusesAnAliasInPlace(t *testing.T) {
	findings := check(t, remediation.Draft{Edits: []remediation.Edit{
		{Path: oldPath, Content: "{{ config(alias='marketing_table_b') }}\nselect 1 as id\n"},
	}})
	codes := make([]string, 0, len(findings))
	for _, f := range findings {
		codes = append(codes, f.Code+" "+f.File)
	}
	assert.Equal(t, []string{
		"rename_missing " + oldPath,
		"edit_outside_rename " + oldPath,
		"reference_left " + ymlPath,
		"reference_left " + refPath,
	}, codes)
}

func TestMatchesPlan_RefusesAReferenceEditedButStillNamingTheOldModel(t *testing.T) {
	findings := check(t, remediation.Draft{Edits: []remediation.Edit{
		{Path: newPath, RenamedFrom: oldPath, Content: "select 1 as id\n"},
		{Path: ymlPath, Content: "models:\n  - name: table_b_v2\n"},
		{Path: refPath, Content: "select id from {{ ref('table_b') }}\n"},
	}})
	require.Len(t, findings, 1)
	assert.Equal(t, "reference_left", findings[0].Code)
	assert.Equal(t, refPath+" still names the model table_b, which the fix renames to table_b_v2", findings[0].Message)
}

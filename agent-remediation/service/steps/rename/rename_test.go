package rename_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/adapters/repofs"
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

// noNodes is a catalog that holds no node.
type noNodes struct{}

func (noNodes) Locate(context.Context, string) (string, string, error) { return "", "", nil }

// someNodes is a catalog that holds exactly the given unique ids.
type someNodes map[string]bool

func (n someNodes) Locate(_ context.Context, id string) (string, string, error) {
	if n[id] {
		return "models/x.sql", "other-project", nil
	}
	return "", "", nil
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
			NodeID: "analytics.table_b", RelationID: "analytics.table_b", Project: "marketing",
			FilePath: "models/table_b.sql", OtherProject: "service-1", OtherFilePath: "services/service-1/models/table_b.sql",
		},
	}
}

func sourceFiles() remediation.SourceFiles {
	return remediation.SourceFiles{
		Files: map[string]string{oldPath: "select 1 as id\n"}, Order: []string{oldPath}, Primary: oldPath, Project: "marketing",
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
		Project: "marketing", Relation: "analytics.table_b",
		OtherProject: "service-1", OtherFilePath: "services/service-1/models/table_b.sql",
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
	s := rename.NewModelRename(archive, finder, noNodes{}, map[string]string{"marketing": "services/marketing"}, nil)

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
	s := rename.NewModelRename(&fakeArchive{}, &fakeFinder{refs: refs}, noNodes{}, map[string]string{"marketing": "services/marketing"}, nil)

	out := gather(t, s)

	assert.True(t, out.Declined)
	assert.Equal(t,
		"Renaming it to table_b_v2 fixes the duplicate, but the model has no metadata: add continuo.entity or continuo.grain to its schema.yml entry.",
		out.Decline)
}

func TestModelRename_PlansAModelWithNoMetadataWhenItsProjectDeclaresNone(t *testing.T) {
	refs := references()
	refs.DeclaresKind, refs.OthersDeclareKind = false, false
	s := rename.NewModelRename(&fakeArchive{}, &fakeFinder{refs: refs}, noNodes{}, map[string]string{"marketing": "services/marketing"}, nil)

	assert.False(t, gather(t, s).Declined)
}

func TestModelRename_Declines(t *testing.T) {
	cases := map[string]struct {
		archive *fakeArchive
		paths   map[string]string
		want    string
	}{
		"no project directory for the project": {&fakeArchive{}, map[string]string{}, `no repository directory is configured for project "marketing"`},
		"the repository is not available":      {&fakeArchive{err: ports.ErrSourceNotFound}, map[string]string{"marketing": "services/marketing"}, "repository owner/demo at commit abc is not available"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			out := gather(t, rename.NewModelRename(tc.archive, &fakeFinder{refs: references()}, noNodes{}, tc.paths, nil))
			assert.True(t, out.Declined)
			assert.Equal(t, tc.want, out.Decline)
		})
	}
}

func TestModelRename_AnotherFetchErrorIsTheStepsError(t *testing.T) {
	s := rename.NewModelRename(&fakeArchive{err: errors.New("timeout")}, &fakeFinder{}, noNodes{}, map[string]string{"marketing": "services/marketing"}, nil)
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

// checkoutArchive hands out a fresh temporary checkout holding files.
type checkoutArchive struct {
	t     *testing.T
	files map[string]string
}

func (a checkoutArchive) Fetch(context.Context, string, string) (string, func(), error) {
	root := a.t.TempDir()
	for p, content := range a.files {
		full := filepath.Join(root, filepath.FromSlash(p))
		require.NoError(a.t, os.MkdirAll(filepath.Dir(full), 0o750))
		require.NoError(a.t, os.WriteFile(full, []byte(content), 0o600))
	}
	return root, func() {}, nil
}

// repoFiles is the repository the plan was made from.
func repoFiles() map[string]string {
	return map[string]string{
		oldPath: "select 1 as id\n",
		ymlPath: references().Files[ymlPath],
		refPath: references().Files[refPath],
	}
}

// checkOver runs rename_matches_plan on d over a checkout of files, with the
// real patcher and finder.
func checkOver(t *testing.T, files map[string]string, d remediation.Draft) []remediation.Finding {
	t.Helper()
	scratch := step.NewScratch(checkoutArchive{t: t, files: files}, repofs.CheckoutPatcher{}, "owner/demo", "abc")
	defer scratch.Close()
	c := rename.NewMatchesPlan(repofs.ModelReferenceFinder{}, map[string]string{"marketing": "services/marketing"})
	out, err := c.Check(context.Background(), step.CheckInput{
		Draft:    d,
		Evidence: remediation.NewEvidence().With(remediation.KeyModelRename, plan()),
		Scratch:  scratch,
	})
	require.NoError(t, err)
	return out.Result.Findings
}

func check(t *testing.T, d remediation.Draft) []remediation.Finding {
	t.Helper()
	return checkOver(t, repoFiles(), d)
}

func codes(findings []remediation.Finding) []string {
	out := make([]string, 0, len(findings))
	for _, f := range findings {
		out = append(out, f.Code+" "+f.File)
	}
	return out
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
	assert.Equal(t, []string{
		"rename_missing " + oldPath,
		"edit_outside_rename " + oldPath,
		"reference_left " + ymlPath,
		"reference_left " + refPath,
	}, codes(findings))
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

// TestMatchesPlan_ReadsAnyPropertiesFileTheFinderReads pins that the check and
// model_rename share one rule for "names the model": an entry whose name is
// not its first key, or written in flow style, still counts.
func TestMatchesPlan_ReadsAnyPropertiesFileTheFinderReads(t *testing.T) {
	cases := map[string]string{
		"name not the first key": "models:\n  - description: the old one\n    name: table_b\n",
		"flow style":             "models: [{name: table_b, description: the old one}]\n",
	}
	for name, leftover := range cases {
		t.Run(name, func(t *testing.T) {
			findings := check(t, remediation.Draft{Edits: []remediation.Edit{
				{Path: newPath, RenamedFrom: oldPath, Content: "select 1 as id\n"},
				{Path: ymlPath, Content: leftover},
				{Path: refPath, Content: "select * from {{ ref('table_b_v2') }}\n"},
			}})
			assert.Equal(t, []string{"reference_left " + ymlPath}, codes(findings))
		})
	}
}

// TestModelRename_ANameAnotherProjectBuildsIsTaken pins that the new name is
// free across projects: when the live topology already holds
// analytics.table_b_v2, the plan moves on to table_b_v3.
func TestModelRename_ANameAnotherProjectBuildsIsTaken(t *testing.T) {
	s := rename.NewModelRename(&fakeArchive{}, &fakeFinder{refs: references()}, someNodes{"analytics.table_b_v2": true},
		map[string]string{"marketing": "services/marketing"}, nil)

	out := gather(t, s)

	plan := out.Evidence[remediation.KeyModelRename].(remediation.ModelRename)
	assert.Equal(t, "table_b_v3", plan.NewName)
	assert.Equal(t, "services/marketing/models/table_b_v3.sql", plan.NewPath)
}

// TestModelRename_AnAliasThatClashesIsWhatChanges pins the alias case: the
// model marketing_orders writes analytics.orders through its alias, so moving
// its file would leave the clash; the plan changes the alias and keeps the file.
func TestModelRename_AnAliasThatClashesIsWhatChanges(t *testing.T) {
	f := failure()
	f.Target.NodeID, f.Target.RelationID, f.Target.FilePath = "analytics.marketing_orders", "analytics.orders", "models/marketing_orders.sql"
	files := remediation.SourceFiles{
		Files:   map[string]string{"services/marketing/models/marketing_orders.sql": "{{ config(alias='orders') }}\nselect 1 as id\n"},
		Order:   []string{"services/marketing/models/marketing_orders.sql"},
		Primary: "services/marketing/models/marketing_orders.sql", Project: "marketing",
	}
	s := rename.NewModelRename(&fakeArchive{}, &fakeFinder{refs: ports.ModelReferences{ModelNames: map[string]bool{"marketing_orders": true}}},
		noNodes{}, map[string]string{"marketing": "services/marketing"}, nil)

	out, err := s.Gather(context.Background(), step.ContextInput{Failure: f, Evidence: remediation.NewEvidence().With(remediation.KeySourceFiles, files)})
	require.NoError(t, err)

	plan := out.Evidence[remediation.KeyModelRename].(remediation.ModelRename)
	assert.True(t, plan.ChangesAlias)
	assert.Equal(t, "orders", plan.OldName)
	assert.Equal(t, "orders_v2", plan.NewName)
	assert.Equal(t, plan.OldPath, plan.NewPath, "the file stays where it is")
}

// TestPrompt_AnAliasPlanAsksForTheAliasAndMovesNoFile pins the prompt of the
// alias case: the request asks to change the alias, and the answered model
// file is an edit in place.
func TestPrompt_AnAliasPlanAsksForTheAliasAndMovesNoFile(t *testing.T) {
	p := plan()
	p.ChangesAlias, p.OldName, p.NewName, p.NewPath = true, "orders", "orders_v2", oldPath
	m := &fakeModel{res: ports.ProposeResult{
		Files:      []ports.ProposedFile{{Path: oldPath, Content: "{{ config(alias='orders_v2') }}\nselect 1 as id\n"}},
		Confidence: "high",
	}}
	ev := remediation.NewEvidence().With(remediation.KeySourceFiles, sourceFiles()).With(remediation.KeyModelRename, p)

	out, err := rename.NewPrompt().Propose(context.Background(), step.PromptInput{Failure: failure(), Evidence: ev}, m)
	require.NoError(t, err)

	assert.Contains(t, m.reqs[0].User, "Change the model's alias from orders to orders_v2.")
	require.Len(t, out.Draft.Edits, 1)
	assert.Empty(t, out.Draft.Edits[0].RenamedFrom, "an alias change moves no file")
	assert.Equal(t, "select 1 as id\n", out.Draft.Edits[0].Original)
}

// TestPrompt_AClusterWithNoPlanIsDeclined pins a cluster gathered before its
// type listed model_rename: the prompt declines with what to do, calling no model.
func TestPrompt_AClusterWithNoPlanIsDeclined(t *testing.T) {
	m := &fakeModel{}
	ev := remediation.NewEvidence().With(remediation.KeySourceFiles, sourceFiles())

	out, err := rename.NewPrompt().Propose(context.Background(), step.PromptInput{Failure: failure(), Evidence: ev}, m)
	require.NoError(t, err)

	assert.True(t, out.Declined)
	assert.Contains(t, out.Decline, "use Try again")
	assert.Empty(t, m.reqs)
}

// TestMatchesPlan_ADraftWithNoPlanIsRefused pins that a pre-flight pass over a
// draft drawn before renames were planned refuses it, rather than failing the
// step on every drive.
func TestMatchesPlan_ADraftWithNoPlanIsRefused(t *testing.T) {
	c := rename.NewMatchesPlan(repofs.ModelReferenceFinder{}, map[string]string{"marketing": "services/marketing"})
	out, err := c.Check(context.Background(), step.CheckInput{
		Draft:    remediation.Draft{Edits: []remediation.Edit{{Path: oldPath, Content: "select 2"}}},
		Evidence: remediation.NewEvidence(),
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"rename_not_planned "}, codes(out.Result.Findings))
}

// TestMatchesPlan_TheFileTheMoveDeletesLeavesNothingBehind pins that the old
// file, gone once the move is applied, is not read as naming the old model:
// here it mentions ref('table_b') in a comment.
func TestMatchesPlan_TheFileTheMoveDeletesLeavesNothingBehind(t *testing.T) {
	files := repoFiles()
	files[oldPath] = "-- built before ref('table_b') existed\nselect 1 as id\n"
	assert.Empty(t, checkOver(t, files, remediation.Draft{Edits: []remediation.Edit{
		{Path: newPath, RenamedFrom: oldPath, Content: "-- built before\nselect 1 as id\n"},
		{Path: ymlPath, Content: "models:\n  - name: table_b_v2\n"},
		{Path: refPath, Content: "select * from {{ ref('table_b_v2') }}\n"},
	}}))
}

// TestMatchesPlan_AnAliasPlanNeedsNoMoveAndReadsNoReference pins the check of
// the alias case: no move is required, the model's name is unchanged so no
// file can be left naming an old one, and an edit outside the plan is refused.
func TestMatchesPlan_AnAliasPlanNeedsNoMoveAndReadsNoReference(t *testing.T) {
	p := plan()
	p.ChangesAlias, p.OldName, p.NewName, p.NewPath = true, "orders", "orders_v2", oldPath
	c := rename.NewMatchesPlan(repofs.ModelReferenceFinder{}, map[string]string{"marketing": "services/marketing"})
	run := func(d remediation.Draft) []string {
		out, err := c.Check(context.Background(), step.CheckInput{Draft: d, Evidence: remediation.NewEvidence().With(remediation.KeyModelRename, p)})
		require.NoError(t, err)
		return codes(out.Result.Findings)
	}
	assert.Empty(t, run(remediation.Draft{Edits: []remediation.Edit{{Path: oldPath, Content: "{{ config(alias='orders_v2') }}\nselect 1"}}}))
	assert.Equal(t, []string{"edit_outside_rename services/marketing/models/elsewhere.sql"},
		run(remediation.Draft{Edits: []remediation.Edit{{Path: "services/marketing/models/elsewhere.sql", Content: "select 2"}}}))
}

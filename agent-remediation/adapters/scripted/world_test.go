package scripted

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/adapters/memory"
	"github.com/carolsimone/continuo/agent-remediation/domain/fix"
	"github.com/carolsimone/continuo/agent-remediation/service/conformance"
	"github.com/carolsimone/continuo/agent-remediation/service/llmcache"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/promptlog"
)

var ctx = context.Background()

func about(node string) context.Context {
	return promptlog.ContextWithFailure(ctx, promptlog.Failure{NodeID: node})
}

func answer(rationale string) ports.ProposeResult { return ports.ProposeResult{Rationale: rationale} }

func TestModel_AnswersACallFromTheAnswersOfItsTarget(t *testing.T) {
	m := newModel(
		[]ports.ProposeResult{answer("default-1"), answer("default-2")},
		map[string][]ports.ProposeResult{"model.a": {answer("a-1"), answer("a-2")}},
	)
	var seen []int
	m.setOnCall(func(n int) { seen = append(seen, n) })

	var got []string
	for _, c := range []context.Context{about("model.a"), about("model.b"), about("model.a"), ctx} {
		res, err := m.Propose(c, ports.ProposeRequest{User: "u"})
		require.NoError(t, err)
		got = append(got, res.Rationale)
	}

	assert.Equal(t, []string{"a-1", "default-1", "a-2", "default-2"}, got)
	assert.Equal(t, []int{1, 2, 3, 4}, seen)
	targets := make([]string, 0, 4)
	for _, c := range m.recorded() {
		targets = append(targets, c.Target)
	}
	assert.Equal(t, []string{"model.a", "model.b", "model.a", ""}, targets)
}

// Each answer is given once, so a run that calls the model more often than
// its scenario scripted fails rather than being answered from the start of
// the list again.
func TestModel_RefusesACallPastTheLastAnswerOfItsList(t *testing.T) {
	m := newModel(
		[]ports.ProposeResult{answer("default")},
		map[string][]ports.ProposeResult{"model.a": {answer("a-1")}},
	)

	_, err := m.Propose(about("model.a"), ports.ProposeRequest{})
	require.NoError(t, err)
	_, err = m.Propose(about("model.a"), ports.ProposeRequest{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), `1 model answers for calls about "model.a", and this is call 2`)

	res, err := m.Propose(about("model.b"), ports.ProposeRequest{})
	require.NoError(t, err, "another list keeps its own count")
	assert.Equal(t, "default", res.Rationale)
	assert.Len(t, m.recorded(), 2, "a refused call is not recorded")
}

func TestModel_RefusesACallTheScriptHasNoAnswerFor(t *testing.T) {
	m := newModel(nil, map[string][]ports.ProposeResult{"model.a": {answer("a")}})

	_, err := m.Propose(about("model.b"), ports.ProposeRequest{})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "model.b")
	assert.Empty(t, m.recorded())
}

func TestWorld_AnswersARepeatedRequestFromTheResponseCache(t *testing.T) {
	w, err := NewWorld(memory.NewStore(), conformance.Fixture{}, conformance.Script{Answers: []ports.ProposeResult{answer("a")}})
	require.NoError(t, err)
	keyed := llmcache.ContextWithIdempotencyKey(about("model.a"), "remediation:x:try:1:cluster:1:round:1")
	req := ports.ProposeRequest{System: "s", User: "u", ToolName: "propose_fix"}

	first, err := w.LLM().Propose(keyed, req)
	require.NoError(t, err)
	second, err := w.LLM().Propose(keyed, req)
	require.NoError(t, err)

	assert.Equal(t, "a", first.Rationale)
	assert.True(t, second.Cached)
	assert.Len(t, w.ModelCalls(), 1)
}

func TestCheckout_HoldsTheFilesAtTheTriggersCommitOnly(t *testing.T) {
	c := checkout{repo: "o/r", commit: "sha", files: map[string]string{
		"services/a/models/x.sql":    "select 1",
		"services/a/models/x.yml":    "version: 2",
		"services/a/models/sub/y.sq": "nested",
	}}

	got, err := c.ReadFile(ctx, "o/r", "sha", "services/a/models/x.sql")
	require.NoError(t, err)
	assert.Equal(t, "select 1", got)
	_, err = c.ReadFile(ctx, "o/r", "other", "services/a/models/x.sql")
	assert.ErrorIs(t, err, ports.ErrSourceNotFound)
	_, err = c.ReadFile(ctx, "o/r", "sha", "services/a/missing.sql")
	assert.ErrorIs(t, err, ports.ErrSourceNotFound)

	listed, err := c.ListDir(ctx, "o/r", "sha", "services/a/models/")
	require.NoError(t, err)
	assert.Equal(t, []string{"services/a/models/x.sql", "services/a/models/x.yml"}, listed)
	_, err = c.ListDir(ctx, "o/r", "sha", "services/b")
	assert.ErrorIs(t, err, ports.ErrSourceNotFound)

	_, _, err = c.Fetch(ctx, "o/other", "sha")
	assert.ErrorIs(t, err, ports.ErrSourceNotFound)
}

func TestCheckout_FetchWritesEveryFileAndCleanupRemovesThem(t *testing.T) {
	c := checkout{repo: "o/r", commit: "sha", files: map[string]string{"services/a/contracts/n.yml": "nodes: []\n"}}

	root, cleanup, err := c.Fetch(ctx, "o/r", "sha")
	require.NoError(t, err)
	body, err := os.ReadFile(filepath.Clean(filepath.Join(root, "services", "a", "contracts", "n.yml"))) //nolint:gosec // G304: a file of the checkout the test just fetched.
	require.NoError(t, err)
	assert.Equal(t, "nodes: []\n", string(body))

	cleanup()
	_, err = os.Stat(root)
	assert.True(t, os.IsNotExist(err))
}

func TestNewWorld_RefusesARepoFileOutsideTheRepository(t *testing.T) {
	_, err := NewWorld(memory.NewStore(), conformance.Fixture{File: "f.yaml", RepoFiles: map[string]string{"../escape.sql": "x"}}, conformance.Script{})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "../escape.sql")
}

// Every path outside the repository is named, in path order, whatever order
// the paths were added to the fixture in.
func TestNewWorld_NamesEveryPathOutsideTheRepositoryInPathOrder(t *testing.T) {
	for _, paths := range [][]string{
		{"/z/abs.sql", "ok.sql", "../b.sql", "../a.sql"},
		{"../a.sql", "../b.sql", "ok.sql", "/z/abs.sql"},
	} {
		files := map[string]string{}
		for _, p := range paths {
			files[p] = "x"
		}
		for range 20 {
			_, err := NewWorld(memory.NewStore(), conformance.Fixture{File: "f.yaml", RepoFiles: files}, conformance.Script{})

			require.Error(t, err)
			assert.Equal(t, `f.yaml: repo files "../a.sql", "../b.sql", "/z/abs.sql" are not paths inside the repository`, err.Error())
		}
	}
}

// Files are written in path order, so a file that is also another file's
// directory fails on the same path whatever order the files were added in.
func TestCheckout_FetchWritesTheFilesInPathOrder(t *testing.T) {
	for _, paths := range [][]string{{"m/a/b.sql", "m/a"}, {"m/a", "m/a/b.sql"}} {
		files := map[string]string{}
		for _, p := range paths {
			files[p] = "content of " + p
		}
		c := checkout{repo: "o/r", commit: "sha", files: files}
		for range 20 {
			_, _, err := c.Fetch(ctx, "o/r", "sha")

			require.Error(t, err)
			assert.Contains(t, err.Error(), "create directory for m/a/b.sql")
		}
	}
}

func TestObjectStore_ReadsTheFixturesObjectsAndEveryArtifactWritten(t *testing.T) {
	s := newObjectStore(map[string]string{"s3://logs/a.log": "log"})

	got, err := s.Fetch(ctx, "s3://logs/a.log")
	require.NoError(t, err)
	assert.Equal(t, "log", got)
	uri, err := s.Write(ctx, "proposed-fix/rel/n/try-1.source.sql", "select 2", "text/plain")
	require.NoError(t, err)
	assert.Equal(t, "s3://conformance/proposed-fix/rel/n/try-1.source.sql", uri)
	got, err = s.Fetch(ctx, uri)
	require.NoError(t, err)
	assert.Equal(t, "select 2", got)
	_, err = s.Fetch(ctx, "s3://logs/missing.log")
	assert.ErrorIs(t, err, ports.ErrNotFound)
}

func TestReleaseChecks_FailsTheRunsOfTheFirstTriesWithTheGateErrors(t *testing.T) {
	p := newReleaseChecks(1, map[string]string{"model.a": "still broken"}, fixedClock{now: startTime})
	require.NoError(t, p.Submit(ctx, ports.ReleaseCheckRequest{CheckID: "run-1", Try: 1}))
	require.NoError(t, p.Submit(ctx, ports.ReleaseCheckRequest{CheckID: "run-2", Try: 2}))

	first, err := p.Status(ctx, "run-1")
	require.NoError(t, err)
	assert.Equal(t, ports.ReleaseCheckStatus{Phase: fix.PhaseFailed, ActivatedAt: startTime, NodeErrors: map[string]string{"model.a": "still broken"}}, first)
	second, err := p.Status(ctx, "run-2")
	require.NoError(t, err)
	assert.Equal(t, ports.ReleaseCheckStatus{Phase: fix.PhasePassed, ActivatedAt: startTime}, second)
	_, err = p.Status(ctx, "run-3")
	require.Error(t, err)
	assert.Len(t, p.requests(), 2)
}

func TestPackager_PackagesAContractDirectoryAndRefusesAsTheFixtureSays(t *testing.T) {
	root := t.TempDir()
	contracts := filepath.Join(root, "contracts")
	require.NoError(t, os.MkdirAll(contracts, 0o750))

	pkgs := map[string]string{"pyarrow": "26.0.0"}
	packed := conformance.Packager{Packed: "packed"}
	_, err := packager{p: packed}.Pack(ctx, contracts, root, "svc", "", pkgs)
	require.Error(t, err, "a directory with no contract yaml")

	require.NoError(t, os.WriteFile(filepath.Join(contracts, "n.odcs.yaml"), []byte("id: s.n\n"), 0o600))
	bundle, err := packager{p: packed}.Pack(ctx, contracts, root, "svc", "", pkgs)
	require.NoError(t, err)
	assert.Equal(t, []byte("packed"), bundle)

	_, err = packager{p: packed}.Pack(ctx, contracts, root, "svc", "", nil)
	require.Error(t, err, "a bundle packed with no package list")

	_, err = packager{p: conformance.Packager{Refuse: "bad read"}}.Pack(ctx, contracts, root, "svc", "", pkgs)
	assert.ErrorIs(t, err, ports.ErrContractRejected)
	assert.Contains(t, err.Error(), "bad read")

	_, err = packager{p: packed}.Pack(ctx, filepath.Join(root, "missing"), root, "svc", "", pkgs)
	assert.Error(t, err)
}

func TestCatalog_AnswersFromTheFixture(t *testing.T) {
	c := catalog{c: conformance.Catalog{
		Locations:       map[string]conformance.Location{"model.a": {FilePath: "models/a.sql", Project: "svc"}},
		CurrentVersions: map[string]ports.CurrentVersion{"model.a": {RawCode: "select 1"}},
	}}

	file, project, err := c.Locate(ctx, "model.a")
	require.NoError(t, err)
	assert.Equal(t, "models/a.sql", file)
	assert.Equal(t, "svc", project)
	file, project, err = c.Locate(ctx, "model.b")
	require.NoError(t, err, "a node the fixture does not place has no location, which is no error")
	assert.Empty(t, file)
	assert.Empty(t, project)
	v, ok, err := c.CurrentVersion(ctx, "model.a")
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, "select 1", v.RawCode)
	_, ok, err = c.CurrentVersion(ctx, "model.b")
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestCatalog_AnswersTheCurrentContractFromTheFixture(t *testing.T) {
	c := catalog{c: conformance.Catalog{Contracts: map[string]string{"py.a": "id: py.a\n"}}}

	doc, err := c.CurrentContract(ctx, "py.a")
	require.NoError(t, err)
	assert.Equal(t, "id: py.a\n", doc)
	doc, err = c.CurrentContract(ctx, "py.b")
	require.NoError(t, err, "a node with no contract is no error")
	assert.Empty(t, doc)
}

func TestImagePackages_AnswerFromTheFixtureOrNone(t *testing.T) {
	got, err := imagePackages{r: conformance.Release{ImagePackages: map[string]string{"pyarrow": "26.0.0"}}}.ImagePackages(ctx, "rel", "svc")
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"pyarrow": "26.0.0"}, got)

	_, err = imagePackages{}.ImagePackages(ctx, "rel", "svc")
	assert.ErrorIs(t, err, ports.ErrNoImagePackages)
}

func TestWorld_ReadsBackWhatTheStoreHolds(t *testing.T) {
	store := memory.NewStore()
	w, err := NewWorld(store, conformance.Fixture{}, conformance.Script{})
	require.NoError(t, err)
	u := w.NewUoW()
	require.NoError(t, u.Begin(ctx))
	require.NoError(t, u.FixRepo().Upsert(ctx, fix.Fix{ReleaseID: "rel", Try: 2, Status: fix.StatusFailed}))
	require.NoError(t, u.FixRepo().Upsert(ctx, fix.Fix{ReleaseID: "rel", Try: 1, Status: fix.StatusSkipped}))
	require.NoError(t, u.Commit())

	tries := w.FixRows()
	require.Len(t, tries, 2)
	assert.Equal(t, 1, tries[0].Fix.Try)
	assert.Equal(t, 2, tries[1].Fix.Try)
	assert.Empty(t, w.Remediations())
	assert.Empty(t, w.Tries(uuid.Nil))
}

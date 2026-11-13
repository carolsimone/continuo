package scripted

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/adapters/memory"
	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
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

func TestModel_AnswersACallFromTheAnswersOfItsTargetAndStartsOverAtTheEnd(t *testing.T) {
	m := newModel(
		[]ports.ProposeResult{answer("default")},
		map[string][]ports.ProposeResult{"model.a": {answer("a-1"), answer("a-2")}},
	)
	var seen []int
	m.setOnCall(func(n int) { seen = append(seen, n) })

	var got []string
	for _, c := range []context.Context{about("model.a"), about("model.b"), about("model.a"), ctx, about("model.a")} {
		res, err := m.Propose(c, ports.ProposeRequest{User: "u"})
		require.NoError(t, err)
		got = append(got, res.Rationale)
	}

	assert.Equal(t, []string{"a-1", "default", "a-2", "default", "a-1"}, got)
	assert.Equal(t, []int{1, 2, 3, 4, 5}, seen)
	assert.Len(t, m.requests(), 5)
}

func TestModel_RefusesACallTheScriptHasNoAnswerFor(t *testing.T) {
	m := newModel(nil, map[string][]ports.ProposeResult{"model.a": {answer("a")}})

	_, err := m.Propose(about("model.b"), ports.ProposeRequest{})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "model.b")
	assert.Empty(t, m.requests())
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
	assert.Len(t, w.ModelRequests(), 1)
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
	body, err := os.ReadFile(filepath.Join(root, "services", "a", "contracts", "n.yml"))
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

func TestObjectStore_ReadsTheFixturesObjectsAndEveryArtifactWritten(t *testing.T) {
	s := newObjectStore(map[string]string{"s3://logs/a.log": "log"})

	got, err := s.Fetch(ctx, "s3://logs/a.log")
	require.NoError(t, err)
	assert.Equal(t, "log", got)
	uri, err := s.Write(ctx, "proposed-fix/rel/n/attempt-1.source.sql", "select 2", "text/plain")
	require.NoError(t, err)
	assert.Equal(t, "s3://conformance/proposed-fix/rel/n/attempt-1.source.sql", uri)
	got, err = s.Fetch(ctx, uri)
	require.NoError(t, err)
	assert.Equal(t, "select 2", got)
	_, err = s.Fetch(ctx, "s3://logs/missing.log")
	assert.ErrorIs(t, err, ports.ErrNotFound)
}

func TestPipeline_FailsTheRunsOfTheFirstAttemptsWithTheGateErrors(t *testing.T) {
	p := newPipeline(1, map[string]string{"model.a": "still broken"}, fixedClock{now: startTime})
	require.NoError(t, p.Submit(ctx, ports.VerificationRequest{RunID: "run-1", Attempt: 1}))
	require.NoError(t, p.Submit(ctx, ports.VerificationRequest{RunID: "run-2", Attempt: 2}))

	first, err := p.Status(ctx, "run-1")
	require.NoError(t, err)
	assert.Equal(t, ports.VerificationStatus{Phase: proposal.PhaseFailed, ActivatedAt: startTime, NodeErrors: map[string]string{"model.a": "still broken"}}, first)
	second, err := p.Status(ctx, "run-2")
	require.NoError(t, err)
	assert.Equal(t, ports.VerificationStatus{Phase: proposal.PhasePassed, ActivatedAt: startTime}, second)
	_, err = p.Status(ctx, "run-3")
	require.Error(t, err)
	assert.Len(t, p.runs(), 2)
}

func TestPackager_PackagesAContractDirectoryAndRefusesAsTheFixtureSays(t *testing.T) {
	root := t.TempDir()
	contracts := filepath.Join(root, "contracts")
	require.NoError(t, os.MkdirAll(contracts, 0o750))

	_, err := packager{p: conformance.Packager{Merged: "merged"}}.Merge(ctx, contracts, root, "svc", "")
	require.Error(t, err, "a directory with no contract yaml")

	require.NoError(t, os.WriteFile(filepath.Join(contracts, "n.yml"), []byte("nodes: []\n"), 0o600))
	merged, err := packager{p: conformance.Packager{Merged: "merged"}}.Merge(ctx, contracts, root, "svc", "")
	require.NoError(t, err)
	assert.Equal(t, []byte("merged"), merged)

	_, err = packager{p: conformance.Packager{Refuse: "bad read"}}.Merge(ctx, contracts, root, "svc", "")
	assert.ErrorIs(t, err, ports.ErrContractRejected)
	assert.Contains(t, err.Error(), "bad read")

	_, err = packager{p: conformance.Packager{Merged: "merged"}}.Merge(ctx, filepath.Join(root, "missing"), root, "svc", "")
	assert.Error(t, err)
}

func TestCatalog_AnswersFromTheFixture(t *testing.T) {
	c := catalog{c: conformance.Catalog{
		Locations:       map[string]conformance.Location{"model.a": {FilePath: "models/a.sql", Service: "svc"}},
		CurrentVersions: map[string]ports.CurrentVersion{"model.a": {RawCode: "select 1"}},
	}}

	file, service, err := c.Locate(ctx, "model.a")
	require.NoError(t, err)
	assert.Equal(t, "models/a.sql", file)
	assert.Equal(t, "svc", service)
	_, _, err = c.Locate(ctx, "model.b")
	assert.True(t, errors.Is(err, errNotInGraph))
	v, ok, err := c.CurrentVersion(ctx, "model.a")
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, "select 1", v.RawCode)
	_, ok, err = c.CurrentVersion(ctx, "model.b")
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestWorld_ReadsBackWhatTheStoreHolds(t *testing.T) {
	store := memory.NewStore()
	w, err := NewWorld(store, conformance.Fixture{}, conformance.Script{})
	require.NoError(t, err)
	u := w.NewUoW()
	require.NoError(t, u.Begin(ctx))
	require.NoError(t, u.ProposalRepo().Upsert(ctx, proposal.Proposal{ReleaseID: "rel", Attempt: 2, Status: proposal.StatusFailed}))
	require.NoError(t, u.ProposalRepo().Upsert(ctx, proposal.Proposal{ReleaseID: "rel", Attempt: 1, Status: proposal.StatusSkipped}))
	require.NoError(t, u.Commit())

	attempts := w.Attempts()
	require.Len(t, attempts, 2)
	assert.Equal(t, 1, attempts[0].Proposal.Attempt)
	assert.Equal(t, 2, attempts[1].Proposal.Attempt)
	assert.Empty(t, w.Remediations())
	assert.Empty(t, w.Tries(uuid.Nil))
}

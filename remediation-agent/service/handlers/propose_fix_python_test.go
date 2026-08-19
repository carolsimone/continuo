package handlers

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/remediation-agent/adapters/repofs"
	"github.com/carolsimone/continuo/remediation-agent/domain/proposal"
	"github.com/carolsimone/continuo/remediation-agent/service/ports"
	"github.com/carolsimone/continuo/remediation-agent/service/uow"
)

// handlerArchive serves a prepared checkout to the python lane.
type handlerArchive struct{ root string }

func (a handlerArchive) Fetch(context.Context, string, string) (string, func(), error) {
	return a.root, func() {}, nil
}

// handlerPackager returns a fixed merged contract.
type handlerPackager struct{}

func (handlerPackager) Merge(context.Context, string, string, string, string) ([]byte, error) {
	return []byte("merged: contract\n"), nil
}

// handlerReleases accepts any shadow submission and records it.
type handlerReleases struct{ submissions []ports.ShadowSubmission }

func (r *handlerReleases) Submit(_ context.Context, s ports.ShadowSubmission) error {
	r.submissions = append(r.submissions, s)
	return nil
}

func (r *handlerReleases) Verdict(context.Context, string) (ports.ShadowVerdict, error) {
	return ports.ShadowVerdict{}, nil
}

func (r *handlerReleases) ImageTag(context.Context, string, string) (string, error) {
	return "ghcr.io/o/service-py:v9", nil
}

// pythonCheckout writes a repository checkout declaring the failing node.
func pythonCheckout(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	dir := filepath.Join(root, "services", "service-py", "contracts")
	require.NoError(t, os.MkdirAll(dir, 0o750))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "kpis.yml"),
		[]byte("nodes:\n  - schema: analytics\n    table: py_daily_kpis\n"), 0o600))
	return root
}

// TestProposeFix_PythonValidation_RecordsVerifying verifies what the driver has
// to do differently for a fix that cannot be judged synchronously: it persists
// the attempt as 'verifying' with the shadow release that will judge it and the
// raw trigger that produced it, and it emits nothing — the proposal is not a
// proposal yet, so announcing it would surface an unverified fix to operators.
func TestProposeFix_PythonValidation_RecordsVerifying(t *testing.T) {
	u := newFakeUoW()
	rel := &handlerReleases{}
	llm := newFakeLLM(ports.ProposeResult{
		Files: []ports.ProposedFile{{
			Path:    "services/service-py/contracts/kpis.yml",
			Content: "nodes:\n  - schema: analytics\n    table: py_daily_kpis\n    owner: data\n",
		}},
		Rationale:  "declared the missing column",
		Confidence: "high",
		Model:      "m",
	}, nil)
	art := &fakeArtifacts{}

	d := deps(u, fakeEvidence{}, &llm, art)
	d.RepoArchive = handlerArchive{root: pythonCheckout(t)}
	d.ContractLocator = repofs.NewLocator(slog.Default())
	d.Packager = handlerPackager{}
	d.Releases = rel
	d.PriorAttempts = u.pr
	d.SQLDialect = "postgres"
	d.CandidateSource = fakeCandidateSource{err: ports.ErrNotFound}
	d.Logger = slog.Default()
	d.NewUoW = func() uow.UnitOfWork { return u }

	tr := baseTrigger()
	tr.NodeID = "analytics.py_daily_kpis"
	tr.NodeType = "python-model"
	tr.Service = "svc-py"
	tr.RawPayload = []byte(`{"node_id":"analytics.py_daily_kpis","node_type":"python-model"}`)

	require.NoError(t, ProposeFix(context.Background(), d, tr))

	require.Len(t, u.pr.inserted, 1)
	got := u.pr.inserted[0]
	require.Equal(t, proposal.StatusVerifying, got.Status)
	require.Equal(t, "shadow-r1-analytics.py_daily_kpis-a1", got.ShadowReleaseID)
	require.Equal(t, tr.RawPayload, got.TriggerPayload,
		"a verifying row must carry the trigger that produced it, so the attempt can be rebuilt once the shadow release answers")
	require.Equal(t, "services/service-py/contracts/kpis.yml", got.FilePath)
	require.Len(t, got.Edits, 1)

	require.Empty(t, u.ob.entries, "an unverified fix must not be announced")
	require.Len(t, rel.submissions, 1)
}

// TestProposeFix_DbtValidation_StoresNoTriggerPayload verifies that a row that
// never enters verification carries no trigger payload: only the reconciler's
// rows need one, and storing every trigger would put the full inbound payload
// on every proposal row for nothing.
func TestProposeFix_DbtValidation_StoresNoTriggerPayload(t *testing.T) {
	u := newFakeUoW()
	llm := newFakeLLM(ports.ProposeResult{ProposedSQL: "SELECT 1", Rationale: "r", Confidence: "high", Model: "m"}, nil)
	art := &fakeArtifacts{}
	d := deps(u, fakeEvidence{vals: map[string]string{"s3://b/sql": "SELECT 0"}}, &llm, art)

	tr := baseTrigger()
	tr.RawPayload = []byte(`{"node_id":"s.n"}`)

	require.NoError(t, ProposeFix(context.Background(), d, tr))

	require.Len(t, u.pr.inserted, 1)
	require.Empty(t, u.pr.inserted[0].TriggerPayload)
	require.Empty(t, u.pr.inserted[0].ShadowReleaseID)
}

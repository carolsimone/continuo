package handlers

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/adapters/repofs"
	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/uow"
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

// pythonCheckout writes a repository checkout declaring the failing node, under
// the project root the "svc" service is configured with.
func pythonCheckout(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	dir := filepath.Join(root, "services", "svc", "contracts")
	require.NoError(t, os.MkdirAll(dir, 0o750))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "kpis.yml"),
		[]byte("nodes:\n  - schema: analytics\n    table: py_daily_kpis\n"), 0o600))
	return root
}

// pythonDeps wires the driver for a python contract fix over checkout root.
func pythonDeps(t *testing.T, u *fakeUoW, llm *fakeLLM, art *fakeArtifacts, gw *fakeGateway, root string) Deps {
	t.Helper()
	d := deps(u, fakeEvidence{}, llm, art)
	d.RepoArchive = handlerArchive{root: root}
	contracts := repofs.NewLocator(slog.Default())
	d.ContractLocator = contracts
	d.ContractInspector = contracts
	d.Packager = handlerPackager{}
	d.Releases = gw
	d.PriorAttempts = u.pr
	d.SQLDialect = "postgres"
	d.CandidateSource = fakeCandidateSource{err: ports.ErrNotFound}
	d.NewUoW = func() uow.UnitOfWork { return u }
	return d
}

// pythonTrigger is a validation rejection of one python node in service "svc".
func pythonTrigger() Trigger {
	tr := baseTrigger()
	tr.RawPayload = []byte(`{"release_id":"r1","nodes":[{"node_id":"analytics.py_daily_kpis","node_type":"python-model"}]}`)
	tr.Nodes = []TriggerNode{{
		NodeID:         "analytics.py_daily_kpis",
		ErrorSignature: "sig",
		Category:       "logic",
		NodeType:       "python-model",
		Service:        "svc",
		DBTLogURI:      "s3://b/log",
	}}
	return tr
}

// pythonFixResult is a repair that declares what the node produces while
// leaving the fields identifying the node exactly as the repository declared
// them — an answer that changes one is refused before packaging.
func pythonFixResult() ports.ProposeResult {
	return ports.ProposeResult{
		Files: []ports.ProposedFile{{
			Path:    "services/svc/contracts/kpis.yml",
			Content: "nodes:\n  - schema: analytics\n    table: py_daily_kpis\n    output_columns:\n      - name: revenue\n",
		}},
		Rationale:  "declared the missing column",
		Confidence: "high",
		Model:      "m",
	}
}

// TestProposeFix_PythonValidation_RecordsVerifying verifies what the driver has
// to do differently for a fix that cannot be judged synchronously: it persists
// the attempt as 'verifying' with the shadow release that will judge it and the
// raw trigger that produced it, uploads the packaged contract the shadow runs
// under that release's own id, and emits nothing — the proposal is not a
// proposal yet, so announcing it would surface an unverified fix to operators.
func TestProposeFix_PythonValidation_RecordsVerifying(t *testing.T) {
	u := newFakeUoW()
	llm := newFakeLLM(pythonFixResult(), nil)
	art := &fakeArtifacts{}
	gw := &fakeGateway{imageTag: "ghcr.io/o/svc:v9"}
	d := pythonDeps(t, u, &llm, art, gw, pythonCheckout(t))
	tr := pythonTrigger()

	require.NoError(t, ProposeFix(context.Background(), d, tr))

	require.Len(t, u.pr.inserted, 1)
	got := u.pr.inserted[0]
	require.Equal(t, proposal.StatusVerifying, got.Status)
	require.Equal(t, "shadow-r1-svc-a1", got.ShadowReleaseID)
	require.Equal(t, tr.RawPayload, got.TriggerPayload,
		"a verifying row must carry the trigger that produced it, so the attempt can be rebuilt once the shadow release answers")
	require.Equal(t, "services/svc/contracts/kpis.yml", got.FilePath)
	require.Len(t, got.Edits, 1)
	require.Equal(t, "analytics.py_daily_kpis", got.Edits[0].TargetNodeID)
	require.Equal(t, []proposal.Verification{{
		Service: "svc", Kind: ports.ShadowKindPython, ShadowReleaseID: "shadow-r1-svc-a1",
	}}, got.Verifications)
	require.Equal(t, proposal.StatusVerifying, got.NodeOutcomes["analytics.py_daily_kpis"].Status)

	// A python shadow runs the packaged contract written under its own id;
	// there is no project to lay a source overlay over.
	require.Equal(t, "merged: contract\n", art.written["svc/shadow-r1-svc-a1/contract.yaml"])
	require.NotContains(t, art.written, "svc/shadow-r1-svc-a1/source-overlay.tar.gz")

	require.Len(t, gw.submitted, 1)
	require.Equal(t, ports.ShadowSubmission{
		ReleaseID: "shadow-r1-svc-a1", Service: "svc", ImageTag: "ghcr.io/o/svc:v9",
		Repo: "o/r", CommitSHA: "abc", Kind: ports.ShadowKindPython,
	}, gw.submitted[0])
	require.Empty(t, u.ob.entries, "an unverified fix must not be announced")
}

// TestProposeFix_PythonValidation_EditOutsideEveryConfiguredService covers the
// install whose python service has no repository-path mapping: the fix names a
// real file, but no shadow release could ever run it, and a redelivery would map
// it no better. The attempt ends failed with the reason recorded rather than
// being retried forever.
func TestProposeFix_PythonValidation_EditOutsideEveryConfiguredService(t *testing.T) {
	u := newFakeUoW()
	llm := newFakeLLM(pythonFixResult(), nil)
	art := &fakeArtifacts{}
	gw := &fakeGateway{imageTag: "ghcr.io/o/svc:v9"}
	d := pythonDeps(t, u, &llm, art, gw, pythonCheckout(t))
	// The fix's file lives under services/svc, which this install does not map.
	d.ServiceRepoPaths = map[string]string{"other": "services/other"}

	require.NoError(t, ProposeFix(context.Background(), d, pythonTrigger()))

	require.Len(t, u.pr.inserted, 1)
	got := u.pr.inserted[0]
	require.Equal(t, proposal.StatusFailed, got.Status)
	require.Contains(t, got.NodeOutcomes["analytics.py_daily_kpis"].Reason, "services/svc/contracts/kpis.yml")
	require.Empty(t, gw.submitted, "no release slot is spent on edits nothing can run")
	require.Empty(t, u.ob.entries)
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
	tr.RawPayload = []byte(`{"release_id":"r1","nodes":[{"node_id":"s.n"}]}`)

	require.NoError(t, ProposeFix(context.Background(), d, tr))

	require.Len(t, u.pr.inserted, 1)
	require.Empty(t, u.pr.inserted[0].TriggerPayload)
	require.Empty(t, u.pr.inserted[0].ShadowReleaseID)
}

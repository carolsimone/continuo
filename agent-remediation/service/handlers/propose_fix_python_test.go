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

// TestProposeFix_PythonValidation_ServiceDerivedFromTheEditPath pins that the
// contract and the edits it packages always land on the same shadow release.
// The trigger's own service field names something this install has no repository
// path for; the contract is still keyed by the service the fix's file belongs to,
// so the release is submitted as python. Keyed by the trigger's field instead,
// the lookup would miss and these very edits would be verified as a dbt project
// — silently, since a dbt overlay of a contract yaml compiles to nothing.
func TestProposeFix_PythonValidation_ServiceDerivedFromTheEditPath(t *testing.T) {
	u := newFakeUoW()
	llm := newFakeLLM(pythonFixResult(), nil)
	art := &fakeArtifacts{}
	gw := &fakeGateway{imageTag: "ghcr.io/o/svc:v9"}
	d := pythonDeps(t, u, &llm, art, gw, pythonCheckout(t))
	tr := pythonTrigger()
	// The node's declared service is not a key of ServiceRepoPaths; its contract
	// file lives under the "svc" service's project root.
	tr.Nodes[0].Service = "svc-py"

	require.NoError(t, ProposeFix(context.Background(), d, tr))

	require.Len(t, u.pr.inserted, 1)
	got := u.pr.inserted[0]
	require.Equal(t, proposal.StatusVerifying, got.Status)
	require.Equal(t, []proposal.Verification{{
		Service: "svc", Kind: ports.ShadowKindPython, ShadowReleaseID: "shadow-r1-svc-a1",
	}}, got.Verifications)
	require.Equal(t, "merged: contract\n", art.written["svc/shadow-r1-svc-a1/contract.yaml"])
	require.NotContains(t, art.written, "svc/shadow-r1-svc-a1/source-overlay.tar.gz")
	require.Len(t, gw.submitted, 1)
	require.Equal(t, "svc", gw.submitted[0].Service)
	require.Equal(t, ports.ShadowKindPython, gw.submitted[0].Kind)
}

// TestProposeFix_PythonContractWithDbtEditInOneService covers the attempt that
// would have to be two releases at once: a python node's contract fix and a dbt
// model's source fix land in the same service. A release parses one manifest
// kind, so whichever lane were chosen would verify half the change and silently
// ignore the rest. The attempt is refused whole instead.
func TestProposeFix_PythonContractWithDbtEditInOneService(t *testing.T) {
	u := newFakeUoW()
	// Cluster order is by node id: the python node first, then the dbt node's
	// two-step source fix.
	llm := fakeLLM{queue: []ports.ProposeResult{
		pythonFixResult(),
		{ProposedSQL: "select customer_id from t", Rationale: "typo", Confidence: "high", Model: "m"},
		{ProposedSQL: "select customer_id from t", Rationale: "typo", Confidence: "high", Model: "m"},
	}}
	art := &fakeArtifacts{}
	gw := &fakeGateway{imageTag: "ghcr.io/o/svc:v9"}
	d := pythonDeps(t, u, &llm, art, gw, pythonCheckout(t))
	d.Evidence = fakeEvidence{vals: map[string]string{
		"s3://b/sql": "select custmer_id from t",
		"s3://b/log": "column does not exist",
	}}
	d.CandidateSource = fakeCandidateSource{src: ports.CandidateSource{RawCode: "select custmer_id from t", Runtime: ports.RuntimeDbt}}

	tr := pythonTrigger()
	// The dbt node from baseTrigger, in the same service as the python node's
	// contract file.
	tr.Nodes = append(tr.Nodes, baseTrigger().Nodes...)

	require.NoError(t, ProposeFix(context.Background(), d, tr))

	require.Len(t, u.pr.inserted, 1)
	got := u.pr.inserted[0]
	require.Equal(t, proposal.StatusFailed, got.Status)
	const reason = "service svc mixes a python contract with dbt edits in one attempt; a release verifies one manifest kind"
	for _, id := range []string{"analytics.py_daily_kpis", "s.n"} {
		require.Equal(t, proposal.StatusFailed, got.NodeOutcomes[id].Status)
		require.Equal(t, reason, got.NodeOutcomes[id].Reason)
	}
	require.Empty(t, got.Verifications)
	require.Empty(t, gw.submitted, "no release slot is spent on a change one release cannot verify")
	require.NotContains(t, art.written, "svc/shadow-r1-svc-a1/contract.yaml",
		"every service is checked before any is submitted, so nothing is uploaded either")
	require.Empty(t, u.ob.entries)
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

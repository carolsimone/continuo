package handlers

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
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

// packedBundle is the odcs-bundle.json handlerPackager packs.
const packedBundle = `{"bundle_version": 1, "service": "svc"}`

// handlerPackager returns packedBundle and records the package lists it was
// given.
type handlerPackager struct{ packages *[]map[string]string }

func (p handlerPackager) Pack(_ context.Context, _, _, _, _ string, imagePackages map[string]string) ([]byte, error) {
	if p.packages != nil {
		*p.packages = append(*p.packages, imagePackages)
	}
	return []byte(packedBundle), nil
}

// noContracts is a catalog in which no node has a contract.
type noContracts struct{}

func (noContracts) CurrentContract(context.Context, string) (string, error) { return "", nil }

// handlerImages answers the image package list of every release and service,
// or err.
type handlerImages struct {
	packages map[string]string
	err      error
}

func (i handlerImages) ImagePackages(context.Context, string, string) (map[string]string, error) {
	return i.packages, i.err
}

// pythonCheckout writes a repository checkout declaring the failing node, under
// the project root the "svc" service is configured with.
func pythonCheckout(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	dir := filepath.Join(root, "services", "svc", "contracts")
	require.NoError(t, os.MkdirAll(dir, 0o750))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "kpis.odcs.yaml"), []byte(kpisContract("")), 0o600))
	return root
}

// kpisContract is the failing node's ODCS contract, with columns as the
// schema object's properties.
func kpisContract(columns string) string {
	return "apiVersion: v3.2.0\nkind: DataContract\nid: analytics.py_daily_kpis\n" +
		"schema:\n  - name: py_daily_kpis\n    physicalType: table\n    properties:\n" + columns
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
	d.ImagePackages = handlerImages{packages: map[string]string{"pyarrow": "26.0.0"}}
	d.CurrentContracts = noContracts{}
	d.Pipeline = gw
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
	tr.RawPayload = []byte(`{"release_id":"r1","nodes":[{"node_id":"analytics.py_daily_kpis","node_type":"python-node"}]}`)
	tr.Nodes = []TriggerNode{{
		NodeID:         "analytics.py_daily_kpis",
		ErrorSignature: "sig",
		Category:       "logic",
		NodeType:       "python-node",
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
			Path:    "services/svc/contracts/kpis.odcs.yaml",
			Content: kpisContract("      - {name: revenue, physicalType: INTEGER}\n"),
		}},
		Rationale:  "declared the missing column",
		Confidence: "high",
		Model:      "m",
	}
}

// TestProposeFix_PythonValidation_RecordsVerifying verifies what the driver has
// to do differently for a fix that cannot be judged synchronously: it persists
// the attempt as 'verifying' with the verification run that will judge it and
// the raw trigger that produced it, uploads the packed odcs-bundle.json the
// run reads under that run's own id, and emits nothing — the proposal is not a
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
	require.Equal(t, "verify-r1-svc-a1", got.VerificationRunID)
	require.Equal(t, tr.RawPayload, got.TriggerPayload,
		"a verifying row must carry the trigger that produced it, so the attempt can be rebuilt once the verification run answers")
	require.Equal(t, "services/svc/contracts/kpis.odcs.yaml", got.FilePath)
	require.Len(t, got.Edits, 1)
	require.Equal(t, "analytics.py_daily_kpis", got.Edits[0].TargetNodeID)
	require.Equal(t, []proposal.Verification{{
		Service: "svc", Kind: ports.VerificationKindPythonODCS, RunID: "verify-r1-svc-a1", Phase: proposal.PhaseQueued,
	}}, got.Verifications)
	require.Equal(t, proposal.StatusVerifying, got.NodeOutcomes["analytics.py_daily_kpis"].Status)

	// A python verification run reads the packed bundle written under its own
	// id; there is no project to lay a source overlay over.
	require.Equal(t, packedBundle, art.written["svc/verify-r1-svc-a1/odcs-bundle.json"])
	require.NotContains(t, art.written, "svc/verify-r1-svc-a1/source-overlay.tar.gz")

	require.Len(t, gw.submitted, 1)
	require.Equal(t, ports.VerificationRequest{
		RunID: "verify-r1-svc-a1", Service: "svc", ImageTag: "ghcr.io/o/svc:v9",
		Kind: ports.VerificationKindPythonODCS,
		// The run names the rejected release it judges, so release-controller
		// assembles that release's candidate for the service it changed.
		VerifiesReleaseID: "r1", Attempt: 1,
	}, gw.submitted[0])
	require.Empty(t, u.ob.entries, "an unverified fix must not be announced")
}

// TestProposeFix_PythonValidation_ServiceDerivedFromTheEditPath pins that the
// contract and the edits it packages always land on the same verification run.
// The trigger's own service field names something this install has no repository
// path for; the contract is still keyed by the service the fix's file belongs to,
// so the run is submitted as python. Keyed by the trigger's field instead,
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
		Service: "svc", Kind: ports.VerificationKindPythonODCS, RunID: "verify-r1-svc-a1", Phase: proposal.PhaseQueued,
	}}, got.Verifications)
	require.Equal(t, packedBundle, art.written["svc/verify-r1-svc-a1/odcs-bundle.json"])
	require.NotContains(t, art.written, "svc/verify-r1-svc-a1/source-overlay.tar.gz")
	require.Len(t, gw.submitted, 1)
	require.Equal(t, "svc", gw.submitted[0].Service)
	require.Equal(t, ports.VerificationKindPythonODCS, gw.submitted[0].Kind)
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
	require.Empty(t, gw.submitted, "no release-queue slot is spent on a change one release cannot verify")
	require.NotContains(t, art.written, "svc/verify-r1-svc-a1/odcs-bundle.json",
		"every service is checked before any is submitted, so nothing is uploaded either")
	require.Empty(t, u.ob.entries)
}

// TestProposeFix_PythonValidation_EditOutsideEveryConfiguredService covers the
// install whose python service has no repository-path mapping: the fix names a
// real file, but no verification run could ever run it, and a redelivery would
// map it no better. The attempt ends failed with the reason recorded rather
// than being retried forever.
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
	require.Contains(t, got.NodeOutcomes["analytics.py_daily_kpis"].Reason, "services/svc/contracts/kpis.odcs.yaml")
	require.Empty(t, gw.submitted, "no release-queue slot is spent on edits nothing can run")
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
	require.Empty(t, u.pr.inserted[0].VerificationRunID)
}

// TestVerify_UploadsAnODCSBundleAsPythonODCS pins what a python fix is
// verified with: the bundle packed with the package list of the rejected
// release's image, uploaded as <service>/<run id>/odcs-bundle.json and
// submitted as kind python-odcs.
func TestVerify_UploadsAnODCSBundleAsPythonODCS(t *testing.T) {
	u := newFakeUoW()
	llm := newFakeLLM(pythonFixResult(), nil)
	art := &fakeArtifacts{}
	gw := &fakeGateway{imageTag: "ghcr.io/o/svc:v9"}
	d := pythonDeps(t, u, &llm, art, gw, pythonCheckout(t))
	var packed []map[string]string
	d.Packager = handlerPackager{packages: &packed}
	d.ImagePackages = handlerImages{packages: map[string]string{"pyarrow": "26.0.0", "duckdb": "1.4.0"}}

	require.NoError(t, ProposeFix(context.Background(), d, pythonTrigger()))

	require.Equal(t, []map[string]string{{"pyarrow": "26.0.0", "duckdb": "1.4.0"}}, packed)
	require.Equal(t, packedBundle, art.written["svc/verify-r1-svc-a1/odcs-bundle.json"])
	require.Equal(t, "application/json", art.contentTypes["svc/verify-r1-svc-a1/odcs-bundle.json"])
	require.NotContains(t, art.written, "svc/verify-r1-svc-a1/contract.yaml")
	require.Len(t, gw.submitted, 1)
	require.Equal(t, ports.VerificationKindPythonODCS, gw.submitted[0].Kind)
	require.Equal(t, "python-odcs", gw.submitted[0].Kind)
}

// TestProposeFix_NoImagePackagesRecordsASkippedRowAndLogsWhy covers a release
// none of whose bundles records the service's image packages: no fix can be
// packed, so the model is not called and nothing is uploaded or submitted. The
// attempt's row is skipped, with the node skipped for the reason, and the
// reason is logged.
func TestProposeFix_NoImagePackagesRecordsASkippedRowAndLogsWhy(t *testing.T) {
	u := newFakeUoW()
	llm := newFakeLLM(pythonFixResult(), nil)
	art := &fakeArtifacts{}
	gw := &fakeGateway{imageTag: "ghcr.io/o/svc:v9"}
	d := pythonDeps(t, u, &llm, art, gw, pythonCheckout(t))
	var packed []map[string]string
	d.Packager = handlerPackager{packages: &packed}
	noPackages := fmt.Errorf("release r1, service svc: %w", ports.ErrNoImagePackages)
	d.ImagePackages = handlerImages{err: noPackages}
	var logs strings.Builder
	d.Logger = slog.New(slog.NewJSONHandler(&logs, nil))

	require.NoError(t, ProposeFix(context.Background(), d, pythonTrigger()))

	require.Zero(t, llm.calls, "no model call is spent on a fix that cannot be packed")
	require.Empty(t, packed)
	require.Empty(t, gw.submitted)
	require.Empty(t, art.written)
	require.Len(t, u.pr.inserted, 1)
	got := u.pr.inserted[0]
	require.Equal(t, proposal.StatusSkipped, got.Status)
	require.Empty(t, got.Verifications)
	require.Empty(t, got.Edits)
	outcome := got.NodeOutcomes["analytics.py_daily_kpis"]
	require.Equal(t, proposal.StatusSkipped, outcome.Status)
	require.Equal(t, noPackages.Error(), outcome.Reason)
	require.Contains(t, logs.String(), `"reason":"`+noPackages.Error()+`"`)
	require.Empty(t, u.ob.entries)
}

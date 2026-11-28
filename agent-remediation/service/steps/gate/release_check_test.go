package gate_test

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"log/slog"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/domain/fix"
	"github.com/carolsimone/continuo/agent-remediation/domain/flow"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/service/overlay"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
	"github.com/carolsimone/continuo/agent-remediation/service/steps/gate"
	"github.com/carolsimone/continuo/pkg/domain/model"
)

var errBoom = errors.New("boom")

var repoPaths = map[string]string{
	"sales":     "dbt/projects/sales",
	"ledger":    "python/ledger",
	"zz-ledger": "python/zz-ledger",
}

type write struct{ key, body, contentType string }

type fakeArtifacts struct {
	writes []write
	err    error
}

func (f *fakeArtifacts) Write(_ context.Context, key, body, contentType string) (string, error) {
	if f.err != nil {
		return "", f.err
	}
	f.writes = append(f.writes, write{key, body, contentType})
	return "s3://artifacts/" + key, nil
}

type fakeEvidence struct {
	bodies map[string]string
	err    error
}

func (f *fakeEvidence) Fetch(_ context.Context, uri string) (string, error) {
	if f.err != nil {
		return "", f.err
	}
	b, ok := f.bodies[uri]
	if !ok {
		return "", ports.ErrNotFound
	}
	return b, nil
}

type fakeReleases struct{ err error }

func (f fakeReleases) ImageTag(_ context.Context, releaseID, project string) (string, error) {
	if f.err != nil {
		return "", f.err
	}
	return "tag-" + releaseID + "-" + project, nil
}

func (fakeReleases) FailingNodes(context.Context, string) (map[string]string, error) {
	return nil, nil
}

func (fakeReleases) ContractBundle(context.Context, string) (string, string, error) {
	return "", "", errors.New("the gate never reads the release's contract bundle")
}

type fakeReleaseChecks struct {
	submitted   []ports.ReleaseCheckRequest
	submitErr   error
	statuses    map[string]ports.ReleaseCheckStatus
	statusErrs  map[string]error
	statusReads []string
}

func (f *fakeReleaseChecks) Submit(_ context.Context, r ports.ReleaseCheckRequest) error {
	if f.submitErr != nil {
		return f.submitErr
	}
	f.submitted = append(f.submitted, r)
	return nil
}

func (f *fakeReleaseChecks) Status(_ context.Context, checkID string) (ports.ReleaseCheckStatus, error) {
	f.statusReads = append(f.statusReads, checkID)
	if err := f.statusErrs[checkID]; err != nil {
		return ports.ReleaseCheckStatus{}, err
	}
	return f.statuses[checkID], nil
}

type fixedClock struct{ now time.Time }

func (c fixedClock) Now() time.Time { return c.now }

var (
	t0      = time.Date(2026, 11, 13, 12, 0, 0, 0, time.UTC)
	timeout = 30 * time.Minute
)

type rig struct {
	step      *gate.ReleaseCheck
	artifacts *fakeArtifacts
	checks    *fakeReleaseChecks
	evidence  *fakeEvidence
	clock     *fixedClock
	releases  fakeReleases
}

func newRig() *rig {
	r := &rig{
		artifacts: &fakeArtifacts{},
		checks:    &fakeReleaseChecks{statuses: map[string]ports.ReleaseCheckStatus{}, statusErrs: map[string]error{}},
		evidence: &fakeEvidence{bodies: map[string]string{
			"s3://c/orders":  "select 1 as orders",
			"s3://c/margins": "select 2 as margins",
		}},
		clock: &fixedClock{now: t0.Add(time.Minute)},
	}
	r.build()
	return r
}

func (r *rig) build() {
	r.step = gate.NewReleaseCheck(r.evidence, r.artifacts, r.releases, r.checks, repoPaths, r.clock, timeout, nil)
}

func triggerOf() remediation.Trigger {
	return remediation.Trigger{
		ReleaseID: "rel-1",
		Round:     1,
		Nodes: []remediation.TriggerNode{
			{NodeID: "model.sales.orders", NodeType: "dbt-model", Project: "sales"},
			{NodeID: "model.sales.margins", NodeType: "dbt-model", Project: "sales"},
			{NodeID: "python.ledger.postings", NodeType: "python-node", Project: "ledger"},
		},
	}
}

func dbtCluster() step.ClusterDraft {
	return step.ClusterDraft{
		Cluster: remediation.Cluster{Cluster: 1, Members: []string{"model.sales.orders", "model.sales.margins"}},
		Draft: remediation.Draft{Edits: []remediation.Edit{
			{Path: "dbt/projects/sales/models/orders.sql", ContentURI: "s3://c/orders", TargetNodeID: "model.sales.orders"},
			{Path: "dbt/projects/sales/models/marts/margins.sql", ContentURI: "s3://c/margins", TargetNodeID: "model.sales.margins"},
		}},
		Evidence: remediation.NewEvidence(),
	}
}

const pythonContract = `{"bundle_version": 1, "project": "ledger", "image_packages": {"pyarrow": "26.0.0"}, "nodes": []}`

func pythonCluster() step.ClusterDraft {
	return step.ClusterDraft{
		Cluster: remediation.Cluster{Cluster: 2, Members: []string{"python.ledger.postings"}},
		Draft: remediation.Draft{Edits: []remediation.Edit{
			{Path: "python/ledger/contracts/postings.yaml", Content: "ignored: inline content", ContentURI: "s3://c/unused", TargetNodeID: "python.ledger.postings"},
		}},
		Evidence: remediation.NewEvidence().With(remediation.KeyPackagedContract, remediation.Bytes(pythonContract)),
	}
}

func gateInput(drafts ...step.ClusterDraft) step.GateInput {
	return step.GateInput{
		Try:         remediation.Try{Try: 2, CreatedAt: t0},
		Trigger:     triggerOf(),
		Drafts:      drafts,
		SubmittedAt: t0,
	}
}

func untar(t *testing.T, gz string) map[string]string {
	t.Helper()
	zr, err := gzip.NewReader(bytes.NewReader([]byte(gz)))
	require.NoError(t, err)
	tr := tar.NewReader(zr)
	out := map[string]string{}
	for {
		h, err := tr.Next()
		if errors.Is(err, io.EOF) {
			return out
		}
		require.NoError(t, err)
		b, err := io.ReadAll(tr)
		require.NoError(t, err)
		out[h.Name] = string(b)
	}
}

func TestMeta(t *testing.T) {
	m := newRig().step.Meta()
	assert.Equal(t, flow.StepName("release_check"), m.Name)
	assert.Equal(t, step.KindCheck, m.Kind)
	assert.Equal(t, []flow.Stage{flow.StageGate}, m.Stages)
	assert.True(t, m.MayStandAsGateRun)
	assert.Empty(t, m.Needs)
}

func TestSubmit_DbtProjectSubmitsAnOverlayOfTheEditsContent(t *testing.T) {
	r := newRig()
	res, err := r.step.Submit(context.Background(), gateInput(dbtCluster()))
	require.NoError(t, err)

	assert.Equal(t, remediation.CheckResult{Check: "release_check", Waiting: true, CheckIDs: []string{"verify-rel-1-sales-r1-t2"}}, res.Result)
	assert.Equal(t, []fix.ReleaseCheck{
		{Project: "sales", Kind: "dbt", CheckID: "verify-rel-1-sales-r1-t2", Phase: fix.PhaseQueued},
	}, res.ReleaseChecks, "the runner records the checks with the fix as submitReleaseChecks returned them")
	require.Len(t, r.checks.submitted, 1)
	assert.Equal(t, ports.ReleaseCheckRequest{
		CheckID:           "verify-rel-1-sales-r1-t2",
		Project:           "sales",
		ImageTag:          "tag-rel-1-sales",
		Kind:              "dbt",
		VerifiesReleaseID: "rel-1",
		Round:             1,
		Try:               2,
		SourceOverlayURI:  "s3://artifacts/sales/verify-rel-1-sales-r1-t2/source-overlay.tar.gz",
	}, r.checks.submitted[0])

	require.Len(t, r.artifacts.writes, 1)
	w := r.artifacts.writes[0]
	assert.Equal(t, "sales/verify-rel-1-sales-r1-t2/source-overlay.tar.gz", w.key)
	assert.Equal(t, "application/gzip", w.contentType)
	// The overlay holds each edit's content as the artifact stored it, at its
	// path within the project's directory.
	assert.Equal(t, map[string]string{
		"models/orders.sql":        "select 1 as orders",
		"models/marts/margins.sql": "select 2 as margins",
	}, untar(t, w.body))
	want, err := overlay.Build([]overlay.File{
		{Path: "models/marts/margins.sql", Content: []byte("select 2 as margins")},
		{Path: "models/orders.sql", Content: []byte("select 1 as orders")},
	}, nil)
	require.NoError(t, err)
	assert.Equal(t, string(want), w.body)
}

// TestReleaseCheck_AMovedFileIsListedForDeletion pins that a renamed file is
// absent from the check's project as it is from the pull request's tree: the
// overlay writes the new path and lists the old one, both relative to the
// project's directory.
func TestReleaseCheck_AMovedFileIsListedForDeletion(t *testing.T) {
	r := newRig()
	c := dbtCluster()
	c.Draft.Edits = []remediation.Edit{{
		Path:         "dbt/projects/sales/models/orders_v2.sql",
		RenamedFrom:  "dbt/projects/sales/models/orders.sql",
		ContentURI:   "s3://c/orders",
		TargetNodeID: "model.sales.orders",
	}}
	_, err := r.step.Submit(context.Background(), gateInput(c))
	require.NoError(t, err)

	require.Len(t, r.artifacts.writes, 1)
	assert.Equal(t, map[string]string{
		"models/orders_v2.sql": "select 1 as orders",
		overlay.DeleteListName: "models/orders.sql\n",
	}, untar(t, r.artifacts.writes[0].body))
}

func TestReleaseCheck_UploadsAnODCSBundle(t *testing.T) {
	r := newRig()
	res, err := r.step.Submit(context.Background(), gateInput(pythonCluster()))
	require.NoError(t, err)

	assert.Equal(t, []string{"verify-rel-1-ledger-r1-t2"}, res.Result.CheckIDs)
	assert.True(t, res.Result.Waiting)
	assert.Equal(t, []fix.ReleaseCheck{
		{Project: "ledger", Kind: "python", CheckID: "verify-rel-1-ledger-r1-t2", Phase: fix.PhaseQueued},
	}, res.ReleaseChecks)
	assert.Equal(t, []ports.ReleaseCheckRequest{{
		CheckID:           "verify-rel-1-ledger-r1-t2",
		Project:           "ledger",
		ImageTag:          "tag-rel-1-ledger",
		Kind:              "python",
		VerifiesReleaseID: "rel-1",
		Round:             1,
		Try:               2,
	}}, r.checks.submitted)
	assert.Equal(t, []write{{
		key: "ledger/verify-rel-1-ledger-r1-t2/odcs-bundle.json", body: pythonContract, contentType: "application/json",
	}}, r.artifacts.writes)
}

// notABundleCluster is a python cluster whose packaged contract is contract.
func notABundleCluster(contract string) step.ClusterDraft {
	c := pythonCluster()
	c.Evidence = remediation.NewEvidence().With(remediation.KeyPackagedContract, remediation.Bytes(contract))
	return c
}

// A packaged contract is verified only as an ODCS producer bundle: a JSON
// object with a bundle_version key, whatever its spacing or key order.
func TestReleaseCheck_VerifiesAnODCSBundleAsKindPython(t *testing.T) {
	for name, contract := range map[string]string{
		"an ODCS producer bundle":     pythonContract,
		"an ODCS bundle with spacing": "\n  {\"nodes\": [], \"bundle_version\": 1}\n",
	} {
		t.Run(name, func(t *testing.T) {
			r := newRig()
			res, err := r.step.Submit(context.Background(), gateInput(notABundleCluster(contract)))
			require.NoError(t, err)
			require.Len(t, res.ReleaseChecks, 1)
			assert.Equal(t, "python", res.ReleaseChecks[0].Kind)
			assert.Equal(t, []write{{
				key: "ledger/verify-rel-1-ledger-r1-t2/odcs-bundle.json", body: contract, contentType: "application/json",
			}}, r.artifacts.writes)
		})
	}
}

// zzLedgerCluster is a python cluster of the project zz-ledger, whose name
// sorts after the dbt project sales, packaging contract.
func zzLedgerCluster(contract string) step.ClusterDraft {
	return step.ClusterDraft{
		Cluster: remediation.Cluster{Cluster: 3, Members: []string{"python.zz-ledger.postings"}},
		Draft: remediation.Draft{Edits: []remediation.Edit{
			{Path: "python/zz-ledger/contracts/postings.yaml", ContentURI: "s3://c/unused", TargetNodeID: "python.zz-ledger.postings"},
		}},
		Evidence: remediation.NewEvidence().With(remediation.KeyPackagedContract, remediation.Bytes(contract)),
	}
}

// Any other packaged contract is nothing a release check can judge: the
// gate refuses the try with a finding before it writes or submits anything, so
// the try fails and the next one starts. The refusing project sorts after the
// dbt project, so a refusal made while the checks are written would already have
// written the dbt check's overlay.
func TestSubmit_RefusesAPackagedContractThatIsNotAnODCSBundle(t *testing.T) {
	const msg = "the packaged contract is not an ODCS producer bundle: project zz-ledger"
	for name, contract := range map[string]string{
		"a yaml document":                      "version: 1\nnodes:\n  - id: python.ledger.postings\n    script: scripts/postings.py\n",
		"a JSON object with no bundle_version": `{"version": 1, "nodes": []}`,
		"a JSON array":                         `[{"bundle_version": 1}]`,
		"a JSON null":                          `null`,
		"no bytes":                             "",
	} {
		t.Run(name, func(t *testing.T) {
			r := newRig()
			in := gateInput(dbtCluster(), zzLedgerCluster(contract))
			in.Trigger.Nodes = append(in.Trigger.Nodes,
				remediation.TriggerNode{NodeID: "python.zz-ledger.postings", NodeType: "python-node", Project: "zz-ledger"})

			v, err := r.step.Submit(context.Background(), in)
			require.NoError(t, err)

			assert.False(t, v.Result.Waiting)
			assert.False(t, v.Result.Passed())
			assert.Equal(t, "release_check", v.Result.Check)
			assert.Equal(t, []remediation.Finding{{Code: "packaged_contract_not_a_bundle", Message: msg}}, v.Result.Findings)
			assert.Equal(t, msg, v.VerifyError)
			assert.Empty(t, v.ReleaseChecks)
			assert.Empty(t, r.artifacts.writes, "a try one project cannot verify spends no check on the others")
			assert.Empty(t, r.checks.submitted, "no release check is submitted")

			_, err = r.step.Poll(context.Background(), in, remediation.CheckResult{Check: "release_check", Waiting: true, CheckIDs: idsOf(in)})
			assert.ErrorContains(t, err, msg, "a poll cannot rebuild the checks of a try Submit refused")
		})
	}
}

func TestPoll_ReportsTheKindTheRunWasSubmittedAs(t *testing.T) {
	r := newRig()
	in := gateInput(pythonCluster())
	submitted, err := r.step.Submit(context.Background(), in)
	require.NoError(t, err)

	res, err := r.step.Poll(context.Background(), in, submitted.Result)
	require.NoError(t, err)
	require.Len(t, res.ReleaseChecks, 1)
	assert.Equal(t, "python", res.ReleaseChecks[0].Kind, "Poll rebuilds the checks with the kind Submit gave them")
}

func TestSubmit_TwoProjectsSubmitOneRunEachInProjectOrder(t *testing.T) {
	r := newRig()
	res, err := r.step.Submit(context.Background(), gateInput(dbtCluster(), pythonCluster()))
	require.NoError(t, err)
	assert.Equal(t, []string{"verify-rel-1-ledger-r1-t2", "verify-rel-1-sales-r1-t2"}, res.Result.CheckIDs)
	assert.Equal(t, []fix.ReleaseCheck{
		{Project: "ledger", Kind: "python", CheckID: "verify-rel-1-ledger-r1-t2", Phase: fix.PhaseQueued},
		{Project: "sales", Kind: "dbt", CheckID: "verify-rel-1-sales-r1-t2", Phase: fix.PhaseQueued},
	}, res.ReleaseChecks)
	require.Len(t, r.checks.submitted, 2)
	assert.Equal(t, "python", r.checks.submitted[0].Kind)
	assert.Equal(t, "dbt", r.checks.submitted[1].Kind)
}

func TestSubmit_RefusesWhatNoRunCouldJudgeBeforeWritingAnything(t *testing.T) {
	r := newRig()
	mixed := pythonCluster()
	mixed.Draft.Edits = append(mixed.Draft.Edits, remediation.Edit{Path: "python/ledger/models/x.sql", TargetNodeID: "model.sales.orders"})
	twice := dbtCluster()
	twice.Draft.Edits = append(twice.Draft.Edits, twice.Draft.Edits[0])
	outside := step.ClusterDraft{Draft: remediation.Draft{Edits: []remediation.Edit{{Path: "docs/x.md"}}}, Evidence: remediation.NewEvidence()}
	cases := map[string]struct {
		in   step.GateInput
		code string
		msg  string
	}{
		"unmapped":  {gateInput(dbtCluster(), outside), "edit_outside_projects", "edit belongs to no configured project: docs/x.md"},
		"duplicate": {gateInput(twice), "path_edited_twice", "two edits change the same file: dbt/projects/sales/models/orders.sql"},
		"mixed": {gateInput(dbtCluster(), mixed), "mixed_manifest_kinds",
			"project ledger mixes a python contract with dbt edits in one try; a release verifies one manifest kind"},
	}
	for name, c := range cases {
		v, err := r.step.Submit(context.Background(), c.in)
		require.NoError(t, err, name)
		assert.False(t, v.Result.Waiting, name)
		assert.False(t, v.Result.Passed(), name)
		assert.Equal(t, "release_check", v.Result.Check, name)
		assert.Equal(t, []remediation.Finding{{Code: c.code, Message: c.msg}}, v.Result.Findings, name)
		assert.Equal(t, c.msg, v.VerifyError, name)
		assert.Empty(t, v.ReleaseChecks, name)
	}
	assert.Empty(t, r.artifacts.writes, "a try one project cannot verify spends no check on the others")
	assert.Empty(t, r.checks.submitted)

	_, err := r.step.Submit(context.Background(), gateInput())
	assert.Error(t, err, "a try with no edit at all is a defect of the caller, not a refusal")
}

// A refusal fails the try and the next try is shown why, so it is logged as a
// warning, not as an error.
func TestSubmit_LogsARefusalAsAWarning(t *testing.T) {
	var logs bytes.Buffer
	r := newRig()
	r.step = gate.NewReleaseCheck(r.evidence, r.artifacts, r.releases, r.checks, repoPaths, r.clock, timeout,
		slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	twice := dbtCluster()
	twice.Draft.Edits = append(twice.Draft.Edits, twice.Draft.Edits[0])

	v, err := r.step.Submit(context.Background(), gateInput(twice))

	require.NoError(t, err)
	require.NotEmpty(t, v.Result.Findings)
	assert.Contains(t, logs.String(), "level=WARN")
	assert.Contains(t, logs.String(), "the try fails and the next try is shown why")
	assert.NotContains(t, logs.String(), "level=ERROR")
}

func TestSubmit_ReturnsTheErrorOfEachCollaborator(t *testing.T) {
	cases := map[string]func(*rig){
		"evidence":  func(r *rig) { r.evidence.err = errBoom },
		"artifacts": func(r *rig) { r.artifacts.err = errBoom },
		"releases":  func(r *rig) { r.releases = fakeReleases{err: errBoom} },
		"checks":    func(r *rig) { r.checks.submitErr = errBoom },
	}
	for name, breakIt := range cases {
		r := newRig()
		breakIt(r)
		r.build()
		_, err := r.step.Submit(context.Background(), gateInput(dbtCluster()))
		assert.ErrorIs(t, err, errBoom, name)
	}
}

// --- Poll ---

func status(phase fix.Phase, activated time.Time, nodeErrors map[string]string) ports.ReleaseCheckStatus {
	return ports.ReleaseCheckStatus{Phase: phase, ActivatedAt: activated, NodeErrors: nodeErrors}
}

// idsOf are the check ids Submit would have named for the input's drafts: a
// python check for the ledger project and a dbt check for sales, in project order.
func idsOf(in step.GateInput) []string {
	var ids []string
	for _, d := range in.Drafts {
		switch {
		case len(d.Draft.Edits) == 0:
		case strings.HasPrefix(d.Draft.Edits[0].Path, "python/ledger"):
			ids = append(ids, ledgerCheck)
		default:
			ids = append(ids, salesCheck)
		}
	}
	slices.Sort(ids)
	return ids
}

func poll(t *testing.T, r *rig, in step.GateInput) step.GateVerdict {
	t.Helper()
	v, err := r.step.Poll(context.Background(), in, remediation.CheckResult{Check: "release_check", Waiting: true, CheckIDs: idsOf(in)})
	require.NoError(t, err)
	return v
}

const (
	salesCheck  = "verify-rel-1-sales-r1-t2"
	ledgerCheck = "verify-rel-1-ledger-r1-t2"
)

func TestPoll_QueuedAndRunningRunsKeepTheGateWaiting(t *testing.T) {
	r := newRig()
	r.checks.statuses[ledgerCheck] = status(fix.PhaseRunning, t0.Add(30*time.Second), nil)
	r.checks.statuses[salesCheck] = status(fix.PhaseQueued, time.Time{}, nil)
	v := poll(t, r, gateInput(dbtCluster(), pythonCluster()))

	assert.Equal(t, remediation.CheckResult{Check: "release_check", Waiting: true, CheckIDs: []string{ledgerCheck, salesCheck}}, v.Result)
	assert.Empty(t, v.VerifyError)
	act := t0.Add(30 * time.Second)
	want := []fix.ReleaseCheck{
		{Project: "ledger", Kind: "python", CheckID: ledgerCheck, Phase: fix.PhaseRunning, ActivatedAt: &act},
		{Project: "sales", Kind: "dbt", CheckID: salesCheck, Phase: fix.PhaseQueued},
	}
	assert.Equal(t, want, v.ReleaseChecks)
	assert.Equal(t, want, v.Phases, "every check seen queued or running is reported for the fix row")
}

func TestPoll_EveryRunPassedPassesTheGate(t *testing.T) {
	r := newRig()
	act := t0.Add(time.Minute)
	r.checks.statuses[ledgerCheck] = status(fix.PhasePassed, act, nil)
	r.checks.statuses[salesCheck] = status(fix.PhasePassed, act, nil)
	v := poll(t, r, gateInput(dbtCluster(), pythonCluster()))

	assert.True(t, v.Result.Passed())
	assert.Equal(t, "release_check", v.Result.Check)
	assert.Empty(t, v.VerifyError)
	assert.Empty(t, v.Phases, "a terminal phase is written with the outcome, not as progress")
	require.Len(t, v.ReleaseChecks, 2)
	for _, ver := range v.ReleaseChecks {
		assert.Equal(t, fix.PhasePassed, ver.Phase)
		assert.Equal(t, &act, ver.ActivatedAt)
		assert.Empty(t, ver.Error)
	}
}

func TestPoll_APassedRunWaitsForTheOtherOne(t *testing.T) {
	r := newRig()
	r.checks.statuses[ledgerCheck] = status(fix.PhasePassed, t0, nil)
	r.checks.statuses[salesCheck] = status(fix.PhaseRunning, t0, nil)
	v := poll(t, r, gateInput(dbtCluster(), pythonCluster()))
	assert.True(t, v.Result.Waiting)
	require.Len(t, v.Phases, 1)
	assert.Equal(t, salesCheck, v.Phases[0].CheckID)
}

func TestPoll_AFailedRunFailsTheGateWithAFindingPerNode(t *testing.T) {
	r := newRig()
	r.checks.statuses[ledgerCheck] = status(fix.PhasePassed, t0, nil)
	r.checks.statuses[salesCheck] = status(fix.PhaseFailed, t0, map[string]string{
		"model.sales.orders": "column x does not exist",
		"model.finance.up":   "downstream broke",
	})
	v := poll(t, r, gateInput(dbtCluster(), pythonCluster()))

	assert.False(t, v.Result.Passed())
	assert.False(t, v.Result.Waiting)
	assert.Equal(t, []remediation.Finding{
		{Code: "release_check_failed", Message: "model.finance.up: downstream broke"},
		{Code: "release_check_failed", Message: "model.sales.orders: column x does not exist"},
	}, v.Result.Findings)
	// The errors of the nodes this try fixed come first and alone.
	assert.Equal(t, "model.sales.orders: column x does not exist", v.VerifyError)
	require.Len(t, v.ReleaseChecks, 2)
	assert.Equal(t, "model.finance.up: downstream broke; model.sales.orders: column x does not exist", v.ReleaseChecks[1].Error)
	assert.Equal(t, fix.PhaseFailed, v.ReleaseChecks[1].Phase)
	assert.Empty(t, v.ReleaseChecks[0].Error)
}

func TestPoll_WhenEveryOwnNodePassedEveryFailingNodeIsNamed(t *testing.T) {
	r := newRig()
	r.checks.statuses[salesCheck] = status(fix.PhaseFailed, t0, map[string]string{
		"model.zeta.b":  "b broke",
		"model.alpha.a": "a broke",
	})
	v := poll(t, r, gateInput(dbtCluster()))
	assert.Equal(t, "model.alpha.a: a broke; model.zeta.b: b broke", v.VerifyError)
}

func TestPoll_AFailureWithNoErrorAtAllNamesTheFailedRun(t *testing.T) {
	r := newRig()
	r.checks.statuses[ledgerCheck] = status(fix.PhasePassed, t0, nil)
	r.checks.statuses[salesCheck] = status(fix.PhaseFailed, t0, nil)
	v := poll(t, r, gateInput(dbtCluster(), pythonCluster()))

	assert.Equal(t, "release check "+salesCheck+" failed without a per-node error", v.VerifyError)
	require.Len(t, v.Result.Findings, 1)
	assert.Equal(t, "release_check_failed", v.Result.Findings[0].Code)
	assert.Equal(t, v.VerifyError, v.Result.Findings[0].Message)
	assert.False(t, v.Result.Passed(), "a failure with no node error must not read as a pass")
}

// TestPoll_AFailureThatNamesNoNodeCarriesTheRunsOwnError pins the text the next
// try is shown when a check fails without naming a node: the check's own reason
// and detail, not a sentence that only says it failed.
func TestPoll_AFailureThatNamesNoNodeCarriesTheRunsOwnError(t *testing.T) {
	r := newRig()
	r.checks.statuses[ledgerCheck] = status(fix.PhasePassed, t0, nil)
	r.checks.statuses[salesCheck] = ports.ReleaseCheckStatus{
		Phase: fix.PhaseFailed, ActivatedAt: t0,
		CheckError: "nothing_to_validate — the release changes no node",
	}
	v := poll(t, r, gateInput(dbtCluster(), pythonCluster()))

	want := "release check " + salesCheck + " failed: nothing_to_validate — the release changes no node"
	assert.Equal(t, want, v.VerifyError)
	assert.Equal(t, []remediation.Finding{{Code: "release_check_failed", Message: want}}, v.Result.Findings)
	require.Len(t, v.ReleaseChecks, 2)
	assert.Equal(t, "nothing_to_validate — the release changes no node", v.ReleaseChecks[1].Error)
}

func TestPoll_AFailedRunWinsOverARunStillRunning(t *testing.T) {
	r := newRig()
	r.checks.statuses[ledgerCheck] = status(fix.PhaseRunning, t0, nil)
	r.checks.statuses[salesCheck] = status(fix.PhaseFailed, t0, map[string]string{"model.sales.orders": "bad"})
	v := poll(t, r, gateInput(dbtCluster(), pythonCluster()))
	assert.False(t, v.Result.Waiting)
	assert.Equal(t, "model.sales.orders: bad", v.VerifyError)
	require.Len(t, v.Phases, 1)
	assert.Equal(t, ledgerCheck, v.Phases[0].CheckID)
}

func TestPoll_ARunRunningPastTheBudgetTimesOut(t *testing.T) {
	r := newRig()
	started := t0.Add(time.Minute)
	r.checks.statuses[salesCheck] = status(fix.PhaseRunning, started, nil)

	r.clock.now = started.Add(timeout - time.Second)
	assert.True(t, poll(t, r, gateInput(dbtCluster())).Result.Waiting, "inside the budget the check is left alone")

	r.clock.now = started.Add(timeout)
	v := poll(t, r, gateInput(dbtCluster()))
	assert.False(t, v.Result.Waiting)
	assert.Equal(t, "release check timed out", v.VerifyError)
	assert.Equal(t, []remediation.Finding{{Code: "release_check_timed_out", Message: "release check timed out"}}, v.Result.Findings)
	require.Len(t, v.ReleaseChecks, 1)
	assert.Equal(t, fix.PhaseRunning, v.ReleaseChecks[0].Phase)
}

func TestPoll_AQueuedRunNeverTimesOutHoweverLongTheTryHasWaited(t *testing.T) {
	r := newRig()
	r.checks.statuses[salesCheck] = status(fix.PhaseQueued, time.Time{}, nil)
	r.clock.now = t0.Add(10 * timeout)
	assert.True(t, poll(t, r, gateInput(dbtCluster())).Result.Waiting)
}

func TestPoll_TheBudgetOfARunCountsFromItsOwnStart(t *testing.T) {
	r := newRig()
	// The first check finished long ago; the second started just now.
	r.checks.statuses[ledgerCheck] = status(fix.PhasePassed, t0, nil)
	r.checks.statuses[salesCheck] = status(fix.PhaseRunning, t0.Add(2*timeout), nil)
	r.clock.now = t0.Add(2*timeout + time.Minute)
	assert.True(t, poll(t, r, gateInput(dbtCluster(), pythonCluster())).Result.Waiting)
}

func TestPoll_AnUnreadableRunIsRetriedInsideTheBudget(t *testing.T) {
	r := newRig()
	r.checks.statusErrs[salesCheck] = errBoom
	r.clock.now = t0.Add(timeout - time.Second)
	v := poll(t, r, gateInput(dbtCluster()))
	assert.True(t, v.Result.Waiting)
	assert.Equal(t, []string{salesCheck}, v.Result.CheckIDs)
	assert.Empty(t, v.VerifyError)
}

func TestPoll_AnUnreadableRunTimesOutOnceTheTryIsOlderThanTheBudget(t *testing.T) {
	r := newRig()
	r.checks.statusErrs[salesCheck] = errBoom
	r.clock.now = t0.Add(timeout)
	v := poll(t, r, gateInput(dbtCluster()))
	assert.False(t, v.Result.Waiting)
	assert.Equal(t, "release check timed out", v.VerifyError)
	assert.Equal(t, "release_check_timed_out", v.Result.Findings[0].Code)
	assert.Equal(t, []string{salesCheck}, v.Result.CheckIDs)
	assert.Empty(t, v.ReleaseChecks, "an unread check keeps the values recorded for it")
	assert.Empty(t, v.Phases)
}

// TestPoll_TheUnreadableBudgetCountsFromTheSubmit pins that a try drafted long
// ago and submitted just now is not timed out by one unreadable status.
func TestPoll_TheUnreadableBudgetCountsFromTheSubmit(t *testing.T) {
	r := newRig()
	r.checks.statusErrs[salesCheck] = errBoom
	in := gateInput(dbtCluster())
	in.Try.CreatedAt = t0.Add(-3 * timeout)
	in.SubmittedAt = t0
	r.clock.now = t0.Add(time.Minute)
	assert.True(t, poll(t, r, in).Result.Waiting, "drafting time is not charged to the gate")

	r.clock.now = t0.Add(timeout)
	assert.False(t, poll(t, r, in).Result.Waiting, "the submit's own age is")
}

func TestPoll_RefusesAnInputWithNoSubmitTime(t *testing.T) {
	r := newRig()
	in := gateInput(dbtCluster())
	in.SubmittedAt = time.Time{}
	_, err := r.step.Poll(context.Background(), in, remediation.CheckResult{Check: "release_check", Waiting: true, CheckIDs: []string{salesCheck}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "submit")
	assert.Empty(t, r.checks.statusReads, "no check is read")
}

func TestPoll_AnUnreadableRunStopsTheReadAndKeepsWhatWasRead(t *testing.T) {
	r := newRig()
	r.checks.statuses[ledgerCheck] = status(fix.PhaseRunning, t0, nil)
	r.checks.statusErrs[salesCheck] = errBoom
	v := poll(t, r, gateInput(dbtCluster(), pythonCluster()))
	assert.True(t, v.Result.Waiting)
	assert.Equal(t, []string{ledgerCheck, salesCheck}, v.Result.CheckIDs)
	// Only the check that was read is reported: writing the unread one as queued
	// would reset the phase and activation time recorded for it.
	act := t0
	want := []fix.ReleaseCheck{{Project: "ledger", Kind: "python", CheckID: ledgerCheck, Phase: fix.PhaseRunning, ActivatedAt: &act}}
	assert.Equal(t, want, v.ReleaseChecks)
	assert.Equal(t, want, v.Phases)
}

func TestPoll_WhenTheFirstRunIsUnreadableNothingIsReportedForAnyRun(t *testing.T) {
	r := newRig()
	r.checks.statusErrs[ledgerCheck] = errBoom
	r.checks.statuses[salesCheck] = status(fix.PhaseRunning, t0, nil)
	v := poll(t, r, gateInput(dbtCluster(), pythonCluster()))
	assert.True(t, v.Result.Waiting)
	assert.Empty(t, v.ReleaseChecks)
	assert.Empty(t, v.Phases)
	assert.Equal(t, []string{ledgerCheck}, r.checks.statusReads, "the read stops at the first unreadable check")
}

func TestPoll_WhenNoDraftedNodeFailsEveryFailingNodeIsNamed(t *testing.T) {
	r := newRig()
	// The draft covers one trigger node, which passed. The failure names
	// another trigger node no draft covers, and a node the trigger never
	// carried: neither is the try's own, so every failing node is named.
	narrow := dbtCluster()
	narrow.Cluster.Members = []string{"model.sales.orders"}
	r.checks.statuses[salesCheck] = status(fix.PhaseFailed, t0, map[string]string{
		"model.sales.margins": "margins broke",
		"model.zeta.other":    "other broke",
	})
	v := poll(t, r, gateInput(narrow))
	assert.Equal(t, "model.sales.margins: margins broke; model.zeta.other: other broke", v.VerifyError)
}

func TestPoll_ASettledNodesErrorDoesNotLeadTheReason(t *testing.T) {
	r := newRig()
	// model.sales.margins is handed over (unfixable, so settled for every
	// try): it is in the trigger, sorts before the drafted node, and its check
	// error is reported beside the drafted node's. Only the drafted node's
	// error is the try's own.
	in := gateInput(func() step.ClusterDraft {
		d := dbtCluster()
		d.Cluster.Members = []string{"model.sales.orders"}
		return d
	}())
	in.Trigger.Nodes[1].Unfixable = &remediation.Unfixable{Reason: model.HandoverReasonClassifierInfra, Detail: "could not connect to server: connection refused"}
	r.checks.statuses[salesCheck] = status(fix.PhaseFailed, t0, map[string]string{
		"model.sales.margins": "connection refused",
		"model.sales.orders":  "column x does not exist",
	})
	v := poll(t, r, in)
	assert.Equal(t, "model.sales.orders: column x does not exist", v.VerifyError)
}

func TestPoll_RefusesRunsThatAreNotTheOnesTheWaitingResultNames(t *testing.T) {
	r := newRig()
	in := gateInput(dbtCluster())
	_, err := r.step.Poll(context.Background(), in, remediation.CheckResult{Check: "release_check", Waiting: true, CheckIDs: []string{"verify-other-a1"}})
	assert.Error(t, err)
	assert.Empty(t, r.checks.statusReads)
}

func TestPoll_ReadsEachRunOncePerPoll(t *testing.T) {
	r := newRig()
	r.checks.statuses[ledgerCheck] = status(fix.PhasePassed, t0, nil)
	r.checks.statuses[salesCheck] = status(fix.PhasePassed, t0, nil)
	poll(t, r, gateInput(dbtCluster(), pythonCluster()))
	assert.Equal(t, []string{ledgerCheck, salesCheck}, r.checks.statusReads)
}

func TestPoll_RefusesATryThatHasNoRuns(t *testing.T) {
	_, err := newRig().step.Poll(context.Background(), gateInput(), remediation.CheckResult{})
	assert.Error(t, err)
}

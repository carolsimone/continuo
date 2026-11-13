package gate_test

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/domain/workflow"
	"github.com/carolsimone/continuo/agent-remediation/service/overlay"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
	"github.com/carolsimone/continuo/agent-remediation/service/steps/gate"
)

var errBoom = errors.New("boom")

var repoPaths = map[string]string{
	"sales":  "dbt/services/sales",
	"ledger": "python/ledger",
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

func (f fakeReleases) ImageTag(_ context.Context, releaseID, service string) (string, error) {
	if f.err != nil {
		return "", f.err
	}
	return "tag-" + releaseID + "-" + service, nil
}

func (fakeReleases) FailingNodes(context.Context, string) (map[string]string, error) {
	return nil, nil
}

type fakePipeline struct {
	submitted   []ports.VerificationRequest
	submitErr   error
	statuses    map[string]ports.VerificationStatus
	statusErrs  map[string]error
	statusReads []string
}

func (f *fakePipeline) Submit(_ context.Context, r ports.VerificationRequest) error {
	if f.submitErr != nil {
		return f.submitErr
	}
	f.submitted = append(f.submitted, r)
	return nil
}

func (f *fakePipeline) Status(_ context.Context, runID string) (ports.VerificationStatus, error) {
	f.statusReads = append(f.statusReads, runID)
	if err := f.statusErrs[runID]; err != nil {
		return ports.VerificationStatus{}, err
	}
	return f.statuses[runID], nil
}

type fixedClock struct{ now time.Time }

func (c fixedClock) Now() time.Time { return c.now }

var (
	t0      = time.Date(2026, 11, 13, 12, 0, 0, 0, time.UTC)
	timeout = 30 * time.Minute
)

type rig struct {
	step      *gate.VerificationRun
	artifacts *fakeArtifacts
	pipeline  *fakePipeline
	evidence  *fakeEvidence
	clock     *fixedClock
	releases  fakeReleases
}

func newRig() *rig {
	r := &rig{
		artifacts: &fakeArtifacts{},
		pipeline:  &fakePipeline{statuses: map[string]ports.VerificationStatus{}, statusErrs: map[string]error{}},
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
	r.step = gate.NewVerificationRun(r.evidence, r.artifacts, r.releases, r.pipeline, repoPaths, r.clock, timeout, nil)
}

func triggerOf() remediation.Trigger {
	return remediation.Trigger{
		ReleaseID: "rel-1",
		Nodes: []remediation.TriggerNode{
			{NodeID: "model.sales.orders", NodeType: "dbt-model", Service: "sales"},
			{NodeID: "model.sales.margins", NodeType: "dbt-model", Service: "sales"},
			{NodeID: "python.ledger.postings", NodeType: "python-node", Service: "ledger"},
		},
	}
}

func dbtCluster() step.ClusterDraft {
	return step.ClusterDraft{
		Cluster: remediation.Cluster{Cluster: 1, Members: []string{"model.sales.orders", "model.sales.margins"}},
		Draft: remediation.Draft{Edits: []remediation.Edit{
			{Path: "dbt/services/sales/models/orders.sql", ContentURI: "s3://c/orders", TargetNodeID: "model.sales.orders"},
			{Path: "dbt/services/sales/models/marts/margins.sql", ContentURI: "s3://c/margins", TargetNodeID: "model.sales.margins"},
		}},
		Evidence: remediation.NewEvidence(),
	}
}

const pythonContract = "nodes:\n  - id: postings\n"

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
		Try:         remediation.Try{Try: 1, Attempt: 2, CreatedAt: t0},
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
	assert.Equal(t, workflow.StepName("verification_run"), m.Name)
	assert.Equal(t, step.KindCheck, m.Kind)
	assert.Equal(t, []workflow.Stage{workflow.StageGate}, m.Stages)
	assert.True(t, m.MayStandAsGateRun)
	assert.Empty(t, m.Needs)
}

func TestSubmit_DbtServiceSubmitsAnOverlayOfTheEditsContent(t *testing.T) {
	r := newRig()
	res, err := r.step.Submit(context.Background(), gateInput(dbtCluster()))
	require.NoError(t, err)

	assert.Equal(t, remediation.CheckResult{Check: "verification_run", Waiting: true, RunIDs: []string{"verify-rel-1-sales-a2"}}, res.Result)
	assert.Equal(t, []proposal.Verification{
		{Service: "sales", Kind: "dbt", RunID: "verify-rel-1-sales-a2", Phase: proposal.PhaseQueued},
	}, res.Verifications, "the runner records the runs with the proposal as submitVerifications returned them")
	require.Len(t, r.pipeline.submitted, 1)
	assert.Equal(t, ports.VerificationRequest{
		RunID:             "verify-rel-1-sales-a2",
		Service:           "sales",
		ImageTag:          "tag-rel-1-sales",
		Kind:              "dbt",
		VerifiesReleaseID: "rel-1",
		Attempt:           2,
		SourceOverlayURI:  "s3://artifacts/sales/verify-rel-1-sales-a2/source-overlay.tar.gz",
	}, r.pipeline.submitted[0])

	require.Len(t, r.artifacts.writes, 1)
	w := r.artifacts.writes[0]
	assert.Equal(t, "sales/verify-rel-1-sales-a2/source-overlay.tar.gz", w.key)
	assert.Equal(t, "application/gzip", w.contentType)
	// The overlay holds each edit's content as the artifact stored it, at its
	// path within the service's project.
	assert.Equal(t, map[string]string{
		"models/orders.sql":        "select 1 as orders",
		"models/marts/margins.sql": "select 2 as margins",
	}, untar(t, w.body))
	want, err := overlay.Build([]overlay.File{
		{Path: "models/marts/margins.sql", Content: []byte("select 2 as margins")},
		{Path: "models/orders.sql", Content: []byte("select 1 as orders")},
	})
	require.NoError(t, err)
	assert.Equal(t, string(want), w.body)
}

func TestSubmit_PythonServiceSubmitsThePackagedContractAndNoOverlay(t *testing.T) {
	r := newRig()
	res, err := r.step.Submit(context.Background(), gateInput(pythonCluster()))
	require.NoError(t, err)

	assert.Equal(t, []string{"verify-rel-1-ledger-a2"}, res.Result.RunIDs)
	assert.True(t, res.Result.Waiting)
	assert.Equal(t, []proposal.Verification{
		{Service: "ledger", Kind: "python", RunID: "verify-rel-1-ledger-a2", Phase: proposal.PhaseQueued},
	}, res.Verifications)
	assert.Equal(t, []ports.VerificationRequest{{
		RunID:             "verify-rel-1-ledger-a2",
		Service:           "ledger",
		ImageTag:          "tag-rel-1-ledger",
		Kind:              "python",
		VerifiesReleaseID: "rel-1",
		Attempt:           2,
	}}, r.pipeline.submitted)
	assert.Equal(t, []write{{
		key: "ledger/verify-rel-1-ledger-a2/contract.yaml", body: pythonContract, contentType: "application/yaml",
	}}, r.artifacts.writes)
}

func TestSubmit_TwoServicesSubmitOneRunEachInServiceOrder(t *testing.T) {
	r := newRig()
	res, err := r.step.Submit(context.Background(), gateInput(dbtCluster(), pythonCluster()))
	require.NoError(t, err)
	assert.Equal(t, []string{"verify-rel-1-ledger-a2", "verify-rel-1-sales-a2"}, res.Result.RunIDs)
	assert.Equal(t, []proposal.Verification{
		{Service: "ledger", Kind: "python", RunID: "verify-rel-1-ledger-a2", Phase: proposal.PhaseQueued},
		{Service: "sales", Kind: "dbt", RunID: "verify-rel-1-sales-a2", Phase: proposal.PhaseQueued},
	}, res.Verifications)
	require.Len(t, r.pipeline.submitted, 2)
	assert.Equal(t, "python", r.pipeline.submitted[0].Kind)
	assert.Equal(t, "dbt", r.pipeline.submitted[1].Kind)
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
		"unmapped":  {gateInput(dbtCluster(), outside), "edit_outside_services", "edit belongs to no configured service: docs/x.md"},
		"duplicate": {gateInput(twice), "path_edited_twice", "two edits change the same file: dbt/services/sales/models/orders.sql"},
		"mixed": {gateInput(dbtCluster(), mixed), "mixed_manifest_kinds",
			"service ledger mixes a python contract with dbt edits in one attempt; a release verifies one manifest kind"},
	}
	for name, c := range cases {
		v, err := r.step.Submit(context.Background(), c.in)
		require.NoError(t, err, name)
		assert.False(t, v.Result.Waiting, name)
		assert.False(t, v.Result.Passed(), name)
		assert.Equal(t, "verification_run", v.Result.Check, name)
		assert.Equal(t, []remediation.Finding{{Code: c.code, Message: c.msg}}, v.Result.Findings, name)
		assert.Equal(t, c.msg, v.VerifyError, name)
		assert.Empty(t, v.Verifications, name)
	}
	assert.Empty(t, r.artifacts.writes, "a try one service cannot verify spends no run on the others")
	assert.Empty(t, r.pipeline.submitted)

	_, err := r.step.Submit(context.Background(), gateInput())
	assert.Error(t, err, "a try with no edit at all is a defect of the caller, not a refusal")
}

func TestSubmit_ReturnsTheErrorOfEachCollaborator(t *testing.T) {
	cases := map[string]func(*rig){
		"evidence":  func(r *rig) { r.evidence.err = errBoom },
		"artifacts": func(r *rig) { r.artifacts.err = errBoom },
		"releases":  func(r *rig) { r.releases = fakeReleases{err: errBoom} },
		"pipeline":  func(r *rig) { r.pipeline.submitErr = errBoom },
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

func status(phase proposal.Phase, activated time.Time, nodeErrors map[string]string) ports.VerificationStatus {
	return ports.VerificationStatus{Phase: phase, ActivatedAt: activated, NodeErrors: nodeErrors}
}

// idsOf are the run ids Submit would have named for the input's drafts: a
// python run for the ledger service and a dbt run for sales, in service order.
func idsOf(in step.GateInput) []string {
	var ids []string
	for _, d := range in.Drafts {
		switch {
		case len(d.Draft.Edits) == 0:
		case strings.HasPrefix(d.Draft.Edits[0].Path, "python/ledger"):
			ids = append(ids, ledgerRun)
		default:
			ids = append(ids, salesRun)
		}
	}
	slices.Sort(ids)
	return ids
}

func poll(t *testing.T, r *rig, in step.GateInput) step.GateVerdict {
	t.Helper()
	v, err := r.step.Poll(context.Background(), in, remediation.CheckResult{Check: "verification_run", Waiting: true, RunIDs: idsOf(in)})
	require.NoError(t, err)
	return v
}

const (
	salesRun  = "verify-rel-1-sales-a2"
	ledgerRun = "verify-rel-1-ledger-a2"
)

func TestPoll_QueuedAndRunningRunsKeepTheGateWaiting(t *testing.T) {
	r := newRig()
	r.pipeline.statuses[ledgerRun] = status(proposal.PhaseRunning, t0.Add(30*time.Second), nil)
	r.pipeline.statuses[salesRun] = status(proposal.PhaseQueued, time.Time{}, nil)
	v := poll(t, r, gateInput(dbtCluster(), pythonCluster()))

	assert.Equal(t, remediation.CheckResult{Check: "verification_run", Waiting: true, RunIDs: []string{ledgerRun, salesRun}}, v.Result)
	assert.Empty(t, v.VerifyError)
	act := t0.Add(30 * time.Second)
	want := []proposal.Verification{
		{Service: "ledger", Kind: "python", RunID: ledgerRun, Phase: proposal.PhaseRunning, ActivatedAt: &act},
		{Service: "sales", Kind: "dbt", RunID: salesRun, Phase: proposal.PhaseQueued},
	}
	assert.Equal(t, want, v.Verifications)
	assert.Equal(t, want, v.Phases, "every run seen queued or running is reported for the proposal row")
}

func TestPoll_EveryRunPassedPassesTheGate(t *testing.T) {
	r := newRig()
	act := t0.Add(time.Minute)
	r.pipeline.statuses[ledgerRun] = status(proposal.PhasePassed, act, nil)
	r.pipeline.statuses[salesRun] = status(proposal.PhasePassed, act, nil)
	v := poll(t, r, gateInput(dbtCluster(), pythonCluster()))

	assert.True(t, v.Result.Passed())
	assert.Equal(t, "verification_run", v.Result.Check)
	assert.Empty(t, v.VerifyError)
	assert.Empty(t, v.Phases, "a terminal phase is written with the outcome, not as progress")
	require.Len(t, v.Verifications, 2)
	for _, ver := range v.Verifications {
		assert.Equal(t, proposal.PhasePassed, ver.Phase)
		assert.Equal(t, &act, ver.ActivatedAt)
		assert.Empty(t, ver.Error)
	}
}

func TestPoll_APassedRunWaitsForTheOtherOne(t *testing.T) {
	r := newRig()
	r.pipeline.statuses[ledgerRun] = status(proposal.PhasePassed, t0, nil)
	r.pipeline.statuses[salesRun] = status(proposal.PhaseRunning, t0, nil)
	v := poll(t, r, gateInput(dbtCluster(), pythonCluster()))
	assert.True(t, v.Result.Waiting)
	require.Len(t, v.Phases, 1)
	assert.Equal(t, salesRun, v.Phases[0].RunID)
}

func TestPoll_AFailedRunFailsTheGateWithAFindingPerNode(t *testing.T) {
	r := newRig()
	r.pipeline.statuses[ledgerRun] = status(proposal.PhasePassed, t0, nil)
	r.pipeline.statuses[salesRun] = status(proposal.PhaseFailed, t0, map[string]string{
		"model.sales.orders": "column x does not exist",
		"model.finance.up":   "downstream broke",
	})
	v := poll(t, r, gateInput(dbtCluster(), pythonCluster()))

	assert.False(t, v.Result.Passed())
	assert.False(t, v.Result.Waiting)
	assert.Equal(t, []remediation.Finding{
		{Code: "verification_failed", Message: "model.finance.up: downstream broke"},
		{Code: "verification_failed", Message: "model.sales.orders: column x does not exist"},
	}, v.Result.Findings)
	// The errors of the nodes this try fixed come first and alone.
	assert.Equal(t, "model.sales.orders: column x does not exist", v.VerifyError)
	require.Len(t, v.Verifications, 2)
	assert.Equal(t, "model.finance.up: downstream broke; model.sales.orders: column x does not exist", v.Verifications[1].Error)
	assert.Equal(t, proposal.PhaseFailed, v.Verifications[1].Phase)
	assert.Empty(t, v.Verifications[0].Error)
}

func TestPoll_WhenEveryOwnNodePassedEveryFailingNodeIsNamed(t *testing.T) {
	r := newRig()
	r.pipeline.statuses[salesRun] = status(proposal.PhaseFailed, t0, map[string]string{
		"model.zeta.b":  "b broke",
		"model.alpha.a": "a broke",
	})
	v := poll(t, r, gateInput(dbtCluster()))
	assert.Equal(t, "model.alpha.a: a broke; model.zeta.b: b broke", v.VerifyError)
}

func TestPoll_AFailureWithNoNodeErrorNamesTheFirstRun(t *testing.T) {
	r := newRig()
	r.pipeline.statuses[ledgerRun] = status(proposal.PhasePassed, t0, nil)
	r.pipeline.statuses[salesRun] = status(proposal.PhaseFailed, t0, nil)
	v := poll(t, r, gateInput(dbtCluster(), pythonCluster()))

	assert.Equal(t, "verification run "+ledgerRun+" failed without a per-node error", v.VerifyError)
	require.Len(t, v.Result.Findings, 1)
	assert.Equal(t, "verification_failed", v.Result.Findings[0].Code)
	assert.Equal(t, v.VerifyError, v.Result.Findings[0].Message)
	assert.False(t, v.Result.Passed(), "a failure with no node error must not read as a pass")
}

func TestPoll_AFailedRunWinsOverARunStillRunning(t *testing.T) {
	r := newRig()
	r.pipeline.statuses[ledgerRun] = status(proposal.PhaseRunning, t0, nil)
	r.pipeline.statuses[salesRun] = status(proposal.PhaseFailed, t0, map[string]string{"model.sales.orders": "bad"})
	v := poll(t, r, gateInput(dbtCluster(), pythonCluster()))
	assert.False(t, v.Result.Waiting)
	assert.Equal(t, "model.sales.orders: bad", v.VerifyError)
	require.Len(t, v.Phases, 1)
	assert.Equal(t, ledgerRun, v.Phases[0].RunID)
}

func TestPoll_ARunRunningPastTheBudgetTimesOut(t *testing.T) {
	r := newRig()
	started := t0.Add(time.Minute)
	r.pipeline.statuses[salesRun] = status(proposal.PhaseRunning, started, nil)

	r.clock.now = started.Add(timeout - time.Second)
	assert.True(t, poll(t, r, gateInput(dbtCluster())).Result.Waiting, "inside the budget the run is left alone")

	r.clock.now = started.Add(timeout)
	v := poll(t, r, gateInput(dbtCluster()))
	assert.False(t, v.Result.Waiting)
	assert.Equal(t, "verification timed out", v.VerifyError)
	assert.Equal(t, []remediation.Finding{{Code: "verification_timed_out", Message: "verification timed out"}}, v.Result.Findings)
	require.Len(t, v.Verifications, 1)
	assert.Equal(t, proposal.PhaseRunning, v.Verifications[0].Phase)
}

func TestPoll_AQueuedRunNeverTimesOutHoweverLongTheTryHasWaited(t *testing.T) {
	r := newRig()
	r.pipeline.statuses[salesRun] = status(proposal.PhaseQueued, time.Time{}, nil)
	r.clock.now = t0.Add(10 * timeout)
	assert.True(t, poll(t, r, gateInput(dbtCluster())).Result.Waiting)
}

func TestPoll_TheBudgetOfARunCountsFromItsOwnStart(t *testing.T) {
	r := newRig()
	// The first run finished long ago; the second started just now.
	r.pipeline.statuses[ledgerRun] = status(proposal.PhasePassed, t0, nil)
	r.pipeline.statuses[salesRun] = status(proposal.PhaseRunning, t0.Add(2*timeout), nil)
	r.clock.now = t0.Add(2*timeout + time.Minute)
	assert.True(t, poll(t, r, gateInput(dbtCluster(), pythonCluster())).Result.Waiting)
}

func TestPoll_AnUnreadableRunIsRetriedInsideTheBudget(t *testing.T) {
	r := newRig()
	r.pipeline.statusErrs[salesRun] = errBoom
	r.clock.now = t0.Add(timeout - time.Second)
	v := poll(t, r, gateInput(dbtCluster()))
	assert.True(t, v.Result.Waiting)
	assert.Equal(t, []string{salesRun}, v.Result.RunIDs)
	assert.Empty(t, v.VerifyError)
}

func TestPoll_AnUnreadableRunTimesOutOnceTheTryIsOlderThanTheBudget(t *testing.T) {
	r := newRig()
	r.pipeline.statusErrs[salesRun] = errBoom
	r.clock.now = t0.Add(timeout)
	v := poll(t, r, gateInput(dbtCluster()))
	assert.False(t, v.Result.Waiting)
	assert.Equal(t, "verification timed out", v.VerifyError)
	assert.Equal(t, "verification_timed_out", v.Result.Findings[0].Code)
	assert.Equal(t, []string{salesRun}, v.Result.RunIDs)
	assert.Empty(t, v.Verifications, "an unread run keeps the values recorded for it")
	assert.Empty(t, v.Phases)
}

// TestPoll_TheUnreadableBudgetCountsFromTheSubmit pins that a try drafted long
// ago and submitted just now is not timed out by one unreadable status.
func TestPoll_TheUnreadableBudgetCountsFromTheSubmit(t *testing.T) {
	r := newRig()
	r.pipeline.statusErrs[salesRun] = errBoom
	in := gateInput(dbtCluster())
	in.Try.CreatedAt = t0.Add(-3 * timeout)
	in.SubmittedAt = t0
	r.clock.now = t0.Add(time.Minute)
	assert.True(t, poll(t, r, in).Result.Waiting, "drafting time is not charged to the gate")

	r.clock.now = t0.Add(timeout)
	assert.False(t, poll(t, r, in).Result.Waiting, "the submit's own age is")
}

func TestPoll_WithNoSubmitTimeTheUnreadableBudgetCountsFromTheTry(t *testing.T) {
	r := newRig()
	r.pipeline.statusErrs[salesRun] = errBoom
	in := gateInput(dbtCluster())
	in.SubmittedAt = time.Time{}
	r.clock.now = t0.Add(timeout - time.Second)
	assert.True(t, poll(t, r, in).Result.Waiting)
	r.clock.now = t0.Add(timeout)
	assert.False(t, poll(t, r, in).Result.Waiting)
}

func TestPoll_AnUnreadableRunStopsTheReadAndKeepsWhatWasRead(t *testing.T) {
	r := newRig()
	r.pipeline.statuses[ledgerRun] = status(proposal.PhaseRunning, t0, nil)
	r.pipeline.statusErrs[salesRun] = errBoom
	v := poll(t, r, gateInput(dbtCluster(), pythonCluster()))
	assert.True(t, v.Result.Waiting)
	assert.Equal(t, []string{ledgerRun, salesRun}, v.Result.RunIDs)
	// Only the run that was read is reported: writing the unread one as queued
	// would reset the phase and activation time recorded for it.
	act := t0
	want := []proposal.Verification{{Service: "ledger", Kind: "python", RunID: ledgerRun, Phase: proposal.PhaseRunning, ActivatedAt: &act}}
	assert.Equal(t, want, v.Verifications)
	assert.Equal(t, want, v.Phases)
}

func TestPoll_WhenTheFirstRunIsUnreadableNothingIsReportedForAnyRun(t *testing.T) {
	r := newRig()
	r.pipeline.statusErrs[ledgerRun] = errBoom
	r.pipeline.statuses[salesRun] = status(proposal.PhaseRunning, t0, nil)
	v := poll(t, r, gateInput(dbtCluster(), pythonCluster()))
	assert.True(t, v.Result.Waiting)
	assert.Empty(t, v.Verifications)
	assert.Empty(t, v.Phases)
	assert.Equal(t, []string{ledgerRun}, r.pipeline.statusReads, "the read stops at the first unreadable run")
}

func TestPoll_TheReasonNamesOnlyTheNodesTheTriggerCarries(t *testing.T) {
	r := newRig()
	// The cluster covers one trigger node. The failure names another trigger
	// node no draft covers, and a node the trigger never carried.
	narrow := dbtCluster()
	narrow.Cluster.Members = []string{"model.sales.orders"}
	r.pipeline.statuses[salesRun] = status(proposal.PhaseFailed, t0, map[string]string{
		"model.sales.margins": "margins broke",
		"model.zeta.other":    "other broke",
	})
	v := poll(t, r, gateInput(narrow))
	assert.Equal(t, "model.sales.margins: margins broke", v.VerifyError)
}

func TestPoll_RefusesRunsThatAreNotTheOnesTheWaitingResultNames(t *testing.T) {
	r := newRig()
	in := gateInput(dbtCluster())
	_, err := r.step.Poll(context.Background(), in, remediation.CheckResult{Check: "verification_run", Waiting: true, RunIDs: []string{"verify-other-a1"}})
	assert.Error(t, err)
	assert.Empty(t, r.pipeline.statusReads)
}

func TestPoll_ReadsEachRunOncePerPoll(t *testing.T) {
	r := newRig()
	r.pipeline.statuses[ledgerRun] = status(proposal.PhasePassed, t0, nil)
	r.pipeline.statuses[salesRun] = status(proposal.PhasePassed, t0, nil)
	poll(t, r, gateInput(dbtCluster(), pythonCluster()))
	assert.Equal(t, []string{ledgerRun, salesRun}, r.pipeline.statusReads)
}

func TestPoll_RefusesATryThatHasNoRuns(t *testing.T) {
	_, err := newRig().step.Poll(context.Background(), gateInput(), remediation.CheckResult{})
	assert.Error(t, err)
}

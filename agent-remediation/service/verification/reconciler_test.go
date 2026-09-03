package verification

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	rredis "github.com/carolsimone/continuo/agent-remediation/adapters/redis"
	"github.com/carolsimone/continuo/agent-remediation/domain/event"
	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/domain/repository"
	"github.com/carolsimone/continuo/agent-remediation/service/handlers"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/uow"
	"github.com/carolsimone/continuo/pkg/messageprocessing"
	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
)

// testNow is the instant every test's clock reports; row ages are expressed
// relative to it.
var testNow = time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)

// testTimeout is the verification budget the harness runs with, short enough
// that a row's age can be written as an obvious "well under" / "well over".
const testTimeout = 20 * time.Minute

// originalMessageID is the Redis Stream id of the remediation.requested
// message that produced the first attempt. It is claimed in the harness's
// message-processing store exactly as the first attempt's own transaction
// claimed it, so a retry that replays it is a dedup hit.
const originalMessageID = "1700000000000-0"

type fakeClock struct{ t time.Time }

func (c fakeClock) Now() time.Time { return c.t }

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// triggerPayload builds a remediation.requested:v2 payload carrying nodes as
// the release's failing set, under the release header every row in these tests
// was triggered by.
func triggerPayload(nodes []map[string]any) []byte {
	raw, err := json.Marshal(map[string]any{
		"event_id":          "evt-1",
		"source":            "validation",
		"release_id":        "rel-1",
		"remediation_round": 1,
		"repo":              "o/r",
		"commit_sha":        "sha-1",
		"nodes":             nodes,
	})
	if err != nil {
		panic(err)
	}
	return raw
}

// requestedPayload is the one-node trigger a python validation failure arrives
// as, which the reconciler replays to start the next attempt.
func requestedPayload() []byte {
	return triggerPayload([]map[string]any{{
		"node_id":         "analytics.orders",
		"category":        "validation",
		"error_signature": "sig-1",
		"reason":          "contract_mismatch",
		"node_type":       "python-model",
		"service":         "svc",
	}})
}

// batchPayload is the trigger of a release whose whole failing set one attempt
// addressed, one node per id.
func batchPayload(nodeIDs ...string) []byte {
	nodes := make([]map[string]any, 0, len(nodeIDs))
	for _, id := range nodeIDs {
		nodes = append(nodes, map[string]any{
			"node_id":         id,
			"category":        "validation",
			"error_signature": "sig-" + id,
			"reason":          "contract_mismatch",
			"node_type":       "python-model",
			"service":         "svc",
		})
	}
	return triggerPayload(nodes)
}

// verifyingRow is a proposal awaiting its verification runs' outcomes, aged by
// age relative to testNow. It addresses one failing node, verified by the one
// verification run the service that node belongs to was submitted as. The
// verification starts at PhaseQueued, matching the phase the driver records
// when it submits the run — so a status read that still reports "queued"
// records no phase transition.
func verifyingRow(id string, attempt int, age time.Duration) proposal.View {
	run := fmt.Sprintf("verify-rel-1-svc-a%d", attempt)
	return proposal.View{
		ID:              id,
		Source:          "validation",
		ReleaseID:       "rel-1",
		NodeID:          "analytics.orders",
		ResolvedNodeIDs: []string{"analytics.orders"},
		ErrorSignature:  "sig-1",
		Attempt:         attempt,
		Status:          proposal.StatusVerifying,
		NodeOutcomes: map[string]proposal.NodeOutcome{
			"analytics.orders": {Status: proposal.StatusVerifying},
		},
		Verifications:     []proposal.Verification{{Service: "svc", Kind: "python", RunID: run, Phase: proposal.PhaseQueued}},
		VerificationRunID: run,
		TriggerPayload:    requestedPayload(),
		Confidence:        proposal.ConfidenceHigh,
		Rationale:         "corrected the declared column type",
		ProposedSQLURI:    "s3://b/proposed.yaml",
		DiffURI:           "s3://b/diff.patch",
		Edits: []proposal.FileEdit{{
			Path:         "contracts/svc.yml",
			ContentURI:   "s3://b/proposed.yaml",
			DiffURI:      "s3://b/diff.patch",
			TargetNodeID: "analytics.orders",
		}},
		SourceResolved: true,
		Model:          "test-model",
		CreatedAt:      testNow.Add(-age),
	}
}

// fakeProposalRepo is an in-memory ProposalRepository holding only what these
// tests exercise: the verifying-row listing and CAS transitions the reconciler
// drives, plus the attempt count and inserts the real ProposeFix takes when
// the reconciler starts the next attempt. Every other method comes from the
// embedded interface, which is nil, so an unexpected call panics loudly
// instead of silently answering with a zero value.
type fakeProposalRepo struct {
	repository.ProposalRepository
	rows       []*proposal.View
	attempts   int
	upserted   []proposal.Proposal
	generating []proposal.Proposal
	listErr    error
	markErr    error
	// phaseWrites records every UpdateVerificationPhase call: which proposal,
	// which run, and the phase it was moved to.
	phaseWrites []struct {
		id, runID string
		phase     proposal.Phase
	}
	// markFailedCalls records every MarkVerifyFailed call's arguments, so a
	// test can inspect the final per-verification summary a failing pass
	// wrote, not just the row's own resulting fields.
	markFailedCalls []struct {
		id            string
		verifyErr     string
		verifications []proposal.Verification
	}
}

func newFakeProposalRepo(views ...proposal.View) *fakeProposalRepo {
	r := &fakeProposalRepo{}
	for i := range views {
		v := views[i]
		r.rows = append(r.rows, &v)
	}
	return r
}

func (r *fakeProposalRepo) row(id string) *proposal.View {
	for _, v := range r.rows {
		if v.ID == id {
			return v
		}
	}
	return nil
}

func (r *fakeProposalRepo) ListVerifying(context.Context) ([]proposal.View, error) {
	if r.listErr != nil {
		return nil, r.listErr
	}
	var out []proposal.View
	for _, v := range r.rows {
		if v.Status == proposal.StatusVerifying {
			out = append(out, *v)
		}
	}
	return out, nil
}

func (r *fakeProposalRepo) MarkVerified(_ context.Context, id string, verifications []proposal.Verification) (bool, error) {
	if r.markErr != nil {
		return false, r.markErr
	}
	v := r.row(id)
	if v == nil || v.Status != proposal.StatusVerifying {
		return false, nil
	}
	v.Status = proposal.StatusProposed
	v.Verifications = verifications
	return true, nil
}

func (r *fakeProposalRepo) MarkVerifyFailed(_ context.Context, id, verifyErr string, verifications []proposal.Verification) (bool, error) {
	if r.markErr != nil {
		return false, r.markErr
	}
	r.markFailedCalls = append(r.markFailedCalls, struct {
		id            string
		verifyErr     string
		verifications []proposal.Verification
	}{id, verifyErr, verifications})
	v := r.row(id)
	if v == nil || v.Status != proposal.StatusVerifying {
		return false, nil
	}
	v.Status = proposal.StatusFailed
	v.VerifyError = verifyErr
	v.Verifications = verifications
	return true, nil
}

// UpdateVerificationPhase records the call and, when the row still exists,
// writes the phase (and activation) onto the matching verification in place —
// the same effect the real UPDATE statement has — so a later pass in the same
// test sees the phase it just recorded.
func (r *fakeProposalRepo) UpdateVerificationPhase(_ context.Context, id, runID string, phase proposal.Phase, activatedAt *time.Time) error {
	r.phaseWrites = append(r.phaseWrites, struct {
		id, runID string
		phase     proposal.Phase
	}{id, runID, phase})
	v := r.row(id)
	if v == nil {
		return nil
	}
	for i := range v.Verifications {
		if v.Verifications[i].RunID == runID {
			v.Verifications[i].Phase = phase
			v.Verifications[i].ActivatedAt = activatedAt
			break
		}
	}
	return nil
}

func (r *fakeProposalRepo) CountAttempts(context.Context, string, int) (int, error) {
	return r.attempts, nil
}

func (r *fakeProposalRepo) InsertGenerating(_ context.Context, p proposal.Proposal) error {
	r.generating = append(r.generating, p)
	v := proposal.View{
		ID: fmt.Sprintf("%s-a%d", p.NodeID, p.Attempt), Source: p.Source,
		ReleaseID: p.ReleaseID, NodeID: p.NodeID, ErrorSignature: p.ErrorSignature,
		Attempt: p.Attempt, Status: p.Status, CreatedAt: p.CreatedAt,
	}
	r.rows = append(r.rows, &v)
	return nil
}

func (r *fakeProposalRepo) FailGenerating(_ context.Context, releaseID, reason string) (int, error) {
	n := 0
	for _, v := range r.rows {
		if v.Status != proposal.StatusGenerating || v.ReleaseID != releaseID {
			continue
		}
		v.Status = proposal.StatusFailed
		v.Rationale = reason
		n++
	}
	return n, nil
}

func (r *fakeProposalRepo) Upsert(_ context.Context, p proposal.Proposal) error {
	r.upserted = append(r.upserted, p)
	return nil
}

// fakeOutbox collects the outbox entries written inside the reconciler's
// transactions. Only Create is exercised.
type fakeOutbox struct {
	outbox.Repository
	entries []*outbox.Entry
}

func (o *fakeOutbox) Create(_ context.Context, e *outbox.Entry) error {
	o.entries = append(o.entries, e)
	return nil
}

// fakeMsgProc is an in-memory message_processing store implementing both
// dedup axes the real table enforces: (message_id, stream_name) and, when
// set, outbox_entry_id. It is what makes a replayed trigger identity
// observable — a claim that collides answers "already processed".
type fakeMsgProc struct {
	messageprocessing.Repository
	byKey    map[string]uuid.UUID
	byOutbox map[uuid.UUID]uuid.UUID
}

func newFakeMsgProc() *fakeMsgProc {
	return &fakeMsgProc{byKey: map[string]uuid.UUID{}, byOutbox: map[uuid.UUID]uuid.UUID{}}
}

func msgKey(messageID, streamName string) string { return messageID + "|" + streamName }

func (m *fakeMsgProc) InsertIfNotExists(_ context.Context, row *messageprocessing.MessageProcessing) (uuid.UUID, bool, error) {
	if id, ok := m.byKey[msgKey(row.MessageID, row.StreamName)]; ok {
		return id, false, nil
	}
	if row.OutboxEntryID != nil {
		if id, ok := m.byOutbox[*row.OutboxEntryID]; ok {
			return id, false, nil
		}
	}
	id := uuid.New()
	m.byKey[msgKey(row.MessageID, row.StreamName)] = id
	if row.OutboxEntryID != nil {
		m.byOutbox[*row.OutboxEntryID] = id
	}
	return id, true, nil
}

func (m *fakeMsgProc) AlreadyProcessed(_ context.Context, messageID, streamName string, outboxEntryID *uuid.UUID) (bool, error) {
	if _, ok := m.byKey[msgKey(messageID, streamName)]; ok {
		return true, nil
	}
	if outboxEntryID != nil {
		if _, ok := m.byOutbox[*outboxEntryID]; ok {
			return true, nil
		}
	}
	return false, nil
}

func (m *fakeMsgProc) GetByID(_ context.Context, id uuid.UUID) (*messageprocessing.MessageProcessing, error) {
	return &messageprocessing.MessageProcessing{ID: id}, nil
}

// fakeUoW satisfies uow.UnitOfWork over the in-memory repositories, counting
// commits so a test can assert a transaction closed rather than rolled back.
type fakeUoW struct {
	pr       *fakeProposalRepo
	ob       *fakeOutbox
	mp       *fakeMsgProc
	commits  int
	beginErr error
}

func (u *fakeUoW) Begin(context.Context) error                         { return u.beginErr }
func (u *fakeUoW) Commit() error                                       { u.commits++; return nil }
func (u *fakeUoW) Rollback() error                                     { return nil }
func (u *fakeUoW) ProposalRepo() repository.ProposalRepository         { return u.pr }
func (u *fakeUoW) OutboxRepo() outbox.Repository                       { return u.ob }
func (u *fakeUoW) MessageProcessingRepo() messageprocessing.Repository { return u.mp }

// fakePipeline scripts one status (or error) per verification run id and
// records the runs it was asked to read, so a test can assert both what the
// reconciler did with a status and that it asked for one at all.
type fakePipeline struct {
	statuses map[string]ports.VerificationStatus
	errs     map[string]error
	reads    []string
}

func newFakePipeline() *fakePipeline {
	return &fakePipeline{statuses: map[string]ports.VerificationStatus{}, errs: map[string]error{}}
}

func (p *fakePipeline) Submit(context.Context, ports.VerificationRequest) error { return nil }

func (p *fakePipeline) Status(_ context.Context, runID string) (ports.VerificationStatus, error) {
	p.reads = append(p.reads, runID)
	if err, ok := p.errs[runID]; ok {
		return ports.VerificationStatus{}, err
	}
	return p.statuses[runID], nil
}

// recordingProposer captures every trigger the reconciler starts a next
// attempt with, and optionally forwards it to a real proposer so a test can
// assert what the driver then did with it.
type recordingProposer struct {
	triggers []handlers.Trigger
	err      error
	delegate FixProposer
}

func (p *recordingProposer) propose(ctx context.Context, t handlers.Trigger) error {
	p.triggers = append(p.triggers, t)
	if p.delegate != nil {
		return p.delegate(ctx, t)
	}
	return p.err
}

// harness wires a Reconciler over the in-memory collaborators above and keeps
// each of them addressable for assertions.
type harness struct {
	repo       *fakeProposalRepo
	pipeline   *fakePipeline
	uow        *fakeUoW
	proposer   *recordingProposer
	reconciler *Reconciler
}

func newHarness(t *testing.T, rows ...proposal.View) *harness {
	t.Helper()
	repo := newFakeProposalRepo(rows...)
	u := &fakeUoW{pr: repo, ob: &fakeOutbox{}, mp: newFakeMsgProc()}
	p := newFakePipeline()
	pr := &recordingProposer{}
	h := &harness{repo: repo, pipeline: p, uow: u, proposer: pr}
	h.reconciler = New(Deps{
		Lister:   repo,
		Pipeline: p,
		NewUoW:   func() uow.UnitOfWork { return u },
		Decode:   rredis.TriggerFromPayload,
		Propose:  pr.propose,
		Clock:    fakeClock{t: testNow},
		Logger:   testLogger(),
		Timeout:  testTimeout,
	})
	return h
}

// seedVerifying adds a proposal awaiting verification to the harness's
// repository, addressing "analytics.orders" and carrying verifications as
// given, aged one minute. It returns the row as recorded, so a test can
// assert against its assigned id.
func (h *harness) seedVerifying(releaseID string, verifications []proposal.Verification) proposal.View {
	v := proposal.View{
		ID:              releaseID + "-p1",
		Source:          "validation",
		ReleaseID:       releaseID,
		NodeID:          "analytics.orders",
		ResolvedNodeIDs: []string{"analytics.orders"},
		ErrorSignature:  "sig-1",
		Attempt:         1,
		Status:          proposal.StatusVerifying,
		NodeOutcomes: map[string]proposal.NodeOutcome{
			"analytics.orders": {Status: proposal.StatusVerifying},
		},
		Verifications:     verifications,
		VerificationRunID: verifications[0].RunID,
		TriggerPayload:    requestedPayload(),
		CreatedAt:         testNow.Add(-time.Minute),
	}
	h.repo.rows = append(h.repo.rows, &v)
	return v
}

// handlerDeps builds the driver dependencies the real ProposeFix runs with
// over this harness's stores. Only the collaborators the attempt-cap path
// touches are wired: at the cap ProposeFix records an escalated row without
// ever reaching a fixer.
func (h *harness) handlerDeps(maxAttempts int) handlers.Deps {
	return handlers.Deps{
		NewUoW:      func() uow.UnitOfWork { return h.uow },
		Clock:       fakeClock{t: testNow},
		Logger:      testLogger(),
		MaxAttempts: maxAttempts,
	}
}

// TestReconcileOnce_PassedVerificationProposesTheFix covers the branch that
// makes a verified fix reachable by a human: a verification run that passed
// finalizes the proposal to 'proposed' and emits exactly one
// remediation.proposed:v1 outbox entry, both in the same transaction. Without
// it a green fix is produced and never surfaces.
func TestReconcileOnce_PassedVerificationProposesTheFix(t *testing.T) {
	row := verifyingRow("p1", 1, time.Minute)
	h := newHarness(t, row)
	h.pipeline.statuses[row.VerificationRunID] = ports.VerificationStatus{Phase: proposal.PhasePassed}

	h.reconciler.ReconcileOnce(context.Background())

	assert.Equal(t, proposal.StatusProposed, h.repo.row("p1").Status)
	require.Len(t, h.uow.ob.entries, 1)
	entry := h.uow.ob.entries[0]
	assert.Equal(t, streams.RemediationProposedV1, entry.StreamName)
	assert.Equal(t, event.EventType, entry.EventType)
	var payload event.RemediationProposed
	require.NoError(t, json.Unmarshal(entry.Payload, &payload))
	assert.Equal(t, "rel-1", payload.ReleaseID)
	assert.Equal(t, "analytics.orders", payload.NodeID)
	assert.Equal(t, []string{"analytics.orders"}, payload.ResolvedNodeIDs)
	assert.Equal(t, 1, payload.Attempt)
	assert.Equal(t, "s3://b/proposed.yaml", payload.ProposedSQLURI)
	require.Len(t, payload.Edits, 1, "the announcement must carry every file the attempt changed")
	assert.Equal(t, "contracts/svc.yml", payload.Edits[0].Path)
	assert.Equal(t, "analytics.orders", payload.Edits[0].TargetNodeID)
	assert.True(t, payload.SourceResolved)
	assert.Equal(t, 1, h.uow.commits, "the status flip and the event must commit together")
	assert.Empty(t, h.proposer.triggers, "a verified fix must not start another attempt")
}

// TestReconcileOnce_PassedVerificationCarriesTheRemediationRound pins that the
// enqueued remediation.proposed:v1 event carries the attempt's own
// remediation round. Without it a round-2 (or later) attempt that passes
// verification would surface with remediation_round dropped to zero, and a UI
// reader that treats 0 as round 1 would misfile it under the wrong round.
func TestReconcileOnce_PassedVerificationCarriesTheRemediationRound(t *testing.T) {
	row := verifyingRow("p1", 1, time.Minute)
	row.RemediationRound = 2
	h := newHarness(t, row)
	h.pipeline.statuses[row.VerificationRunID] = ports.VerificationStatus{Phase: proposal.PhasePassed}

	h.reconciler.ReconcileOnce(context.Background())

	require.Len(t, h.uow.ob.entries, 1)
	var payload event.RemediationProposed
	require.NoError(t, json.Unmarshal(h.uow.ob.entries[0].Payload, &payload))
	assert.Equal(t, 2, payload.RemediationRound)
}

// TestReconcileOnce_PassedVerificationEmitsOnceAcrossTicks pins the CAS: a
// second pass over an already-finalized proposal writes nothing and emits
// nothing, so a repeated tick cannot duplicate the event.
func TestReconcileOnce_PassedVerificationEmitsOnceAcrossTicks(t *testing.T) {
	row := verifyingRow("p1", 1, time.Minute)
	h := newHarness(t, row)
	h.pipeline.statuses[row.VerificationRunID] = ports.VerificationStatus{Phase: proposal.PhasePassed}

	h.reconciler.ReconcileOnce(context.Background())
	h.reconciler.ReconcileOnce(context.Background())

	assert.Len(t, h.uow.ob.entries, 1)
}

// TestReconcileOnce_TwoVerificationsProposesOnlyWhenBothPassed covers the
// attempt whose edits span two services: one verification run per service
// judges them, and the attempt is a proposal only once every one of those
// runs has passed. Resolving on the first passed run would announce a fix
// whose other half is still running — or was about to fail.
func TestReconcileOnce_TwoVerificationsProposesOnlyWhenBothPassed(t *testing.T) {
	row := verifyingRow("p1", 1, time.Minute)
	row.Verifications = []proposal.Verification{
		{Service: "svc", Kind: "dbt", RunID: "verify-rel-1-svc-a1", Phase: proposal.PhaseQueued},
		{Service: "other", Kind: "dbt", RunID: "verify-rel-1-other-a1", Phase: proposal.PhaseQueued},
	}
	h := newHarness(t, row)
	h.pipeline.statuses["verify-rel-1-svc-a1"] = ports.VerificationStatus{Phase: proposal.PhasePassed}
	h.pipeline.statuses["verify-rel-1-other-a1"] = ports.VerificationStatus{Phase: proposal.PhaseRunning, ActivatedAt: testNow}

	h.reconciler.ReconcileOnce(context.Background())

	assert.Equal(t, 2, len(h.pipeline.reads), "every verification run judging the attempt is read once per pass")
	assert.Equal(t, proposal.StatusVerifying, h.repo.row("p1").Status, "one run still running: wait")
	assert.Empty(t, h.uow.ob.entries, "nothing may be announced while a run is still running")

	h.pipeline.statuses["verify-rel-1-other-a1"] = ports.VerificationStatus{Phase: proposal.PhasePassed}

	h.reconciler.ReconcileOnce(context.Background())

	assert.Equal(t, proposal.StatusProposed, h.repo.row("p1").Status)
	require.Len(t, h.uow.ob.entries, 1)
	var payload event.RemediationProposed
	require.NoError(t, json.Unmarshal(h.uow.ob.entries[0].Payload, &payload))
	assert.Equal(t, []string{"analytics.orders"}, payload.ResolvedNodeIDs)
}

// TestReconcileOnce_OneFailedVerificationFailsTheWholeAttempt is the other
// half of the multi-service model: the attempt is one proposal, so a single
// service's verification run failing fails the attempt even while the other
// service's run is still running. The fix cannot be split.
func TestReconcileOnce_OneFailedVerificationFailsTheWholeAttempt(t *testing.T) {
	row := verifyingRow("p1", 1, time.Minute)
	row.Verifications = []proposal.Verification{
		{Service: "svc", Kind: "dbt", RunID: "verify-rel-1-svc-a1", Phase: proposal.PhaseQueued},
		{Service: "other", Kind: "dbt", RunID: "verify-rel-1-other-a1", Phase: proposal.PhaseQueued},
	}
	h := newHarness(t, row)
	h.pipeline.statuses["verify-rel-1-svc-a1"] = ports.VerificationStatus{Phase: proposal.PhaseRunning, ActivatedAt: testNow}
	h.pipeline.statuses["verify-rel-1-other-a1"] = ports.VerificationStatus{
		Phase:      proposal.PhaseFailed,
		NodeErrors: map[string]string{"analytics.orders": "still wrong"},
	}

	h.reconciler.ReconcileOnce(context.Background())

	assert.Equal(t, proposal.StatusFailed, h.repo.row("p1").Status)
	assert.Equal(t, "analytics.orders: still wrong", h.repo.row("p1").VerifyError)
	assert.Len(t, h.proposer.triggers, 1, "a failed attempt starts the next one")
}

// TestReconcileOnce_RowNamingOnlyAVerificationRunIDStillResolves covers the row
// that records no per-service verifications and names its single verification
// run in VerificationRunID alone: that run is the one verification judging
// it, so the row resolves exactly like any other.
func TestReconcileOnce_RowNamingOnlyAVerificationRunIDStillResolves(t *testing.T) {
	row := verifyingRow("p1", 1, time.Minute)
	row.Verifications = nil
	h := newHarness(t, row)
	h.pipeline.statuses[row.VerificationRunID] = ports.VerificationStatus{Phase: proposal.PhasePassed}

	h.reconciler.ReconcileOnce(context.Background())

	assert.Equal(t, 1, len(h.pipeline.reads), "the row's single verification run must be read exactly once")
	assert.Equal(t, proposal.StatusProposed, h.repo.row("p1").Status)
	assert.Len(t, h.uow.ob.entries, 1)
}

// TestReconcileOnce_FailedVerificationFailsTheAttemptAndRetries covers the
// branch that keeps the heal loop alive: a failed verification run records
// the failing node's error on the attempt and immediately starts the next one
// from the stored trigger. Without it the loop dies after one attempt.
func TestReconcileOnce_FailedVerificationFailsTheAttemptAndRetries(t *testing.T) {
	row := verifyingRow("p1", 1, time.Minute)
	h := newHarness(t, row)
	h.pipeline.statuses[row.VerificationRunID] = ports.VerificationStatus{
		Phase:      proposal.PhaseFailed,
		NodeErrors: map[string]string{"analytics.orders": "column total_amount is numeric, contract declares text"},
	}

	h.reconciler.ReconcileOnce(context.Background())

	stored := h.repo.row("p1")
	assert.Equal(t, proposal.StatusFailed, stored.Status)
	assert.Equal(t, "analytics.orders: column total_amount is numeric, contract declares text", stored.VerifyError)
	assert.Empty(t, h.uow.ob.entries, "a failed fix must not be announced as proposed")

	require.Len(t, h.proposer.triggers, 1)
	next := h.proposer.triggers[0]
	assert.Equal(t, "validation", next.Source)
	assert.Equal(t, "rel-1", next.ReleaseID)
	require.Len(t, next.Nodes, 1)
	assert.Equal(t, "analytics.orders", next.Nodes[0].NodeID)
	assert.Equal(t, "sig-1", next.Nodes[0].ErrorSignature)
	assert.Equal(t, "python-model", next.Nodes[0].NodeType)
}

// TestReconcileOnce_FailedVerificationNamesEveryFailingNodeWhenTheFixedOnePassed
// covers the failure whose failing node is not the node being fixed: the
// recorded evidence must name what actually failed instead of leaving the
// attempt with a blank reason.
func TestReconcileOnce_FailedVerificationNamesEveryFailingNodeWhenTheFixedOnePassed(t *testing.T) {
	row := verifyingRow("p1", 1, time.Minute)
	h := newHarness(t, row)
	h.pipeline.statuses[row.VerificationRunID] = ports.VerificationStatus{
		Phase:      proposal.PhaseFailed,
		NodeErrors: map[string]string{"analytics.returns": "relation missing", "analytics.customers": "row count 0"},
	}

	h.reconciler.ReconcileOnce(context.Background())

	assert.Equal(t,
		"analytics.customers: row count 0; analytics.returns: relation missing",
		h.repo.row("p1").VerifyError)
}

// TestReconcileOnce_FailedVerificationWithNoNodeErrorsStillRecordsAReason
// covers a failure release-controller reported no per-node error for: the
// attempt still records why it ended, naming the run that judged it.
func TestReconcileOnce_FailedVerificationWithNoNodeErrorsStillRecordsAReason(t *testing.T) {
	row := verifyingRow("p1", 1, time.Minute)
	h := newHarness(t, row)
	h.pipeline.statuses[row.VerificationRunID] = ports.VerificationStatus{Phase: proposal.PhaseFailed}

	h.reconciler.ReconcileOnce(context.Background())

	assert.Equal(t,
		"verification run "+row.VerificationRunID+" failed without a per-node error",
		h.repo.row("p1").VerifyError)
}

// TestReconcileOnce_FailedVerificationRetriesTheWholeBatch covers the partial
// failure, which is the common shape once one attempt addresses a whole
// failing set: the verification run accepted the fix for some of the
// attempt's nodes and still fails others. The attempt is one proposal over
// one set of edits and it failed as a unit, so the whole batch is retried —
// the edits that did hold up died with the failed attempt, and a retry over
// the still-failing nodes alone would end in a pull request missing them.
// The recorded evidence still names exactly which node failed.
func TestReconcileOnce_FailedVerificationRetriesTheWholeBatch(t *testing.T) {
	row := verifyingRow("p1", 1, time.Minute)
	row.ResolvedNodeIDs = []string{"s.a", "s.b"}
	row.TriggerPayload = batchPayload("s.a", "s.b")
	h := newHarness(t, row)
	h.pipeline.statuses[row.VerificationRunID] = ports.VerificationStatus{
		Phase:      proposal.PhaseFailed,
		NodeErrors: map[string]string{"s.b": "still broken"},
	}

	h.reconciler.ReconcileOnce(context.Background())

	assert.Equal(t, proposal.StatusFailed, h.repo.row("p1").Status)
	assert.Equal(t, "s.b: still broken", h.repo.row("p1").VerifyError,
		"the evidence names the node that actually failed")
	require.Len(t, h.proposer.triggers, 1)
	assert.Equal(t, []string{"s.a", "s.b"}, h.proposer.triggers[0].NodeIDs(),
		"the retry replays the whole batch: a fix that passed is not carried over from a failed attempt")
	assert.Equal(t, "verify-retry:p1", h.proposer.triggers[0].MessageID)
	assert.Nil(t, h.proposer.triggers[0].OutboxEntryID)
}

// TestReconcileOnce_FailedVerificationWithCollateralFailureRetriesEveryNode
// covers the failure that names no node this attempt fixed: every node it
// fixed passed, and something else in the release failed. The whole failing
// set is retried, with the collateral failure recorded as the evidence the
// next attempt reads.
func TestReconcileOnce_FailedVerificationWithCollateralFailureRetriesEveryNode(t *testing.T) {
	row := verifyingRow("p1", 1, time.Minute)
	row.ResolvedNodeIDs = []string{"s.a", "s.b"}
	row.TriggerPayload = batchPayload("s.a", "s.b")
	h := newHarness(t, row)
	h.pipeline.statuses[row.VerificationRunID] = ports.VerificationStatus{
		Phase:      proposal.PhaseFailed,
		NodeErrors: map[string]string{"s.downstream": "relation does not exist"},
	}

	h.reconciler.ReconcileOnce(context.Background())

	assert.Equal(t, proposal.StatusFailed, h.repo.row("p1").Status)
	assert.Equal(t, "s.downstream: relation does not exist", h.repo.row("p1").VerifyError,
		"every fixed node passed, so the recorded reason must name what actually failed")
	require.Len(t, h.proposer.triggers, 1)
	assert.Equal(t, []string{"s.a", "s.b"}, h.proposer.triggers[0].NodeIDs(),
		"no fixed node is implicated, so none may be narrowed away")
}

// TestReconcileOnce_RetryCarriesAFreshDedupIdentity is the regression that
// keeps the retry from being a silent no-op. ProposeFix opens with a dedup
// pre-check keyed on the trigger's message id, and the first attempt's own
// transaction already claimed the inbound message. Replaying the stored
// payload verbatim would hand the driver that same identity, which reports
// "already processed" and returns having done nothing — no attempt, no row,
// no error anywhere. The rebuilt trigger must therefore carry an identity of
// its own, unique to the attempt it follows.
func TestReconcileOnce_RetryCarriesAFreshDedupIdentity(t *testing.T) {
	row := verifyingRow("p1", 1, time.Minute)
	h := newHarness(t, row)
	h.pipeline.statuses[row.VerificationRunID] = ports.VerificationStatus{
		Phase:      proposal.PhaseFailed,
		NodeErrors: map[string]string{"analytics.orders": "still wrong"},
	}
	// The first attempt claimed the inbound remediation.requested message, as
	// its recording transaction does in production.
	upstream := uuid.New()
	_, _, err := h.uow.mp.InsertIfNotExists(context.Background(), &messageprocessing.MessageProcessing{
		MessageID: originalMessageID, StreamName: streams.RemediationRequestedV2, OutboxEntryID: &upstream,
	})
	require.NoError(t, err)

	// The retry runs through the real driver, at the attempt cap so it records
	// an escalated row without ever reaching a fixer. That row is the observable
	// proof the driver actually ran: a dedup hit would return before it.
	h.repo.attempts = 3
	h.proposer.delegate = func(ctx context.Context, tr handlers.Trigger) error {
		return handlers.ProposeFix(ctx, h.handlerDeps(3), tr)
	}

	h.reconciler.ReconcileOnce(context.Background())

	require.Len(t, h.proposer.triggers, 1)
	next := h.proposer.triggers[0]
	assert.Equal(t, "verify-retry:"+row.ID, next.MessageID,
		"the retry must not reuse the Redis message id of the trigger that started the first attempt")
	assert.Nil(t, next.OutboxEntryID,
		"the retry must not reuse the upstream outbox entry id either — it is the second dedup axis")

	require.Len(t, h.repo.upserted, 1, "the driver must have run: a replayed identity would have deduped it away")
	assert.Equal(t, proposal.StatusEscalated, h.repo.upserted[0].Status)
}

// TestProposeFix_ReplayedMessageIDIsANoOp is the control for the regression
// above: it drives the driver with the trigger exactly as the stored payload
// decodes it, keeping the original Redis message id, and shows that this does
// nothing at all. It is what the retry would silently become without a fresh
// identity, and it is why the reconciler mints one.
func TestProposeFix_ReplayedMessageIDIsANoOp(t *testing.T) {
	h := newHarness(t)
	_, _, err := h.uow.mp.InsertIfNotExists(context.Background(), &messageprocessing.MessageProcessing{
		MessageID: originalMessageID, StreamName: streams.RemediationRequestedV2,
	})
	require.NoError(t, err)

	replay, err := rredis.TriggerFromPayload(requestedPayload())
	require.NoError(t, err)
	replay.MessageID = originalMessageID

	h.repo.attempts = 3
	require.NoError(t, handlers.ProposeFix(context.Background(), h.handlerDeps(3), replay))

	assert.Empty(t, h.repo.upserted, "a replayed message id deduplicates the whole attempt away")
}

// TestReconcileOnce_FailedAtTheAttemptCapEscalates covers the branch that
// stops the loop: at the cap the next attempt the reconciler starts records an
// escalated row instead of proposing anything, so an unfixable failure ends
// visibly rather than cycling forever.
func TestReconcileOnce_FailedAtTheAttemptCapEscalates(t *testing.T) {
	row := verifyingRow("p3", 3, time.Minute)
	h := newHarness(t, row)
	h.pipeline.statuses[row.VerificationRunID] = ports.VerificationStatus{
		Phase:      proposal.PhaseFailed,
		NodeErrors: map[string]string{"analytics.orders": "third failure"},
	}
	h.repo.attempts = 3
	h.proposer.delegate = func(ctx context.Context, tr handlers.Trigger) error {
		return handlers.ProposeFix(ctx, h.handlerDeps(3), tr)
	}

	h.reconciler.ReconcileOnce(context.Background())

	assert.Equal(t, proposal.StatusFailed, h.repo.row("p3").Status)
	require.Len(t, h.repo.upserted, 1)
	assert.Equal(t, proposal.StatusEscalated, h.repo.upserted[0].Status)
	assert.Equal(t, 4, h.repo.upserted[0].Attempt)
	assert.Empty(t, h.repo.generating, "no fix may be generated once the cap is reached")
	assert.Empty(t, h.uow.ob.entries, "an escalated attempt announces nothing")
}

// TestReconcileOnce_NonTerminalWithinTheTimeoutIsLeftAlone covers the branch
// that lets a verification run finish: a status that is still queued is read
// and then left untouched, neither finalized nor retried — and, since it
// still reports the phase the row was submitted with, records no phase
// transition either.
func TestReconcileOnce_NonTerminalWithinTheTimeoutIsLeftAlone(t *testing.T) {
	row := verifyingRow("p1", 1, time.Minute)
	h := newHarness(t, row)
	h.pipeline.statuses[row.VerificationRunID] = ports.VerificationStatus{Phase: proposal.PhaseQueued}

	h.reconciler.ReconcileOnce(context.Background())

	assert.Equal(t, 1, len(h.pipeline.reads), "the reconciler must read the status for every verifying row")
	assert.Equal(t, proposal.StatusVerifying, h.repo.row("p1").Status)
	assert.Empty(t, h.repo.row("p1").VerifyError)
	assert.Empty(t, h.proposer.triggers)
	assert.Empty(t, h.uow.ob.entries)
	assert.Zero(t, h.uow.commits)
	assert.Empty(t, h.repo.phaseWrites, "queued → queued records no phase transition")
}

// TestReconcileOnce_NonTerminalPastTheTimeoutFailsTheAttempt covers the branch
// that keeps a wedged verification run from pinning a proposal in 'verifying'
// forever: past the budget the attempt is failed with the timeout as its
// recorded reason and the next attempt starts, exactly as a failure does.
// The budget is spent from the moment the run started running, so the
// status carries an activation that far back.
func TestReconcileOnce_NonTerminalPastTheTimeoutFailsTheAttempt(t *testing.T) {
	row := verifyingRow("p1", 1, testTimeout+time.Minute)
	h := newHarness(t, row)
	h.pipeline.statuses[row.VerificationRunID] = ports.VerificationStatus{
		Phase: proposal.PhaseRunning, ActivatedAt: testNow.Add(-(testTimeout + time.Minute)),
	}

	h.reconciler.ReconcileOnce(context.Background())

	stored := h.repo.row("p1")
	assert.Equal(t, proposal.StatusFailed, stored.Status)
	assert.Equal(t, "verification timed out", stored.VerifyError)
	assert.Len(t, h.proposer.triggers, 1)
}

// TestReconcileOnce_SerialVerificationsAreBudgetedIndividually pins that the
// verification budget is spent per run rather than across the attempt as a
// whole.
//
// Verification runs join one global release queue and run one at a time, so
// an attempt that edited several services waits its runs out one after
// another: the span from the first run's activation to the last one's
// verdict grows with the number of services, while no single run runs any
// longer than it otherwise would. Charging that whole span to one budget
// would fail an attempt whose runs are all perfectly healthy, purely for
// having touched more than one service.
func TestReconcileOnce_SerialVerificationsAreBudgetedIndividually(t *testing.T) {
	row := verifyingRow("p1", 1, testTimeout+time.Hour)
	row.Verifications = []proposal.Verification{
		{Service: "svc", Kind: "dbt", RunID: "verify-rel-1-svc-a1", Phase: proposal.PhaseQueued},
		{Service: "other", Kind: "dbt", RunID: "verify-rel-1-other-a1", Phase: proposal.PhaseQueued},
	}
	h := newHarness(t, row)
	// The first service's run started half an hour ago and has already
	// answered; the second's started five minutes ago and is still running.
	h.pipeline.statuses["verify-rel-1-svc-a1"] = ports.VerificationStatus{
		Phase: proposal.PhasePassed, ActivatedAt: testNow.Add(-30 * time.Minute),
	}
	h.pipeline.statuses["verify-rel-1-other-a1"] = ports.VerificationStatus{
		Phase: proposal.PhaseRunning, ActivatedAt: testNow.Add(-5 * time.Minute),
	}

	h.reconciler.ReconcileOnce(context.Background())

	assert.Equal(t, proposal.StatusVerifying, h.repo.row("p1").Status,
		"only the run still running spends a budget, and it has spent five minutes of its own")
	assert.Empty(t, h.proposer.triggers)
	// The still-running run's transition from queued to running is recorded;
	// the passed run's phase is never written mid-pass — it is carried by the
	// terminal statement once every run has answered.
	require.Len(t, h.repo.phaseWrites, 1)
	assert.Equal(t, proposal.PhaseRunning, h.repo.phaseWrites[0].phase)
	assert.Equal(t, "verify-rel-1-other-a1", h.repo.phaseWrites[0].runID)

	// That run now wedges: on its own activation it is past the budget.
	h.pipeline.statuses["verify-rel-1-other-a1"] = ports.VerificationStatus{
		Phase: proposal.PhaseRunning, ActivatedAt: testNow.Add(-(testTimeout + 5*time.Minute)),
	}

	h.reconciler.ReconcileOnce(context.Background())

	assert.Equal(t, proposal.StatusFailed, h.repo.row("p1").Status)
	assert.Equal(t, "verification timed out", h.repo.row("p1").VerifyError)
	assert.Len(t, h.proposer.triggers, 1)
	// Still running, same phase as already stored: the second pass records no
	// further transition before the timeout finalizes the row.
	assert.Len(t, h.repo.phaseWrites, 1, "no new phase write once the run is already recorded as running")
}

// TestReconcileOnce_QueuedTimeDoesNotSpendTheVerificationBudget pins what the
// timeout is a budget FOR.
//
// A verification run joins the same global FIFO queue as every other release
// and only one runs at a time, so a backlog — or one long normal release —
// can hold it queued for the entire window. Measuring from when the proposal
// was recorded therefore failed an attempt whose run had not run at all, and
// would do the same to every retry behind it, burning the whole per-failure
// budget on runs that were never given a chance to answer.
func TestReconcileOnce_QueuedTimeDoesNotSpendTheVerificationBudget(t *testing.T) {
	t.Run("still queued", func(t *testing.T) {
		row := verifyingRow("p1", 1, testTimeout+time.Hour)
		h := newHarness(t, row)
		// No activation: release-controller has not started this run.
		h.pipeline.statuses[row.VerificationRunID] = ports.VerificationStatus{Phase: proposal.PhaseQueued}

		h.reconciler.ReconcileOnce(context.Background())

		assert.Equal(t, proposal.StatusVerifying, h.repo.row("p1").Status,
			"a run that has not left the queue has not spent any of its verification budget")
		assert.Empty(t, h.proposer.triggers)
		assert.Zero(t, h.uow.commits)
	})

	t.Run("queued long, running briefly", func(t *testing.T) {
		row := verifyingRow("p1", 1, testTimeout+time.Hour)
		h := newHarness(t, row)
		h.pipeline.statuses[row.VerificationRunID] = ports.VerificationStatus{
			Phase: proposal.PhaseRunning, ActivatedAt: testNow.Add(-time.Minute),
		}

		h.reconciler.ReconcileOnce(context.Background())

		assert.Equal(t, proposal.StatusVerifying, h.repo.row("p1").Status,
			"the budget runs from when the run started, not from when the attempt was recorded")
		assert.Empty(t, h.proposer.triggers)
	})
}

// TestReconcileOnce_UnreadableStatusWithinTheTimeoutBurnsNoAttempt pins the
// choice made about a status read that fails: release-controller being
// briefly unreachable and a run id it will never know produce the same
// error, so neither may be counted as a failure. The row is left for the
// next tick.
func TestReconcileOnce_UnreadableStatusWithinTheTimeoutBurnsNoAttempt(t *testing.T) {
	row := verifyingRow("p1", 1, time.Minute)
	h := newHarness(t, row)
	h.pipeline.errs[row.VerificationRunID] = errors.New("get verification run verify-x: status 503: upstream unavailable")

	h.reconciler.ReconcileOnce(context.Background())

	assert.Equal(t, proposal.StatusVerifying, h.repo.row("p1").Status)
	assert.Empty(t, h.proposer.triggers)
	assert.Zero(t, h.uow.commits)
}

// TestReconcileOnce_UnreadableStatusPastTheTimeoutFailsTheAttempt is the
// other half of that choice: waiting on an unreadable status is still
// bounded, so a verification run whose status never becomes readable ends the
// attempt instead of pinning it in 'verifying' forever.
func TestReconcileOnce_UnreadableStatusPastTheTimeoutFailsTheAttempt(t *testing.T) {
	row := verifyingRow("p1", 1, testTimeout+time.Minute)
	h := newHarness(t, row)
	h.pipeline.errs[row.VerificationRunID] = errors.New("get verification run verify-x: status 404: not found")

	h.reconciler.ReconcileOnce(context.Background())

	assert.Equal(t, proposal.StatusFailed, h.repo.row("p1").Status)
	assert.Equal(t, "verification timed out", h.repo.row("p1").VerifyError)
}

// TestReconcileOnce_OneUnresolvableRowDoesNotBlockTheRest pins the
// best-effort loop: a row whose status cannot be read is skipped and the
// rows behind it are still resolved in the same pass.
func TestReconcileOnce_OneUnresolvableRowDoesNotBlockTheRest(t *testing.T) {
	stuck := verifyingRow("p1", 1, time.Minute)
	healthy := verifyingRow("p2", 1, time.Minute)
	healthy.VerificationRunID = "verify-rel-1-other-a1"
	healthy.Verifications = []proposal.Verification{
		{Service: "other", Kind: "python", RunID: healthy.VerificationRunID, Phase: proposal.PhaseQueued},
	}
	h := newHarness(t, stuck, healthy)
	h.pipeline.errs[stuck.VerificationRunID] = errors.New("connection refused")
	h.pipeline.statuses[healthy.VerificationRunID] = ports.VerificationStatus{Phase: proposal.PhasePassed}

	h.reconciler.ReconcileOnce(context.Background())

	assert.Equal(t, proposal.StatusVerifying, h.repo.row("p1").Status)
	assert.Equal(t, proposal.StatusProposed, h.repo.row("p2").Status)
}

// TestReconcileOnce_ListFailureIsSurvived pins that a failed listing ends the
// pass without touching anything and without wedging the loop: the next pass,
// once the store answers again, resolves the row it could not read before.
func TestReconcileOnce_ListFailureIsSurvived(t *testing.T) {
	row := verifyingRow("p1", 1, time.Minute)
	h := newHarness(t, row)
	h.pipeline.statuses[row.VerificationRunID] = ports.VerificationStatus{Phase: proposal.PhasePassed}
	h.repo.listErr = errors.New("db down")

	h.reconciler.ReconcileOnce(context.Background())
	assert.Zero(t, len(h.pipeline.reads))
	assert.Equal(t, proposal.StatusVerifying, h.repo.row("p1").Status)

	h.repo.listErr = nil
	h.reconciler.ReconcileOnce(context.Background())
	assert.Equal(t, proposal.StatusProposed, h.repo.row("p1").Status)
}

// TestReconcileOnce_RowWithoutAStoredTriggerIsFailedButNotRetried pins the
// degenerate row: an attempt whose trigger payload was never stored can still
// be closed out with its reason, but there is nothing to rebuild a next
// attempt from, so none is started.
func TestReconcileOnce_RowWithoutAStoredTriggerIsFailedButNotRetried(t *testing.T) {
	row := verifyingRow("p1", 1, time.Minute)
	row.TriggerPayload = nil
	h := newHarness(t, row)
	h.pipeline.statuses[row.VerificationRunID] = ports.VerificationStatus{
		Phase:      proposal.PhaseFailed,
		NodeErrors: map[string]string{"analytics.orders": "boom"},
	}

	h.reconciler.ReconcileOnce(context.Background())

	assert.Equal(t, proposal.StatusFailed, h.repo.row("p1").Status)
	assert.Equal(t, "analytics.orders: boom", h.repo.row("p1").VerifyError)
	assert.Empty(t, h.proposer.triggers)
}

// TestRunStopsOnContextCancellation pins the loop's shutdown: Run returns when
// its context is cancelled rather than leaking a ticker goroutine.
func TestRunStopsOnContextCancellation(t *testing.T) {
	h := newHarness(t)
	h.reconciler.interval = time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { h.reconciler.Run(ctx); close(done) }()
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after its context was cancelled")
	}
}

// failingArchive is a ports.RepoArchive whose fetch always fails, which is how
// this test makes the real driver commit an in-flight attempt and then return
// an error — the python fixer's first act is to fetch the checkout.
type failingArchive struct{}

func (failingArchive) Fetch(context.Context, string, string) (string, func(), error) {
	return "", nil, errors.New("github unreachable")
}

// TestReconcileOnce_FailedNextAttemptLeavesNoGeneratingRow pins the cleanup a
// retry needs and the stream consumer does not.
//
// The driver commits a 'generating' row of its own, in its own transaction,
// immediately before calling the model — that row is what the release page
// renders as "Generating fix…". When the driver then errors on the consumer's
// path, the message is left unacknowledged and the redelivery reuses that row.
// Nothing redelivers here: this attempt was started by a run's status, not by
// a consumed message, and no sweep exists. So the row would say a fix is
// being generated for as long as the database keeps it.
func TestReconcileOnce_FailedNextAttemptLeavesNoGeneratingRow(t *testing.T) {
	row := verifyingRow("p1", 1, time.Minute)
	h := newHarness(t, row)
	h.pipeline.statuses[row.VerificationRunID] = ports.VerificationStatus{
		Phase:      proposal.PhaseFailed,
		NodeErrors: map[string]string{"analytics.orders": "still wrong"},
	}

	// The real driver, with the repository fetch the python fixer opens with
	// made to fail: it marks the next attempt in flight, then returns.
	deps := h.handlerDeps(5)
	deps.RepoArchive = failingArchive{}
	h.proposer.delegate = func(ctx context.Context, t handlers.Trigger) error {
		return handlers.ProposeFix(ctx, deps, t)
	}

	h.reconciler.ReconcileOnce(context.Background())

	require.Len(t, h.repo.generating, 1,
		"fixture check: the driver must have marked the next attempt in flight")
	for _, v := range h.repo.rows {
		assert.NotEqual(t, proposal.StatusGenerating, v.Status,
			"proposal %s was left in flight with nothing that will ever finish it", v.ID)
	}
}

// TestProposedEventPromotesOnlyTheVerifyingNodeOutcomes pins what the
// announcement says happened to each node the attempt covered. Only the nodes
// that were waiting on verification become proposed; a node the attempt
// already skipped or failed keeps the outcome and reason it was recorded with,
// since no run judged it.
func TestProposedEventPromotesOnlyTheVerifyingNodeOutcomes(t *testing.T) {
	row := verifyingRow("p1", 1, time.Minute)
	row.ResolvedNodeIDs = []string{"s.a", "s.b"}
	row.NodeOutcomes = map[string]proposal.NodeOutcome{
		"s.a": {Status: proposal.StatusVerifying},
		"s.b": {Status: proposal.StatusSkipped, Reason: "no source to fix"},
	}

	got := proposedEvent(row)

	assert.Equal(t, map[string]proposal.NodeOutcome{
		"s.a": {Status: proposal.StatusProposed},
		"s.b": {Status: proposal.StatusSkipped, Reason: "no source to fix"},
	}, got.NodeOutcomes)
	assert.Equal(t, []string{"s.a", "s.b"}, got.ResolvedNodeIDs)
	assert.Equal(t, row.Edits, got.Edits)
	assert.Equal(t, row.Verifications, got.Verifications)
	assert.Equal(t, row.VerificationRunID, got.VerificationRunID)
}

// TestReconcile_RecordsThePhaseOnlyWhenItChanges pins recordPhase's guard: a
// status read that reports the same phase the row already carries writes
// nothing, while a genuine transition is recorded on the matching
// verification.
func TestReconcile_RecordsThePhaseOnlyWhenItChanges(t *testing.T) {
	h := newHarness(t)
	row := h.seedVerifying("rel-1", []proposal.Verification{{Service: "svc", Kind: "dbt", RunID: "verify-rel-1-svc-a1", Phase: proposal.PhaseQueued}})
	h.pipeline.statuses["verify-rel-1-svc-a1"] = ports.VerificationStatus{Phase: proposal.PhaseQueued}
	h.reconciler.ReconcileOnce(context.Background())
	require.Empty(t, h.repo.phaseWrites, "queued → queued writes nothing")

	h.pipeline.statuses["verify-rel-1-svc-a1"] = ports.VerificationStatus{Phase: proposal.PhaseRunning, ActivatedAt: testNow}
	h.reconciler.ReconcileOnce(context.Background())
	require.Len(t, h.repo.phaseWrites, 1)
	assert.Equal(t, proposal.PhaseRunning, h.repo.phaseWrites[0].phase)
	assert.Equal(t, row.ID, h.repo.phaseWrites[0].id)
}

// TestReconcile_FinalizesWithTerminalPhasesAndErrors pins that a multi-service
// attempt's final per-run summary carries each run's own terminal phase and
// error text, in the order the row's verifications were recorded.
func TestReconcile_FinalizesWithTerminalPhasesAndErrors(t *testing.T) {
	h := newHarness(t)
	h.seedVerifying("rel-1", []proposal.Verification{
		{Service: "svc", Kind: "dbt", RunID: "verify-rel-1-svc-a1", Phase: proposal.PhaseRunning},
		{Service: "other", Kind: "dbt", RunID: "verify-rel-1-other-a1", Phase: proposal.PhaseRunning},
	})
	h.pipeline.statuses["verify-rel-1-svc-a1"] = ports.VerificationStatus{Phase: proposal.PhasePassed}
	h.pipeline.statuses["verify-rel-1-other-a1"] = ports.VerificationStatus{Phase: proposal.PhaseFailed, NodeErrors: map[string]string{"model.other.x": "boom"}}
	h.reconciler.ReconcileOnce(context.Background())
	require.Len(t, h.repo.markFailedCalls, 1)
	final := h.repo.markFailedCalls[0].verifications
	require.Len(t, final, 2)
	assert.Equal(t, proposal.PhasePassed, final[0].Phase)
	assert.Equal(t, proposal.PhaseFailed, final[1].Phase)
	assert.Equal(t, "model.other.x: boom", final[1].Error)
}

package shadowverify

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

	"github.com/carolsimone/continuo/pkg/messageprocessing"
	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
	rredis "github.com/carolsimone/continuo/agent-remediation/adapters/redis"
	"github.com/carolsimone/continuo/agent-remediation/domain/event"
	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/domain/repository"
	"github.com/carolsimone/continuo/agent-remediation/service/handlers"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/uow"
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

// verifyingRow is a proposal awaiting its shadow releases' verdicts, aged by
// age relative to testNow. It addresses one failing node, verified by the one
// shadow release the service that node belongs to was submitted as.
func verifyingRow(id string, attempt int, age time.Duration) proposal.View {
	shadow := fmt.Sprintf("shadow-rel-1-svc-a%d", attempt)
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
		Verifications:   []proposal.Verification{{Service: "svc", Kind: "python", ShadowReleaseID: shadow}},
		ShadowReleaseID: shadow,
		TriggerPayload:  requestedPayload(),
		Confidence:      proposal.ConfidenceHigh,
		Rationale:       "corrected the declared column type",
		ProposedSQLURI:  "s3://b/proposed.yaml",
		DiffURI:         "s3://b/diff.patch",
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

func (r *fakeProposalRepo) MarkVerified(_ context.Context, id string) (bool, error) {
	if r.markErr != nil {
		return false, r.markErr
	}
	v := r.row(id)
	if v == nil || v.Status != proposal.StatusVerifying {
		return false, nil
	}
	v.Status = proposal.StatusProposed
	return true, nil
}

func (r *fakeProposalRepo) MarkVerifyFailed(_ context.Context, id, verifyErr string) (bool, error) {
	if r.markErr != nil {
		return false, r.markErr
	}
	v := r.row(id)
	if v == nil || v.Status != proposal.StatusVerifying {
		return false, nil
	}
	v.Status = proposal.StatusFailed
	v.VerifyError = verifyErr
	return true, nil
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

// fakeGateway scripts one verdict (or error) per shadow release id and counts
// the reads, so a test can assert both what the reconciler did with a verdict
// and that it asked for one at all.
type fakeGateway struct {
	verdicts map[string]ports.ShadowVerdict
	errs     map[string]error
	calls    int
}

func newFakeGateway() *fakeGateway {
	return &fakeGateway{verdicts: map[string]ports.ShadowVerdict{}, errs: map[string]error{}}
}

func (g *fakeGateway) Verdict(_ context.Context, releaseID string) (ports.ShadowVerdict, error) {
	g.calls++
	if err, ok := g.errs[releaseID]; ok {
		return ports.ShadowVerdict{}, err
	}
	return g.verdicts[releaseID], nil
}

func (g *fakeGateway) Submit(context.Context, ports.ShadowSubmission) error { return nil }

func (g *fakeGateway) ImageTag(context.Context, string, string) (string, error) { return "tag", nil }

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
	repo     *fakeProposalRepo
	gateway  *fakeGateway
	uow      *fakeUoW
	proposer *recordingProposer
	rec      *Reconciler
}

func newHarness(rows ...proposal.View) *harness {
	repo := newFakeProposalRepo(rows...)
	u := &fakeUoW{pr: repo, ob: &fakeOutbox{}, mp: newFakeMsgProc()}
	g := newFakeGateway()
	p := &recordingProposer{}
	h := &harness{repo: repo, gateway: g, uow: u, proposer: p}
	h.rec = New(Deps{
		Lister:   repo,
		Releases: g,
		NewUoW:   func() uow.UnitOfWork { return u },
		Decode:   rredis.TriggerFromPayload,
		Propose:  p.propose,
		Clock:    fakeClock{t: testNow},
		Logger:   testLogger(),
		Timeout:  testTimeout,
	})
	return h
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

// TestReconcileOnce_ValidatedShadowProposesTheFix covers the branch that makes
// a verified fix reachable by a human: a shadow release that validated
// finalizes the proposal to 'proposed' and emits exactly one
// remediation.proposed:v1 outbox entry, both in the same transaction. Without
// it a green fix is produced and never surfaces.
func TestReconcileOnce_ValidatedShadowProposesTheFix(t *testing.T) {
	row := verifyingRow("p1", 1, time.Minute)
	h := newHarness(row)
	h.gateway.verdicts[row.ShadowReleaseID] = ports.ShadowVerdict{Terminal: true, Validated: true}

	h.rec.ReconcileOnce(context.Background())

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

// TestReconcileOnce_ValidatedShadowCarriesTheRemediationRound pins that the
// enqueued remediation.proposed:v1 event carries the attempt's own
// remediation round. Without it a round-2 (or later) attempt that a shadow
// release validates would surface with remediation_round dropped to zero, and
// a UI reader that treats 0 as round 1 would misfile it under the wrong round.
func TestReconcileOnce_ValidatedShadowCarriesTheRemediationRound(t *testing.T) {
	row := verifyingRow("p1", 1, time.Minute)
	row.RemediationRound = 2
	h := newHarness(row)
	h.gateway.verdicts[row.ShadowReleaseID] = ports.ShadowVerdict{Terminal: true, Validated: true}

	h.rec.ReconcileOnce(context.Background())

	require.Len(t, h.uow.ob.entries, 1)
	var payload event.RemediationProposed
	require.NoError(t, json.Unmarshal(h.uow.ob.entries[0].Payload, &payload))
	assert.Equal(t, 2, payload.RemediationRound)
}

// TestReconcileOnce_ValidatedShadowEmitsOnceAcrossTicks pins the CAS: a second
// pass over an already-finalized proposal writes nothing and emits nothing,
// so a repeated tick cannot duplicate the event.
func TestReconcileOnce_ValidatedShadowEmitsOnceAcrossTicks(t *testing.T) {
	row := verifyingRow("p1", 1, time.Minute)
	h := newHarness(row)
	h.gateway.verdicts[row.ShadowReleaseID] = ports.ShadowVerdict{Terminal: true, Validated: true}

	h.rec.ReconcileOnce(context.Background())
	h.rec.ReconcileOnce(context.Background())

	assert.Len(t, h.uow.ob.entries, 1)
}

// TestReconcileOnce_TwoVerificationsProposesOnlyWhenBothValidated covers the
// attempt whose edits span two services: one shadow release per service judges
// them, and the attempt is a proposal only once every one of those releases has
// validated. Resolving on the first validated release would announce a fix
// whose other half is still running — or was about to be rejected.
func TestReconcileOnce_TwoVerificationsProposesOnlyWhenBothValidated(t *testing.T) {
	row := verifyingRow("p1", 1, time.Minute)
	row.Verifications = []proposal.Verification{
		{Service: "svc", Kind: "dbt", ShadowReleaseID: "shadow-rel-1-svc-a1"},
		{Service: "other", Kind: "dbt", ShadowReleaseID: "shadow-rel-1-other-a1"},
	}
	h := newHarness(row)
	h.gateway.verdicts["shadow-rel-1-svc-a1"] = ports.ShadowVerdict{Terminal: true, Validated: true}
	h.gateway.verdicts["shadow-rel-1-other-a1"] = ports.ShadowVerdict{ActivatedAt: testNow}

	h.rec.ReconcileOnce(context.Background())

	assert.Equal(t, 2, h.gateway.calls, "every shadow release judging the attempt is read once per pass")
	assert.Equal(t, proposal.StatusVerifying, h.repo.row("p1").Status, "one shadow still running: wait")
	assert.Empty(t, h.uow.ob.entries, "nothing may be announced while a shadow release is still running")

	h.gateway.verdicts["shadow-rel-1-other-a1"] = ports.ShadowVerdict{Terminal: true, Validated: true}

	h.rec.ReconcileOnce(context.Background())

	assert.Equal(t, proposal.StatusProposed, h.repo.row("p1").Status)
	require.Len(t, h.uow.ob.entries, 1)
	var payload event.RemediationProposed
	require.NoError(t, json.Unmarshal(h.uow.ob.entries[0].Payload, &payload))
	assert.Equal(t, []string{"analytics.orders"}, payload.ResolvedNodeIDs)
}

// TestReconcileOnce_OneRejectedVerificationFailsTheWholeAttempt is the other
// half of the multi-service model: the attempt is one proposal, so a single
// service's shadow release rejecting it fails the attempt even while the other
// service's release is still running. The fix cannot be split.
func TestReconcileOnce_OneRejectedVerificationFailsTheWholeAttempt(t *testing.T) {
	row := verifyingRow("p1", 1, time.Minute)
	row.Verifications = []proposal.Verification{
		{Service: "svc", Kind: "dbt", ShadowReleaseID: "shadow-rel-1-svc-a1"},
		{Service: "other", Kind: "dbt", ShadowReleaseID: "shadow-rel-1-other-a1"},
	}
	h := newHarness(row)
	h.gateway.verdicts["shadow-rel-1-svc-a1"] = ports.ShadowVerdict{ActivatedAt: testNow}
	h.gateway.verdicts["shadow-rel-1-other-a1"] = ports.ShadowVerdict{
		Terminal:   true,
		NodeErrors: map[string]string{"analytics.orders": "still wrong"},
	}

	h.rec.ReconcileOnce(context.Background())

	assert.Equal(t, proposal.StatusFailed, h.repo.row("p1").Status)
	assert.Equal(t, "analytics.orders: still wrong", h.repo.row("p1").VerifyError)
	assert.Len(t, h.proposer.triggers, 1, "a rejected attempt starts the next one")
}

// TestReconcileOnce_RowNamingOnlyAShadowReleaseIDStillResolves covers the row
// that records no per-service verifications and names its single shadow release
// in ShadowReleaseID alone: that release is the one verification judging it, so
// the row resolves exactly like any other.
func TestReconcileOnce_RowNamingOnlyAShadowReleaseIDStillResolves(t *testing.T) {
	row := verifyingRow("p1", 1, time.Minute)
	row.Verifications = nil
	h := newHarness(row)
	h.gateway.verdicts[row.ShadowReleaseID] = ports.ShadowVerdict{Terminal: true, Validated: true}

	h.rec.ReconcileOnce(context.Background())

	assert.Equal(t, 1, h.gateway.calls, "the row's single shadow release must be read exactly once")
	assert.Equal(t, proposal.StatusProposed, h.repo.row("p1").Status)
	assert.Len(t, h.uow.ob.entries, 1)
}

// TestReconcileOnce_RejectedShadowFailsTheAttemptAndRetries covers the branch
// that keeps the heal loop alive: a rejected shadow records the failing node's
// error on the attempt and immediately starts the next one from the stored
// trigger. Without it the loop dies after one attempt.
func TestReconcileOnce_RejectedShadowFailsTheAttemptAndRetries(t *testing.T) {
	row := verifyingRow("p1", 1, time.Minute)
	h := newHarness(row)
	h.gateway.verdicts[row.ShadowReleaseID] = ports.ShadowVerdict{
		Terminal:   true,
		Validated:  false,
		NodeErrors: map[string]string{"analytics.orders": "column total_amount is numeric, contract declares text"},
	}

	h.rec.ReconcileOnce(context.Background())

	stored := h.repo.row("p1")
	assert.Equal(t, proposal.StatusFailed, stored.Status)
	assert.Equal(t, "analytics.orders: column total_amount is numeric, contract declares text", stored.VerifyError)
	assert.Empty(t, h.uow.ob.entries, "a rejected fix must not be announced as proposed")

	require.Len(t, h.proposer.triggers, 1)
	next := h.proposer.triggers[0]
	assert.Equal(t, "validation", next.Source)
	assert.Equal(t, "rel-1", next.ReleaseID)
	require.Len(t, next.Nodes, 1)
	assert.Equal(t, "analytics.orders", next.Nodes[0].NodeID)
	assert.Equal(t, "sig-1", next.Nodes[0].ErrorSignature)
	assert.Equal(t, "python-model", next.Nodes[0].NodeType)
}

// TestReconcileOnce_RejectedShadowNamesEveryFailingNodeWhenTheFixedOnePassed
// covers the rejection whose failing node is not the node being fixed: the
// recorded evidence must name what actually failed instead of leaving the
// attempt with a blank reason.
func TestReconcileOnce_RejectedShadowNamesEveryFailingNodeWhenTheFixedOnePassed(t *testing.T) {
	row := verifyingRow("p1", 1, time.Minute)
	h := newHarness(row)
	h.gateway.verdicts[row.ShadowReleaseID] = ports.ShadowVerdict{
		Terminal:   true,
		Validated:  false,
		NodeErrors: map[string]string{"analytics.returns": "relation missing", "analytics.customers": "row count 0"},
	}

	h.rec.ReconcileOnce(context.Background())

	assert.Equal(t,
		"analytics.customers: row count 0; analytics.returns: relation missing",
		h.repo.row("p1").VerifyError)
}

// TestReconcileOnce_RejectedShadowWithNoNodeErrorsStillRecordsAReason covers a
// rejection release-controller reported no per-node error for: the attempt
// still records why it ended, naming the release that judged it.
func TestReconcileOnce_RejectedShadowWithNoNodeErrorsStillRecordsAReason(t *testing.T) {
	row := verifyingRow("p1", 1, time.Minute)
	h := newHarness(row)
	h.gateway.verdicts[row.ShadowReleaseID] = ports.ShadowVerdict{Terminal: true, Validated: false}

	h.rec.ReconcileOnce(context.Background())

	assert.Equal(t,
		"shadow release "+row.ShadowReleaseID+" was rejected without a per-node error",
		h.repo.row("p1").VerifyError)
}

// TestReconcileOnce_RejectedShadowRegroupsOnlyTheStillFailingNodes covers the
// partial rejection, which is the common shape once one attempt addresses a
// whole failing set: the shadow release accepted the fix for some of the
// attempt's nodes and still rejects others. Only the nodes it still rejects are
// carried into the next attempt, so the ones already fixed are neither re-fixed
// nor charged another model call.
func TestReconcileOnce_RejectedShadowRegroupsOnlyTheStillFailingNodes(t *testing.T) {
	row := verifyingRow("p1", 1, time.Minute)
	row.ResolvedNodeIDs = []string{"s.a", "s.b"}
	row.TriggerPayload = batchPayload("s.a", "s.b")
	h := newHarness(row)
	h.gateway.verdicts[row.ShadowReleaseID] = ports.ShadowVerdict{
		Terminal:   true,
		NodeErrors: map[string]string{"s.b": "still broken"},
	}

	h.rec.ReconcileOnce(context.Background())

	assert.Equal(t, proposal.StatusFailed, h.repo.row("p1").Status)
	assert.Equal(t, "s.b: still broken", h.repo.row("p1").VerifyError)
	require.Len(t, h.proposer.triggers, 1)
	assert.Equal(t, []string{"s.b"}, h.proposer.triggers[0].NodeIDs(),
		"only the node the shadow still rejects is retried")
	assert.Equal(t, "shadow-verify:p1", h.proposer.triggers[0].MessageID)
}

// TestReconcileOnce_RejectedShadowWithCollateralFailureRetriesEveryNode covers
// the rejection that names no node this attempt fixed: every node it fixed
// passed, and something else in the release failed. There is nothing to narrow
// to — no fixed node is implicated — so the whole failing set is retried, with
// the collateral failure recorded as the evidence the next attempt reads.
func TestReconcileOnce_RejectedShadowWithCollateralFailureRetriesEveryNode(t *testing.T) {
	row := verifyingRow("p1", 1, time.Minute)
	row.ResolvedNodeIDs = []string{"s.a", "s.b"}
	row.TriggerPayload = batchPayload("s.a", "s.b")
	h := newHarness(row)
	h.gateway.verdicts[row.ShadowReleaseID] = ports.ShadowVerdict{
		Terminal:   true,
		NodeErrors: map[string]string{"s.downstream": "relation does not exist"},
	}

	h.rec.ReconcileOnce(context.Background())

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
	h := newHarness(row)
	h.gateway.verdicts[row.ShadowReleaseID] = ports.ShadowVerdict{
		Terminal: true, Validated: false,
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
	// an escalated row without reaching a fixer. That row is the observable
	// proof the driver actually ran: a dedup hit would return before it.
	h.repo.attempts = 3
	h.proposer.delegate = func(ctx context.Context, tr handlers.Trigger) error {
		return handlers.ProposeFix(ctx, h.handlerDeps(3), tr)
	}

	h.rec.ReconcileOnce(context.Background())

	require.Len(t, h.proposer.triggers, 1)
	next := h.proposer.triggers[0]
	assert.Equal(t, "shadow-verify:"+row.ID, next.MessageID,
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
	h := newHarness()
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

// TestReconcileOnce_RejectedAtTheAttemptCapEscalates covers the branch that
// stops the loop: at the cap the next attempt the reconciler starts records an
// escalated row instead of proposing anything, so an unfixable failure ends
// visibly rather than cycling forever.
func TestReconcileOnce_RejectedAtTheAttemptCapEscalates(t *testing.T) {
	row := verifyingRow("p3", 3, time.Minute)
	h := newHarness(row)
	h.gateway.verdicts[row.ShadowReleaseID] = ports.ShadowVerdict{
		Terminal: true, Validated: false,
		NodeErrors: map[string]string{"analytics.orders": "third failure"},
	}
	h.repo.attempts = 3
	h.proposer.delegate = func(ctx context.Context, tr handlers.Trigger) error {
		return handlers.ProposeFix(ctx, h.handlerDeps(3), tr)
	}

	h.rec.ReconcileOnce(context.Background())

	assert.Equal(t, proposal.StatusFailed, h.repo.row("p3").Status)
	require.Len(t, h.repo.upserted, 1)
	assert.Equal(t, proposal.StatusEscalated, h.repo.upserted[0].Status)
	assert.Equal(t, 4, h.repo.upserted[0].Attempt)
	assert.Empty(t, h.repo.generating, "no fix may be generated once the cap is reached")
	assert.Empty(t, h.uow.ob.entries, "an escalated attempt announces nothing")
}

// TestReconcileOnce_NonTerminalWithinTheTimeoutIsLeftAlone covers the branch
// that lets a shadow release finish: a verdict that is still running is read
// and then left untouched, neither finalized nor retried.
func TestReconcileOnce_NonTerminalWithinTheTimeoutIsLeftAlone(t *testing.T) {
	row := verifyingRow("p1", 1, testTimeout-time.Minute)
	h := newHarness(row)
	h.gateway.verdicts[row.ShadowReleaseID] = ports.ShadowVerdict{Terminal: false}

	h.rec.ReconcileOnce(context.Background())

	assert.Equal(t, 1, h.gateway.calls, "the reconciler must read the verdict for every verifying row")
	assert.Equal(t, proposal.StatusVerifying, h.repo.row("p1").Status)
	assert.Empty(t, h.repo.row("p1").VerifyError)
	assert.Empty(t, h.proposer.triggers)
	assert.Empty(t, h.uow.ob.entries)
	assert.Zero(t, h.uow.commits)
}

// TestReconcileOnce_NonTerminalPastTheTimeoutFailsTheAttempt covers the branch
// that keeps a wedged shadow release from pinning a proposal in 'verifying'
// forever: past the budget the attempt is failed with the timeout as its
// recorded reason and the next attempt starts, exactly as a rejection does.
// The budget is spent from the moment the release started running, so the
// verdict carries an activation that far back.
func TestReconcileOnce_NonTerminalPastTheTimeoutFailsTheAttempt(t *testing.T) {
	row := verifyingRow("p1", 1, testTimeout+time.Minute)
	h := newHarness(row)
	h.gateway.verdicts[row.ShadowReleaseID] = ports.ShadowVerdict{
		Terminal: false, ActivatedAt: testNow.Add(-(testTimeout + time.Minute)),
	}

	h.rec.ReconcileOnce(context.Background())

	stored := h.repo.row("p1")
	assert.Equal(t, proposal.StatusFailed, stored.Status)
	assert.Equal(t, "shadow verification timed out", stored.VerifyError)
	assert.Len(t, h.proposer.triggers, 1)
}

// TestReconcileOnce_SerialVerificationsAreBudgetedIndividually pins that the
// verification budget is spent per shadow release rather than across the
// attempt as a whole.
//
// Shadow releases join one global release queue and run one at a time, so an
// attempt that edited several services waits its releases out one after
// another: the span from the first release's activation to the last one's
// verdict grows with the number of services, while no single release runs any
// longer than it otherwise would. Charging that whole span to one budget would
// fail an attempt whose releases are all perfectly healthy, purely for having
// touched more than one service.
func TestReconcileOnce_SerialVerificationsAreBudgetedIndividually(t *testing.T) {
	row := verifyingRow("p1", 1, testTimeout+time.Hour)
	row.Verifications = []proposal.Verification{
		{Service: "svc", Kind: "dbt", ShadowReleaseID: "shadow-rel-1-svc-a1"},
		{Service: "other", Kind: "dbt", ShadowReleaseID: "shadow-rel-1-other-a1"},
	}
	h := newHarness(row)
	// The first service's release started half an hour ago and has already
	// answered; the second's started five minutes ago and is still running.
	h.gateway.verdicts["shadow-rel-1-svc-a1"] = ports.ShadowVerdict{
		Terminal: true, Validated: true, ActivatedAt: testNow.Add(-30 * time.Minute),
	}
	h.gateway.verdicts["shadow-rel-1-other-a1"] = ports.ShadowVerdict{
		ActivatedAt: testNow.Add(-5 * time.Minute),
	}

	h.rec.ReconcileOnce(context.Background())

	assert.Equal(t, proposal.StatusVerifying, h.repo.row("p1").Status,
		"only the release still running spends a budget, and it has spent five minutes of its own")
	assert.Empty(t, h.proposer.triggers)
	assert.Zero(t, h.uow.commits)

	// That release now wedges: on its own activation it is past the budget.
	h.gateway.verdicts["shadow-rel-1-other-a1"] = ports.ShadowVerdict{
		ActivatedAt: testNow.Add(-(testTimeout + 5*time.Minute)),
	}

	h.rec.ReconcileOnce(context.Background())

	assert.Equal(t, proposal.StatusFailed, h.repo.row("p1").Status)
	assert.Equal(t, "shadow verification timed out", h.repo.row("p1").VerifyError)
	assert.Len(t, h.proposer.triggers, 1)
}

// TestReconcileOnce_QueuedTimeDoesNotSpendTheVerificationBudget pins what the
// timeout is a budget FOR.
//
// A shadow release joins the same global FIFO queue as every other release and
// only one release runs at a time, so a backlog — or one long normal release —
// can hold it in 'received' for the entire window. Measuring from when the
// proposal was recorded therefore failed an attempt whose release had not run
// at all, and would do the same to every retry behind it, burning the whole
// per-failure budget on releases that were never given a chance to answer.
func TestReconcileOnce_QueuedTimeDoesNotSpendTheVerificationBudget(t *testing.T) {
	t.Run("still queued", func(t *testing.T) {
		row := verifyingRow("p1", 1, testTimeout+time.Hour)
		h := newHarness(row)
		// No activation: release-controller has not started this release.
		h.gateway.verdicts[row.ShadowReleaseID] = ports.ShadowVerdict{Terminal: false}

		h.rec.ReconcileOnce(context.Background())

		assert.Equal(t, proposal.StatusVerifying, h.repo.row("p1").Status,
			"a release that has not left the queue has not spent any of its verification budget")
		assert.Empty(t, h.proposer.triggers)
		assert.Zero(t, h.uow.commits)
	})

	t.Run("queued long, running briefly", func(t *testing.T) {
		row := verifyingRow("p1", 1, testTimeout+time.Hour)
		h := newHarness(row)
		h.gateway.verdicts[row.ShadowReleaseID] = ports.ShadowVerdict{
			Terminal: false, ActivatedAt: testNow.Add(-time.Minute),
		}

		h.rec.ReconcileOnce(context.Background())

		assert.Equal(t, proposal.StatusVerifying, h.repo.row("p1").Status,
			"the budget runs from when the release started, not from when the attempt was recorded")
		assert.Empty(t, h.proposer.triggers)
	})
}

// TestReconcileOnce_UnreadableVerdictWithinTheTimeoutBurnsNoAttempt pins the
// choice made about a verdict read that fails: release-controller being
// briefly unreachable and a release id it will never know produce the same
// error, so neither may be counted as a rejection. The row is left for the
// next tick.
func TestReconcileOnce_UnreadableVerdictWithinTheTimeoutBurnsNoAttempt(t *testing.T) {
	row := verifyingRow("p1", 1, time.Minute)
	h := newHarness(row)
	h.gateway.errs[row.ShadowReleaseID] = errors.New("get release shadow-x: status 503: upstream unavailable")

	h.rec.ReconcileOnce(context.Background())

	assert.Equal(t, proposal.StatusVerifying, h.repo.row("p1").Status)
	assert.Empty(t, h.proposer.triggers)
	assert.Zero(t, h.uow.commits)
}

// TestReconcileOnce_UnreadableVerdictPastTheTimeoutFailsTheAttempt is the
// other half of that choice: waiting on an unreadable verdict is still
// bounded, so a shadow release whose verdict never becomes readable ends the
// attempt instead of pinning it in 'verifying' forever.
func TestReconcileOnce_UnreadableVerdictPastTheTimeoutFailsTheAttempt(t *testing.T) {
	row := verifyingRow("p1", 1, testTimeout+time.Minute)
	h := newHarness(row)
	h.gateway.errs[row.ShadowReleaseID] = errors.New("get release shadow-x: status 404: not found")

	h.rec.ReconcileOnce(context.Background())

	assert.Equal(t, proposal.StatusFailed, h.repo.row("p1").Status)
	assert.Equal(t, "shadow verification timed out", h.repo.row("p1").VerifyError)
}

// TestReconcileOnce_OneUnresolvableRowDoesNotBlockTheRest pins the
// best-effort loop: a row whose verdict cannot be read is skipped and the
// rows behind it are still resolved in the same pass.
func TestReconcileOnce_OneUnresolvableRowDoesNotBlockTheRest(t *testing.T) {
	stuck := verifyingRow("p1", 1, time.Minute)
	healthy := verifyingRow("p2", 1, time.Minute)
	healthy.ShadowReleaseID = "shadow-rel-1-other-a1"
	healthy.Verifications = []proposal.Verification{
		{Service: "other", Kind: "python", ShadowReleaseID: healthy.ShadowReleaseID},
	}
	h := newHarness(stuck, healthy)
	h.gateway.errs[stuck.ShadowReleaseID] = errors.New("connection refused")
	h.gateway.verdicts[healthy.ShadowReleaseID] = ports.ShadowVerdict{Terminal: true, Validated: true}

	h.rec.ReconcileOnce(context.Background())

	assert.Equal(t, proposal.StatusVerifying, h.repo.row("p1").Status)
	assert.Equal(t, proposal.StatusProposed, h.repo.row("p2").Status)
}

// TestReconcileOnce_ListFailureIsSurvived pins that a failed listing ends the
// pass without touching anything and without wedging the loop: the next pass,
// once the store answers again, resolves the row it could not read before.
func TestReconcileOnce_ListFailureIsSurvived(t *testing.T) {
	row := verifyingRow("p1", 1, time.Minute)
	h := newHarness(row)
	h.gateway.verdicts[row.ShadowReleaseID] = ports.ShadowVerdict{Terminal: true, Validated: true}
	h.repo.listErr = errors.New("db down")

	h.rec.ReconcileOnce(context.Background())
	assert.Zero(t, h.gateway.calls)
	assert.Equal(t, proposal.StatusVerifying, h.repo.row("p1").Status)

	h.repo.listErr = nil
	h.rec.ReconcileOnce(context.Background())
	assert.Equal(t, proposal.StatusProposed, h.repo.row("p1").Status)
}

// TestReconcileOnce_RowWithoutAStoredTriggerIsFailedButNotRetried pins the
// degenerate row: an attempt whose trigger payload was never stored can still
// be closed out with its reason, but there is nothing to rebuild a next
// attempt from, so none is started.
func TestReconcileOnce_RowWithoutAStoredTriggerIsFailedButNotRetried(t *testing.T) {
	row := verifyingRow("p1", 1, time.Minute)
	row.TriggerPayload = nil
	h := newHarness(row)
	h.gateway.verdicts[row.ShadowReleaseID] = ports.ShadowVerdict{
		Terminal: true, Validated: false,
		NodeErrors: map[string]string{"analytics.orders": "boom"},
	}

	h.rec.ReconcileOnce(context.Background())

	assert.Equal(t, proposal.StatusFailed, h.repo.row("p1").Status)
	assert.Equal(t, "analytics.orders: boom", h.repo.row("p1").VerifyError)
	assert.Empty(t, h.proposer.triggers)
}

// TestRunStopsOnContextCancellation pins the loop's shutdown: Run returns when
// its context is cancelled rather than leaking a ticker goroutine.
func TestRunStopsOnContextCancellation(t *testing.T) {
	h := newHarness()
	h.rec.interval = time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { h.rec.Run(ctx); close(done) }()
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
// Nothing redelivers here: this attempt was started by a release's verdict,
// not by a consumed message, and no sweep exists. So the row would say a fix
// is being generated for as long as the database keeps it.
func TestReconcileOnce_FailedNextAttemptLeavesNoGeneratingRow(t *testing.T) {
	row := verifyingRow("p1", 1, time.Minute)
	h := newHarness(row)
	h.gateway.verdicts[row.ShadowReleaseID] = ports.ShadowVerdict{
		Terminal: true, Validated: false,
		NodeErrors: map[string]string{"analytics.orders": "still wrong"},
	}

	// The real driver, with the repository fetch the python fixer opens with
	// made to fail: it marks the next attempt in flight, then returns.
	deps := h.handlerDeps(5)
	deps.RepoArchive = failingArchive{}
	h.proposer.delegate = func(ctx context.Context, t handlers.Trigger) error {
		return handlers.ProposeFix(ctx, deps, t)
	}

	h.rec.ReconcileOnce(context.Background())

	require.Len(t, h.repo.generating, 1,
		"fixture check: the driver must have marked the next attempt in flight")
	for _, v := range h.repo.rows {
		assert.NotEqual(t, proposal.StatusGenerating, v.Status,
			"proposal %s was left in flight with nothing that will ever finish it", v.ID)
	}
}

// TestProposedEventPromotesOnlyTheVerifyingNodeOutcomes pins what the
// announcement says happened to each node the attempt covered. Only the nodes
// that were waiting on a shadow release become proposed; a node the attempt
// already skipped or failed keeps the outcome and reason it was recorded with,
// since no release judged it.
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
	assert.Equal(t, row.ShadowReleaseID, got.ShadowReleaseID)
}

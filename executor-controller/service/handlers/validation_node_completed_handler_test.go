package handlers_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/carolsimone/continuo/executor-controller/domain/command"
	"github.com/carolsimone/continuo/executor-controller/domain/events"
	"github.com/carolsimone/continuo/executor-controller/domain/model"
	"github.com/carolsimone/continuo/executor-controller/service/handlers"
	"github.com/carolsimone/continuo/executor-controller/service/uow"
	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// nodeCompletedDeploymentsRepo is a configurable in-memory DeploymentRepository
// for the validation.node.completed handler tests. It returns byReleaseNode from
// GetByReleaseNode (or sql.ErrNoRows when nil), records Save calls, and serves
// pending/results to the aggregate gate.
type nodeCompletedDeploymentsRepo struct {
	byReleaseNode *model.Deployment
	getErr        error
	pending       int
	results       []*model.Deployment
	saved         []*model.Deployment
}

func (r *nodeCompletedDeploymentsRepo) Add(context.Context, *model.Deployment) error { return nil }
func (r *nodeCompletedDeploymentsRepo) GetDueBatch(context.Context, int) ([]*model.Deployment, error) {
	return nil, nil
}
func (r *nodeCompletedDeploymentsRepo) Save(_ context.Context, d *model.Deployment) error {
	r.saved = append(r.saved, d)
	return nil
}
func (r *nodeCompletedDeploymentsRepo) GetByReleaseNode(context.Context, string, string, model.Mode) (*model.Deployment, error) {
	if r.getErr != nil {
		return nil, r.getErr
	}
	if r.byReleaseNode == nil {
		return nil, sql.ErrNoRows
	}
	return r.byReleaseNode, nil
}
func (r *nodeCompletedDeploymentsRepo) PendingValidationCount(context.Context, string, model.Mode) (int, error) {
	return r.pending, nil
}
func (r *nodeCompletedDeploymentsRepo) ListValidationResults(context.Context, string, model.Mode) ([]*model.Deployment, error) {
	return r.results, nil
}
func (r *nodeCompletedDeploymentsRepo) ListValidationByRelease(context.Context, string, model.Mode) ([]*model.Deployment, error) {
	return nil, nil
}

// chainedDeploymentsRepo is a map-backed DeploymentRepository for gating
// propagation tests. GetByReleaseNode, Save, and ListValidationByRelease all
// operate against the same nodes map, so a Save made before ListValidationByRelease
// is always visible — matching production's within-transaction consistency.
type chainedDeploymentsRepo struct {
	nodes map[string]*model.Deployment // keyed by nodeID
}

func (r *chainedDeploymentsRepo) Add(context.Context, *model.Deployment) error { return nil }
func (r *chainedDeploymentsRepo) GetDueBatch(context.Context, int) ([]*model.Deployment, error) {
	return nil, nil
}
func (r *chainedDeploymentsRepo) Save(_ context.Context, d *model.Deployment) error {
	r.nodes[d.NodeID()] = d
	return nil
}
func (r *chainedDeploymentsRepo) GetByReleaseNode(_ context.Context, _ string, nodeID string, _ model.Mode) (*model.Deployment, error) {
	d, ok := r.nodes[nodeID]
	if !ok {
		return nil, sql.ErrNoRows
	}
	return d, nil
}
func (r *chainedDeploymentsRepo) PendingValidationCount(context.Context, string, model.Mode) (int, error) {
	// For gating tests the aggregate gate should not fire: return a non-zero count
	// so EmitValidationAggregateIfComplete exits early (pending nodes still exist).
	count := 0
	for _, d := range r.nodes {
		if d.Status() == model.StatusPending || d.Status() == model.StatusBlocked || d.Status() == model.StatusDeployed {
			count++
		}
	}
	return count, nil
}
func (r *chainedDeploymentsRepo) ListValidationResults(context.Context, string, model.Mode) ([]*model.Deployment, error) {
	return nil, nil
}
func (r *chainedDeploymentsRepo) ListValidationByRelease(_ context.Context, _ string, _ model.Mode) ([]*model.Deployment, error) {
	out := make([]*model.Deployment, 0, len(r.nodes))
	for _, d := range r.nodes {
		out = append(out, d)
	}
	return out, nil
}

// StatusOf returns the current Status of the node with the given nodeID.
func (r *chainedDeploymentsRepo) StatusOf(nodeID string) model.Status {
	if d, ok := r.nodes[nodeID]; ok {
		return d.Status()
	}
	return ""
}

// chainNode describes one node in a test chain.
type chainNode struct {
	node   string
	status model.Status
	ups    []string // upstream node IDs (in-set)
}

// newFakeUoWWithChain builds a FakeUnitOfWork whose DeploymentsRepo is a
// chainedDeploymentsRepo seeded with the supplied chain. Each chainNode is
// constructed as a validation deployment; the caller-specified status is
// applied by driving the aggregate through the appropriate transitions.
func newFakeUoWWithChain(t *testing.T, releaseID string, chain []chainNode) *fakeChainUoW {
	t.Helper()
	repo := &chainedDeploymentsRepo{nodes: make(map[string]*model.Deployment, len(chain))}
	now := time.Now()
	for _, cn := range chain {
		cmd := command.ValidationDeployTask{
			ReleaseID:       releaseID,
			NodeID:          cn.node,
			ServiceName:     "dbt",
			SchemaName:      "public",
			TableName:       cn.node,
			NodeType:        "dbt-model",
			ImageTag:        "sha-test",
			JobName:         "validate-" + cn.node,
			UpstreamNodeIDs: cn.ups,
		}
		hasUpstreams := len(cn.ups) > 0
		d := model.NewValidationDeployment(cmd, nil, now, hasUpstreams)
		// Drive to the requested status.
		switch cn.status {
		case model.StatusDeployed:
			require.NoError(t, d.MarkDeployed(now), "seed %s to deployed", cn.node)
		case model.StatusBlocked:
			// NewValidationDeployment with hasUpstreams=true already sets blocked.
		case model.StatusPending:
			// Default for hasUpstreams=false.
		case model.StatusSkipped:
			// NewValidationDeployment with hasUpstreams=true starts blocked; Skip
			// transitions it directly to the terminal skipped state.
			require.NoError(t, d.Skip("pre-seeded terminal", now), "seed %s to skipped", cn.node)
		}
		repo.nodes[cn.node] = d
	}
	base := &uow.FakeUnitOfWork{
		Deployments:         repo,
		Outbox:              &fakeOutboxRepo{},
		ValidationAggregate: &fakeAggRepo{won: false},
	}
	return &fakeChainUoW{FakeUnitOfWork: base, repo: repo}
}

// fakeChainUoW wraps FakeUnitOfWork and exposes StatusOf for gating assertions.
type fakeChainUoW struct {
	*uow.FakeUnitOfWork
	repo *chainedDeploymentsRepo
}

// StatusOf returns the live Status of nodeID from the backing map.
func (f *fakeChainUoW) StatusOf(nodeID string) model.Status {
	return f.repo.StatusOf(nodeID)
}

type fakeOutboxRepo struct {
	created []*outbox.Entry
}

func (r *fakeOutboxRepo) Create(_ context.Context, e *outbox.Entry) error {
	r.created = append(r.created, e)
	return nil
}
func (r *fakeOutboxRepo) GetPendingBatch(context.Context, int) ([]*outbox.Entry, error) {
	return nil, nil
}
func (r *fakeOutboxRepo) MarkProcessed(context.Context, uuid.UUID) error        { return nil }
func (r *fakeOutboxRepo) MarkProcessedBatch(context.Context, []uuid.UUID) error { return nil }
func (r *fakeOutboxRepo) MarkFailed(context.Context, uuid.UUID, string) error   { return nil }
func (r *fakeOutboxRepo) IncrementRetry(context.Context, uuid.UUID) error       { return nil }

type fakeAggRepo struct {
	won        bool
	claimCalls int
	lockCalls  int
}

func (r *fakeAggRepo) LockRelease(context.Context, string, model.Mode) error {
	r.lockCalls++
	return nil
}

func (r *fakeAggRepo) ClaimEmission(context.Context, string, model.Mode, time.Time) (bool, error) {
	r.claimCalls++
	return r.won, nil
}

// deployedValidationNode builds a validation deployment in status=deployed (ready
// to receive an outcome) for (releaseID, nodeID).
func deployedValidationNode(t *testing.T, releaseID, nodeID string) *model.Deployment {
	t.Helper()
	cmd := command.ValidationDeployTask{
		ReleaseID: releaseID, NodeID: nodeID, ServiceName: "dbt",
		SchemaName: "public", TableName: "orders", NodeType: "dbt-model",
		ImageTag: "sha-abc", JobName: "validate-" + nodeID,
	}
	now := time.Now()
	d := model.NewValidationDeployment(cmd, nil, now, false)
	require.NoError(t, d.MarkDeployed(now))
	return d
}

func TestValidationNodeCompletedHandler_RecordsOutcomeAndTriggersAggregate(t *testing.T) {
	dep := deployedValidationNode(t, "rel-1", "model.shop.orders")
	depl := &nodeCompletedDeploymentsRepo{
		byReleaseNode: dep,
		pending:       0, // this node is the last terminal one
		results:       []*model.Deployment{dep},
	}
	agg := &fakeAggRepo{won: true}
	outboxRepo := &fakeOutboxRepo{}
	u := &uow.FakeUnitOfWork{Deployments: depl, Outbox: outboxRepo, ValidationAggregate: agg}

	evt := events.ValidationNodeCompleted{
		ReleaseID: "rel-1", NodeID: "model.shop.orders",
		Outcome: "ok", DBTLogURI: "s3://logs/orders",
	}

	h := handlers.NewValidationNodeCompletedHandler(discardLogger())
	require.NoError(t, h.Handle(context.Background(), u, evt, uuid.New()))

	require.Len(t, depl.saved, 1, "outcome saved")
	assert.Equal(t, "ok", depl.saved[0].Outcome())
	assert.Equal(t, "s3://logs/orders", depl.saved[0].DBTLogURI())
	require.NotNil(t, depl.saved[0].OutcomeAt())

	assert.Equal(t, 1, agg.claimCalls, "aggregate gate ran once node is terminal")
	require.Len(t, outboxRepo.created, 1, "validation terminal (kind=complete) emitted")
	assert.Equal(t, streams.ValidationCompletedV1, outboxRepo.created[0].StreamName)
}

func TestValidationNodeCompletedHandler_NoOpAggregateWhileNodesPending(t *testing.T) {
	dep := deployedValidationNode(t, "rel-1", "model.shop.orders")
	depl := &nodeCompletedDeploymentsRepo{
		byReleaseNode: dep,
		pending:       2, // sibling nodes still outstanding
	}
	agg := &fakeAggRepo{won: true}
	outboxRepo := &fakeOutboxRepo{}
	u := &uow.FakeUnitOfWork{Deployments: depl, Outbox: outboxRepo, ValidationAggregate: agg}

	evt := events.ValidationNodeCompleted{ReleaseID: "rel-1", NodeID: "model.shop.orders", Outcome: "ok"}

	h := handlers.NewValidationNodeCompletedHandler(discardLogger())
	require.NoError(t, h.Handle(context.Background(), u, evt, uuid.New()))

	require.Len(t, depl.saved, 1, "this node's outcome still recorded")
	assert.Equal(t, 0, agg.claimCalls, "no aggregate claim while siblings pending")
	assert.Empty(t, outboxRepo.created, "no emission while pending")
}

func TestValidationNodeCompletedHandler_UnknownReleaseNodeIsAcked(t *testing.T) {
	depl := &nodeCompletedDeploymentsRepo{byReleaseNode: nil} // GetByReleaseNode -> sql.ErrNoRows
	agg := &fakeAggRepo{won: true}
	outboxRepo := &fakeOutboxRepo{}
	u := &uow.FakeUnitOfWork{Deployments: depl, Outbox: outboxRepo, ValidationAggregate: agg}

	evt := events.ValidationNodeCompleted{ReleaseID: "rel-unknown", NodeID: "model.x.y", Outcome: "ok"}

	h := handlers.NewValidationNodeCompletedHandler(discardLogger())
	require.NoError(t, h.Handle(context.Background(), u, evt, uuid.New()),
		"unknown (release,node) is ACKed, not errored")

	assert.Empty(t, depl.saved, "nothing recorded for unknown deployment")
	assert.Equal(t, 0, agg.claimCalls)
	assert.Empty(t, outboxRepo.created)
}

func TestValidationNodeCompletedHandler_RedeliveryIsNoOp(t *testing.T) {
	// The deployment already carries a recorded outcome — the message is a
	// redelivery. The handler must ACK (nil) without re-recording or re-emitting.
	dep := deployedValidationNode(t, "rel-1", "model.shop.orders")
	now := time.Now()
	require.NoError(t, dep.RecordOutcome("ok", "s3://logs/orders", "", now))

	depl := &nodeCompletedDeploymentsRepo{
		byReleaseNode: dep,
		pending:       0,
		results:       []*model.Deployment{dep},
	}
	agg := &fakeAggRepo{won: true}
	outboxRepo := &fakeOutboxRepo{}
	u := &uow.FakeUnitOfWork{Deployments: depl, Outbox: outboxRepo, ValidationAggregate: agg}

	evt := events.ValidationNodeCompleted{
		ReleaseID: "rel-1", NodeID: "model.shop.orders", Outcome: "ok", DBTLogURI: "s3://logs/orders",
	}

	h := handlers.NewValidationNodeCompletedHandler(discardLogger())
	require.NoError(t, h.Handle(context.Background(), u, evt, uuid.New()),
		"redelivery is a no-op ACK")

	assert.Empty(t, depl.saved, "no second Save on redelivery")
	assert.Equal(t, 0, agg.claimCalls, "no duplicate aggregate claim on redelivery")
	assert.Empty(t, outboxRepo.created, "no duplicate validation.completed emission")
}

func TestValidationNodeCompleted_UnblocksReadyDownstream(t *testing.T) {
	// Chain: a1 (deployed) → a2 (blocked, ups=[a1]) → a3 (blocked, ups=[a2]).
	// When a1 completes ok, a2's only upstream is now ok so it should be unblocked
	// to pending. a3 still waits on a2 (not yet ok) and must stay blocked.
	h := handlers.NewValidationNodeCompletedHandler(discardLogger())
	u := newFakeUoWWithChain(t, "rel", []chainNode{
		{node: "a1", status: model.StatusDeployed},
		{node: "a2", status: model.StatusBlocked, ups: []string{"a1"}},
		{node: "a3", status: model.StatusBlocked, ups: []string{"a2"}},
	})
	err := h.Handle(context.Background(), u, events.ValidationNodeCompleted{
		ReleaseID: "rel", NodeID: "a1", Outcome: "ok",
	}, uuid.Nil)
	require.NoError(t, err)
	require.Equal(t, model.StatusPending, u.StatusOf("a2"), "a2's only upstream a1 succeeded — should be unblocked")
	require.Equal(t, model.StatusBlocked, u.StatusOf("a3"), "a3 still waits on a2 which is not yet ok")
}

func TestValidationNodeCompleted_SkipsTransitiveDownstreamOnFailure(t *testing.T) {
	// Chain: a1 (deployed) → a2 (blocked, ups=[a1]) → a3 (blocked, ups=[a2]).
	// When a1 fails, a2 and a3 must both be skipped transitively — neither can
	// ever be validated since a2's upstream failed.
	h := handlers.NewValidationNodeCompletedHandler(discardLogger())
	u := newFakeUoWWithChain(t, "rel", []chainNode{
		{node: "a1", status: model.StatusDeployed},
		{node: "a2", status: model.StatusBlocked, ups: []string{"a1"}},
		{node: "a3", status: model.StatusBlocked, ups: []string{"a2"}},
	})
	err := h.Handle(context.Background(), u, events.ValidationNodeCompleted{
		ReleaseID: "rel", NodeID: "a1", Outcome: "failed",
	}, uuid.Nil)
	require.NoError(t, err)
	require.Equal(t, model.StatusSkipped, u.StatusOf("a2"), "a2 must be skipped — upstream a1 failed")
	require.Equal(t, model.StatusSkipped, u.StatusOf("a3"), "a3 must be transitively skipped")
}

func TestValidationNodeCompleted_MultiUpstreamStaysBlockedUntilAllOk(t *testing.T) {
	// Chain: a (deployed), b (deployed), c (blocked, ups=[a,b]).
	// Completing a with "ok" must NOT unblock c because b has no recorded outcome
	// yet. Only after b also completes "ok" should c become pending.
	// This pins the ALL-upstreams semantics: "any upstream ok" would incorrectly
	// unblock c on the first Handle call.
	h := handlers.NewValidationNodeCompletedHandler(discardLogger())
	u := newFakeUoWWithChain(t, "rel-multi", []chainNode{
		{node: "a", status: model.StatusDeployed},
		{node: "b", status: model.StatusDeployed},
		{node: "c", status: model.StatusBlocked, ups: []string{"a", "b"}},
	})

	// First completion: a ok — c must stay blocked because b is not yet ok.
	require.NoError(t, h.Handle(context.Background(), u, events.ValidationNodeCompleted{
		ReleaseID: "rel-multi", NodeID: "a", Outcome: "ok",
	}, uuid.Nil))
	require.Equal(t, model.StatusBlocked, u.StatusOf("c"),
		"c must remain blocked after only a completes ok — b has no outcome yet")

	// Second completion: b ok — now both upstreams are ok so c unblocks to pending.
	require.NoError(t, h.Handle(context.Background(), u, events.ValidationNodeCompleted{
		ReleaseID: "rel-multi", NodeID: "b", Outcome: "ok",
	}, uuid.Nil))
	require.Equal(t, model.StatusPending, u.StatusOf("c"),
		"c must be unblocked to pending once both a and b have outcome ok")
}

func TestValidationNodeCompleted_SkipLeavesAlreadyTerminalDescendantAlone(t *testing.T) {
	// Chain: a1 (deployed) → a2 (blocked, ups=[a1]) → a3 (skipped, ups=[a2]).
	// a3 is already terminal (skipped) before the Handle call.
	// When a1 fails, a2 must become skipped. The propagateGating reachable BFS
	// visits a3 as well, but the Status==StatusBlocked guard must stop it from
	// calling Skip again — Skip rejects a second call from a non-blocked status.
	h := handlers.NewValidationNodeCompletedHandler(discardLogger())
	u := newFakeUoWWithChain(t, "rel-terminal", []chainNode{
		{node: "a1", status: model.StatusDeployed},
		{node: "a2", status: model.StatusBlocked, ups: []string{"a1"}},
		// a3 is seeded as skipped (already terminal) and depends on a2.
		// newFakeUoWWithChain drives it: hasUpstreams=true → blocked → Skip.
		{node: "a3", status: model.StatusSkipped, ups: []string{"a2"}},
	})

	require.NoError(t, h.Handle(context.Background(), u, events.ValidationNodeCompleted{
		ReleaseID: "rel-terminal", NodeID: "a1", Outcome: "failed",
	}, uuid.Nil))

	require.Equal(t, model.StatusSkipped, u.StatusOf("a2"),
		"a2 must be skipped — upstream a1 failed")
	require.Equal(t, model.StatusSkipped, u.StatusOf("a3"),
		"a3 was already terminal (skipped) and must not be re-touched")
}

func TestValidationNodeCompleted_DiamondConvergence(t *testing.T) {
	// Diamond topology: a→b, a→c, b→d, c→d (d has ups=[b,c]).
	// a starts deployed; b, c, d start blocked.
	// When a completes ok:
	//   - b and c each have only one upstream (a) which is now ok → both unblock to pending.
	//   - d has two upstreams (b and c); neither has an outcome yet → d stays blocked.
	// This exercises the seen-set in the BFS reachable walk and the multi-parent
	// index: d must not be unblocked prematurely even though a is transitively above it.
	h := handlers.NewValidationNodeCompletedHandler(discardLogger())
	u := newFakeUoWWithChain(t, "rel-diamond", []chainNode{
		{node: "a", status: model.StatusDeployed},
		{node: "b", status: model.StatusBlocked, ups: []string{"a"}},
		{node: "c", status: model.StatusBlocked, ups: []string{"a"}},
		{node: "d", status: model.StatusBlocked, ups: []string{"b", "c"}},
	})

	require.NoError(t, h.Handle(context.Background(), u, events.ValidationNodeCompleted{
		ReleaseID: "rel-diamond", NodeID: "a", Outcome: "ok",
	}, uuid.Nil))

	require.Equal(t, model.StatusPending, u.StatusOf("b"),
		"b has only one upstream (a) which succeeded — must unblock to pending")
	require.Equal(t, model.StatusPending, u.StatusOf("c"),
		"c has only one upstream (a) which succeeded — must unblock to pending")
	require.Equal(t, model.StatusBlocked, u.StatusOf("d"),
		"d requires both b and c to have outcome ok — must remain blocked")
}

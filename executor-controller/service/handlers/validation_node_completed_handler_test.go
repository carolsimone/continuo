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
func (r *nodeCompletedDeploymentsRepo) GetByReleaseNode(context.Context, string, string) (*model.Deployment, error) {
	if r.getErr != nil {
		return nil, r.getErr
	}
	if r.byReleaseNode == nil {
		return nil, sql.ErrNoRows
	}
	return r.byReleaseNode, nil
}
func (r *nodeCompletedDeploymentsRepo) PendingValidationCount(context.Context, string) (int, error) {
	return r.pending, nil
}
func (r *nodeCompletedDeploymentsRepo) ListValidationResults(context.Context, string) ([]*model.Deployment, error) {
	return r.results, nil
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
func (r *fakeOutboxRepo) MarkProcessed(context.Context, uuid.UUID) error      { return nil }
func (r *fakeOutboxRepo) MarkFailed(context.Context, uuid.UUID, string) error { return nil }
func (r *fakeOutboxRepo) IncrementRetry(context.Context, uuid.UUID) error     { return nil }

type fakeAggRepo struct {
	won        bool
	claimCalls int
}

func (r *fakeAggRepo) ClaimEmission(context.Context, string, time.Time) (bool, error) {
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
	d := model.NewValidationDeployment(cmd, nil, now)
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
	require.Len(t, outboxRepo.created, 1, "validation.completed:v1 emitted")
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
	require.NoError(t, dep.RecordOutcome("ok", "s3://logs/orders", now))

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

package handlers_test

import (
	"context"
	"testing"
	"time"

	"github.com/carolsimone/continuo/executor-controller/domain/command"
	"github.com/carolsimone/continuo/executor-controller/domain/events"
	"github.com/carolsimone/continuo/executor-controller/domain/model"
	"github.com/carolsimone/continuo/executor-controller/service/handlers"
	"github.com/carolsimone/continuo/executor-controller/service/uow"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// deployedSeedBuildNode builds a seed-build deployment in status=deployed (ready
// to receive an outcome) for (releaseID, nodeID).
func deployedSeedBuildNode(t *testing.T, releaseID, nodeID string) *model.Deployment {
	t.Helper()
	cmd := command.ValidationDeployTask{
		ReleaseID: releaseID, NodeID: nodeID, ServiceName: "dbt",
		SchemaName: "public", TableName: nodeID, NodeType: "dbt-seed",
		ImageTag: "sha-seed", JobName: "seed-" + nodeID,
		CandidateSchema: "_candidate_" + releaseID,
	}
	now := time.Now()
	d := model.NewSeedBuildDeployment(cmd, nil, now)
	require.NoError(t, d.MarkDeployed(now))
	return d
}

func TestSeedBuildNodeCompletedHandler_RecordsOutcomeAndTriggersAggregate(t *testing.T) {
	dep := deployedSeedBuildNode(t, "rel-1", "seed.shop.fx")
	depl := &nodeCompletedDeploymentsRepo{
		byReleaseNode: dep,
		pending:       0, // last seed
		results:       []*model.Deployment{dep},
	}
	agg := &fakeAggRepo{won: true}
	outboxRepo := &fakeOutboxRepo{}
	u := &uow.FakeUnitOfWork{Deployments: depl, Outbox: outboxRepo, ValidationAggregate: agg}

	evt := events.SeedBuildNodeCompleted{
		ReleaseID: "rel-1", NodeID: "seed.shop.fx",
		Outcome: "ok", DBTLogURI: "s3://logs/fx",
	}

	h := handlers.NewSeedBuildNodeCompletedHandler(discardLogger())
	require.NoError(t, h.Handle(context.Background(), u, evt, uuid.New()))

	require.Len(t, depl.saved, 1, "outcome saved")
	assert.Equal(t, "ok", depl.saved[0].Outcome())
	assert.Equal(t, "s3://logs/fx", depl.saved[0].DBTLogURI())
	require.NotNil(t, depl.saved[0].OutcomeAt())

	assert.Equal(t, 1, agg.claimCalls, "seed-build aggregate gate ran once seed is terminal")
	require.Len(t, outboxRepo.created, 1, "seed.build.completed:v1 emitted")
}

func TestSeedBuildNodeCompletedHandler_NoOpWhileSeedsPending(t *testing.T) {
	dep := deployedSeedBuildNode(t, "rel-1", "seed.shop.fx")
	depl := &nodeCompletedDeploymentsRepo{
		byReleaseNode: dep,
		pending:       2, // sibling seeds still outstanding
	}
	agg := &fakeAggRepo{won: true}
	outboxRepo := &fakeOutboxRepo{}
	u := &uow.FakeUnitOfWork{Deployments: depl, Outbox: outboxRepo, ValidationAggregate: agg}

	evt := events.SeedBuildNodeCompleted{ReleaseID: "rel-1", NodeID: "seed.shop.fx", Outcome: "ok"}

	h := handlers.NewSeedBuildNodeCompletedHandler(discardLogger())
	require.NoError(t, h.Handle(context.Background(), u, evt, uuid.New()))

	require.Len(t, depl.saved, 1, "this seed's outcome still recorded")
	assert.Equal(t, 0, agg.claimCalls, "no aggregate claim while siblings pending")
	assert.Empty(t, outboxRepo.created, "no emission while pending")
}

func TestSeedBuildNodeCompletedHandler_UnknownReleaseNodeIsAcked(t *testing.T) {
	depl := &nodeCompletedDeploymentsRepo{byReleaseNode: nil} // GetByReleaseNode -> sql.ErrNoRows
	agg := &fakeAggRepo{won: true}
	outboxRepo := &fakeOutboxRepo{}
	u := &uow.FakeUnitOfWork{Deployments: depl, Outbox: outboxRepo, ValidationAggregate: agg}

	evt := events.SeedBuildNodeCompleted{ReleaseID: "rel-unknown", NodeID: "seed.x", Outcome: "ok"}

	h := handlers.NewSeedBuildNodeCompletedHandler(discardLogger())
	require.NoError(t, h.Handle(context.Background(), u, evt, uuid.New()),
		"unknown (release,node) is ACKed, not errored")

	assert.Empty(t, depl.saved)
	assert.Equal(t, 0, agg.claimCalls)
	assert.Empty(t, outboxRepo.created)
}

func TestSeedBuildNodeCompletedHandler_RedeliveryIsNoOp(t *testing.T) {
	dep := deployedSeedBuildNode(t, "rel-1", "seed.shop.fx")
	now := time.Now()
	require.NoError(t, dep.RecordOutcome("ok", "s3://logs/fx", "", "", now))

	depl := &nodeCompletedDeploymentsRepo{
		byReleaseNode: dep,
		pending:       0,
		results:       []*model.Deployment{dep},
	}
	agg := &fakeAggRepo{won: true}
	outboxRepo := &fakeOutboxRepo{}
	u := &uow.FakeUnitOfWork{Deployments: depl, Outbox: outboxRepo, ValidationAggregate: agg}

	evt := events.SeedBuildNodeCompleted{
		ReleaseID: "rel-1", NodeID: "seed.shop.fx", Outcome: "ok", DBTLogURI: "s3://logs/fx",
	}

	h := handlers.NewSeedBuildNodeCompletedHandler(discardLogger())
	require.NoError(t, h.Handle(context.Background(), u, evt, uuid.New()),
		"redelivery is a no-op ACK")

	assert.Empty(t, depl.saved, "no second Save on redelivery")
	assert.Equal(t, 0, agg.claimCalls, "no duplicate aggregate claim on redelivery")
	assert.Empty(t, outboxRepo.created, "no duplicate seed.build.completed emission")
}

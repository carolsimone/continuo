// executor-controller/service/handlers/compile_node_completed_handler_test.go
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

// deployedCompileNode builds a compile deployment in status=deployed (ready
// to receive an outcome) for (releaseID, nodeID).
func deployedCompileNode(t *testing.T, releaseID, nodeID string) *model.Deployment {
	t.Helper()
	cmd := command.ValidationDeployTask{
		ReleaseID: releaseID, NodeID: nodeID, ServiceName: "dbt",
		SchemaName: "public", TableName: nodeID, NodeType: "dbt-model",
		ImageTag: "sha-compile", JobName: "compile-" + nodeID,
		CandidateSchema: "",
	}
	now := time.Now()
	d := model.NewCompileDeployment(cmd, nil, now)
	require.NoError(t, d.MarkDeployed(now))
	return d
}

func TestCompileNodeCompletedHandler_RecordsOutcomeAndTriggersAggregate(t *testing.T) {
	dep := deployedCompileNode(t, "rel-1", "model.shop.fx")
	depl := &nodeCompletedDeploymentsRepo{
		byReleaseNode: dep,
		pending:       0, // single compile node — last (and only) terminal
		results:       []*model.Deployment{dep},
	}
	agg := &fakeAggRepo{won: true}
	outboxRepo := &fakeOutboxRepo{}
	u := &uow.FakeUnitOfWork{Deployments: depl, Outbox: outboxRepo, ValidationAggregate: agg}

	evt := events.CompileNodeCompleted{
		ReleaseID: "rel-1", NodeID: "model.shop.fx",
		Outcome: "ok", DBTLogURI: "s3://logs/fx",
	}

	h := handlers.NewCompileNodeCompletedHandler(discardLogger())
	require.NoError(t, h.Handle(context.Background(), u, evt, uuid.New()))

	require.Len(t, depl.saved, 1, "outcome saved")
	assert.Equal(t, "ok", depl.saved[0].Outcome())
	assert.Equal(t, "s3://logs/fx", depl.saved[0].DBTLogURI())
	require.NotNil(t, depl.saved[0].OutcomeAt())

	assert.Equal(t, 1, agg.claimCalls, "compile aggregate gate ran once compile node is terminal")
	require.Len(t, outboxRepo.created, 1, "compile.completed:v1 emitted")
}

// TestCompileNodeCompletedHandler_RecordsFailedContainer verifies the
// handler threads evt.FailedContainer into RecordOutcome so the saved
// deployment (and, via the aggregate gate, the emitted compile.completed:v1
// per-node entry) carries the failing container's name.
func TestCompileNodeCompletedHandler_RecordsFailedContainer(t *testing.T) {
	dep := deployedCompileNode(t, "rel-3", "model.shop.fx")
	depl := &nodeCompletedDeploymentsRepo{
		byReleaseNode: dep,
		pending:       0,
		results:       []*model.Deployment{dep},
	}
	agg := &fakeAggRepo{won: true}
	outboxRepo := &fakeOutboxRepo{}
	u := &uow.FakeUnitOfWork{Deployments: depl, Outbox: outboxRepo, ValidationAggregate: agg}

	evt := events.CompileNodeCompleted{
		ReleaseID: "rel-3", NodeID: "model.shop.fx",
		Outcome: "failed", DBTLogURI: "s3://logs/fx", FailedContainer: "parse-prod",
	}

	h := handlers.NewCompileNodeCompletedHandler(discardLogger())
	require.NoError(t, h.Handle(context.Background(), u, evt, uuid.New()))

	require.Len(t, depl.saved, 1)
	assert.Equal(t, "parse-prod", depl.saved[0].FailedContainer())
}

func TestCompileNodeCompletedHandler_FailedOutcomeStillEmitsAggregate(t *testing.T) {
	dep := deployedCompileNode(t, "rel-2", "model.shop.fx")
	depl := &nodeCompletedDeploymentsRepo{
		byReleaseNode: dep,
		pending:       0,
		results:       []*model.Deployment{dep},
	}
	agg := &fakeAggRepo{won: true}
	outboxRepo := &fakeOutboxRepo{}
	u := &uow.FakeUnitOfWork{Deployments: depl, Outbox: outboxRepo, ValidationAggregate: agg}

	evt := events.CompileNodeCompleted{
		ReleaseID: "rel-2", NodeID: "model.shop.fx",
		Outcome: "failed", DBTLogURI: "s3://logs/fx",
	}

	h := handlers.NewCompileNodeCompletedHandler(discardLogger())
	require.NoError(t, h.Handle(context.Background(), u, evt, uuid.New()))

	require.Len(t, depl.saved, 1, "outcome saved even on failure")
	assert.Equal(t, "failed", depl.saved[0].Outcome())
	assert.Equal(t, 1, agg.claimCalls, "compile aggregate gate still runs on failure")
	require.Len(t, outboxRepo.created, 1, "compile.completed:v1 emitted with failed status")
}

func TestCompileNodeCompletedHandler_UnknownReleaseNodeIsAcked(t *testing.T) {
	depl := &nodeCompletedDeploymentsRepo{byReleaseNode: nil} // GetByReleaseNode -> sql.ErrNoRows
	agg := &fakeAggRepo{won: true}
	outboxRepo := &fakeOutboxRepo{}
	u := &uow.FakeUnitOfWork{Deployments: depl, Outbox: outboxRepo, ValidationAggregate: agg}

	evt := events.CompileNodeCompleted{ReleaseID: "rel-unknown", NodeID: "model.x", Outcome: "ok"}

	h := handlers.NewCompileNodeCompletedHandler(discardLogger())
	require.NoError(t, h.Handle(context.Background(), u, evt, uuid.New()),
		"unknown (release,node) is ACKed, not errored")

	assert.Empty(t, depl.saved)
	assert.Equal(t, 0, agg.claimCalls)
	assert.Empty(t, outboxRepo.created)
}

func TestCompileNodeCompletedHandler_RedeliveryIsNoOp(t *testing.T) {
	dep := deployedCompileNode(t, "rel-1", "model.shop.fx")
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

	evt := events.CompileNodeCompleted{
		ReleaseID: "rel-1", NodeID: "model.shop.fx", Outcome: "ok", DBTLogURI: "s3://logs/fx",
	}

	h := handlers.NewCompileNodeCompletedHandler(discardLogger())
	require.NoError(t, h.Handle(context.Background(), u, evt, uuid.New()),
		"redelivery is a no-op ACK")

	assert.Empty(t, depl.saved, "no second Save on redelivery")
	assert.Equal(t, 0, agg.claimCalls, "no duplicate aggregate claim on redelivery")
	assert.Empty(t, outboxRepo.created, "no duplicate compile.completed emission")
}

func TestCompileNodeCompletedHandler_CrossModeIsolation(t *testing.T) {
	// A co-existing validation deployment for the same release must not be
	// affected when the compile node settles. The handler looks up by
	// (release_id, node_id, ModeCompile) so it never touches validation rows.
	// Here we just verify the handler only saves the compile deployment and not
	// any validation row (the repo mock already scopes GetByReleaseNode by mode).
	dep := deployedCompileNode(t, "rel-mixed", "compile.node")
	depl := &nodeCompletedDeploymentsRepo{
		byReleaseNode: dep,
		pending:       0,
		results:       []*model.Deployment{dep},
	}
	agg := &fakeAggRepo{won: true}
	outboxRepo := &fakeOutboxRepo{}
	u := &uow.FakeUnitOfWork{Deployments: depl, Outbox: outboxRepo, ValidationAggregate: agg}

	evt := events.CompileNodeCompleted{
		ReleaseID: "rel-mixed", NodeID: "compile.node", Outcome: "ok",
	}
	h := handlers.NewCompileNodeCompletedHandler(discardLogger())
	require.NoError(t, h.Handle(context.Background(), u, evt, uuid.New()))

	// Only the one compile deployment is saved.
	require.Len(t, depl.saved, 1)
	assert.Equal(t, model.ModeCompile, depl.saved[0].Mode())
}

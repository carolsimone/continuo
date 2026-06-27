package handlers_test

import (
	"context"
	"encoding/json"
	"testing"

	domainEvent "github.com/carolsimone/continuo/orchestrator/domain/event"
	domainModel "github.com/carolsimone/continuo/orchestrator/domain/model"
	orchDomain "github.com/carolsimone/continuo/orchestrator/domain"
	"github.com/carolsimone/continuo/orchestrator/service/handlers"
	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newSeedBuildOnPromoteHandler(t *testing.T) (*handlers.SeedBuildOnPromoteHandler, *fakeUnitOfWork) {
	t.Helper()
	uow := newFakeUnitOfWork()
	return handlers.NewSeedBuildOnPromoteHandler(uow, newTestLogger()), uow
}

// queryModelEntries returns all outbox entries written to query.model:v1.
func queryModelEntries(uow *fakeUnitOfWork) []*pkgoutbox.Entry {
	var out []*pkgoutbox.Entry
	for _, e := range uow.outboxRepo.CreatedEntries {
		if e.StreamName == streams.QueryModelV1 {
			out = append(out, e)
		}
	}
	return out
}

// decodeNodeReady deserialises an outbox entry payload into NodeReadyForExecution.
func decodeNodeReady(t *testing.T, e *pkgoutbox.Entry) orchDomain.NodeReadyForExecution {
	t.Helper()
	var ev orchDomain.NodeReadyForExecution
	require.NoError(t, json.Unmarshal(e.Payload, &ev))
	return ev
}

func TestSeedBuildOnPromote_EmitsQueryModelForChangedSeeds(t *testing.T) {
	h, uow := newSeedBuildOnPromoteHandler(t)
	ctx := context.Background()

	in := domainModel.PromoteReleaseInput{
		ReleaseID: "r1",
		Topology: []domainEvent.ReleasePromotedNode{
			{
				UniqueID:    "seed.core.fx",
				NodeType:    "dbt-seed",
				ServiceName: "core",
				SchemaName:  "analytics",
				TableName:   "fx",
				ImageTag:    "t1",
				Changed:     true,
			},
			{
				UniqueID:    "seed.core.old",
				NodeType:    "dbt-seed",
				ServiceName: "core",
				SchemaName:  "analytics",
				TableName:   "old",
				ImageTag:    "t1",
				Changed:     false, // unchanged: skip
			},
			{
				UniqueID:    "model.fin.rep",
				NodeType:    "dbt-model",
				ServiceName: "fin",
				SchemaName:  "fin",
				TableName:   "rep",
				ImageTag:    "t2",
				Changed:     true, // not a seed: skip
			},
		},
	}

	require.NoError(t, h.Handle(ctx, "msg-1", nil, in))

	rows := queryModelEntries(uow)
	require.Len(t, rows, 1)

	ev := decodeNodeReady(t, rows[0])
	assert.Equal(t, "core", ev.ServiceName)
	assert.Equal(t, "fx", ev.TableName)
	assert.Equal(t, "dbt-seed", ev.NodeType)
	assert.Equal(t, "t1", ev.ImageTag)
	// TaskID and ScheduleID must be valid non-empty UUID strings.
	assert.NotEmpty(t, ev.TaskID, "TaskID must be set")
	assert.NotEmpty(t, ev.ScheduleID, "ScheduleID must be set")
	assert.NotEmpty(t, ev.ScheduleName, "ScheduleName must be set")
	assert.NotEmpty(t, ev.JobName, "JobName must be set")
	// EventType on the outbox entry.
	assert.Equal(t, "node_ready_for_execution", rows[0].EventType)
}

func TestSeedBuildOnPromote_NoChangedSeeds_EmitsNothing(t *testing.T) {
	h, uow := newSeedBuildOnPromoteHandler(t)
	ctx := context.Background()

	in := domainModel.PromoteReleaseInput{
		ReleaseID: "r2",
		Topology: []domainEvent.ReleasePromotedNode{
			{UniqueID: "seed.x.y", NodeType: "dbt-seed", ServiceName: "x", SchemaName: "s", TableName: "y", Changed: false},
			{UniqueID: "model.x.z", NodeType: "dbt-model", ServiceName: "x", SchemaName: "s", TableName: "z", Changed: true},
		},
	}

	require.NoError(t, h.Handle(ctx, "msg-2", nil, in))
	rows := queryModelEntries(uow)
	assert.Empty(t, rows, "no query.model entries should be emitted when no changed seeds")
}

func TestSeedBuildOnPromote_MultipleChangedSeeds_EmitsOnePerSeed(t *testing.T) {
	h, uow := newSeedBuildOnPromoteHandler(t)
	ctx := context.Background()

	in := domainModel.PromoteReleaseInput{
		ReleaseID: "r3",
		Topology: []domainEvent.ReleasePromotedNode{
			{UniqueID: "seed.svc.a", NodeType: "dbt-seed", ServiceName: "svc", SchemaName: "sc", TableName: "a", ImageTag: "v1", Changed: true},
			{UniqueID: "seed.svc.b", NodeType: "dbt-seed", ServiceName: "svc", SchemaName: "sc", TableName: "b", ImageTag: "v1", Changed: true},
		},
	}

	require.NoError(t, h.Handle(ctx, "msg-3", nil, in))
	rows := queryModelEntries(uow)
	require.Len(t, rows, 2, "one query.model entry per changed seed")

	tables := map[string]bool{}
	for _, r := range rows {
		ev := decodeNodeReady(t, r)
		tables[ev.TableName] = true
		assert.Equal(t, "dbt-seed", ev.NodeType)
	}
	assert.True(t, tables["a"])
	assert.True(t, tables["b"])
}

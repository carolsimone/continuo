package postgres_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/domain/model"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/identity"
	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/run-controller/adapters/postgres"
	"github.com/carolsimone/continuo/run-controller/adapters/publisher"
	"github.com/carolsimone/continuo/run-controller/domain/aggregate/run"
	"github.com/carolsimone/continuo/run-controller/domain/runplan"
	"github.com/carolsimone/continuo/run-controller/domain/topology"
	"github.com/carolsimone/continuo/run-controller/service/ports"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newPlanningUoW returns a unit of work over db.
func newPlanningUoW(db *sqlx.DB) *postgres.PostgresUnitOfWork {
	logger := discardLogger()
	return postgres.NewPostgresUnitOfWork(db, ports.SystemClock{}, logger, nil)
}

// failureGraph is e2e-schedule-failure: ftable_a and ftable_b feed ftable_c,
// which feeds ftable_d and ftable_e, which feed ftable_f; ftable_a feeds
// ftable_g, which feeds ftable_h.
func failureGraph() topology.Graph {
	tables := []string{"ftable_a", "ftable_b", "ftable_c", "ftable_d", "ftable_e", "ftable_f", "ftable_g", "ftable_h"}
	nodes := make([]topology.Node, 0, len(tables))
	for _, table := range tables {
		nodes = append(nodes, topology.Node{
			Key: topology.KeyOf("e2e_schema", table), ServiceName: "service-1", SchemaName: "e2e_schema",
			TableName: table, NodeType: "dbt-model", ImageTag: "service-1:5", ScheduleName: "e2e-schedule-failure",
		})
	}
	e := func(up, down string) topology.Edge {
		return topology.Edge{Upstream: topology.KeyOf("e2e_schema", up), Downstream: topology.KeyOf("e2e_schema", down)}
	}
	return topology.NewGraph(nodes, []topology.Edge{
		e("ftable_a", "ftable_c"), e("ftable_b", "ftable_c"), e("ftable_c", "ftable_d"), e("ftable_c", "ftable_e"),
		e("ftable_d", "ftable_f"), e("ftable_e", "ftable_f"), e("ftable_a", "ftable_g"), e("ftable_g", "ftable_h"),
	})
}

// newStartRun returns a new cron run on release R5 under a schedule name no
// other test uses, and deletes it with its tasks and outbox rows when the test
// ends.
func newStartRun(t *testing.T, db *sqlx.DB, now time.Time) *run.Run {
	t.Helper()
	r, err := run.NewPendingRun(pkgevents.DefaultTenantID, uuid.New(), "start-"+uuid.NewString()[:8], run.KindCron, nil, identity.SystemUserID,
		model.OperationRun, run.ReleaseStamp{ReleaseID: "R5", PromotionSeq: 12}, now)
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx := context.Background()
		_, _ = db.ExecContext(ctx, `DELETE FROM run_controller_outbox WHERE aggregate_id = $1`, r.RunID())
		_, _ = db.ExecContext(ctx, `DELETE FROM runs WHERE run_id = $1`, r.RunID())
	})
	return r
}

// A run is created, started and saved in one transaction: its row first, then
// its tasks, its plan and one task.dispatch:v1 outbox row per ready task, all
// committed together.
func TestStartedRun_StoresItsTasksPlanAndDispatches(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	now := time.Now().UTC().Truncate(time.Microsecond)
	g := failureGraph()
	plan, err := runplan.WholeDAG{Operation: model.OperationRun}.Select(g)
	require.NoError(t, err)
	r := newStartRun(t, db, now)

	u := newPlanningUoW(db)
	require.NoError(t, u.Begin(ctx))
	require.NoError(t, u.Run().SaveRun(ctx, r))
	events, err := r.Start(ctx, u.TaskCollection(pkgevents.DefaultTenantID), plan, g, testTaskDefaults, now)
	require.NoError(t, err)
	require.NoError(t, u.Run().SaveRun(ctx, r))
	require.NoError(t, u.Outbox().Append(ctx, events, uuid.Nil))
	require.NoError(t, u.Commit())

	var row struct {
		Status    string     `db:"status"`
		Total     int32      `db:"total_task_count"`
		Terminal  int32      `db:"terminal_task_count"`
		PlannedAt *time.Time `db:"planned_at"`
		StartedAt *time.Time `db:"started_at"`
	}
	require.NoError(t, db.Get(&row, `
		SELECT status, total_task_count, terminal_task_count, planned_at, started_at
		FROM runs WHERE run_id = $1`, r.RunID()))
	assert.Equal(t, "running", row.Status)
	assert.Equal(t, int32(8), row.Total)
	assert.Equal(t, int32(0), row.Terminal)
	require.NotNil(t, row.PlannedAt)
	assert.True(t, now.Equal(*row.PlannedAt))
	require.NotNil(t, row.StartedAt)
	assert.True(t, now.Equal(*row.StartedAt))

	var dispatched []string
	require.NoError(t, db.Select(&dispatched, `
		SELECT node_key FROM tasks
		WHERE run_id = $1 AND dispatched_at IS NOT NULL ORDER BY node_key`, r.RunID()))
	assert.Equal(t, []string{"e2e_schema.ftable_a", "e2e_schema.ftable_b"}, dispatched)

	var taskIDs []string
	require.NoError(t, db.Select(&taskIDs, `
		SELECT payload->'payload'->>'task_id' FROM run_controller_outbox
		WHERE aggregate_id = $1 AND stream_name = $2 ORDER BY payload->'payload'->>'table_name'`,
		r.RunID(), streams.TaskDispatchV1))
	assert.Equal(t, []string{
		run.TaskIDFor(pkgevents.DefaultTenantID, r.RunID(), "e2e_schema.ftable_a").String(),
		run.TaskIDFor(pkgevents.DefaultTenantID, r.RunID(), "e2e_schema.ftable_b").String(),
	}, taskIDs)

	loaded, err := newPlanningUoW(db).Run().GetRun(ctx, pkgevents.DefaultTenantID, r.RunID())
	require.NoError(t, err)
	require.NotNil(t, loaded.PlannedAt())
	assert.True(t, now.Equal(*loaded.PlannedAt()))
}

// A task row names its run, so a run must be saved before it is started: tasks
// of a run with no row are refused by the foreign key and the transaction
// stores nothing.
func TestStartedRun_TasksNeedTheRunRowFirst(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	now := time.Now().UTC().Truncate(time.Microsecond)
	g := failureGraph()
	plan, err := runplan.WholeDAG{Operation: model.OperationRun}.Select(g)
	require.NoError(t, err)
	r := newStartRun(t, db, now)

	u := newPlanningUoW(db)
	require.NoError(t, u.Begin(ctx))
	defer u.Rollback()
	_, err = r.Start(ctx, u.TaskCollection(pkgevents.DefaultTenantID), plan, g, testTaskDefaults, now)
	require.Error(t, err)
	assert.Equal(t, "23503", pqCode(t, err))
}

// A TaskDispatched is stored as the task.dispatch:v1 row whose payload is the
// golden fixture's, and which the publisher renders, under the envelope of the
// row, as the fixture's fields.
func TestOutboxPublisher_TaskDispatchedMatchesTheGoldenFixture(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "pkg", "events", "testdata", "task_dispatch_v1.json"))
	require.NoError(t, err)
	var fixture struct {
		Input struct {
			EventID    string                 `json:"event_id"`
			OccurredAt time.Time              `json:"occurred_at"`
			Payload    pkgevents.TaskDispatch `json:"payload"`
		} `json:"input"`
		Fields map[string]json.RawMessage `json:"fields"`
	}
	require.NoError(t, json.Unmarshal(raw, &fixture))
	in := fixture.Input.Payload
	runID := uuid.MustParse(in.RunID)
	t.Cleanup(func() { _, _ = db.ExecContext(ctx, `DELETE FROM run_controller_outbox WHERE aggregate_id = $1`, runID) })

	tx, err := db.BeginTxx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, postgres.NewOutboxPublisher(tx, discardLogger()).Append(ctx, []run.DomainEvent{run.TaskDispatched{
		ID:           runID,
		TenantID:     "default",
		ScheduleName: in.ScheduleName,
		TaskID:       uuid.MustParse(in.TaskID),
		Attempt:      in.Attempt,
		KnownStatus:  run.AttemptStatus(in.KnownStatus),
		Node: topology.Node{
			Key: topology.NodeKey(in.NodeKey), ServiceName: in.ServiceName, SchemaName: in.SchemaName,
			TableName: in.TableName, NodeType: in.NodeType, ImageTag: in.ImageTag, SecretRef: in.SecretRef,
		},
		JobName:   in.JobName,
		Operation: model.Operation(in.Operation),
		Timeout:   time.Duration(in.TimeoutSeconds) * time.Second,
	}}, uuid.Nil))
	require.NoError(t, tx.Commit())

	var stored struct {
		StreamName    string `db:"stream_name"`
		AggregateType string `db:"aggregate_type"`
		EventType     string `db:"event_type"`
		Payload       []byte `db:"payload"`
	}
	require.NoError(t, db.Get(&stored, `
		SELECT stream_name, aggregate_type, event_type, payload FROM run_controller_outbox WHERE aggregate_id = $1`, runID))
	assert.Equal(t, streams.TaskDispatchV1, stored.StreamName)
	assert.Equal(t, "scheduler", stored.AggregateType)
	assert.Equal(t, streams.TaskDispatchV1, stored.EventType)
	tenantID, wire, err := pkgoutbox.UnwrapTenant(stored.Payload)
	require.NoError(t, err)
	assert.Equal(t, "default", tenantID, "the row stores the run's tenant")
	var payload pkgevents.TaskDispatch
	require.NoError(t, json.Unmarshal(wire, &payload))
	assert.Equal(t, in, payload, "the row's payload is the dispatch")

	rendered, err := publisher.NewOutboxPublisher(nil, discardLogger()).Render(&pkgoutbox.Entry{
		ID: uuid.MustParse(fixture.Input.EventID), CreatedAt: fixture.Input.OccurredAt,
		StreamName: stored.StreamName, EventType: stored.EventType, Payload: stored.Payload,
	})
	require.NoError(t, err)
	require.Len(t, rendered, len(fixture.Fields))
	for key, want := range fixture.Fields {
		if key == "payload" {
			var w, g any
			require.NoError(t, json.Unmarshal(want, &w))
			require.NoError(t, json.Unmarshal([]byte(rendered[key].(string)), &g))
			assert.Equal(t, w, g)
			continue
		}
		var w string
		require.NoError(t, json.Unmarshal(want, &w))
		assert.Equal(t, w, rendered[key], key)
	}
}

// testTaskDefaults are the task settings the adapter tests plan with: the
// chart's defaults.
var testTaskDefaults = run.TaskDefaults{MaxRetries: 2, Timeout: 24 * time.Hour}

//go:build integration

package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/domain/repository"
)

// GetRunDetail reads the run of the tenant it is given with every field the
// run reads show; another tenant's run, like an unknown one, is not found.
func TestGetRunDetail_ReadsTheTenantsRun(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	runs := postgres.NewRunRepository(db, discardLogger())
	runID := tenantRunOn(t, db, acmeTenant, "rel-detail", "cancelling")
	// The run's timestamps may not precede its creation.
	at := time.Now().UTC().Add(time.Hour).Truncate(time.Microsecond)
	_, err := db.Exec(`
		UPDATE runs SET kind = 'rebase', operation = 'test', initiated_by = 'ada', failure_reason = 'stalled',
		       started_at = $3, last_progress_at = $3, cancelled_at = $3, cancelled_by = 'grace',
		       cancellation_reason = 'wrong release'
		WHERE tenant_id = $1 AND run_id = $2`, acmeTenant, runID, at)
	require.NoError(t, err)

	got, err := runs.GetRunDetail(ctx, acmeTenant, runID)

	require.NoError(t, err)
	assert.Equal(t, runID, got.RunID)
	assert.Equal(t, "rebase", got.Kind)
	assert.Equal(t, "test", got.Operation)
	assert.Equal(t, run.RunStatusCancelling, got.Status)
	assert.Equal(t, "stalled", got.FailureReason)
	assert.Equal(t, "rel-detail", got.ReleaseID)
	assert.Equal(t, int64(1), got.PromotionSeq)
	assert.Equal(t, "ada", got.InitiatedBy)
	require.NotNil(t, got.StartedAt)
	assert.True(t, at.Equal(*got.StartedAt))
	require.NotNil(t, got.LastProgressAt)
	assert.True(t, at.Equal(*got.LastProgressAt))
	require.NotNil(t, got.CancelledAt)
	assert.True(t, at.Equal(*got.CancelledAt))
	assert.Equal(t, "grace", got.CancelledBy)
	assert.Equal(t, "wrong release", got.CancellationReason)
	assert.Nil(t, got.CompletedAt)

	_, err = runs.GetRunDetail(ctx, pkgevents.DefaultTenantID, runID)
	assert.ErrorIs(t, err, repository.ErrRunNotFound, "another tenant's run")
	_, err = runs.GetRunDetail(ctx, acmeTenant, uuid.New())
	assert.ErrorIs(t, err, repository.ErrRunNotFound, "an unknown run")
}

// ListRunTasks pages the tenant's run newest first with the wait fields, and
// an other tenant's run has no tasks.
func TestListRunTasks_ReadsTheTenantsTasks(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	tasks := postgres.NewTaskReadRepository(postgres.NewTaskCollection(db, discardLogger()))
	runID := tenantRun(t, db, acmeTenant, "running")
	table := "detail_" + uuid.NewString()[:8]
	since := time.Date(2026, 10, 27, 9, 30, 0, 0, time.UTC)
	waiter := waitingTask(t, db, acmeTenant, runID, table, since)
	_, err := db.Exec(`UPDATE tasks SET attempt = 1 WHERE task_id = $1`, waiter)
	require.NoError(t, err)
	succeeded := tenantTask(t, db, acmeTenant, runID, table+"_ok", run.TaskStatusSucceeded)

	all, total, err := tasks.ListRunTasks(ctx, acmeTenant, runID, nil, 50, 0)
	require.NoError(t, err)
	assert.Equal(t, 2, total)
	require.Len(t, all, 2)
	byID := map[uuid.UUID]int{all[0].TaskID: 0, all[1].TaskID: 1}
	w := all[byID[waiter]]
	assert.Equal(t, runID, w.RunID)
	assert.Equal(t, table, w.TableName)
	assert.Equal(t, run.TaskStatusPending, w.Status)
	assert.Equal(t, 1, w.Attempt)
	assert.Equal(t, 2, w.MaxRetries)
	assert.Equal(t, run.WaitReasonNodeLease, w.WaitReason)
	require.NotNil(t, w.WaitingSince)
	assert.True(t, since.Equal(*w.WaitingSince))
	assert.Empty(t, all[byID[succeeded]].WaitReason)

	ok := run.TaskStatusSucceeded
	filtered, total, err := tasks.ListRunTasks(ctx, acmeTenant, runID, &ok, 50, 0)
	require.NoError(t, err)
	assert.Equal(t, 1, total)
	require.Len(t, filtered, 1)
	assert.Equal(t, succeeded, filtered[0].TaskID)

	other, total, err := tasks.ListRunTasks(ctx, pkgevents.DefaultTenantID, runID, nil, 50, 0)
	require.NoError(t, err)
	assert.Zero(t, total)
	assert.Empty(t, other)
}

// The node reads read the tenant they are given.
func TestNodeReadRepository_ReadsTheTenantsNodes(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	nodes := postgres.NewNodeReadRepository(func(tenantID string) postgres.NodeRunRepository {
		return postgres.NewNodeRunRepository(db, tenantID, discardLogger())
	})
	runID := tenantRun(t, db, acmeTenant, "running")
	table := "nodes_" + uuid.NewString()[:8]
	tenantTask(t, db, acmeTenant, runID, table, run.TaskStatusRunning)

	names, err := nodes.ListNodeNames(ctx, acmeTenant, "service-1")
	require.NoError(t, err)
	assert.Contains(t, names, table)
	names, err = nodes.ListNodeNames(ctx, pkgevents.DefaultTenantID, "service-1")
	require.NoError(t, err)
	assert.NotContains(t, names, table)

	history, err := nodes.ListNodeTasks(ctx, acmeTenant, "service-1", "e2e_schema", table, "", 0)
	require.NoError(t, err)
	assert.Len(t, history, 1)
	history, err = nodes.ListNodeTasks(ctx, pkgevents.DefaultTenantID, "service-1", "e2e_schema", table, "", 0)
	require.NoError(t, err)
	assert.Empty(t, history)

	services, err := nodes.ListNodeServices(ctx, acmeTenant)
	require.NoError(t, err)
	assert.Contains(t, services, "service-1")

	catalog, total, err := nodes.ListNodes(ctx, acmeTenant, table, "", "", 10, 0)
	require.NoError(t, err)
	assert.Equal(t, 1, total)
	require.Len(t, catalog, 1)
	assert.Equal(t, table, catalog[0].TableName)
}

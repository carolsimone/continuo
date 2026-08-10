package postgres_test

import (
	"context"
	"testing"

	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/google/uuid"
)

// TestTaskExecution_RunResultsURI_RoundTrip persists an execution carrying the
// structured-result key and reads it back through both surfaces that expose it:
// the task_execution repository and the per-node run-history join. A column
// missing from any one of the repository's four SQL statements fails here.
func TestTaskExecution_RunResultsURI_RoundTrip(t *testing.T) {
	db := newTestDB(t)
	execRepo := postgres.NewTaskExecutionRepository(db, discardLogger())
	nodeRepo := postgres.NewNodeRunRepository(db, discardLogger())
	ctx := context.Background()

	const key = "run-results/task-executions/svc-py/analytics/pyrr/e.json"

	sid, tid := uuid.New(), uuid.New()
	if _, err := db.Exec(`INSERT INTO scheduler_tracker
		(schedule_id, schedule_name, status, created_at, initialization_status, operation)
		VALUES ($1,'rr-node','succeeded',now(),'completed','run')`, sid); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Exec(`DELETE FROM scheduler_tracker WHERE schedule_id = $1`, sid) })
	if _, err := db.Exec(`INSERT INTO task_tracker
		(task_id, schedule_id, created_at, service_name, schema_name, table_name, job_name, status, max_retries, operation)
		VALUES ($1,$2,now(),'svc-py','analytics','pyrr',$3,'succeeded',3,'run')`,
		tid, sid, "j-"+tid.String()); err != nil {
		t.Fatal(err)
	}

	withKey := &postgres.TaskExecution{ID: uuid.New(), TaskID: tid, RunResultsURI: strPtr(key)}
	if err := execRepo.Create(ctx, withKey); err != nil {
		t.Fatalf("create execution: %v", err)
	}

	got, err := execRepo.GetByID(ctx, withKey.ID)
	if err != nil {
		t.Fatalf("get execution: %v", err)
	}
	if got.RunResultsURI == nil || *got.RunResultsURI != key {
		t.Fatalf("GetByID RunResultsURI = %v, want %q", got.RunResultsURI, key)
	}

	runs, err := nodeRepo.List(ctx, "svc-py", "analytics", "pyrr", "run", 50)
	if err != nil {
		t.Fatalf("list node runs: %v", err)
	}
	if len(runs) != 1 {
		t.Fatalf("node runs = %d, want 1", len(runs))
	}
	if runs[0].RunResultsURI == nil || *runs[0].RunResultsURI != key {
		t.Fatalf("node run RunResultsURI = %v, want %q — the run-history join must expose it",
			runs[0].RunResultsURI, key)
	}
}

// TestTaskExecution_RunResultsURI_NilWhenAbsent verifies an execution whose
// container printed no result block — every dbt execution — reads back as nil
// rather than an empty string.
func TestTaskExecution_RunResultsURI_NilWhenAbsent(t *testing.T) {
	db := newTestDB(t)
	execRepo := postgres.NewTaskExecutionRepository(db, discardLogger())
	ctx := context.Background()

	sid, tid := uuid.New(), uuid.New()
	if _, err := db.Exec(`INSERT INTO scheduler_tracker
		(schedule_id, schedule_name, status, created_at, initialization_status, operation)
		VALUES ($1,'rr-none','succeeded',now(),'completed','run')`, sid); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Exec(`DELETE FROM scheduler_tracker WHERE schedule_id = $1`, sid) })
	if _, err := db.Exec(`INSERT INTO task_tracker
		(task_id, schedule_id, created_at, service_name, schema_name, table_name, job_name, status, max_retries, operation)
		VALUES ($1,$2,now(),'service-1','analytics','dbtrr',$3,'succeeded',3,'run')`,
		tid, sid, "j-"+tid.String()); err != nil {
		t.Fatal(err)
	}

	row := &postgres.TaskExecution{ID: uuid.New(), TaskID: tid}
	if err := execRepo.Create(ctx, row); err != nil {
		t.Fatalf("create execution: %v", err)
	}

	got, err := execRepo.GetByID(ctx, row.ID)
	if err != nil {
		t.Fatalf("get execution: %v", err)
	}
	if got.RunResultsURI != nil {
		t.Fatalf("RunResultsURI = %q, want nil for an execution with no result block", *got.RunResultsURI)
	}
}

func strPtr(s string) *string { return &s }

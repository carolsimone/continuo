package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/carolsimone/continuo/state/adapters/postgres"
	"github.com/google/uuid"
)

// TestListNodes_FiltersByOperation seeds one node identity with both a model
// run (operation=run) and a test run (operation=test) and asserts ListNodes
// scopes its catalog stats to the requested operation: querying "test"
// returns only the test-run slice, querying "run" returns only the model-run
// slice.
func TestListNodes_FiltersByOperation(t *testing.T) {
	db := newTestDB(t)
	repo := postgres.NewNodeRunRepository(db, discardLogger())
	ctx := context.Background()

	seed := func(op, taskStatus string) {
		sid, tid := uuid.New(), uuid.New()
		if _, err := db.Exec(`INSERT INTO scheduler_tracker
			(schedule_id, schedule_name, status, created_at, initialization_status, operation)
			VALUES ($1,'op-node','succeeded',now(),'completed',$2)`, sid, op); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`INSERT INTO task_tracker
			(task_id, schedule_id, created_at, service_name, schema_name, table_name, job_name, status, max_retries, operation)
			VALUES ($1,$2,now(),'svc','analytics','opnode',$3,$4,3,$5)`,
			tid, sid, "j-"+tid.String(), taskStatus, op); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { db.Exec(`DELETE FROM scheduler_tracker WHERE schedule_id = $1`, sid) })
	}
	seed("run", "succeeded")
	seed("test", "failed")
	time.Sleep(10 * time.Millisecond)

	testRows, _, err := repo.ListNodes(ctx, "opnode", "", "test", 50, 0)
	if err != nil {
		t.Fatalf("ListNodes test: %v", err)
	}
	if len(testRows) != 1 || testRows[0].LastStatus != "failed" {
		t.Fatalf("test slice = %+v, want 1 failed row", testRows)
	}
	runRows, _, err := repo.ListNodes(ctx, "opnode", "", "run", 50, 0)
	if err != nil {
		t.Fatalf("ListNodes run: %v", err)
	}
	if len(runRows) != 1 || runRows[0].LastStatus != "succeeded" {
		t.Fatalf("run slice = %+v, want 1 succeeded row", runRows)
	}
}

// TestListNodeRuns_FiltersByOperation seeds a single test-operation task and
// asserts List scopes the per-node run-history query by operation: querying
// "test" returns the row, querying "run" (the default dimension) returns
// nothing for the same node identity.
func TestListNodeRuns_FiltersByOperation(t *testing.T) {
	db := newTestDB(t)
	repo := postgres.NewNodeRunRepository(db, discardLogger())
	ctx := context.Background()

	sid, tid := uuid.New(), uuid.New()
	if _, err := db.Exec(`INSERT INTO scheduler_tracker
		(schedule_id, schedule_name, status, created_at, initialization_status, operation)
		VALUES ($1,'op-runs','succeeded',now(),'completed','test')`, sid); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO task_tracker
		(task_id, schedule_id, created_at, service_name, schema_name, table_name, job_name, status, max_retries, operation)
		VALUES ($1,$2,now(),'svc','analytics','opruns',$3,'failed',3,'test')`,
		tid, sid, "j-"+tid.String()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Exec(`DELETE FROM scheduler_tracker WHERE schedule_id = $1`, sid) })

	testRuns, err := repo.List(ctx, "svc", "analytics", "opruns", "test", 50)
	if err != nil {
		t.Fatalf("List test: %v", err)
	}
	if len(testRuns) != 1 || testRuns[0].Operation != "test" {
		t.Fatalf("test runs = %+v, want 1 test row", testRuns)
	}
	runRuns, err := repo.List(ctx, "svc", "analytics", "opruns", "run", 50)
	if err != nil {
		t.Fatalf("List run: %v", err)
	}
	if len(runRuns) != 0 {
		t.Fatalf("run runs = %+v, want empty", runRuns)
	}
}

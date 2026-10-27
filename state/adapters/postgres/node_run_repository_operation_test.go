package postgres_test

import (
	"context"
	"testing"
	"time"

	pkgevents "github.com/carolsimone/continuo/pkg/events"
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
	repo := postgres.NewNodeRunRepository(db, pkgevents.DefaultTenantID, discardLogger())
	ctx := context.Background()

	seed := func(op, taskStatus string) {
		sid, tid := uuid.New(), uuid.New()
		if _, err := db.Exec(`INSERT INTO runs
			(tenant_id, run_id, schedule_name, status, created_at, operation)
			VALUES ('default', $1,'op-node','succeeded',now(),$2)`, sid, op); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`INSERT INTO tasks
			(tenant_id, task_id, run_id, created_at, service_name, schema_name, table_name, job_name, status, max_retries, operation)
			VALUES ((SELECT tenant_id FROM runs WHERE run_id = $2), $1,$2,now(),'svc','analytics','opnode',$3,$4,3,$5)`,
			tid, sid, "j-"+tid.String(), taskStatus, op); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { db.Exec(`DELETE FROM runs WHERE run_id = $1`, sid) })
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
	repo := postgres.NewNodeRunRepository(db, pkgevents.DefaultTenantID, discardLogger())
	ctx := context.Background()

	sid, tid := uuid.New(), uuid.New()
	if _, err := db.Exec(`INSERT INTO runs
		(tenant_id, run_id, schedule_name, status, created_at, operation)
		VALUES ('default', $1,'op-runs','succeeded',now(),'test')`, sid); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO tasks
		(tenant_id, task_id, run_id, created_at, service_name, schema_name, table_name, job_name, status, max_retries, operation)
		VALUES ((SELECT tenant_id FROM runs WHERE run_id = $2), $1,$2,now(),'svc','analytics','opruns',$3,'failed',3,'test')`,
		tid, sid, "j-"+tid.String()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Exec(`DELETE FROM runs WHERE run_id = $1`, sid) })

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

// TestListNodes_FoldsFullRefreshIntoRun seeds a model run and a later full
// refresh of the same node and asserts the catalog still shows ONE run-row for
// the node, whose stats span both and whose last status is the full refresh's.
func TestListNodes_FoldsFullRefreshIntoRun(t *testing.T) {
	db := newTestDB(t)
	repo := postgres.NewNodeRunRepository(db, pkgevents.DefaultTenantID, discardLogger())
	ctx := context.Background()

	seed := func(op, taskStatus string) {
		sid, tid := uuid.New(), uuid.New()
		if _, err := db.Exec(`INSERT INTO runs
			(tenant_id, run_id, schedule_name, status, created_at, operation)
			VALUES ('default', $1,'fr-node','succeeded',now(),$2)`, sid, op); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`INSERT INTO tasks
			(tenant_id, task_id, run_id, created_at, service_name, schema_name, table_name, job_name, status, max_retries, operation)
			VALUES ((SELECT tenant_id FROM runs WHERE run_id = $2), $1,$2,now(),'svc','analytics','frnode',$3,$4,3,$5)`,
			tid, sid, "j-"+tid.String(), taskStatus, op); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { db.Exec(`DELETE FROM runs WHERE run_id = $1`, sid) })
	}
	seed("run", "succeeded")
	time.Sleep(10 * time.Millisecond)
	seed("full_refresh", "failed")

	rows, total, err := repo.ListNodes(ctx, "frnode", "", "run", 50, 0)
	if err != nil {
		t.Fatalf("ListNodes: %v", err)
	}
	if len(rows) != 1 || total != 1 {
		t.Fatalf("rows=%+v total=%d, want exactly one run row", rows, total)
	}
	if rows[0].RunCount != 2 || rows[0].LastStatus != "failed" || rows[0].Operation != "run" {
		t.Fatalf("row = %+v, want run_count=2 last_status=failed operation=run", rows[0])
	}
}

// TestListNodeRuns_RunIncludesFullRefresh asserts the default run history of a
// node lists its full refreshes, each carrying its own operation.
func TestListNodeRuns_RunIncludesFullRefresh(t *testing.T) {
	db := newTestDB(t)
	repo := postgres.NewNodeRunRepository(db, pkgevents.DefaultTenantID, discardLogger())
	ctx := context.Background()

	sid, tid := uuid.New(), uuid.New()
	if _, err := db.Exec(`INSERT INTO runs
		(tenant_id, run_id, schedule_name, status, created_at, operation)
		VALUES ('default', $1,'fr-runs','succeeded',now(),'full_refresh')`, sid); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO tasks
		(tenant_id, task_id, run_id, created_at, service_name, schema_name, table_name, job_name, status, max_retries, operation)
		VALUES ((SELECT tenant_id FROM runs WHERE run_id = $2), $1,$2,now(),'svc','analytics','frruns',$3,'succeeded',3,'full_refresh')`,
		tid, sid, "j-"+tid.String()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Exec(`DELETE FROM runs WHERE run_id = $1`, sid) })

	runs, err := repo.List(ctx, "svc", "analytics", "frruns", "run", 50)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(runs) != 1 || runs[0].Operation != "full_refresh" {
		t.Fatalf("runs = %+v, want one row with operation=full_refresh", runs)
	}
	testRuns, err := repo.List(ctx, "svc", "analytics", "frruns", "test", 50)
	if err != nil {
		t.Fatalf("List test: %v", err)
	}
	if len(testRuns) != 0 {
		t.Fatalf("test history = %+v, want empty", testRuns)
	}
}

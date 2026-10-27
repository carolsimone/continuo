package postgres_test

import (
	"testing"

	"github.com/google/uuid"
)

// TestOperationColumns_DefaultAndCheck verifies the V28 columns: a row inserted
// without an operation defaults to 'run', and a value outside the domain is
// rejected by the CHECK constraint.
func TestOperationColumns_DefaultAndCheck(t *testing.T) {
	db := newTestDB(t)

	id := uuid.New()
	_, err := db.Exec(`
		INSERT INTO runs (tenant_id, run_id, schedule_name, status, created_at, initialization_status)
		VALUES ('default', $1, 'op-default', 'pending', now(), 'in_progress')`, id)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	t.Cleanup(func() { db.Exec(`DELETE FROM runs WHERE run_id = $1`, id) })

	var op string
	if err := db.Get(&op, `SELECT operation FROM runs WHERE run_id = $1`, id); err != nil {
		t.Fatalf("select: %v", err)
	}
	if op != "run" {
		t.Fatalf("default operation = %q, want run", op)
	}

	bad := uuid.New()
	_, err = db.Exec(`
		INSERT INTO runs (tenant_id, run_id, schedule_name, status, created_at, initialization_status, operation)
		VALUES ('default', $1, 'op-bad', 'pending', now(), 'in_progress', 'nonsense')`, bad)
	if err == nil {
		db.Exec(`DELETE FROM runs WHERE run_id = $1`, bad)
		t.Fatal("expected CHECK violation for operation='nonsense', got nil")
	}

	// tasks carries its own operation column (denormalized from its run),
	// so verify its default and CHECK independently — a copy-paste slip on the
	// second ALTER would otherwise go undetected. tasks has NOT NULL
	// columns and a FK to runs(run_id), so reuse the parent
	// row inserted above.
	taskDefault := uuid.New()
	_, err = db.Exec(`
		INSERT INTO tasks (tenant_id, task_id, run_id, created_at, service_name, schema_name, table_name, job_name, status, max_retries)
		VALUES ((SELECT tenant_id FROM runs WHERE run_id = $2), $1, $2, now(), 'svc', 'schema', 'tbl', 'job-op-default', 'pending', 3)`, taskDefault, id)
	if err != nil {
		t.Fatalf("insert tasks: %v", err)
	}

	var taskOp string
	if err := db.Get(&taskOp, `SELECT operation FROM tasks WHERE task_id = $1`, taskDefault); err != nil {
		t.Fatalf("select tasks: %v", err)
	}
	if taskOp != "run" {
		t.Fatalf("tasks default operation = %q, want run", taskOp)
	}

	badTask := uuid.New()
	_, err = db.Exec(`
		INSERT INTO tasks (tenant_id, task_id, run_id, created_at, service_name, schema_name, table_name, job_name, status, max_retries, operation)
		VALUES ((SELECT tenant_id FROM runs WHERE run_id = $2), $1, $2, now(), 'svc', 'schema', 'tbl', 'job-op-bad', 'pending', 3, 'nonsense')`, badTask, id)
	if err == nil {
		db.Exec(`DELETE FROM tasks WHERE task_id = $1`, badTask)
		t.Fatal("expected CHECK violation for tasks operation='nonsense', got nil")
	}
}

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
		INSERT INTO scheduler_tracker (schedule_id, schedule_name, status, created_at, initialization_status)
		VALUES ($1, 'op-default', 'pending', now(), 'in_progress')`, id)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	t.Cleanup(func() { db.Exec(`DELETE FROM scheduler_tracker WHERE schedule_id = $1`, id) })

	var op string
	if err := db.Get(&op, `SELECT operation FROM scheduler_tracker WHERE schedule_id = $1`, id); err != nil {
		t.Fatalf("select: %v", err)
	}
	if op != "run" {
		t.Fatalf("default operation = %q, want run", op)
	}

	bad := uuid.New()
	_, err = db.Exec(`
		INSERT INTO scheduler_tracker (schedule_id, schedule_name, status, created_at, initialization_status, operation)
		VALUES ($1, 'op-bad', 'pending', now(), 'in_progress', 'nonsense')`, bad)
	if err == nil {
		db.Exec(`DELETE FROM scheduler_tracker WHERE schedule_id = $1`, bad)
		t.Fatal("expected CHECK violation for operation='nonsense', got nil")
	}
}

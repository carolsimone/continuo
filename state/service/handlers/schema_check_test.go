package handlers

import (
	"context"
	"errors"
	"strings"
	"testing"
)

type fakeSchemaColumns struct {
	missing []string
	err     error
	asked   []string
}

func (f *fakeSchemaColumns) Missing(_ context.Context, want []string) ([]string, error) {
	f.asked = want
	return f.missing, f.err
}

func TestCheckSchema_PassesWhenEveryRequiredColumnExists(t *testing.T) {
	cols := &fakeSchemaColumns{}
	if err := CheckSchema(context.Background(), cols); err != nil {
		t.Fatalf("err = %v", err)
	}
	if strings.Join(cols.asked, ",") != strings.Join(RequiredColumns, ",") {
		t.Fatalf("asked for %v, want %v", cols.asked, RequiredColumns)
	}
}

func TestCheckSchema_RefusesWhileAColumnIsMissing(t *testing.T) {
	err := CheckSchema(context.Background(), &fakeSchemaColumns{missing: []string{"task_attempts.attempt"}})
	if !errors.Is(err, ErrSchemaNotReady) {
		t.Fatalf("err = %v, want ErrSchemaNotReady", err)
	}
	if !strings.Contains(err.Error(), "task_attempts.attempt") {
		t.Fatalf("err = %q, want the missing column named", err)
	}
}

func TestCheckSchema_ReadFailureIsNotAMissingSchema(t *testing.T) {
	err := CheckSchema(context.Background(), &fakeSchemaColumns{err: errors.New("connection refused")})
	if err == nil || errors.Is(err, ErrSchemaNotReady) {
		t.Fatalf("err = %v, want a read error", err)
	}
}

// state refuses to start until every run table whose keys lead with tenant_id
// has the column, and the command requests table exists.
func TestCheckSchema_RequiresTheTenantColumns(t *testing.T) {
	required := map[string]bool{}
	for _, c := range RequiredColumns {
		required[c] = true
	}
	for _, c := range []string{"runs.tenant_id", "tasks.tenant_id", "task_attempts.tenant_id", "command_requests.request_id"} {
		if !required[c] {
			t.Errorf("RequiredColumns lacks %s", c)
		}
	}
}

// state refuses to start until the schedules table has every column and the
// runs table has the slot a scheduled run records.
func TestCheckSchema_RequiresTheSchedulesColumns(t *testing.T) {
	required := map[string]bool{}
	for _, c := range RequiredColumns {
		required[c] = true
	}
	for _, c := range []string{"schedules.tenant_id", "schedules.name", "schedules.cron", "schedules.timezone",
		"schedules.description", "schedules.paused", "schedules.next_fire_at", "schedules.version",
		"schedules.updated_by", "schedules.updated_at", "runs.scheduled_for"} {
		if !required[c] {
			t.Errorf("RequiredColumns lacks %s", c)
		}
	}
}

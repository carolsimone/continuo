package run_test

import (
	"testing"
	"time"

	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/google/uuid"
)

func TestNewPendingRun_RecordsRunStarted(t *testing.T) {
	now := time.Date(2026, 5, 16, 10, 0, 0, 0, time.UTC)
	meta := map[string]run.ServiceMetadata{
		"orders": {ManifestVersion: "v1", ImageTag: "abc"},
	}

	r, evt, err := run.NewPendingRun("daily_orders", run.KindCron, nil, meta, now)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if r.ScheduleName() != "daily_orders" {
		t.Fatalf("schedule_name: got %q want %q", r.ScheduleName(), "daily_orders")
	}
	if r.Status() != run.SchedulerStatusPending {
		t.Fatalf("status: got %q want pending", r.Status())
	}
	if r.Kind() != run.KindCron {
		t.Fatalf("kind: got %q want cron", r.Kind())
	}
	if r.ScheduleID() == uuid.Nil {
		t.Fatalf("schedule_id should be generated")
	}
	started, ok := evt.(run.RunStarted)
	if !ok {
		t.Fatalf("expected RunStarted, got %T", evt)
	}
	if started.ID != r.ScheduleID() || started.Name != "daily_orders" {
		t.Fatalf("event identity mismatch: %+v", started)
	}
	if len(started.ServiceMetadata) != 1 || started.ServiceMetadata["orders"].ImageTag != "abc" {
		t.Fatalf("service metadata not carried: %+v", started.ServiceMetadata)
	}
}

func TestNewPendingRun_RejectsEmptyName(t *testing.T) {
	_, _, err := run.NewPendingRun("", run.KindCron, nil, nil, time.Now())
	if err != run.ErrScheduleNameRequired {
		t.Fatalf("err: got %v want %v", err, run.ErrScheduleNameRequired)
	}
}

func TestNewPendingRun_RejectsInvalidKind(t *testing.T) {
	_, _, err := run.NewPendingRun("x", run.Kind("bogus"), nil, nil, time.Now())
	if err != run.ErrInvalidKind {
		t.Fatalf("err: got %v want %v", err, run.ErrInvalidKind)
	}
}

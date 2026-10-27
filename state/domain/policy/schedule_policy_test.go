package policy_test

import (
	"context"
	"testing"

	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/domain/policy"
)

type fakeRunRepo struct{ active bool }

func (f fakeRunRepo) HasActiveRun(_ context.Context, _ string) (bool, error) {
	return f.active, nil
}

type fakeCatalogRepo struct{ exists bool }

func (f fakeCatalogRepo) ExistsActive(_ context.Context, _ string) (bool, error) {
	return f.exists, nil
}

func TestEnsureNoActiveRun_OkWhenNoActiveRun(t *testing.T) {
	if err := (policy.SchedulePolicy{}).EnsureNoActiveRun(context.Background(), fakeRunRepo{active: false}, "x"); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
}

func TestEnsureNoActiveRun_ErrorsWhenActiveRunExists(t *testing.T) {
	if err := (policy.SchedulePolicy{}).EnsureNoActiveRun(context.Background(), fakeRunRepo{active: true}, "x"); err != run.ErrScheduleHasActiveRun {
		t.Fatalf("err: got %v want ErrScheduleHasActiveRun", err)
	}
}

func TestScheduleExistsInCatalog_OkWhenPresent(t *testing.T) {
	if err := (policy.SchedulePolicy{}).ScheduleExistsInCatalog(context.Background(), fakeCatalogRepo{exists: true}, "x"); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
}

func TestScheduleExistsInCatalog_ErrorsWhenAbsent(t *testing.T) {
	if err := (policy.SchedulePolicy{}).ScheduleExistsInCatalog(context.Background(), fakeCatalogRepo{exists: false}, "x"); err != run.ErrScheduleNotInCatalog {
		t.Fatalf("err: got %v want ErrScheduleNotInCatalog", err)
	}
}

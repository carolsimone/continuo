package policy_test

import (
	"context"
	"testing"

	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/domain/policy"
)

// fakeRunRepo answers HasActiveRun with active, and records in asked, when
// set, the tenant it was asked for.
type fakeRunRepo struct {
	active bool
	asked  *string
}

func (f fakeRunRepo) HasActiveRun(_ context.Context, tenantID, _ string) (bool, error) {
	if f.asked != nil {
		*f.asked = tenantID
	}
	return f.active, nil
}

// fakeCatalogRepo answers ExistsActive with exists, and records in asked,
// when set, the tenant it was asked for.
type fakeCatalogRepo struct {
	exists bool
	asked  *string
}

func (f fakeCatalogRepo) ExistsActive(_ context.Context, tenantID, _ string) (bool, error) {
	if f.asked != nil {
		*f.asked = tenantID
	}
	return f.exists, nil
}

// Both checks ask the repository about the tenant they were given.
func TestSchedulePolicy_AsksForTheTenant(t *testing.T) {
	var runTenant, catalogTenant string
	if err := (policy.SchedulePolicy{}).EnsureNoActiveRun(context.Background(), fakeRunRepo{asked: &runTenant}, "acme", "x"); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if err := (policy.SchedulePolicy{}).ScheduleExistsInCatalog(context.Background(), fakeCatalogRepo{exists: true, asked: &catalogTenant}, "acme", "x"); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if runTenant != "acme" || catalogTenant != "acme" {
		t.Fatalf("asked for tenants %q and %q, want acme", runTenant, catalogTenant)
	}
}

func TestEnsureNoActiveRun_OkWhenNoActiveRun(t *testing.T) {
	if err := (policy.SchedulePolicy{}).EnsureNoActiveRun(context.Background(), fakeRunRepo{active: false}, "default", "x"); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
}

func TestEnsureNoActiveRun_ErrorsWhenActiveRunExists(t *testing.T) {
	if err := (policy.SchedulePolicy{}).EnsureNoActiveRun(context.Background(), fakeRunRepo{active: true}, "default", "x"); err != run.ErrScheduleHasActiveRun {
		t.Fatalf("err: got %v want ErrScheduleHasActiveRun", err)
	}
}

func TestScheduleExistsInCatalog_OkWhenPresent(t *testing.T) {
	if err := (policy.SchedulePolicy{}).ScheduleExistsInCatalog(context.Background(), fakeCatalogRepo{exists: true}, "default", "x"); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
}

func TestScheduleExistsInCatalog_ErrorsWhenAbsent(t *testing.T) {
	if err := (policy.SchedulePolicy{}).ScheduleExistsInCatalog(context.Background(), fakeCatalogRepo{exists: false}, "default", "x"); err != run.ErrScheduleNotInCatalog {
		t.Fatalf("err: got %v want ErrScheduleNotInCatalog", err)
	}
}

package policy_test

import (
	"context"
	"errors"
	"strings"
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

// fakeLiveModels answers CountOf with models, and records in asked, when set,
// the tenant it was asked for.
type fakeLiveModels struct {
	models int
	asked  *string
}

func (f fakeLiveModels) CountOf(_ context.Context, tenantID, _ string) (int, error) {
	if f.asked != nil {
		*f.asked = tenantID
	}
	return f.models, nil
}

// fakeScheduleRows answers Exists with exists, counts its calls in calls when
// set, and records in asked, when set, the tenant it was asked for.
type fakeScheduleRows struct {
	exists bool
	asked  *string
	calls  *int
}

func (f fakeScheduleRows) Exists(_ context.Context, tenantID, _ string) (bool, error) {
	if f.asked != nil {
		*f.asked = tenantID
	}
	if f.calls != nil {
		*f.calls++
	}
	return f.exists, nil
}

// Every check asks the repository about the tenant it was given.
func TestSchedulePolicy_AsksForTheTenant(t *testing.T) {
	var runTenant, modelsTenant, rowsTenant string
	if err := (policy.SchedulePolicy{}).EnsureNoActiveRun(context.Background(), fakeRunRepo{asked: &runTenant}, "acme", "x"); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if err := (policy.SchedulePolicy{}).EnsureScheduleKnown(context.Background(),
		fakeLiveModels{asked: &modelsTenant}, fakeScheduleRows{exists: true, asked: &rowsTenant}, "acme", "x"); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if runTenant != "acme" || modelsTenant != "acme" || rowsTenant != "acme" {
		t.Fatalf("asked for tenants %q, %q and %q, want acme", runTenant, modelsTenant, rowsTenant)
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

// A name a model of the live release carries is known, whether or not it has
// a cron, and its row is not read.
func TestEnsureScheduleKnown_OkWhenAModelCarriesTheName(t *testing.T) {
	calls := 0
	if err := (policy.SchedulePolicy{}).EnsureScheduleKnown(context.Background(),
		fakeLiveModels{models: 2}, fakeScheduleRows{calls: &calls}, "default", "x"); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if calls != 0 {
		t.Fatalf("read the schedule row %d times, want 0", calls)
	}
}

// A name no model carries is known when it has a cron.
func TestEnsureScheduleKnown_OkWhenOnlyACronHasTheName(t *testing.T) {
	if err := (policy.SchedulePolicy{}).EnsureScheduleKnown(context.Background(),
		fakeLiveModels{}, fakeScheduleRows{exists: true}, "default", "x"); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
}

func TestEnsureScheduleKnown_ErrorsWhenNeitherAModelNorACronHasTheName(t *testing.T) {
	err := (policy.SchedulePolicy{}).EnsureScheduleKnown(context.Background(), fakeLiveModels{}, fakeScheduleRows{}, "default", "x")
	if !errors.Is(err, run.ErrScheduleNotFound) {
		t.Fatalf("err: got %v want ErrScheduleNotFound", err)
	}
	if !strings.Contains(err.Error(), `"x"`) {
		t.Fatalf("err %q does not name the schedule", err)
	}
}

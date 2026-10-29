package runservice_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/carolsimone/continuo/pkg/identity"
	"github.com/carolsimone/continuo/run-controller/domain/aggregate/run"
	"github.com/carolsimone/continuo/run-controller/domain/repository"
	"github.com/carolsimone/continuo/run-controller/domain/schedule"
	runv1 "github.com/carolsimone/continuo/run-controller/proto/run/v1"
	"github.com/carolsimone/continuo/run-controller/service/schedules"
)

// everyHour fires on the hour.
type everyHour struct{}

func (everyHour) Next(after time.Time) time.Time { return after.Truncate(time.Hour).Add(time.Hour) }

// refusal is a refused schedule input whose message is its reason, as the
// schedules service returns one.
type refusal string

func (r refusal) Error() string        { return string(r) }
func (r refusal) Is(target error) bool { return target == schedules.ErrInvalid }

// asCaller runs call with the context the gRPC edge builds for a request whose
// identity metadata names user (none when empty).
func asCaller(t *testing.T, user string, call func(ctx context.Context) error) {
	t.Helper()
	ctx := context.Background()
	if user != "" {
		ctx = metadata.NewIncomingContext(ctx, metadata.Pairs(identity.MetadataKey, user))
	}
	_, err := identity.UnaryServerInterceptor()(ctx, nil, &grpc.UnaryServerInfo{},
		func(ctx context.Context, _ any) (any, error) { return nil, call(ctx) })
	require.NoError(t, err)
}

func storedSchedule(paused bool) *schedule.Schedule {
	next := base.Add(time.Hour)
	return schedule.Rehydrate(tenantA, "daily", "0 * * * *", "Europe/Paris", "every hour", paused, &next,
		4, "alice@example.com", base, everyHour{})
}

// UpsertSchedule passes the request's fields with the request's tenant and
// the caller's identity as the author, and maps the saved schedule and its
// next fires.
func TestUpsertSchedule_PassesTheRequestAndTheCallersIdentity(t *testing.T) {
	store := newStore()
	store.upsertOut = storedSchedule(false)
	store.upsertNext = []time.Time{base.Add(time.Hour), base.Add(2 * time.Hour), base.Add(3 * time.Hour)}
	srv := newServer(store)
	req := &runv1.UpsertScheduleRequest{
		ScheduleName: "daily", Cron: "0 * * * *", Timezone: "Europe/Paris", Description: "every hour",
		Paused: true, ExpectedVersion: 3,
	}

	var resp *runv1.UpsertScheduleResponse
	asCaller(t, "alice@example.com", func(ctx context.Context) (err error) {
		resp, err = srv.UpsertSchedule(ctx, req)
		return err
	})

	require.NotNil(t, store.upserted)
	assert.Equal(t, upsertCall{tenant: tenantA, by: "alice@example.com", in: schedules.UpsertInput{
		Name: "daily", Cron: "0 * * * *", Timezone: "Europe/Paris", Description: "every hour", Paused: true, ExpectedVersion: 3,
	}}, *store.upserted)
	got := resp.GetSchedule()
	assert.Equal(t, "daily", got.GetScheduleName())
	assert.Equal(t, "0 * * * *", got.GetCron())
	assert.Equal(t, "Europe/Paris", got.GetTimezone())
	assert.Equal(t, "every hour", got.GetDescription())
	assert.False(t, got.GetPaused())
	assert.True(t, base.Add(time.Hour).Equal(got.GetNextFireAt().AsTime()))
	assert.Equal(t, int64(4), got.GetVersion())
	assert.Equal(t, "alice@example.com", got.GetUpdatedBy())
	assert.True(t, base.Equal(got.GetUpdatedAt().AsTime()))
	require.Len(t, resp.GetNextFireTimes(), 3)
	for i, at := range resp.GetNextFireTimes() {
		assert.True(t, store.upsertNext[i].Equal(at.AsTime()), "fire %d", i)
	}
}

// A caller with no identity is the system sentinel; a paused schedule has no
// next fire.
func TestUpsertSchedule_AnUnauthenticatedCallerIsTheSystem(t *testing.T) {
	store := newStore()
	store.upsertOut = storedSchedule(true)
	srv := newServer(store)

	var resp *runv1.UpsertScheduleResponse
	asCaller(t, "", func(ctx context.Context) (err error) {
		resp, err = srv.UpsertSchedule(ctx, &runv1.UpsertScheduleRequest{ScheduleName: "daily", Cron: "0 * * * *", Timezone: "UTC", Paused: true})
		return err
	})

	assert.Equal(t, identity.SystemUserID, store.upserted.by)
	assert.Nil(t, resp.GetSchedule().GetNextFireAt())
	assert.Empty(t, resp.GetNextFireTimes())
}

func TestDeleteSchedule_DeletesTheRequestsTenantsSchedule(t *testing.T) {
	store := newStore()

	resp, err := newServer(store).DeleteSchedule(ctxOf(tenantB), &runv1.DeleteScheduleRequest{ScheduleName: "daily"})

	require.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Equal(t, []string{tenantB + "/daily"}, store.deleted)
}

func TestPreviewSchedule_MapsTheWordsAndTheFires(t *testing.T) {
	store := newStore()
	store.previewWords = "At 01:00"
	store.previewNext = []time.Time{base, base.Add(24 * time.Hour)}

	resp, err := newServer(store).PreviewSchedule(ctxOf(tenantA), &runv1.PreviewScheduleRequest{Cron: "0 1 * * *", Timezone: "Europe/Paris"})

	require.NoError(t, err)
	assert.Equal(t, [2]string{"0 1 * * *", "Europe/Paris"}, store.previewed)
	assert.Equal(t, "At 01:00", resp.GetCronWords())
	require.Len(t, resp.GetNextFireTimes(), 2)
	assert.True(t, base.Equal(resp.GetNextFireTimes()[0].AsTime()))
	assert.True(t, base.Add(24*time.Hour).Equal(resp.GetNextFireTimes()[1].AsTime()))
}

// The schedule calls map the service's errors: a refused input is
// InvalidArgument with the refusal's message, an unknown name NotFound, a
// stale version Aborted, and anything else Internal.
func TestScheduleCalls_MapTheServiceErrors(t *testing.T) {
	cases := []struct {
		name    string
		err     error
		code    codes.Code
		message string
	}{
		{"refused input", fmt.Errorf("wrapped: %w", refusal(`cron "61 * * * *": end of range (61) above maximum (59): 61`)),
			codes.InvalidArgument, `wrapped: cron "61 * * * *": end of range (61) above maximum (59): 61`},
		{"unknown name", fmt.Errorf(`schedule "weekly": %w`, schedule.ErrUnknownName),
			codes.NotFound, `schedule "weekly": ` + schedule.ErrUnknownName.Error()},
		{"stale version", fmt.Errorf("save: %w", schedule.ErrStaleVersion), codes.Aborted, schedule.ErrStaleVersion.Error()},
		{"failure", errBoom, codes.Internal, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			store := newStore()
			store.scheduleErr = c.err
			srv := newServer(store)
			ctx := ctxOf(tenantA)
			calls := map[string]func() error{
				"UpsertSchedule": func() error {
					_, err := srv.UpsertSchedule(ctx, &runv1.UpsertScheduleRequest{ScheduleName: "daily"})
					return err
				},
				"DeleteSchedule": func() error {
					_, err := srv.DeleteSchedule(ctx, &runv1.DeleteScheduleRequest{ScheduleName: "daily"})
					return err
				},
				"PreviewSchedule": func() error {
					_, err := srv.PreviewSchedule(ctx, &runv1.PreviewScheduleRequest{Cron: "0 1 * * *", Timezone: "UTC"})
					return err
				},
			}
			for rpc, call := range calls {
				err := call()
				assert.Equal(t, c.code, status.Code(err), "%s: %v", rpc, err)
				if c.message != "" {
					assert.Equal(t, c.message, status.Convert(err).Message(), rpc)
				}
			}
		})
	}
}

func TestScheduleCalls_ARequestWithoutATenantIsInternal(t *testing.T) {
	store := newStore()
	srv := newServer(store)
	ctx := context.Background()
	calls := map[string]func() error{
		"UpsertSchedule": func() error {
			_, err := srv.UpsertSchedule(ctx, &runv1.UpsertScheduleRequest{ScheduleName: "daily"})
			return err
		},
		"DeleteSchedule": func() error {
			_, err := srv.DeleteSchedule(ctx, &runv1.DeleteScheduleRequest{ScheduleName: "daily"})
			return err
		},
		"PreviewSchedule": func() error {
			_, err := srv.PreviewSchedule(ctx, &runv1.PreviewScheduleRequest{Cron: "0 1 * * *", Timezone: "UTC"})
			return err
		},
	}
	for rpc, call := range calls {
		assert.Equal(t, codes.Internal, status.Code(call()), rpc)
	}
	assert.Nil(t, store.upserted, "nothing reaches the service")
	assert.Empty(t, store.deleted)
}

// ListSchedules maps each summary: a scheduled name with its cron fields and
// last run, a paused one with no next fire, and a manual one with no cron.
func TestListSchedules_MapsEveryField(t *testing.T) {
	store := newStore()
	lastID := uuid.New()
	last := repository.LastRunSummary{ScheduleName: "daily", RunID: lastID, Status: run.RunStatusRunning, CreatedAt: base, IsActive: true}
	store.scheduleSummaries[tenantA] = []schedules.Summary{
		{Name: "daily", Schedule: storedSchedule(false), Words: "Every hour", ModelCount: 41, LastRun: &last},
		{Name: "e2e-schedule", ModelCount: 5},
		{Name: "hourly", Schedule: storedSchedule(true), ModelCount: 6},
	}

	resp, err := newServer(store).ListSchedules(ctxOf(tenantA), &runv1.ListSchedulesRequest{})

	require.NoError(t, err)
	require.Len(t, resp.GetSchedules(), 3)
	daily, manual, paused := resp.GetSchedules()[0], resp.GetSchedules()[1], resp.GetSchedules()[2]

	assert.Equal(t, "daily", daily.GetScheduleName())
	assert.Equal(t, "0 * * * *", daily.GetCron())
	assert.Equal(t, "Every hour", daily.GetCronWords())
	assert.Equal(t, "Europe/Paris", daily.GetTimezone())
	assert.Equal(t, "every hour", daily.GetDescription())
	assert.False(t, daily.GetPaused())
	assert.True(t, base.Add(time.Hour).Equal(daily.GetNextFireAt().AsTime()))
	assert.Equal(t, int64(4), daily.GetVersion())
	assert.Equal(t, "alice@example.com", daily.GetUpdatedBy())
	assert.True(t, base.Equal(daily.GetUpdatedAt().AsTime()))
	assert.Equal(t, int32(41), daily.GetModelCount())
	assert.True(t, daily.GetHasActiveRun())
	assert.Equal(t, "running", daily.GetLastRunStatus())
	assert.Equal(t, lastID.String(), daily.GetLastRunId())
	assert.True(t, base.Equal(daily.GetLastRunAt().AsTime()))

	assert.Equal(t, "e2e-schedule", manual.GetScheduleName())
	assert.Empty(t, manual.GetCron())
	assert.Empty(t, manual.GetTimezone())
	assert.Zero(t, manual.GetVersion())
	assert.Nil(t, manual.GetNextFireAt())
	assert.Nil(t, manual.GetUpdatedAt())
	assert.Equal(t, int32(5), manual.GetModelCount())
	assert.False(t, manual.GetHasActiveRun())
	assert.Nil(t, manual.GetLastRunAt())
	assert.Empty(t, manual.GetLastRunId())

	assert.True(t, paused.GetPaused())
	assert.Nil(t, paused.GetNextFireAt(), "a paused schedule has no next fire")
	assert.Equal(t, int32(6), paused.GetModelCount())
}

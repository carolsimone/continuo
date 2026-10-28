package schedule

import (
	"bytes"
	"testing"
	"time"

	"github.com/carolsimone/continuo/cli/internal/config"
	"github.com/carolsimone/continuo/cli/internal/output"
	runv1 "github.com/carolsimone/continuo/cli/proto/run/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// resumedHourly is the answer of an upsert that resumed "hourly" at version 6.
func resumedHourly() *runv1.UpsertScheduleResponse {
	return &runv1.UpsertScheduleResponse{
		Schedule: &runv1.Schedule{
			ScheduleName: "hourly", Cron: "0 * * * *", Timezone: "Europe/Paris", Description: "Runs every hour",
			Version: 6, UpdatedBy: "alice@example.com",
			NextFireAt: timestamppb.New(time.Date(2026, 10, 28, 9, 0, 0, 0, time.UTC)),
		},
		NextFireTimes: fires(
			time.Date(2026, 10, 28, 9, 0, 0, 0, time.UTC),
			time.Date(2026, 10, 28, 10, 0, 0, 0, time.UTC),
			time.Date(2026, 10, 28, 11, 0, 0, 0, time.UTC)),
	}
}

func TestResume_ResendsTheVersionItRead(t *testing.T) {
	fake := &fakeCron{
		lists:      []*runv1.ListSchedulesResponse{listOf(pausedHourlySummary())},
		upsertResp: resumedHourly(),
	}

	stdout, stderr, exit := runCron(t, NewResumeCommand, fake, []string{"hourly"}, false, "")

	require.Equal(t, 0, exit, stdout)
	assert.Empty(t, stderr)
	require.Len(t, fake.upserts, 1)
	req := fake.upserts[0]
	assert.EqualValues(t, 5, req.GetExpectedVersion(), "the version of the schedule as read")
	assert.False(t, req.GetPaused())
	assert.Equal(t, "hourly", req.GetScheduleName())
	assert.Equal(t, "0 * * * *", req.GetCron())
	assert.Equal(t, "Europe/Paris", req.GetTimezone())
	assert.Equal(t, "Runs every hour", req.GetDescription())

	out := decodePauseResume(t, stdout)
	assert.False(t, out.Paused)
	assert.True(t, out.Changed)
	assert.EqualValues(t, 6, out.Version)
	assert.Equal(t, "2026-10-28T09:00:00Z", out.NextFireAt, "the first fire counts from the resume")
}

func TestResume_AConcurrentEditExitsWithTheConflictCode(t *testing.T) {
	changed := pausedHourlySummary()
	changed.Version = 6
	changed.UpdatedBy = "carol@example.com"
	fake := &fakeCron{
		lists:     []*runv1.ListSchedulesResponse{listOf(pausedHourlySummary()), listOf(changed)},
		upsertErr: status.Error(codes.Aborted, "the schedule was changed by another edit"),
	}

	stdout, _, exit := runCron(t, NewResumeCommand, fake, []string{"hourly"}, false, "")

	assert.Equal(t, 4, exit)
	e := errorOf(t, stdout)
	assert.Equal(t, output.CodeConflict, e.Code)
	assert.Equal(t, "changed by carol@example.com while this ran; run it again", e.Message)
	assert.Len(t, fake.upserts, 1)
}

// A resume of a schedule that is not paused succeeds and writes nothing.
func TestResume_AScheduleThatIsNotPausedSucceedsWithoutWriting(t *testing.T) {
	fake := &fakeCron{lists: []*runv1.ListSchedulesResponse{listOf(dailySummary())}}

	stdout, _, exit := runCron(t, NewResumeCommand, fake, []string{"daily"}, false, "")

	require.Equal(t, 0, exit, stdout)
	assert.Empty(t, fake.upserts, "nothing is saved")
	out := decodePauseResume(t, stdout)
	assert.False(t, out.Paused)
	assert.False(t, out.Changed)
	assert.EqualValues(t, 3, out.Version)
	assert.Equal(t, "2026-10-28T00:00:00Z", out.NextFireAt, "it already has a next fire")
}

func TestResume_AManualScheduleHasNothingToResume(t *testing.T) {
	fake := &fakeCron{lists: []*runv1.ListSchedulesResponse{listOf(manualSummary("e2e-schedule"))}}

	stdout, _, exit := runCron(t, NewResumeCommand, fake, []string{"e2e-schedule"}, false, "")

	assert.Equal(t, 3, exit)
	e := errorOf(t, stdout)
	assert.Equal(t, output.CodeNotFound, e.Code)
	assert.Contains(t, e.Message, "has no cron")
	assert.Empty(t, fake.upserts)
}

func TestResume_ANameDifferingOnlyByCaseIsNotFoundAndSaysWhichExists(t *testing.T) {
	fake := &fakeCron{lists: []*runv1.ListSchedulesResponse{listOf(pausedHourlySummary())}}

	stdout, _, exit := runCron(t, NewResumeCommand, fake, []string{"HOURLY"}, false, "")

	assert.Equal(t, 3, exit)
	e := errorOf(t, stdout)
	assert.Contains(t, e.Message, "'HOURLY'")
	assert.Contains(t, e.Message, "'hourly'")
	assert.Empty(t, fake.upserts)
}

func TestResume_ForwardsTheActor(t *testing.T) {
	fake := &fakeCron{
		lists:      []*runv1.ListSchedulesResponse{listOf(pausedHourlySummary())},
		upsertResp: resumedHourly(),
	}

	_, _, exit := runCron(t, NewResumeCommand, fake, []string{"hourly"}, false, "okta.example.com|alice")

	require.Equal(t, 0, exit)
	assert.Equal(t, []string{"okta.example.com|alice"}, fake.actors)
}

func TestResume_WrongArgumentCountExits2(t *testing.T) {
	fake := &fakeCron{}

	stdout, _, exit := runCron(t, NewResumeCommand, fake, []string{}, false, "")

	assert.Equal(t, 2, exit)
	assert.Equal(t, output.CodeUsage, errorOf(t, stdout).Code)
	assert.Empty(t, fake.calls)
}

func TestResume_HumanModeWritesOneLineToStderr(t *testing.T) {
	fake := &fakeCron{
		lists:      []*runv1.ListSchedulesResponse{listOf(pausedHourlySummary())},
		upsertResp: resumedHourly(),
	}

	stdout, stderr, exit := runCron(t, NewResumeCommand, fake, []string{"hourly"}, true, "")

	require.Equal(t, 0, exit)
	assert.Empty(t, stdout)
	assert.Contains(t, stderr, "Resumed schedule 'hourly'")
	assert.Contains(t, stderr, "2026-10-28T09:00:00Z")
}

func TestResume_HumanModeSaysWhenItWasNotPaused(t *testing.T) {
	fake := &fakeCron{lists: []*runv1.ListSchedulesResponse{listOf(dailySummary())}}

	_, stderr, exit := runCron(t, NewResumeCommand, fake, []string{"daily"}, true, "")

	require.Equal(t, 0, exit)
	assert.Contains(t, stderr, "not paused")
}

func TestResume_OutputMatchesItsSchema(t *testing.T) {
	fake := &fakeCron{
		lists:      []*runv1.ListSchedulesResponse{listOf(pausedHourlySummary())},
		upsertResp: resumedHourly(),
	}
	stdout, _, exit := runCron(t, NewResumeCommand, fake, []string{"hourly"}, false, "")
	require.Equal(t, 0, exit)
	assertEmitsItsSchema(t, NewResumeCommand(nil, &config.Config{}, &bytes.Buffer{}, &bytes.Buffer{}), stdout)

	noop := &fakeCron{lists: []*runv1.ListSchedulesResponse{listOf(dailySummary())}}
	stdout, _, exit = runCron(t, NewResumeCommand, noop, []string{"daily"}, false, "")
	require.Equal(t, 0, exit)
	assertEmitsItsSchema(t, NewResumeCommand(nil, &config.Config{}, &bytes.Buffer{}, &bytes.Buffer{}), stdout)
}

package schedule

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/carolsimone/continuo/cli/internal/config"
	"github.com/carolsimone/continuo/cli/internal/output"
	runv1 "github.com/carolsimone/continuo/cli/proto/run/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// pauseResumeOut is what pause and resume print.
type pauseResumeOut struct {
	ScheduleName string `json:"schedule_name"`
	Paused       bool   `json:"paused"`
	Changed      bool   `json:"changed"`
	Cron         string `json:"cron"`
	Timezone     string `json:"timezone"`
	Version      int64  `json:"version"`
	NextFireAt   string `json:"next_fire_at"`
}

func decodePauseResume(t *testing.T, stdout string) pauseResumeOut {
	t.Helper()
	var out pauseResumeOut
	require.NoError(t, json.Unmarshal([]byte(stdout), &out), stdout)
	return out
}

// pausedDaily is the answer of an upsert that paused "daily" at version 4.
func pausedDaily() *runv1.UpsertScheduleResponse {
	return &runv1.UpsertScheduleResponse{Schedule: &runv1.Schedule{
		ScheduleName: "daily", Cron: "0 1 * * *", Timezone: "Europe/Paris", Description: "Runs daily at 1am",
		Paused: true, Version: 4, UpdatedBy: "alice@example.com",
	}}
}

func TestPause_ResendsTheVersionItRead(t *testing.T) {
	fake := &fakeCron{
		lists:      []*runv1.ListSchedulesResponse{listOf(dailySummary())},
		upsertResp: pausedDaily(),
	}

	stdout, stderr, exit := runCron(t, NewPauseCommand, fake, []string{"daily"}, false, "")

	require.Equal(t, 0, exit, stdout)
	assert.Empty(t, stderr)
	require.Len(t, fake.upserts, 1)
	req := fake.upserts[0]
	assert.EqualValues(t, 3, req.GetExpectedVersion(), "the version of the schedule as read")
	assert.True(t, req.GetPaused())
	assert.Equal(t, "daily", req.GetScheduleName())
	assert.Equal(t, "0 1 * * *", req.GetCron(), "the cron is kept")
	assert.Equal(t, "Europe/Paris", req.GetTimezone(), "the timezone is kept")
	assert.Equal(t, "Runs daily at 1am", req.GetDescription(), "the description is kept")

	out := decodePauseResume(t, stdout)
	assert.Equal(t, "daily", out.ScheduleName)
	assert.True(t, out.Paused)
	assert.True(t, out.Changed)
	assert.EqualValues(t, 4, out.Version, "the version the save produced")
	assert.Empty(t, out.NextFireAt, "a paused schedule has no next fire")
	assert.Equal(t, []string{"list", "upsert"}, fake.calls)
}

func TestPause_AConcurrentEditExitsWithTheConflictCode(t *testing.T) {
	changed := dailySummary()
	changed.Version = 4
	changed.UpdatedBy = "bob@example.com"
	fake := &fakeCron{
		lists:     []*runv1.ListSchedulesResponse{listOf(dailySummary()), listOf(changed)},
		upsertErr: status.Error(codes.Aborted, "the schedule was changed by another edit"),
	}

	stdout, _, exit := runCron(t, NewPauseCommand, fake, []string{"daily"}, false, "")

	assert.Equal(t, 4, exit)
	e := errorOf(t, stdout)
	assert.Equal(t, output.CodeConflict, e.Code)
	assert.Equal(t, "changed by bob@example.com while this ran; run it again", e.Message)
	assert.True(t, e.Retryable, "running the command again reads the schedule afresh")
	assert.Equal(t, []string{"list", "upsert", "list"}, fake.calls, "one more read names who changed it")
	assert.Len(t, fake.upserts, 1, "the stale version is not sent again")
}

// When the fresh read cannot say who, or the cron is gone, the message names
// "someone".
func TestPause_AConflictWhoseAuthorCannotBeReadNamesSomeone(t *testing.T) {
	removed := listOf(manualSummary("daily"))
	cases := map[string]*fakeCron{
		"the read fails": {
			lists:     []*runv1.ListSchedulesResponse{listOf(dailySummary())},
			listErrs:  []error{nil, status.Error(codes.Unavailable, "down")},
			upsertErr: status.Error(codes.Aborted, "stale"),
		},
		"the cron was removed": {
			lists:     []*runv1.ListSchedulesResponse{listOf(dailySummary()), removed},
			upsertErr: status.Error(codes.Aborted, "stale"),
		},
	}
	for name, fake := range cases {
		t.Run(name, func(t *testing.T) {
			stdout, _, exit := runCron(t, NewPauseCommand, fake, []string{"daily"}, false, "")

			assert.Equal(t, 4, exit)
			assert.Equal(t, "changed by someone while this ran; run it again", errorOf(t, stdout).Message)
		})
	}
}

// A pause of a schedule that is paused succeeds and writes nothing: a save
// would raise its version and credit the pause to the caller.
func TestPause_AnAlreadyPausedScheduleSucceedsWithoutWriting(t *testing.T) {
	fake := &fakeCron{lists: []*runv1.ListSchedulesResponse{listOf(pausedHourlySummary())}}

	stdout, _, exit := runCron(t, NewPauseCommand, fake, []string{"hourly"}, false, "")

	require.Equal(t, 0, exit, stdout)
	assert.Empty(t, fake.upserts, "nothing is saved")
	out := decodePauseResume(t, stdout)
	assert.True(t, out.Paused)
	assert.False(t, out.Changed, "the schedule was paused already")
	assert.EqualValues(t, 5, out.Version, "the version is the one it already has")
	assert.Equal(t, "0 * * * *", out.Cron)
}

func TestPause_AManualScheduleHasNothingToPause(t *testing.T) {
	fake := &fakeCron{lists: []*runv1.ListSchedulesResponse{listOf(manualSummary("e2e-schedule"))}}

	stdout, _, exit := runCron(t, NewPauseCommand, fake, []string{"e2e-schedule"}, false, "")

	assert.Equal(t, 3, exit)
	e := errorOf(t, stdout)
	assert.Equal(t, output.CodeNotFound, e.Code)
	assert.Contains(t, e.Message, "has no cron")
	assert.Contains(t, e.Message, "set-cron", "the message names the command that adds one")
	assert.Empty(t, fake.upserts)
}

func TestPause_AnUnknownNameExits3(t *testing.T) {
	fake := &fakeCron{lists: []*runv1.ListSchedulesResponse{listOf(dailySummary())}}

	stdout, _, exit := runCron(t, NewPauseCommand, fake, []string{"weekly"}, false, "")

	assert.Equal(t, 3, exit)
	e := errorOf(t, stdout)
	assert.Equal(t, output.CodeNotFound, e.Code)
	assert.Contains(t, e.Message, "'weekly'")
	assert.Empty(t, fake.upserts)
}

// Names are case-sensitive: "Daily" pauses nothing, and the message points at
// "daily".
func TestPause_ANameDifferingOnlyByCaseIsNotFoundAndSaysWhichExists(t *testing.T) {
	fake := &fakeCron{lists: []*runv1.ListSchedulesResponse{listOf(dailySummary())}}

	stdout, _, exit := runCron(t, NewPauseCommand, fake, []string{"Daily"}, false, "")

	assert.Equal(t, 3, exit)
	e := errorOf(t, stdout)
	assert.Contains(t, e.Message, "'Daily'")
	assert.Contains(t, e.Message, "case-sensitive")
	assert.Contains(t, e.Message, "'daily'")
	assert.Empty(t, fake.upserts, "nothing is paused")
}

func TestPause_ForwardsTheActor(t *testing.T) {
	fake := &fakeCron{
		lists:      []*runv1.ListSchedulesResponse{listOf(dailySummary())},
		upsertResp: pausedDaily(),
	}

	_, _, exit := runCron(t, NewPauseCommand, fake, []string{"daily"}, false, "okta.example.com|alice")

	require.Equal(t, 0, exit)
	assert.Equal(t, []string{"okta.example.com|alice"}, fake.actors)
}

func TestPause_WrongArgumentCountExits2(t *testing.T) {
	for _, args := range [][]string{{}, {"daily", "extra"}, {"  "}} {
		fake := &fakeCron{}

		stdout, _, exit := runCron(t, NewPauseCommand, fake, args, false, "")

		assert.Equal(t, 2, exit, "args %q", args)
		assert.Equal(t, output.CodeUsage, errorOf(t, stdout).Code)
		assert.Empty(t, fake.calls)
	}
}

func TestPause_MaintenanceExits7(t *testing.T) {
	fake := &fakeCron{
		lists:     []*runv1.ListSchedulesResponse{listOf(dailySummary())},
		upsertErr: maintenanceRefusal(),
	}

	stdout, _, exit := runCron(t, NewPauseCommand, fake, []string{"daily"}, false, "")

	assert.Equal(t, 7, exit)
	assert.Equal(t, output.CodeMaintenance, errorOf(t, stdout).Code)
}

func TestPause_UnavailableExits5(t *testing.T) {
	fake := &fakeCron{listErrs: []error{status.Error(codes.Unavailable, "down")}}

	_, _, exit := runCron(t, NewPauseCommand, fake, []string{"daily"}, false, "")

	assert.Equal(t, 5, exit)
}

func TestPause_HumanModeWritesOneLineToStderr(t *testing.T) {
	fake := &fakeCron{
		lists:      []*runv1.ListSchedulesResponse{listOf(dailySummary())},
		upsertResp: pausedDaily(),
	}

	stdout, stderr, exit := runCron(t, NewPauseCommand, fake, []string{"daily"}, true, "")

	require.Equal(t, 0, exit)
	assert.Empty(t, stdout)
	assert.Contains(t, stderr, "Paused schedule 'daily'")
}

func TestPause_HumanModeSaysWhenItWasPausedAlready(t *testing.T) {
	fake := &fakeCron{lists: []*runv1.ListSchedulesResponse{listOf(pausedHourlySummary())}}

	_, stderr, exit := runCron(t, NewPauseCommand, fake, []string{"hourly"}, true, "")

	require.Equal(t, 0, exit)
	assert.Contains(t, stderr, "already paused")
}

func TestPause_OutputMatchesItsSchema(t *testing.T) {
	fake := &fakeCron{
		lists:      []*runv1.ListSchedulesResponse{listOf(dailySummary())},
		upsertResp: pausedDaily(),
	}
	stdout, _, exit := runCron(t, NewPauseCommand, fake, []string{"daily"}, false, "")
	require.Equal(t, 0, exit)

	assertEmitsItsSchema(t, NewPauseCommand(nil, &config.Config{}, &bytes.Buffer{}, &bytes.Buffer{}), stdout)

	// The no-op answer carries the same keys.
	noop := &fakeCron{lists: []*runv1.ListSchedulesResponse{listOf(pausedHourlySummary())}}
	stdout, _, exit = runCron(t, NewPauseCommand, noop, []string{"hourly"}, false, "")
	require.Equal(t, 0, exit)
	assertEmitsItsSchema(t, NewPauseCommand(nil, &config.Config{}, &bytes.Buffer{}, &bytes.Buffer{}), stdout)
}

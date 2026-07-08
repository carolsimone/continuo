package schedule

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/carolsimone/continuo/cli/internal/client"
	"github.com/carolsimone/continuo/cli/internal/config"
	"github.com/carolsimone/continuo/cli/internal/output"
	statev1 "github.com/carolsimone/continuo/cli/proto/state/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// fakeStateList is a StateClient fake for list_test.go.
// It implements only ListAllSchedules; TriggerSchedule panics if called.
type fakeStateList struct {
	resp *statev1.ListAllSchedulesResponse
	err  error
}

func (f *fakeStateList) TriggerSchedule(_ context.Context, _ string) (*statev1.TriggerScheduleResponse, error) {
	panic("TriggerSchedule should not be called in list tests")
}

func (f *fakeStateList) ListAllSchedules(_ context.Context) (*statev1.ListAllSchedulesResponse, error) {
	return f.resp, f.err
}

func (f *fakeStateList) ListTasks(_ context.Context, _ string, _ statev1.TaskStatus, _, _ int32) (*statev1.ListTasksResponse, error) {
	panic("ListTasks should not be called in list tests")
}

func (f *fakeStateList) CancelSchedule(_ context.Context, _, _, _ string) (*statev1.CancelScheduleResponse, error) {
	panic("CancelSchedule should not be called in list tests")
}

func (f *fakeStateList) Close() error { return nil }

// runList invokes the list command end-to-end with the provided fake client.
// It captures stdout/stderr and returns the exit code.
func runList(t *testing.T, fake client.StateClient, args []string, human bool) (stdout, stderr string, exit int) {
	t.Helper()
	var outBuf, errBuf bytes.Buffer
	cfg := &config.Config{Timeout: 2 * time.Second, Human: human}
	cmd := NewListCommand(
		func(_ context.Context, _ string) (client.StateClient, error) { return fake, nil },
		cfg,
		&outBuf,
		&errBuf,
	)
	cmd.SetArgs(args)
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	err := cmd.Execute()
	exit = 0
	if err != nil {
		var cliErr output.CLIError
		if errors.As(err, &cliErr) {
			exit = cliErr.ExitCode()
		} else {
			exit = 1
		}
	}
	return outBuf.String(), errBuf.String(), exit
}

// scheduleJSON is the expected JSON shape for a single schedule entry.
type scheduleJSON struct {
	ScheduleName   string `json:"schedule_name"`
	CronExpression string `json:"cron_expression"`
	Description    string `json:"description,omitempty"`
	Timezone       string `json:"timezone"`
	IsRunning      bool   `json:"is_running"`
	LastRunId      string `json:"last_run_id"`
	LastRunStatus  string `json:"last_run_status"`
	LastRunAt      string `json:"last_run_at,omitempty"`
}

type listResponseJSON struct {
	Schedules []scheduleJSON `json:"schedules"`
}

func TestList_SuccessEmitsJSONWithAllFields(t *testing.T) {
	lastRunTime := time.Date(2026, 4, 20, 3, 0, 0, 0, time.UTC)
	fake := &fakeStateList{
		resp: &statev1.ListAllSchedulesResponse{
			Schedules: []*statev1.ScheduleSummary{
				{
					ScheduleName:   "daily_ingest",
					CronExpression: "0 3 * * *",
					Timezone:       "UTC",
					IsRunning:      false,
					LastRunId:      "sched_01HXYZ",
					LastRunStatus:  "success",
					LastRunAt:      timestamppb.New(lastRunTime),
				},
			},
		},
	}

	stdout, stderr, exit := runList(t, fake, []string{}, false)

	assert.Equal(t, 0, exit)
	assert.Empty(t, stderr)

	var payload listResponseJSON
	require.NoError(t, json.Unmarshal([]byte(stdout), &payload))
	require.Len(t, payload.Schedules, 1)

	s := payload.Schedules[0]
	assert.Equal(t, "daily_ingest", s.ScheduleName)
	assert.Equal(t, "0 3 * * *", s.CronExpression)
	assert.Equal(t, "UTC", s.Timezone)
	assert.False(t, s.IsRunning)
	assert.Equal(t, "sched_01HXYZ", s.LastRunId)
	assert.Equal(t, "success", s.LastRunStatus)
	assert.Equal(t, "2026-04-20T03:00:00Z", s.LastRunAt)
}

func TestList_EmptySchedulesReturnsEmptyArray(t *testing.T) {
	fake := &fakeStateList{
		resp: &statev1.ListAllSchedulesResponse{
			Schedules: []*statev1.ScheduleSummary{},
		},
	}

	stdout, _, exit := runList(t, fake, []string{}, false)

	assert.Equal(t, 0, exit)

	var payload map[string]json.RawMessage
	require.NoError(t, json.Unmarshal([]byte(stdout), &payload))
	schedulesRaw, ok := payload["schedules"]
	require.True(t, ok, "response must contain 'schedules' key")

	var schedules []interface{}
	require.NoError(t, json.Unmarshal(schedulesRaw, &schedules))
	assert.Empty(t, schedules, "schedules must be an empty array, not null")
}

func TestList_HumanModeRunningSchedule(t *testing.T) {
	lastRunTime := time.Date(2026, 4, 20, 3, 0, 0, 0, time.UTC)
	fake := &fakeStateList{
		resp: &statev1.ListAllSchedulesResponse{
			Schedules: []*statev1.ScheduleSummary{
				{
					ScheduleName:  "daily_ingest",
					IsRunning:     true,
					LastRunStatus: "success",
					LastRunAt:     timestamppb.New(lastRunTime),
				},
			},
		},
	}

	stdout, stderr, exit := runList(t, fake, []string{}, true)

	assert.Equal(t, 0, exit)
	assert.Empty(t, stdout)
	assert.Contains(t, stderr, "daily_ingest")
	assert.Contains(t, stderr, "running")
	assert.Contains(t, stderr, "success")
	assert.Contains(t, stderr, "2026-04-20")
}

func TestList_HumanModeIdleScheduleWithLastRun(t *testing.T) {
	lastRunTime := time.Date(2026, 4, 20, 3, 0, 0, 0, time.UTC)
	fake := &fakeStateList{
		resp: &statev1.ListAllSchedulesResponse{
			Schedules: []*statev1.ScheduleSummary{
				{
					ScheduleName:  "daily_ingest",
					IsRunning:     false,
					LastRunStatus: "success",
					LastRunAt:     timestamppb.New(lastRunTime),
				},
			},
		},
	}

	stdout, stderr, exit := runList(t, fake, []string{}, true)

	assert.Equal(t, 0, exit)
	assert.Empty(t, stdout)
	assert.Contains(t, stderr, "daily_ingest")
	assert.Contains(t, stderr, "idle")
	assert.Contains(t, stderr, "success")
	assert.Contains(t, stderr, "2026-04-20")
}

func TestList_HumanModeIdleScheduleNoLastRun(t *testing.T) {
	fake := &fakeStateList{
		resp: &statev1.ListAllSchedulesResponse{
			Schedules: []*statev1.ScheduleSummary{
				{
					ScheduleName: "daily_ingest",
					IsRunning:    false,
				},
			},
		},
	}

	stdout, stderr, exit := runList(t, fake, []string{}, true)

	assert.Equal(t, 0, exit)
	assert.Empty(t, stdout)
	assert.Contains(t, stderr, "daily_ingest")
	assert.Contains(t, stderr, "idle")
	// no last-run info expected
	assert.NotContains(t, stderr, "last:")
}

func TestList_ZeroLastRunAtOmittedFromJSON(t *testing.T) {
	fake := &fakeStateList{
		resp: &statev1.ListAllSchedulesResponse{
			Schedules: []*statev1.ScheduleSummary{
				{
					ScheduleName:   "daily_ingest",
					CronExpression: "0 3 * * *",
					Timezone:       "UTC",
					IsRunning:      false,
					LastRunId:      "",
					LastRunStatus:  "",
					LastRunAt:      nil, // zero / never run
				},
			},
		},
	}

	stdout, _, exit := runList(t, fake, []string{}, false)

	assert.Equal(t, 0, exit)

	// Unmarshal into raw map to check key presence
	var payload struct {
		Schedules []map[string]json.RawMessage `json:"schedules"`
	}
	require.NoError(t, json.Unmarshal([]byte(stdout), &payload))
	require.Len(t, payload.Schedules, 1)
	_, hasLastRunAt := payload.Schedules[0]["last_run_at"]
	assert.False(t, hasLastRunAt, "last_run_at must be omitted when there is no last run")
}

func TestList_DescriptionOmittedWhenEmpty(t *testing.T) {
	fake := &fakeStateList{
		resp: &statev1.ListAllSchedulesResponse{
			Schedules: []*statev1.ScheduleSummary{
				{
					ScheduleName:   "daily_ingest",
					CronExpression: "0 3 * * *",
					Timezone:       "UTC",
					Description:    "", // empty — must be omitted
				},
			},
		},
	}

	stdout, _, exit := runList(t, fake, []string{}, false)

	assert.Equal(t, 0, exit)
	var payload struct {
		Schedules []map[string]json.RawMessage `json:"schedules"`
	}
	require.NoError(t, json.Unmarshal([]byte(stdout), &payload))
	require.Len(t, payload.Schedules, 1)
	_, hasDesc := payload.Schedules[0]["description"]
	assert.False(t, hasDesc, "description must be omitted when empty")
}

func TestList_DescriptionIncludedWhenNonEmpty(t *testing.T) {
	fake := &fakeStateList{
		resp: &statev1.ListAllSchedulesResponse{
			Schedules: []*statev1.ScheduleSummary{
				{
					ScheduleName:   "daily_ingest",
					CronExpression: "0 3 * * *",
					Timezone:       "UTC",
					Description:    "Ingests daily data",
				},
			},
		},
	}

	stdout, _, exit := runList(t, fake, []string{}, false)

	assert.Equal(t, 0, exit)
	var payload struct {
		Schedules []scheduleJSON `json:"schedules"`
	}
	require.NoError(t, json.Unmarshal([]byte(stdout), &payload))
	require.Len(t, payload.Schedules, 1)
	assert.Equal(t, "Ingests daily data", payload.Schedules[0].Description)
}

func TestList_NotFoundExits3(t *testing.T) {
	fake := &fakeStateList{err: status.Error(codes.NotFound, "not found")}

	stdout, _, exit := runList(t, fake, []string{}, false)

	assert.Equal(t, 3, exit)
	var envelope struct {
		Error output.CLIError `json:"error"`
	}
	require.NoError(t, json.Unmarshal([]byte(stdout), &envelope))
	assert.Equal(t, output.CodeNotFound, envelope.Error.Code)
	assert.False(t, envelope.Error.Retryable)
}

func TestList_UnavailableExits5(t *testing.T) {
	fake := &fakeStateList{err: status.Error(codes.Unavailable, "server down")}

	_, _, exit := runList(t, fake, []string{}, false)

	assert.Equal(t, 5, exit)
}

func TestList_FactoryErrorEmitsJSONAndExits(t *testing.T) {
	var outBuf, errBuf bytes.Buffer
	cfg := &config.Config{Timeout: 2 * time.Second}
	cmd := NewListCommand(
		func(_ context.Context, _ string) (client.StateClient, error) {
			return nil, status.Error(codes.Unavailable, "dial failed")
		},
		cfg,
		&outBuf,
		&errBuf,
	)
	cmd.SetArgs([]string{})
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	err := cmd.Execute()

	var cliErr output.CLIError
	require.True(t, errors.As(err, &cliErr))
	assert.Equal(t, 5, cliErr.ExitCode())
}

func TestList_ExtraArgEmitsUsageEnvelopeExits2(t *testing.T) {
	fake := &fakeStateList{resp: &statev1.ListAllSchedulesResponse{}}

	stdout, _, exit := runList(t, fake, []string{"extra"}, false)

	assert.Equal(t, 2, exit)
	var env map[string]output.CLIError
	require.NoError(t, json.Unmarshal([]byte(stdout), &env))
	assert.Equal(t, output.CodeUsage, env["error"].Code)
}

package schedule

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/carolsimone/continuo/cli/internal/client"
	"github.com/carolsimone/continuo/cli/internal/config"
	"github.com/carolsimone/continuo/cli/internal/output"
	runv1 "github.com/carolsimone/continuo/cli/proto/run/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// fakeStateList is a RunClient fake for list_test.go. It answers
// ListAllSchedules and, embedding the interface, panics on any other call.
type fakeStateList struct {
	client.RunClient
	resp *runv1.ListSchedulesResponse
	err  error
}

func (f *fakeStateList) ListAllSchedules(_ context.Context) (*runv1.ListSchedulesResponse, error) {
	return f.resp, f.err
}

func (f *fakeStateList) Close() error { return nil }

// runList invokes the list command end-to-end with the provided fake client.
// It captures stdout/stderr and returns the exit code.
func runList(t *testing.T, fake client.RunClient, args []string, human bool) (stdout, stderr string, exit int) {
	t.Helper()
	var outBuf, errBuf bytes.Buffer
	cfg := &config.Config{Timeout: 2 * time.Second, Human: human}
	cmd := NewListCommand(
		func(_ context.Context, _ string) (client.RunClient, error) { return fake, nil },
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
	HasActiveRun   bool   `json:"has_active_run"`
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
		resp: &runv1.ListSchedulesResponse{
			Schedules: []*runv1.ScheduleSummary{
				{
					ScheduleName:  "daily_ingest",
					Cron:          "0 3 * * *",
					Timezone:      "UTC",
					HasActiveRun:  false,
					LastRunId:     "sched_01HXYZ",
					LastRunStatus: "success",
					LastRunAt:     timestamppb.New(lastRunTime),
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
	assert.False(t, s.HasActiveRun)
	assert.Equal(t, "sched_01HXYZ", s.LastRunId)
	assert.Equal(t, "success", s.LastRunStatus)
	assert.Equal(t, "2026-04-20T03:00:00Z", s.LastRunAt)
}

func TestList_EmptySchedulesReturnsEmptyArray(t *testing.T) {
	fake := &fakeStateList{
		resp: &runv1.ListSchedulesResponse{
			Schedules: []*runv1.ScheduleSummary{},
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
		resp: &runv1.ListSchedulesResponse{
			Schedules: []*runv1.ScheduleSummary{
				{
					ScheduleName:  "daily_ingest",
					HasActiveRun:  true,
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
		resp: &runv1.ListSchedulesResponse{
			Schedules: []*runv1.ScheduleSummary{
				{
					ScheduleName:  "daily_ingest",
					HasActiveRun:  false,
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
		resp: &runv1.ListSchedulesResponse{
			Schedules: []*runv1.ScheduleSummary{
				{
					ScheduleName: "daily_ingest",
					HasActiveRun: false,
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
		resp: &runv1.ListSchedulesResponse{
			Schedules: []*runv1.ScheduleSummary{
				{
					ScheduleName:  "daily_ingest",
					Cron:          "0 3 * * *",
					Timezone:      "UTC",
					HasActiveRun:  false,
					LastRunId:     "",
					LastRunStatus: "",
					LastRunAt:     nil, // zero / never run
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
		resp: &runv1.ListSchedulesResponse{
			Schedules: []*runv1.ScheduleSummary{
				{
					ScheduleName: "daily_ingest",
					Cron:         "0 3 * * *",
					Timezone:     "UTC",
					Description:  "", // empty — must be omitted
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
		resp: &runv1.ListSchedulesResponse{
			Schedules: []*runv1.ScheduleSummary{
				{
					ScheduleName: "daily_ingest",
					Cron:         "0 3 * * *",
					Timezone:     "UTC",
					Description:  "Ingests daily data",
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
		func(_ context.Context, _ string) (client.RunClient, error) {
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
	fake := &fakeStateList{resp: &runv1.ListSchedulesResponse{}}

	stdout, _, exit := runList(t, fake, []string{"extra"}, false)

	assert.Equal(t, 2, exit)
	var env map[string]output.CLIError
	require.NoError(t, json.Unmarshal([]byte(stdout), &env))
	assert.Equal(t, output.CodeUsage, env["error"].Code)
}

// fullSummary carries every field a ScheduleSummary can: a scheduled "daily"
// with its last run, its models and who changed it.
func fullSummary() *runv1.ScheduleSummary {
	return &runv1.ScheduleSummary{
		ScheduleName:  "daily",
		Cron:          "0 1 * * *",
		Description:   "Runs daily at 1am",
		Timezone:      "Europe/Paris",
		HasActiveRun:  true,
		LastRunAt:     timestamppb.New(time.Date(2026, 10, 27, 0, 0, 0, 0, time.UTC)),
		LastRunStatus: "running",
		LastRunId:     "5d21c0de-7b4d-4e8f-9a1b-2c3d4e5f6a7b",
		Paused:        false,
		NextFireAt:    timestamppb.New(time.Date(2026, 10, 28, 0, 0, 0, 0, time.UTC)),
		Version:       3,
		UpdatedBy:     "alice@example.com",
		UpdatedAt:     timestamppb.New(time.Date(2026, 10, 21, 12, 2, 0, 0, time.UTC)),
		ModelCount:    41,
		CronWords:     "At 01:00 AM",
	}
}

// TestList_OutputMatchesItsSchema holds the output_schema annotation to the
// JSON the command emits: the same keys at the top level and in every entry,
// for an entry that carries every optional field.
func TestList_OutputMatchesItsSchema(t *testing.T) {
	fake := &fakeStateList{resp: &runv1.ListSchedulesResponse{Schedules: []*runv1.ScheduleSummary{fullSummary()}}}
	stdout, _, exit := runList(t, fake, []string{}, false)
	require.Equal(t, 0, exit)

	cmd := NewListCommand(nil, &config.Config{}, &bytes.Buffer{}, &bytes.Buffer{})
	var schema map[string]any
	require.NoError(t, json.Unmarshal([]byte(cmd.Annotations["output_schema"]), &schema))
	var emitted map[string]any
	require.NoError(t, json.Unmarshal([]byte(stdout), &emitted))

	assertSameKeys(t, "", schema, emitted)
}

// An entry with no cron and no run omits its optional fields and keeps the
// others, so the schema's keys are the entry's keys plus the optional ones.
func TestList_AnEntryWithNothingOptionalCarriesOnlyItsRequiredKeys(t *testing.T) {
	fake := &fakeStateList{resp: &runv1.ListSchedulesResponse{Schedules: []*runv1.ScheduleSummary{{ScheduleName: "e2e-schedule", ModelCount: 5}}}}
	stdout, _, exit := runList(t, fake, []string{}, false)
	require.Equal(t, 0, exit)

	cmd := NewListCommand(nil, &config.Config{}, &bytes.Buffer{}, &bytes.Buffer{})
	var schema struct {
		Schedules []map[string]json.RawMessage `json:"schedules"`
	}
	require.NoError(t, json.Unmarshal([]byte(cmd.Annotations["output_schema"]), &schema))
	var emitted struct {
		Schedules []map[string]json.RawMessage `json:"schedules"`
	}
	require.NoError(t, json.Unmarshal([]byte(stdout), &emitted))
	require.Len(t, emitted.Schedules, 1)
	for k := range emitted.Schedules[0] {
		assert.Contains(t, schema.Schedules[0], k, "emitted key %q is missing from the schema", k)
	}
	for _, optional := range []string{"description", "cron_words", "next_fire_at", "last_run_at", "updated_by", "updated_at"} {
		assert.NotContains(t, emitted.Schedules[0], optional, "%s is omitted when it has no value", optional)
	}
}

func TestList_CarriesTheFieldsOfAScheduledSchedule(t *testing.T) {
	fake := &fakeStateList{resp: &runv1.ListSchedulesResponse{Schedules: []*runv1.ScheduleSummary{fullSummary()}}}

	stdout, _, exit := runList(t, fake, []string{}, false)

	require.Equal(t, 0, exit)
	var payload struct {
		Schedules []map[string]any `json:"schedules"`
	}
	require.NoError(t, json.Unmarshal([]byte(stdout), &payload))
	require.Len(t, payload.Schedules, 1)
	s := payload.Schedules[0]
	assert.Equal(t, "daily", s["schedule_name"])
	assert.Equal(t, "0 1 * * *", s["cron_expression"])
	assert.Equal(t, "At 01:00 AM", s["cron_words"])
	assert.Equal(t, "Europe/Paris", s["timezone"])
	assert.Equal(t, true, s["has_cron"])
	assert.Equal(t, false, s["paused"])
	assert.Equal(t, "2026-10-28T00:00:00Z", s["next_fire_at"])
	assert.Equal(t, "Runs daily at 1am", s["description"])
	assert.EqualValues(t, 41, s["model_count"])
	assert.Equal(t, true, s["has_active_run"])
	assert.Equal(t, "5d21c0de-7b4d-4e8f-9a1b-2c3d4e5f6a7b", s["last_run_id"])
	assert.Equal(t, "running", s["last_run_status"])
	assert.Equal(t, "2026-10-27T00:00:00Z", s["last_run_at"])
	assert.Equal(t, "alice@example.com", s["updated_by"])
	assert.Equal(t, "2026-10-21T12:02:00Z", s["updated_at"])
}

func TestList_APausedScheduleHasNoNextFire(t *testing.T) {
	paused := &runv1.ScheduleSummary{
		ScheduleName: "hourly", Cron: "0 * * * *", Timezone: "Europe/Paris", Paused: true, ModelCount: 6,
		UpdatedBy: "bob@example.com", UpdatedAt: timestamppb.New(time.Date(2026, 10, 27, 13, 2, 0, 0, time.UTC)),
	}
	fake := &fakeStateList{resp: &runv1.ListSchedulesResponse{Schedules: []*runv1.ScheduleSummary{paused}}}

	stdout, _, exit := runList(t, fake, []string{}, false)

	require.Equal(t, 0, exit)
	var payload struct {
		Schedules []map[string]any `json:"schedules"`
	}
	require.NoError(t, json.Unmarshal([]byte(stdout), &payload))
	s := payload.Schedules[0]
	assert.Equal(t, true, s["paused"])
	assert.Equal(t, true, s["has_cron"])
	assert.NotContains(t, s, "next_fire_at")
	assert.Equal(t, "bob@example.com", s["updated_by"])
}

// A schedule the live release has and that has no cron is listed with its
// models and no cron: it runs only when triggered.
func TestList_AManualScheduleHasNoCron(t *testing.T) {
	fake := &fakeStateList{resp: &runv1.ListSchedulesResponse{Schedules: []*runv1.ScheduleSummary{{ScheduleName: "e2e-schedule", ModelCount: 5}}}}

	stdout, _, exit := runList(t, fake, []string{}, false)

	require.Equal(t, 0, exit)
	var payload struct {
		Schedules []map[string]any `json:"schedules"`
	}
	require.NoError(t, json.Unmarshal([]byte(stdout), &payload))
	s := payload.Schedules[0]
	assert.Equal(t, false, s["has_cron"])
	assert.Equal(t, "", s["cron_expression"])
	assert.Equal(t, false, s["paused"])
	assert.EqualValues(t, 5, s["model_count"])
}

func TestList_HumanModeShowsTheCronOrThatTheScheduleIsManualOrPaused(t *testing.T) {
	scheduled := fullSummary()
	paused := &runv1.ScheduleSummary{ScheduleName: "hourly", Cron: "0 * * * *", Timezone: "UTC", Paused: true}
	manual := &runv1.ScheduleSummary{ScheduleName: "e2e-schedule"}
	fake := &fakeStateList{resp: &runv1.ListSchedulesResponse{Schedules: []*runv1.ScheduleSummary{scheduled, paused, manual}}}

	_, stderr, exit := runList(t, fake, []string{}, true)

	require.Equal(t, 0, exit)
	lines := strings.Split(strings.TrimSpace(stderr), "\n")
	require.Len(t, lines, 3)
	assert.Contains(t, lines[0], "0 1 * * * Europe/Paris")
	assert.Contains(t, lines[0], "next 2026-10-28T00:00Z")
	assert.Contains(t, lines[1], "0 * * * * UTC (paused)")
	assert.NotContains(t, lines[1], "next ")
	assert.Contains(t, lines[2], "manual")
}

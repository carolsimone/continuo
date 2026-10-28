package schedule

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/carolsimone/continuo/cli/internal/client"
	"github.com/carolsimone/continuo/cli/internal/config"
	"github.com/carolsimone/continuo/cli/internal/output"
	runv1 "github.com/carolsimone/continuo/cli/proto/run/v1"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// fakeCron is the RunClient fake of the schedule-edit commands (set-cron,
// remove-cron, pause, resume). ListAllSchedules answers lists in order and
// repeats the last one, so a test can show a schedule that changed between two
// reads; every write is recorded with the actor that sent it. Any other call
// panics, through the embedded interface.
type fakeCron struct {
	client.RunClient

	lists    []*runv1.ListSchedulesResponse
	listErrs []error
	listed   int

	upsertResp *runv1.UpsertScheduleResponse
	upsertErr  error
	upserts    []*runv1.UpsertScheduleRequest
	deletes    []string
	deleteErr  error
	actors     []string

	previewResp *runv1.PreviewScheduleResponse
	previewErr  error
	previews    []*runv1.PreviewScheduleRequest

	// calls is the order of the RPCs the command made.
	calls []string
}

func (f *fakeCron) ListAllSchedules(_ context.Context) (*runv1.ListSchedulesResponse, error) {
	f.calls = append(f.calls, "list")
	n := f.listed
	f.listed++
	if n < len(f.listErrs) && f.listErrs[n] != nil {
		return nil, f.listErrs[n]
	}
	if len(f.lists) == 0 {
		return &runv1.ListSchedulesResponse{}, nil
	}
	if n >= len(f.lists) {
		n = len(f.lists) - 1
	}
	return f.lists[n], nil
}

func (f *fakeCron) UpsertSchedule(_ context.Context, actor string, req *runv1.UpsertScheduleRequest) (*runv1.UpsertScheduleResponse, error) {
	f.calls = append(f.calls, "upsert")
	f.upserts = append(f.upserts, req)
	f.actors = append(f.actors, actor)
	return f.upsertResp, f.upsertErr
}

func (f *fakeCron) DeleteSchedule(_ context.Context, actor, name string) (*runv1.DeleteScheduleResponse, error) {
	f.calls = append(f.calls, "delete")
	f.deletes = append(f.deletes, name)
	f.actors = append(f.actors, actor)
	return &runv1.DeleteScheduleResponse{}, f.deleteErr
}

func (f *fakeCron) PreviewSchedule(_ context.Context, cron, timezone string) (*runv1.PreviewScheduleResponse, error) {
	f.calls = append(f.calls, "preview")
	f.previews = append(f.previews, &runv1.PreviewScheduleRequest{Cron: cron, Timezone: timezone})
	return f.previewResp, f.previewErr
}

func (f *fakeCron) Close() error { return nil }

// listOf is a ListSchedules answer of the given schedules.
func listOf(s ...*runv1.ScheduleSummary) *runv1.ListSchedulesResponse {
	return &runv1.ListSchedulesResponse{Schedules: s}
}

// dailySummary is a scheduled "daily" at version 3, last edited by alice.
func dailySummary() *runv1.ScheduleSummary {
	return &runv1.ScheduleSummary{
		ScheduleName: "daily",
		Cron:         "0 1 * * *",
		Timezone:     "Europe/Paris",
		Description:  "Runs daily at 1am",
		Version:      3,
		UpdatedBy:    "alice@example.com",
		UpdatedAt:    timestamppb.New(time.Date(2026, 10, 21, 12, 2, 0, 0, time.UTC)),
		ModelCount:   41,
		NextFireAt:   timestamppb.New(time.Date(2026, 10, 28, 0, 0, 0, 0, time.UTC)),
		CronWords:    "At 01:00 AM",
	}
}

// pausedHourlySummary is a paused "hourly" at version 5, last edited by bob.
func pausedHourlySummary() *runv1.ScheduleSummary {
	return &runv1.ScheduleSummary{
		ScheduleName: "hourly",
		Cron:         "0 * * * *",
		Timezone:     "Europe/Paris",
		Description:  "Runs every hour",
		Paused:       true,
		Version:      5,
		UpdatedBy:    "bob@example.com",
		UpdatedAt:    timestamppb.New(time.Date(2026, 10, 27, 13, 2, 0, 0, time.UTC)),
		ModelCount:   6,
		CronWords:    "Every hour",
	}
}

// manualSummary is a schedule the live release has and that has no cron.
func manualSummary(name string) *runv1.ScheduleSummary {
	return &runv1.ScheduleSummary{ScheduleName: name, ModelCount: 5}
}

func fires(ts ...time.Time) []*timestamppb.Timestamp {
	out := make([]*timestamppb.Timestamp, 0, len(ts))
	for _, t := range ts {
		out = append(out, timestamppb.New(t))
	}
	return out
}

// commandBuilder is the shape of every New*Command constructor.
type commandBuilder func(RunClientFactory, *config.Config, io.Writer, io.Writer) *cobra.Command

// runCron invokes a command end to end with the fake and returns what it wrote
// and the exit code. actor is the CONTINUO_ACTOR value.
func runCron(t *testing.T, build commandBuilder, fake client.RunClient, args []string, human bool, actor string) (stdout, stderr string, exit int) {
	t.Helper()
	var outBuf, errBuf bytes.Buffer
	cfg := &config.Config{Timeout: 2 * time.Second, Human: human, Actor: actor}
	cmd := build(func(_ context.Context, _ string) (client.RunClient, error) { return fake, nil }, cfg, &outBuf, &errBuf)
	cmd.SetArgs(args)
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	if err := cmd.Execute(); err != nil {
		var cliErr output.CLIError
		if errors.As(err, &cliErr) {
			return outBuf.String(), errBuf.String(), cliErr.ExitCode()
		}
		return outBuf.String(), errBuf.String(), 1
	}
	return outBuf.String(), errBuf.String(), 0
}

// maintenanceRefusal is the error the dialled client returns for a call the
// server refused because maintenance mode is on.
func maintenanceRefusal() error {
	return fmt.Errorf("%w: %w", output.ErrMaintenance, status.Error(codes.Unavailable, "continuo is in maintenance mode"))
}

// errorOf decodes the CLIError envelope a failed command wrote to stdout.
func errorOf(t *testing.T, stdout string) output.CLIError {
	t.Helper()
	var env struct {
		Error output.CLIError `json:"error"`
	}
	require.NoError(t, json.Unmarshal([]byte(stdout), &env), stdout)
	return env.Error
}

// setCronOut is what set-cron prints.
type setCronOut struct {
	ScheduleName  string   `json:"schedule_name"`
	Cron          string   `json:"cron"`
	CronWords     string   `json:"cron_words"`
	Timezone      string   `json:"timezone"`
	Description   string   `json:"description"`
	Paused        bool     `json:"paused"`
	Version       int64    `json:"version"`
	NextFireTimes []string `json:"next_fire_times"`
}

func decodeSetCron(t *testing.T, stdout string) setCronOut {
	t.Helper()
	var out setCronOut
	require.NoError(t, json.Unmarshal([]byte(stdout), &out), stdout)
	return out
}

// savedDaily is the answer of an upsert that saved "daily" at version 4.
func savedDaily(cron, description string, paused bool, next ...time.Time) *runv1.UpsertScheduleResponse {
	return &runv1.UpsertScheduleResponse{
		Schedule: &runv1.Schedule{
			ScheduleName: "daily", Cron: cron, Timezone: "Europe/Paris", Description: description,
			Paused: paused, Version: 4, UpdatedBy: "alice@example.com",
		},
		NextFireTimes: fires(next...),
	}
}

func TestSetCron_PrintsTheNextThreeFires(t *testing.T) {
	fake := &fakeCron{
		lists: []*runv1.ListSchedulesResponse{listOf(dailySummary())},
		upsertResp: savedDaily("0 2 * * *", "Runs daily at 1am", false,
			time.Date(2026, 10, 28, 1, 0, 0, 0, time.UTC),
			time.Date(2026, 10, 29, 1, 0, 0, 0, time.UTC),
			time.Date(2026, 10, 30, 1, 0, 0, 0, time.UTC)),
		previewResp: &runv1.PreviewScheduleResponse{CronWords: "At 02:00 AM"},
	}

	stdout, stderr, exit := runCron(t, NewSetCronCommand, fake, []string{"daily", "0 2 * * *", "Europe/Paris"}, false, "")

	require.Equal(t, 0, exit, stdout)
	assert.Empty(t, stderr)
	out := decodeSetCron(t, stdout)
	assert.Equal(t, "daily", out.ScheduleName)
	assert.Equal(t, "0 2 * * *", out.Cron)
	assert.Equal(t, "At 02:00 AM", out.CronWords, "the words come from PreviewSchedule")
	assert.Equal(t, "Europe/Paris", out.Timezone)
	assert.False(t, out.Paused)
	assert.EqualValues(t, 4, out.Version)
	assert.Equal(t, []string{"2026-10-28T01:00:00Z", "2026-10-29T01:00:00Z", "2026-10-30T01:00:00Z"}, out.NextFireTimes)
	require.Len(t, fake.previews, 1)
	assert.Equal(t, "0 2 * * *", fake.previews[0].GetCron())
	assert.Equal(t, "Europe/Paris", fake.previews[0].GetTimezone())
}

func TestSetCron_AnOmittedDescriptionKeepsTheCurrentOne(t *testing.T) {
	fake := &fakeCron{
		lists:      []*runv1.ListSchedulesResponse{listOf(manualSummary("other"), dailySummary())},
		upsertResp: savedDaily("0 2 * * *", "Runs daily at 1am", false),
	}

	stdout, _, exit := runCron(t, NewSetCronCommand, fake, []string{"daily", "0 2 * * *", "UTC"}, false, "")

	require.Equal(t, 0, exit, stdout)
	require.Len(t, fake.upserts, 1)
	assert.Equal(t, "Runs daily at 1am", fake.upserts[0].GetDescription(), "the description of the schedule as listed is sent back")
	assert.Equal(t, "Runs daily at 1am", decodeSetCron(t, stdout).Description)
}

func TestSetCron_AGivenDescriptionReplacesTheCurrentOne(t *testing.T) {
	fake := &fakeCron{
		lists:      []*runv1.ListSchedulesResponse{listOf(dailySummary())},
		upsertResp: savedDaily("0 2 * * *", "Nightly load", false),
	}

	_, _, exit := runCron(t, NewSetCronCommand, fake, []string{"daily", "0 2 * * *", "UTC", "Nightly load"}, false, "")

	require.Equal(t, 0, exit)
	require.Len(t, fake.upserts, 1)
	assert.Equal(t, "Nightly load", fake.upserts[0].GetDescription())
}

// The last write wins: no expected version is sent, so another editor's save a
// moment earlier is replaced rather than refused.
func TestSetCron_SendsNoExpectedVersion(t *testing.T) {
	fake := &fakeCron{
		lists:      []*runv1.ListSchedulesResponse{listOf(dailySummary())},
		upsertResp: savedDaily("0 2 * * *", "", false),
	}

	_, _, exit := runCron(t, NewSetCronCommand, fake, []string{"daily", "0 2 * * *", "UTC"}, false, "")

	require.Equal(t, 0, exit)
	require.Len(t, fake.upserts, 1)
	assert.Zero(t, fake.upserts[0].GetExpectedVersion(), "set-cron never checks a version")
}

// A paused schedule keeps its pause when its cron changes; "schedule resume"
// is what starts it.
func TestSetCron_APausedScheduleStaysPaused(t *testing.T) {
	fake := &fakeCron{
		lists:      []*runv1.ListSchedulesResponse{listOf(pausedHourlySummary())},
		upsertResp: &runv1.UpsertScheduleResponse{Schedule: &runv1.Schedule{ScheduleName: "hourly", Cron: "30 * * * *", Timezone: "UTC", Paused: true, Version: 6}},
	}

	stdout, _, exit := runCron(t, NewSetCronCommand, fake, []string{"hourly", "30 * * * *", "UTC"}, false, "")

	require.Equal(t, 0, exit, stdout)
	require.Len(t, fake.upserts, 1)
	assert.True(t, fake.upserts[0].GetPaused())
	out := decodeSetCron(t, stdout)
	assert.True(t, out.Paused)
	assert.Empty(t, out.NextFireTimes, "a paused schedule has no next fire")
}

func TestSetCron_ACronWithSpacesIsOneArgumentSentVerbatim(t *testing.T) {
	fake := &fakeCron{
		lists:      []*runv1.ListSchedulesResponse{listOf(dailySummary())},
		upsertResp: savedDaily("*/15 9-17 * * 1-5", "", false),
	}

	_, _, exit := runCron(t, NewSetCronCommand, fake, []string{"daily", "*/15 9-17 * * 1-5", "UTC"}, false, "")

	require.Equal(t, 0, exit)
	require.Len(t, fake.upserts, 1)
	assert.Equal(t, "*/15 9-17 * * 1-5", fake.upserts[0].GetCron())
	assert.Equal(t, "UTC", fake.upserts[0].GetTimezone())
}

// A cron of five fields that was not quoted reaches the command as five
// arguments. That is a usage error that says to quote it, and nothing is
// written.
func TestSetCron_AnUnquotedCronExits2AndSaysToQuoteIt(t *testing.T) {
	fake := &fakeCron{}

	stdout, _, exit := runCron(t, NewSetCronCommand, fake, []string{"daily", "0", "1", "*", "*", "*", "Europe/Paris"}, false, "")

	assert.Equal(t, 2, exit)
	e := errorOf(t, stdout)
	assert.Equal(t, output.CodeUsage, e.Code)
	assert.Contains(t, e.Message, "quote")
	assert.Empty(t, fake.calls, "no RPC is made")
}

func TestSetCron_TooFewArgumentsExits2(t *testing.T) {
	fake := &fakeCron{}

	stdout, _, exit := runCron(t, NewSetCronCommand, fake, []string{"daily", "0 2 * * *"}, false, "")

	assert.Equal(t, 2, exit)
	assert.Equal(t, output.CodeUsage, errorOf(t, stdout).Code)
	assert.Empty(t, fake.calls)
}

func TestSetCron_ABlankArgumentExits2(t *testing.T) {
	for name, args := range map[string][]string{
		"name":        {" ", "0 2 * * *", "UTC"},
		"cron":        {"daily", "", "UTC"},
		"timezone":    {"daily", "0 2 * * *", "  "},
		"description": {"daily", "0 2 * * *", "UTC", ""},
	} {
		t.Run(name, func(t *testing.T) {
			fake := &fakeCron{}

			stdout, _, exit := runCron(t, NewSetCronCommand, fake, args, false, "")

			assert.Equal(t, 2, exit)
			e := errorOf(t, stdout)
			assert.Equal(t, output.CodeUsage, e.Code)
			assert.Contains(t, e.Message, name)
			assert.Empty(t, fake.calls, "a blank %s is refused before any RPC", name)
		})
	}
}

// A blank description is refused rather than read as "clear it" or as "keep
// it": keeping is what omitting the argument means.
func TestSetCron_ABlankDescriptionSaysToOmitIt(t *testing.T) {
	stdout, _, exit := runCron(t, NewSetCronCommand, &fakeCron{}, []string{"daily", "0 2 * * *", "UTC", " "}, false, "")

	assert.Equal(t, 2, exit)
	assert.Contains(t, errorOf(t, stdout).Message, "omit")
}

// Schedule names are case-sensitive tags. "Daily" is not "daily": it keeps
// nothing of the listed schedule and goes to the server as typed.
func TestSetCron_ANameDifferingOnlyByCaseIsAnotherSchedule(t *testing.T) {
	fake := &fakeCron{
		lists:     []*runv1.ListSchedulesResponse{listOf(dailySummary())},
		upsertErr: status.Error(codes.NotFound, "no model of the live release has the tag Daily"),
	}

	stdout, _, exit := runCron(t, NewSetCronCommand, fake, []string{"Daily", "0 2 * * *", "UTC"}, false, "")

	assert.Equal(t, 3, exit)
	assert.Equal(t, output.CodeNotFound, errorOf(t, stdout).Code)
	require.Len(t, fake.upserts, 1)
	assert.Equal(t, "Daily", fake.upserts[0].GetScheduleName())
	assert.Empty(t, fake.upserts[0].GetDescription(), "the description of 'daily' is not borrowed")
	assert.False(t, fake.upserts[0].GetPaused())
}

func TestSetCron_ANameWithNoModelsAndNoCronExits3(t *testing.T) {
	fake := &fakeCron{upsertErr: status.Error(codes.NotFound, "no model of the live release has the tag ghost")}

	stdout, _, exit := runCron(t, NewSetCronCommand, fake, []string{"ghost", "0 2 * * *", "UTC"}, false, "")

	assert.Equal(t, 3, exit)
	e := errorOf(t, stdout)
	assert.Equal(t, output.CodeNotFound, e.Code)
	assert.Contains(t, e.Message, "ghost")
	assert.Empty(t, fake.previews, "nothing was saved, so no words are read")
}

func TestSetCron_ACronTheParserRefusesExits2WithItsMessage(t *testing.T) {
	fake := &fakeCron{
		lists:     []*runv1.ListSchedulesResponse{listOf(dailySummary())},
		upsertErr: status.Error(codes.InvalidArgument, "cron: expected exactly 5 fields, found 3: [a b c]"),
	}

	stdout, _, exit := runCron(t, NewSetCronCommand, fake, []string{"daily", "a b c", "UTC"}, false, "")

	assert.Equal(t, 2, exit)
	e := errorOf(t, stdout)
	assert.Equal(t, output.CodeUsage, e.Code)
	assert.Equal(t, "cron: expected exactly 5 fields, found 3: [a b c]", e.Message)
}

// The schedule is saved when the words cannot be read, so the command still
// succeeds; the words are left out.
func TestSetCron_StillSucceedsWhenTheWordsCannotBeRead(t *testing.T) {
	fake := &fakeCron{
		lists:      []*runv1.ListSchedulesResponse{listOf(dailySummary())},
		upsertResp: savedDaily("0 2 * * *", "", false, time.Date(2026, 10, 28, 1, 0, 0, 0, time.UTC)),
		previewErr: status.Error(codes.Unavailable, "down"),
	}

	stdout, _, exit := runCron(t, NewSetCronCommand, fake, []string{"daily", "0 2 * * *", "UTC"}, false, "")

	require.Equal(t, 0, exit, stdout)
	var raw map[string]json.RawMessage
	require.NoError(t, json.Unmarshal([]byte(stdout), &raw))
	assert.NotContains(t, raw, "cron_words", "words that were not read are omitted, not blank")
	assert.Len(t, decodeSetCron(t, stdout).NextFireTimes, 1)
}

func TestSetCron_ForwardsTheActor(t *testing.T) {
	fake := &fakeCron{
		lists:      []*runv1.ListSchedulesResponse{listOf(dailySummary())},
		upsertResp: savedDaily("0 2 * * *", "", false),
	}

	_, _, exit := runCron(t, NewSetCronCommand, fake, []string{"daily", "0 2 * * *", "UTC"}, false, "okta.example.com|alice")

	require.Equal(t, 0, exit)
	assert.Equal(t, []string{"okta.example.com|alice"}, fake.actors)
}

func TestSetCron_MaintenanceExits7(t *testing.T) {
	fake := &fakeCron{
		lists:     []*runv1.ListSchedulesResponse{listOf(dailySummary())},
		upsertErr: maintenanceRefusal(),
	}

	stdout, _, exit := runCron(t, NewSetCronCommand, fake, []string{"daily", "0 2 * * *", "UTC"}, false, "")

	assert.Equal(t, 7, exit)
	assert.Equal(t, output.CodeMaintenance, errorOf(t, stdout).Code)
}

func TestSetCron_UnavailableExits5(t *testing.T) {
	fake := &fakeCron{listErrs: []error{status.Error(codes.Unavailable, "down")}}

	_, _, exit := runCron(t, NewSetCronCommand, fake, []string{"daily", "0 2 * * *", "UTC"}, false, "")

	assert.Equal(t, 5, exit)
	assert.Empty(t, fake.upserts)
}

func TestSetCron_HumanModeWritesOneSummaryToStderr(t *testing.T) {
	fake := &fakeCron{
		lists: []*runv1.ListSchedulesResponse{listOf(dailySummary())},
		upsertResp: savedDaily("0 2 * * *", "", false,
			time.Date(2026, 10, 28, 1, 0, 0, 0, time.UTC),
			time.Date(2026, 10, 29, 1, 0, 0, 0, time.UTC)),
		previewResp: &runv1.PreviewScheduleResponse{CronWords: "At 02:00 AM"},
	}

	stdout, stderr, exit := runCron(t, NewSetCronCommand, fake, []string{"daily", "0 2 * * *", "Europe/Paris"}, true, "")

	require.Equal(t, 0, exit)
	assert.Empty(t, stdout)
	assert.Contains(t, stderr, "daily")
	assert.Contains(t, stderr, "0 2 * * *")
	assert.Contains(t, stderr, "At 02:00 AM")
	assert.Contains(t, stderr, "2026-10-28T01:00:00Z")
}

func TestSetCron_OutputMatchesItsSchema(t *testing.T) {
	fake := &fakeCron{
		lists: []*runv1.ListSchedulesResponse{listOf(dailySummary())},
		upsertResp: savedDaily("0 2 * * *", "Runs daily at 1am", false,
			time.Date(2026, 10, 28, 1, 0, 0, 0, time.UTC)),
		previewResp: &runv1.PreviewScheduleResponse{CronWords: "At 02:00 AM"},
	}
	stdout, _, exit := runCron(t, NewSetCronCommand, fake, []string{"daily", "0 2 * * *", "UTC"}, false, "")
	require.Equal(t, 0, exit)

	assertEmitsItsSchema(t, NewSetCronCommand(nil, &config.Config{}, &bytes.Buffer{}, &bytes.Buffer{}), stdout)
}

// assertEmitsItsSchema holds a command's output_schema annotation to the JSON
// it printed: the same keys at every level. The fixture must carry every
// optional field.
func assertEmitsItsSchema(t *testing.T, cmd *cobra.Command, stdout string) {
	t.Helper()
	var schema, emitted map[string]any
	require.NoError(t, json.Unmarshal([]byte(cmd.Annotations["output_schema"]), &schema))
	require.NoError(t, json.Unmarshal([]byte(stdout), &emitted))
	assertSameKeys(t, "", schema, emitted)
}

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

	// upsertErrs are the errors of the first upserts, in order; upsertErr
	// answers every upsert after them.
	upsertErrs []error
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
	n := len(f.upserts)
	f.upserts = append(f.upserts, req)
	f.actors = append(f.actors, actor)
	if n < len(f.upsertErrs) && f.upsertErrs[n] != nil {
		return nil, f.upsertErrs[n]
	}
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
		CronWords:    "At 01:00",
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
		previewResp: &runv1.PreviewScheduleResponse{CronWords: "At 02:00"},
	}

	stdout, stderr, exit := runCron(t, NewSetCronCommand, fake, []string{"daily", "0 2 * * *", "Europe/Paris"}, false, "")

	require.Equal(t, 0, exit, stdout)
	assert.Empty(t, stderr)
	out := decodeSetCron(t, stdout)
	assert.Equal(t, "daily", out.ScheduleName)
	assert.Equal(t, "0 2 * * *", out.Cron)
	assert.Equal(t, "At 02:00", out.CronWords, "the words come from PreviewSchedule")
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

// The version of the schedule as listed travels with the save, so an edit made
// since the list is refused rather than overwritten.
func TestSetCron_SendsTheVersionItRead(t *testing.T) {
	fake := &fakeCron{
		lists:      []*runv1.ListSchedulesResponse{listOf(dailySummary())},
		upsertResp: savedDaily("0 2 * * *", "", false),
	}

	_, _, exit := runCron(t, NewSetCronCommand, fake, []string{"daily", "0 2 * * *", "UTC"}, false, "")

	require.Equal(t, 0, exit)
	require.Len(t, fake.upserts, 1)
	assert.EqualValues(t, 3, fake.upserts[0].GetExpectedVersion(), "the version of the schedule as listed")
}

// A schedule with no cron has no version to check: its first cron is saved
// with none, whether the live release lists the name or no row exists yet.
func TestSetCron_AFirstCronSendsNoVersion(t *testing.T) {
	for name, list := range map[string]*runv1.ListSchedulesResponse{
		"listed with no cron": listOf(manualSummary("ghost")),
		"not listed":          listOf(dailySummary()),
	} {
		t.Run(name, func(t *testing.T) {
			fake := &fakeCron{
				lists:      []*runv1.ListSchedulesResponse{list},
				upsertResp: savedDaily("0 2 * * *", "", false),
			}

			_, _, exit := runCron(t, NewSetCronCommand, fake, []string{"ghost", "0 2 * * *", "UTC"}, false, "")

			require.Equal(t, 0, exit)
			require.Len(t, fake.upserts, 1)
			assert.Zero(t, fake.upserts[0].GetExpectedVersion(), "a first cron is saved with no version check")
		})
	}
}

// Another edit saved between the list and the save is ABORTED. set-cron reads
// the schedule again and saves once more, with the version and the paused flag
// and description that edit left: the cron is the only thing it replaces.
func TestSetCron_ARetryReReadsAndKeepsAConcurrentPauseAndDescription(t *testing.T) {
	pausedMeanwhile := dailySummary()
	pausedMeanwhile.Paused = true
	pausedMeanwhile.Description = "Held for the audit"
	pausedMeanwhile.Version = 4
	pausedMeanwhile.UpdatedBy = "bob@example.com"
	fake := &fakeCron{
		lists:      []*runv1.ListSchedulesResponse{listOf(dailySummary()), listOf(pausedMeanwhile)},
		upsertErrs: []error{status.Error(codes.Aborted, "schedule \"daily\" is at version 4, not 3")},
		upsertResp: &runv1.UpsertScheduleResponse{Schedule: &runv1.Schedule{
			ScheduleName: "daily", Cron: "0 2 * * *", Timezone: "UTC", Description: "Held for the audit", Paused: true, Version: 5,
		}},
	}

	stdout, _, exit := runCron(t, NewSetCronCommand, fake, []string{"daily", "0 2 * * *", "UTC"}, false, "")

	require.Equal(t, 0, exit, stdout)
	require.Len(t, fake.upserts, 2, "one retry")
	first, second := fake.upserts[0], fake.upserts[1]
	assert.EqualValues(t, 3, first.GetExpectedVersion())
	assert.False(t, first.GetPaused())
	assert.Equal(t, "Runs daily at 1am", first.GetDescription())
	assert.EqualValues(t, 4, second.GetExpectedVersion(), "the retry carries the version of the new read")
	assert.True(t, second.GetPaused(), "the pause saved in between is kept")
	assert.Equal(t, "Held for the audit", second.GetDescription(), "the description saved in between is kept")
	assert.Equal(t, "0 2 * * *", second.GetCron(), "the cron is still the one asked for")
	assert.Equal(t, []string{"list", "upsert", "list", "upsert"}, fake.calls[:4], "the retry reads the schedules again before saving")
	out := decodeSetCron(t, stdout)
	assert.True(t, out.Paused)
	assert.EqualValues(t, 5, out.Version)
}

// A description given on the command line is the one asked for: a retry sends
// it again rather than the description another edit left.
func TestSetCron_ARetryKeepsAGivenDescription(t *testing.T) {
	changed := dailySummary()
	changed.Description = "Edited meanwhile"
	changed.Version = 4
	fake := &fakeCron{
		lists:      []*runv1.ListSchedulesResponse{listOf(dailySummary()), listOf(changed)},
		upsertErrs: []error{status.Error(codes.Aborted, "stale")},
		upsertResp: savedDaily("0 2 * * *", "Nightly load", false),
	}

	_, _, exit := runCron(t, NewSetCronCommand, fake, []string{"daily", "0 2 * * *", "UTC", "Nightly load"}, false, "")

	require.Equal(t, 0, exit)
	require.Len(t, fake.upserts, 2)
	assert.Equal(t, "Nightly load", fake.upserts[1].GetDescription())
	assert.EqualValues(t, 4, fake.upserts[1].GetExpectedVersion())
}

// A schedule whose cron another editor removed between the reads has no
// version any more, so its retry is saved as a first cron.
func TestSetCron_ARetryAfterTheCronWasRemovedSendsNoVersion(t *testing.T) {
	fake := &fakeCron{
		lists:      []*runv1.ListSchedulesResponse{listOf(dailySummary()), listOf(manualSummary("daily"))},
		upsertErrs: []error{status.Error(codes.Aborted, "removed")},
		upsertResp: savedDaily("0 2 * * *", "", false),
	}

	_, _, exit := runCron(t, NewSetCronCommand, fake, []string{"daily", "0 2 * * *", "UTC"}, false, "")

	require.Equal(t, 0, exit)
	require.Len(t, fake.upserts, 2)
	assert.Zero(t, fake.upserts[1].GetExpectedVersion())
	assert.False(t, fake.upserts[1].GetPaused())
}

// A second conflict is reported, as pause does, and nothing is saved a third
// time.
func TestSetCron_ASecondConflictExitsWithTheConflictCode(t *testing.T) {
	changed := dailySummary()
	changed.Version = 4
	changed.UpdatedBy = "bob@example.com"
	fake := &fakeCron{
		lists:     []*runv1.ListSchedulesResponse{listOf(dailySummary()), listOf(changed)},
		upsertErr: status.Error(codes.Aborted, "the schedule was changed by another edit"),
	}

	stdout, _, exit := runCron(t, NewSetCronCommand, fake, []string{"daily", "0 2 * * *", "UTC"}, false, "")

	assert.Equal(t, 4, exit)
	e := errorOf(t, stdout)
	assert.Equal(t, output.CodeConflict, e.Code)
	assert.Equal(t, "changed by bob@example.com while this ran; run it again", e.Message)
	require.Len(t, fake.upserts, 2, "one retry, no more")
	assert.EqualValues(t, 4, fake.upserts[1].GetExpectedVersion())
	assert.Empty(t, fake.previews, "nothing was saved, so no words are read")
}

// Only a conflict is retried: a refusal that another read cannot change is
// returned at once.
func TestSetCron_OtherErrorsAreNotRetried(t *testing.T) {
	for name, tc := range map[string]struct {
		err  error
		exit int
	}{
		"invalid argument": {status.Error(codes.InvalidArgument, "cron: never fires"), 2},
		"not found":        {status.Error(codes.NotFound, "no model has the tag"), 3},
		"internal":         {status.Error(codes.Internal, "boom"), 6},
		"maintenance":      {maintenanceRefusal(), 7},
	} {
		t.Run(name, func(t *testing.T) {
			fake := &fakeCron{
				lists:     []*runv1.ListSchedulesResponse{listOf(dailySummary())},
				upsertErr: tc.err,
			}

			_, _, exit := runCron(t, NewSetCronCommand, fake, []string{"daily", "0 2 * * *", "UTC"}, false, "")

			assert.Equal(t, tc.exit, exit)
			assert.Len(t, fake.upserts, 1, "saved once")
			assert.Equal(t, 1, fake.listed, "read once")
		})
	}
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
	assert.Contains(t, e.Message, "ONE argument")
	assert.Contains(t, e.Message, "quote", "a shell needs the five fields quoted to pass them as one")
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
	e := errorOf(t, stdout)
	assert.Equal(t, output.CodeNotFound, e.Code)
	assert.Contains(t, e.Message, "no model of the live release has the tag Daily", "the server's reason is kept")
	assert.Contains(t, e.Message, "did you mean 'daily'?", "the listed name that differs only by case is offered")
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
	assert.NotContains(t, e.Message, "did you mean", "no listed name resembles it")
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
		previewResp: &runv1.PreviewScheduleResponse{CronWords: "At 02:00"},
	}

	stdout, stderr, exit := runCron(t, NewSetCronCommand, fake, []string{"daily", "0 2 * * *", "Europe/Paris"}, true, "")

	require.Equal(t, 0, exit)
	assert.Empty(t, stdout)
	assert.Contains(t, stderr, "daily")
	assert.Contains(t, stderr, "0 2 * * *")
	assert.Contains(t, stderr, "At 02:00")
	assert.Contains(t, stderr, "2026-10-28T01:00:00Z")
}

func TestSetCron_OutputMatchesItsSchema(t *testing.T) {
	fake := &fakeCron{
		lists: []*runv1.ListSchedulesResponse{listOf(dailySummary())},
		upsertResp: savedDaily("0 2 * * *", "Runs daily at 1am", false,
			time.Date(2026, 10, 28, 1, 0, 0, 0, time.UTC)),
		previewResp: &runv1.PreviewScheduleResponse{CronWords: "At 02:00"},
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

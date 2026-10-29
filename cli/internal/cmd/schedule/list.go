package schedule

import (
	"context"
	"fmt"
	"io"

	"github.com/carolsimone/continuo/cli/internal/config"
	"github.com/carolsimone/continuo/cli/internal/output"
	"github.com/carolsimone/continuo/cli/internal/runview"
	runv1 "github.com/carolsimone/continuo/cli/proto/run/v1"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// scheduleEntry is the JSON-serialisable representation of a single schedule.
// Fields with omitempty are conditionally omitted:
//   - description:  omit when empty
//   - cron_words:   omit with no cron, or when the words cannot describe it
//   - next_fire_at: omit while paused, with no cron, or when the cron fires no more
//   - last_run_at:  omit when there is no prior run (nil proto Timestamp)
//   - updated_by, updated_at: omit with no cron (nobody has edited it)
type scheduleEntry struct {
	ScheduleName  string `json:"schedule_name"`
	Cron          string `json:"cron"`
	CronWords     string `json:"cron_words,omitempty"`
	HasCron       bool   `json:"has_cron"`
	Paused        bool   `json:"paused"`
	Version       int64  `json:"version"`
	NextFireAt    string `json:"next_fire_at,omitempty"`
	Description   string `json:"description,omitempty"`
	Timezone      string `json:"timezone"`
	ModelCount    int32  `json:"model_count"`
	HasActiveRun  bool   `json:"has_active_run"`
	LastRunId     string `json:"last_run_id"`
	LastRunStatus string `json:"last_run_status"`
	LastRunAt     string `json:"last_run_at,omitempty"`
	UpdatedBy     string `json:"updated_by,omitempty"`
	UpdatedAt     string `json:"updated_at,omitempty"`
}

// listPayload is the top-level JSON object emitted on success.
// Schedules is initialised to an empty slice so the JSON output is always
// `{"schedules":[]}` rather than `{"schedules":null}`.
type listPayload struct {
	Schedules []scheduleEntry `json:"schedules"`
}

// NewListCommand builds `continuo schedule list`.
func NewListCommand(factory RunClientFactory, cfg *config.Config, stdout, stderr io.Writer) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List all schedules: their cron, pause, models and last run",
		Long: `List all schedules: their cron, pause, models and last run.

Use when the user wants to see which schedules exist, when each runs on its own,
whether one is paused, who last changed a cron, or the outcome of each
schedule's most recent run. It is also how to read a schedule before changing it
with "schedule set-cron", "schedule pause", "schedule resume" or "schedule remove-cron".

A schedule is a schedule name: the models of the live release whose dbt tag
names it. The list holds every name the live release has and every name that
has a cron, ordered by name.

Arguments: none.

Output (stdout, JSON):
  {"schedules":[{"schedule_name":string,"cron":string,
   "cron_words":string,"has_cron":bool,"paused":bool,"version":number,
   "next_fire_at":string,"description":string,"timezone":string,"model_count":number,
   "has_active_run":bool,"last_run_id":string,"last_run_status":string,
   "last_run_at":string,"updated_by":string,"updated_at":string}]}
  A schedule is scheduled (has_cron true, paused false), paused (has_cron true,
  paused true) or manual (has_cron false: it runs only when triggered, and
  cron and timezone are empty). cron_words is the cron in English. cron and
  version are the fields "schedule set-cron", "schedule pause" and "schedule
  resume" print for the schedule they saved; version counts the edits of the
  cron, from 1, and is 0 while the schedule has no cron.
  next_fire_at (RFC3339, UTC) is omitted while paused, with no cron, or when the
  cron fires no more. model_count is how many models of the live release carry
  the schedule's tag; a scheduled schedule with model_count 0 records a failed
  run at each fire until a release brings its models back, it is paused, or its
  cron is removed. updated_by and updated_at say who last changed the cron and
  when (RFC3339, UTC). description, cron_words, next_fire_at, last_run_at,
  updated_by and updated_at are omitted when empty (e.g. never run).
  last_run_status is one of pending|running|cancelling|succeeded|failed|cancelled|skipped;
  cancelling means a cancel was accepted and the run waits for its Jobs to
  stop. has_active_run is true while a pending, running or cancelling run holds the schedule.

Errors:
  usage      (exit 2)  an argument was given
  unavailable(exit 5)  the run-controller service is unreachable
  internal   (exit 6)  unexpected server error`,
		Example: "  continuo schedule list",
		Annotations: map[string]string{
			"output_schema": `{"schedules":[{"schedule_name":"string","cron":"string","cron_words":"string","has_cron":"bool","paused":"bool","version":"number","next_fire_at":"string","description":"string","timezone":"string","model_count":"number","has_active_run":"bool","last_run_id":"string","last_run_status":"string","last_run_at":"string","updated_by":"string","updated_at":"string"}]}`,
			"exit_codes":    `[0,2,5,6]`,
		},
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 0 {
				return emit(stdout, stderr, humanOutput(cmd), output.NewUsageError("list takes no arguments"))
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx, cancel := context.WithTimeout(cmd.Context(), cfg.Timeout)
			defer cancel()

			c, err := factory(ctx, cfg.RunControllerEndpoint)
			if err != nil {
				return emit(stdout, stderr, cfg.Human, output.FromGRPC(err))
			}
			defer func() { _ = c.Close() }()

			resp, err := c.ListAllSchedules(ctx)
			if err != nil {
				return emit(stdout, stderr, cfg.Human, output.FromGRPC(err))
			}

			if cfg.Human {
				return humanList(stderr, resp.GetSchedules())
			}
			return output.EmitSuccess(stdout, toListPayload(resp.GetSchedules()))
		},
	}
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	return cmd
}

// toListPayload converts the proto slice into the JSON payload.
func toListPayload(summaries []*runv1.ScheduleSummary) listPayload {
	entries := make([]scheduleEntry, 0, len(summaries))
	for _, s := range summaries {
		entries = append(entries, scheduleEntry{
			ScheduleName:  s.GetScheduleName(),
			Cron:          s.GetCron(),
			CronWords:     s.GetCronWords(),
			HasCron:       s.GetCron() != "",
			Paused:        s.GetPaused(),
			Version:       s.GetVersion(),
			NextFireAt:    runview.Timestamp(s.GetNextFireAt()),
			Description:   s.GetDescription(),
			Timezone:      s.GetTimezone(),
			ModelCount:    s.GetModelCount(),
			HasActiveRun:  s.GetHasActiveRun(),
			LastRunId:     s.GetLastRunId(),
			LastRunStatus: s.GetLastRunStatus(),
			LastRunAt:     runview.Timestamp(s.GetLastRunAt()),
			UpdatedBy:     s.GetUpdatedBy(),
			UpdatedAt:     runview.Timestamp(s.GetUpdatedAt()),
		})
	}
	return listPayload{Schedules: entries}
}

// humanList writes one line per schedule to stderr in the format:
//
//	<name>  running  <cron>  last: <status> <time>
//	<name>  idle     <cron>  last: <status> <time>
//	<name>  idle     manual
//
// where <cron> is "<cron> <timezone> next <time>" for a scheduled schedule,
// "<cron> <timezone> (paused)" for a paused one and "manual" for one with no
// cron.
func humanList(stderr io.Writer, summaries []*runv1.ScheduleSummary) error {
	for _, s := range summaries {
		state := "idle"
		if s.GetHasActiveRun() {
			state = "running"
		}

		cronPart := "manual"
		if s.GetCron() != "" {
			cronPart = s.GetCron() + " " + s.GetTimezone()
			if s.GetPaused() {
				cronPart += " (paused)"
			} else if next := s.GetNextFireAt(); next != nil && next.IsValid() {
				cronPart += " next " + shortTime(next)
			}
		}

		lastPart := ""
		if s.GetLastRunStatus() != "" {
			lastPart = "  last: " + s.GetLastRunStatus()
			if ts := s.GetLastRunAt(); ts != nil && ts.IsValid() {
				lastPart += " " + shortTime(ts)
			}
		}

		if _, err := fmt.Fprintf(stderr, "%s  %s  %s%s\n", s.GetScheduleName(), state, cronPart, lastPart); err != nil {
			return err
		}
	}
	return nil
}

// shortTime renders ts in UTC to the minute, as "2006-01-02T15:04Z".
func shortTime(ts *timestamppb.Timestamp) string {
	return ts.AsTime().UTC().Format("2006-01-02T15:04Z")
}

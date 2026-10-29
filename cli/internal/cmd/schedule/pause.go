package schedule

import (
	"fmt"
	"io"
	"strings"

	"github.com/carolsimone/continuo/cli/internal/client"
	"github.com/carolsimone/continuo/cli/internal/config"
	"github.com/carolsimone/continuo/cli/internal/output"
	"github.com/carolsimone/continuo/cli/internal/runview"
	runv1 "github.com/carolsimone/continuo/cli/proto/run/v1"
	"github.com/spf13/cobra"
)

// pauseResult is the JSON pause and resume print: the schedule as it stands
// once the command is done. Changed is false when the schedule was in the
// requested state already and nothing was saved. NextFireAt is the next time
// the cron fires, omitted while the schedule is paused or when its cron fires
// no more.
type pauseResult struct {
	ScheduleName string `json:"schedule_name"`
	Paused       bool   `json:"paused"`
	Changed      bool   `json:"changed"`
	Cron         string `json:"cron"`
	Timezone     string `json:"timezone"`
	Version      int64  `json:"version"`
	NextFireAt   string `json:"next_fire_at,omitempty"`
}

// pauseSpec describes one of the two commands that set the paused flag of a
// schedule. They differ in the state they leave the schedule in and in the
// words that tell the caller about it.
type pauseSpec struct {
	use, short, long, example, outputSchema string
	// paused is the state the command leaves the schedule in.
	paused bool
	// verb and done are the command's name and its past tense, for messages.
	verb, done string
	// already is the state a schedule is in when the command changes nothing,
	// as it reads in "Schedule 'x' is <already>; nothing changed".
	already string
}

// NewPauseCommand builds `continuo schedule pause <name>`.
func NewPauseCommand(factory RunClientFactory, cfg *config.Config, stdout, stderr io.Writer) *cobra.Command {
	return newPauseResumeCommand(pauseSpec{
		use:   "pause <name>",
		short: "Pause a schedule: nothing starts on its own until it is resumed",
		long: `Pause a schedule: nothing starts on its own until it is resumed.

Use when the user asks to pause, hold, stop or disable the automatic runs of a
schedule (during an upstream outage, or after a data incident) without losing its cron.
The cron, timezone and description are kept. A run of the schedule already in
progress goes on, and a manual "schedule trigger" still works. Fires that come
due while it is paused do not run later.

The command reads the schedule and saves it back paused, with the version it
read. Pausing a schedule that is paused already succeeds and changes nothing
("changed":false).

Arguments:
  <name>  The schedule to pause, exactly as "schedule list" shows it. Names are
          case-sensitive: "Daily" is not "daily".

Output (stdout, JSON):
  {"schedule_name":string,"paused":bool,"changed":bool,"cron":string,
   "timezone":string,"version":number}
  paused is true. changed is false when the schedule was paused already.
  version is the schedule's version after the command.

Errors:
  usage      (exit 2)  wrong number of arguments, a blank name, or a name longer than 50 characters (the server refuses it)
  not_found  (exit 3)  no schedule has that name, or the schedule has no cron and so nothing to pause
  conflict   (exit 4)  another edit saved the schedule while this ran ("changed by <who> while this ran; run it again"); nothing was changed, so run the command again
  unavailable(exit 5)  the run-controller service is unreachable
  maintenance(exit 7)  continuo is in maintenance mode; edits are refused until it is turned off (not retryable)
  internal   (exit 6)  unexpected server error`,
		example:      "  continuo schedule pause hourly",
		outputSchema: `{"schedule_name":"string","paused":"bool","changed":"bool","cron":"string","timezone":"string","version":"number"}`,
		paused:       true,
		verb:         "pause",
		done:         "Paused",
		already:      "already paused",
	}, factory, cfg, stdout, stderr)
}

// newPauseResumeCommand builds a command that sets the paused flag of the
// named schedule to spec.paused. It lists the schedules, finds the name, and
// saves the schedule back with its cron, timezone and description unchanged and
// its version as the expected version, so an edit made in between is refused
// rather than overwritten. A schedule already in the requested state is left
// alone, because a save would raise its version and credit the caller.
func newPauseResumeCommand(spec pauseSpec, factory RunClientFactory, cfg *config.Config, stdout, stderr io.Writer) *cobra.Command {
	cmd := &cobra.Command{
		Use:     spec.use,
		Short:   spec.short,
		Long:    spec.long,
		Example: spec.example,
		Annotations: map[string]string{
			"output_schema": spec.outputSchema,
			"exit_codes":    `[0,2,3,4,5,6,7]`,
			"mutating":      "true",
		},
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 || strings.TrimSpace(args[0]) == "" {
				return emit(stdout, stderr, humanOutput(cmd), output.NewUsageError(spec.verb+" requires exactly one argument: <name>"))
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]

			c, err := factory(cmd.Context(), cfg.RunControllerEndpoint)
			if err != nil {
				return emit(stdout, stderr, cfg.Human, output.FromGRPC(err))
			}
			defer func() { _ = c.Close() }()

			all, err := listSchedules(cmd, cfg, c)
			if err != nil {
				return emit(stdout, stderr, cfg.Human, output.FromGRPC(err))
			}
			current := findSchedule(all, name)
			if current == nil {
				return emit(stdout, stderr, cfg.Human, noSuchSchedule(name, all))
			}
			if current.GetCron() == "" {
				return emit(stdout, stderr, cfg.Human, output.CLIError{
					Code: output.CodeNotFound,
					Message: fmt.Sprintf("schedule '%s' has no cron, so there is nothing to %s; add one with \"schedule set-cron\"",
						name, spec.verb),
				})
			}

			result := pauseResult{
				ScheduleName: name,
				Paused:       current.GetPaused(),
				Cron:         current.GetCron(),
				Timezone:     current.GetTimezone(),
				Version:      current.GetVersion(),
				NextFireAt:   runview.Timestamp(current.GetNextFireAt()),
			}
			if current.GetPaused() != spec.paused {
				ctx, cancel := callContext(cmd, cfg)
				resp, err := c.UpsertSchedule(ctx, cfg.Actor, &runv1.UpsertScheduleRequest{
					ScheduleName:    name,
					Cron:            current.GetCron(),
					Timezone:        current.GetTimezone(),
					Description:     current.GetDescription(),
					Paused:          spec.paused,
					ExpectedVersion: current.GetVersion(),
				})
				cancel()
				if err != nil {
					return emit(stdout, stderr, cfg.Human, saveError(cmd, cfg, c, name, err))
				}
				saved := resp.GetSchedule()
				result.Changed = true
				result.Paused = saved.GetPaused()
				result.Cron = saved.GetCron()
				result.Timezone = saved.GetTimezone()
				result.Version = saved.GetVersion()
				result.NextFireAt = runview.Timestamp(saved.GetNextFireAt())
			}

			if cfg.Human {
				return output.HumanSuccess(stderr, humanPauseResult(spec, result))
			}
			return output.EmitSuccess(stdout, result)
		},
	}
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	return cmd
}

// noSuchSchedule is the not_found error for a name no schedule has. A name that
// differs from a listed one only by case names that one: schedule names are
// tags and are case-sensitive.
func noSuchSchedule(name string, all []*runv1.ScheduleSummary) output.CLIError {
	return output.CLIError{Code: output.CodeNotFound, Message: fmt.Sprintf("no schedule named '%s'", name) + caseHint(name, all)}
}

// caseHint is the sentence a not_found for name ends with when a listed
// schedule differs from it only by case: schedule names are tags and are
// case-sensitive. It is empty when no listed name resembles name.
func caseHint(name string, all []*runv1.ScheduleSummary) string {
	var similar []string
	for _, s := range all {
		if strings.EqualFold(s.GetScheduleName(), name) {
			similar = append(similar, "'"+s.GetScheduleName()+"'")
		}
	}
	if len(similar) == 0 {
		return ""
	}
	return "; schedule names are case-sensitive, did you mean " + strings.Join(similar, " or ") + "?"
}

// saveError turns a failed save of pause, resume or set-cron into the CLI's
// error. UpsertSchedule answers a conflict only as ABORTED, the expected
// version being stale; the error then says who changed the schedule.
func saveError(cmd *cobra.Command, cfg *config.Config, c client.RunClient, name string, err error) output.CLIError {
	e := output.FromGRPC(err)
	if e.Code != output.CodeConflict {
		return e
	}
	e.Message = "changed by " + whoChanged(cmd, cfg, c, name) + " while this ran; run it again"
	return e
}

// whoChanged names the author of the latest edit of the schedule, read afresh.
// It is "someone" when the read fails or the schedule has no cron any more.
func whoChanged(cmd *cobra.Command, cfg *config.Config, c client.RunClient, name string) string {
	all, err := listSchedules(cmd, cfg, c)
	if err != nil {
		return "someone"
	}
	if s := findSchedule(all, name); s != nil && s.GetUpdatedBy() != "" {
		return s.GetUpdatedBy()
	}
	return "someone"
}

// humanPauseResult is the one line pause and resume write with --human.
func humanPauseResult(spec pauseSpec, r pauseResult) string {
	if !r.Changed {
		return fmt.Sprintf("Schedule '%s' is %s; nothing changed", r.ScheduleName, spec.already)
	}
	line := fmt.Sprintf("%s schedule '%s'", spec.done, r.ScheduleName)
	if r.NextFireAt != "" {
		line += "; next fire " + r.NextFireAt
	}
	return line
}

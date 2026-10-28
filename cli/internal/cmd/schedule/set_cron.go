package schedule

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/carolsimone/continuo/cli/internal/client"
	"github.com/carolsimone/continuo/cli/internal/config"
	"github.com/carolsimone/continuo/cli/internal/output"
	runv1 "github.com/carolsimone/continuo/cli/proto/run/v1"
	"github.com/spf13/cobra"
)

// cronSaved is the JSON set-cron prints: the schedule as saved and its next
// fires. NextFireTimes is always an array, empty while the schedule is paused.
type cronSaved struct {
	ScheduleName  string   `json:"schedule_name"`
	Cron          string   `json:"cron"`
	CronWords     string   `json:"cron_words,omitempty"`
	Timezone      string   `json:"timezone"`
	Description   string   `json:"description,omitempty"`
	Paused        bool     `json:"paused"`
	Version       int64    `json:"version"`
	NextFireTimes []string `json:"next_fire_times"`
}

// setCronTries is how many times set-cron saves the cron when another edit
// keeps saving the schedule first: the save and one retry after reading the
// schedule again.
const setCronTries = 2

// NewSetCronCommand builds `continuo schedule set-cron <name> <cron> <timezone>
// [description]`. It reads the schedules for the description and the pause to
// keep and for the version to expect, then saves the cron. When another edit
// saved the schedule in between it reads again and saves once more, so the
// cron is last-write-wins while a pause or a description saved concurrently is
// never overwritten. The words of the cron come from a preview of the saved
// cron; the schedule is saved whether or not they can be read.
func NewSetCronCommand(factory RunClientFactory, cfg *config.Config, stdout, stderr io.Writer) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "set-cron <name> <cron> <timezone> [description]",
		Short: "Set when a schedule runs on its own: a cron in a timezone",
		Long: `Set when a schedule runs on its own: a cron in a timezone.

Use when the user asks to schedule a schedule, to change the time it runs
automatically, to add a cron to a schedule that has none (one that runs only
when triggered), or to change a cron's timezone or description. The cron decides
only WHEN the schedule runs; which models it runs comes from their dbt tag, so
the name must be a schedule that "schedule list" shows.

Arguments:
  <name>         The schedule name, exactly as "schedule list" shows it. Names
                 are case-sensitive: "Daily" is not "daily".
  <cron>         Five fields (minute hour day-of-month month day-of-week) or a
                 descriptor such as @daily. Pass the five fields as ONE argument
                 (in a shell, quote it): "0 1 * * *". A CRON_TZ= or TZ= prefix
                 is refused: the timezone is the next argument.
  <timezone>     The IANA zone the cron is read in, such as Europe/Paris or UTC.
  [description]  Optional. What the schedule is for. Omit it to keep the current
                 description; an empty value is a usage error.

set-cron sets the cron and the timezone and re-sends the rest of the schedule as
listed: its current paused flag and its description (a [description] given
replaces it). A paused schedule stays paused; "schedule resume" starts it. The
next fire counts from now.
Only a schedule's first cron needs a model of the live release with its tag; a
schedule that has a cron can be changed whatever the live release holds.

set-cron sends the version it read, so a pause or a description saved by another
edit since is never overwritten: it reads the schedule again, saves once more
with what that edit left, and exits 4 if the schedule changed again. The cron
itself is last-write-wins: a cron another editor saved a moment ago is replaced.

Output (stdout, JSON):
  {"schedule_name":string,"cron":string,"cron_words":string,"timezone":string,
   "description":string,"paused":bool,"version":number,"next_fire_times":[string]}
  next_fire_times are the next three fires (RFC3339, UTC), empty while the
  schedule is paused. cron_words is the cron in English and description is the
  saved description; each is omitted when empty.

Errors:
  usage      (exit 2)  wrong number of arguments (a cron whose fields were not passed as one argument arrives as several), a blank argument, or a cron or timezone the parser refuses; the message is the parser's
  not_found  (exit 3)  the schedule has no cron yet and no model of the live release has its tag; a listed name that differs only by case is offered as "did you mean"
  conflict   (exit 4)  another edit saved the schedule twice while this ran ("changed by <who> while this ran; run it again"); nothing was changed by this command, so run it again
  unavailable(exit 5)  the state service is unreachable
  maintenance(exit 7)  continuo is in maintenance mode; edits are refused until it is turned off (not retryable)
  internal   (exit 6)  unexpected server error`,
		Example: `  continuo schedule set-cron daily "0 1 * * *" Europe/Paris  # the five fields are ONE argument; in a shell, quote them
  continuo schedule set-cron daily "0 1 * * *" Europe/Paris "Runs daily at 1am"
  continuo schedule set-cron hourly @hourly UTC`,
		Annotations: map[string]string{
			"output_schema": `{"schedule_name":"string","cron":"string","cron_words":"string","timezone":"string","description":"string","paused":"bool","version":"number","next_fire_times":["string"]}`,
			"exit_codes":    `[0,2,3,4,5,6,7]`,
			"mutating":      "true",
		},
		Args: func(cmd *cobra.Command, args []string) error {
			human := humanOutput(cmd)
			if len(args) < 3 || len(args) > 4 {
				return emit(stdout, stderr, human, output.NewUsageError(
					`set-cron takes <name> <cron> <timezone> and an optional [description]; pass the five fields of the cron as ONE argument (in a shell, quote it), for example "0 1 * * *"`))
			}
			for i, label := range []string{"name", "cron", "timezone", "description"} {
				if i >= len(args) || strings.TrimSpace(args[i]) != "" {
					continue
				}
				msg := "set-cron: " + label + " must not be empty"
				if label == "description" {
					msg += "; omit it to keep the current description"
				}
				return emit(stdout, stderr, human, output.NewUsageError(msg))
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]

			c, err := factory(cmd.Context(), cfg.StateEndpoint)
			if err != nil {
				return emit(stdout, stderr, cfg.Human, output.FromGRPC(err))
			}
			defer func() { _ = c.Close() }()

			resp, failure := saveCron(cmd, cfg, c, args)
			if failure != nil {
				return emit(stdout, stderr, cfg.Human, *failure)
			}

			saved := resp.GetSchedule()
			payload := cronSaved{
				ScheduleName:  name,
				Cron:          saved.GetCron(),
				CronWords:     cronWords(cmd, cfg, c, saved.GetCron(), saved.GetTimezone()),
				Timezone:      saved.GetTimezone(),
				Description:   saved.GetDescription(),
				Paused:        saved.GetPaused(),
				Version:       saved.GetVersion(),
				NextFireTimes: make([]string, 0, len(resp.GetNextFireTimes())),
			}
			for _, ts := range resp.GetNextFireTimes() {
				payload.NextFireTimes = append(payload.NextFireTimes, timestampString(ts))
			}

			if cfg.Human {
				return output.HumanSuccess(stderr, humanCronSaved(payload))
			}
			return output.EmitSuccess(stdout, payload)
		},
	}
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	return cmd
}

// saveCron saves the cron args describe (name, cron, timezone and an optional
// description). Each try reads the schedules and saves with the version it
// read. A save that finds the schedule changed (ABORTED) is tried again, once,
// from a fresh read, so the paused flag and the description it re-sends are
// those of the latest edit. The failure is nil when the cron was saved.
func saveCron(cmd *cobra.Command, cfg *config.Config, c client.RunClient, args []string) (*runv1.UpsertScheduleResponse, *output.CLIError) {
	name := args[0]
	for try := 1; ; try++ {
		all, err := listSchedules(cmd, cfg, c)
		if err != nil {
			e := output.FromGRPC(err)
			return nil, &e
		}
		ctx, cancel := callContext(cmd, cfg)
		resp, err := c.UpsertSchedule(ctx, cfg.Actor, cronRequest(all, args))
		cancel()
		if err == nil {
			return resp, nil
		}
		e := output.FromGRPC(err)
		switch {
		case e.Code == output.CodeConflict && try < setCronTries:
			continue
		case e.Code == output.CodeConflict:
			e = saveError(cmd, cfg, c, name, err)
		case e.Code == output.CodeNotFound:
			e.Message += caseHint(name, all)
		}
		return nil, &e
	}
}

// cronRequest is the save of the cron args describe: the cron and timezone
// asked for, with the description and the paused flag of the schedule as
// listed and the version it was listed at. A description given on the command
// line replaces the listed one. A schedule that is not listed with a cron has
// no version, so its first cron is saved with no check.
func cronRequest(all []*runv1.ScheduleSummary, args []string) *runv1.UpsertScheduleRequest {
	req := &runv1.UpsertScheduleRequest{ScheduleName: args[0], Cron: args[1], Timezone: args[2]}
	if current := findSchedule(all, args[0]); current != nil {
		req.Description = current.GetDescription()
		req.Paused = current.GetPaused()
		req.ExpectedVersion = current.GetVersion()
	}
	if len(args) == 4 {
		req.Description = args[3]
	}
	return req
}

// callContext gives one RPC its own --timeout deadline.
func callContext(cmd *cobra.Command, cfg *config.Config) (context.Context, context.CancelFunc) {
	return context.WithTimeout(cmd.Context(), cfg.Timeout)
}

// listSchedules reads the tenant's schedules.
func listSchedules(cmd *cobra.Command, cfg *config.Config, c client.RunClient) ([]*runv1.ScheduleSummary, error) {
	ctx, cancel := callContext(cmd, cfg)
	defer cancel()
	resp, err := c.ListAllSchedules(ctx)
	if err != nil {
		return nil, err
	}
	return resp.GetSchedules(), nil
}

// findSchedule returns the schedule named name, compared exactly, or nil.
func findSchedule(all []*runv1.ScheduleSummary, name string) *runv1.ScheduleSummary {
	for _, s := range all {
		if s.GetScheduleName() == name {
			return s
		}
	}
	return nil
}

// cronWords reads a saved cron in English. The words are only a courtesy: the
// cron is already saved, so a preview that fails leaves them out.
func cronWords(cmd *cobra.Command, cfg *config.Config, c client.RunClient, cron, timezone string) string {
	ctx, cancel := callContext(cmd, cfg)
	defer cancel()
	resp, err := c.PreviewSchedule(ctx, cron, timezone)
	if err != nil {
		return ""
	}
	return resp.GetCronWords()
}

// humanCronSaved is the one line set-cron writes with --human.
func humanCronSaved(p cronSaved) string {
	line := fmt.Sprintf("Set cron '%s' (%s) on schedule '%s'", p.Cron, p.Timezone, p.ScheduleName)
	if p.CronWords != "" {
		line += ": " + p.CronWords
	}
	if p.Paused {
		return line + "; it is paused, so nothing starts on its own until it is resumed"
	}
	if len(p.NextFireTimes) > 0 {
		line += "; next " + strings.Join(p.NextFireTimes, ", ")
	}
	return line
}

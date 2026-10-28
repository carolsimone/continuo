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

// NewSetCronCommand builds `continuo schedule set-cron <name> <cron> <timezone>
// [description]`. It reads the schedules once, for the description to keep and
// for the pause to keep, then saves the cron with no expected version, so the
// last write wins. The words of the cron come from a preview of the saved cron;
// the schedule is saved whether or not they can be read.
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
                 descriptor such as @daily. A cron of several fields is ONE
                 argument, so quote it: "0 1 * * *". A CRON_TZ= or TZ= prefix is
                 refused: the timezone is the next argument.
  <timezone>     The IANA zone the cron is read in, such as Europe/Paris or UTC.
  [description]  Optional. What the schedule is for. Omit it to keep the current
                 description; an empty value is a usage error.

The last write wins: set-cron sends no version, so it replaces what another
editor saved a moment ago. A paused schedule stays paused; "schedule resume"
starts it. The next fire counts from now.
Only a schedule's first cron needs a model of the live release with its tag; a
schedule that has a cron can be changed whatever the live release holds.

Output (stdout, JSON):
  {"schedule_name":string,"cron":string,"cron_words":string,"timezone":string,
   "description":string,"paused":bool,"version":number,"next_fire_times":[string]}
  next_fire_times are the next three fires (RFC3339, UTC), empty while the
  schedule is paused. cron_words is the cron in English and description is the
  saved description; each is omitted when empty.

Errors:
  usage      (exit 2)  wrong number of arguments (an unquoted cron arrives as several), a blank argument, or a cron or timezone the parser refuses; the message is the parser's
  not_found  (exit 3)  the schedule has no cron yet and no model of the live release has its tag
  unavailable(exit 5)  the state service is unreachable
  maintenance(exit 7)  continuo is in maintenance mode; edits are refused until it is turned off (not retryable)
  internal   (exit 6)  unexpected server error`,
		Example: `  continuo schedule set-cron daily "0 1 * * *" Europe/Paris
  continuo schedule set-cron daily "0 1 * * *" Europe/Paris "Runs daily at 1am"
  continuo schedule set-cron hourly @hourly UTC`,
		Annotations: map[string]string{
			"output_schema": `{"schedule_name":"string","cron":"string","cron_words":"string","timezone":"string","description":"string","paused":"bool","version":"number","next_fire_times":["string"]}`,
			"exit_codes":    `[0,2,3,5,6,7]`,
			"mutating":      "true",
		},
		Args: func(cmd *cobra.Command, args []string) error {
			human := humanOutput(cmd)
			if len(args) < 3 || len(args) > 4 {
				return emit(stdout, stderr, human, output.NewUsageError(
					`set-cron takes <name> <cron> <timezone> and an optional [description]; a cron of several fields is one argument, so quote it, for example "0 1 * * *"`))
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
			name, cron, timezone := args[0], args[1], args[2]

			c, err := factory(cmd.Context(), cfg.StateEndpoint)
			if err != nil {
				return emit(stdout, stderr, cfg.Human, output.FromGRPC(err))
			}
			defer func() { _ = c.Close() }()

			all, err := listSchedules(cmd, cfg, c)
			if err != nil {
				return emit(stdout, stderr, cfg.Human, output.FromGRPC(err))
			}
			req := &runv1.UpsertScheduleRequest{ScheduleName: name, Cron: cron, Timezone: timezone}
			if current := findSchedule(all, name); current != nil {
				req.Description = current.GetDescription()
				req.Paused = current.GetPaused()
			}
			if len(args) == 4 {
				req.Description = args[3]
			}

			ctx, cancel := callContext(cmd, cfg)
			resp, err := c.UpsertSchedule(ctx, cfg.Actor, req)
			cancel()
			if err != nil {
				return emit(stdout, stderr, cfg.Human, output.FromGRPC(err))
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

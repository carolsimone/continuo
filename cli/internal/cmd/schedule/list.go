package schedule

import (
	"context"
	"fmt"
	"io"

	"github.com/carolsimone/continuo/cli/internal/config"
	"github.com/carolsimone/continuo/cli/internal/output"
	statev1 "github.com/carolsimone/continuo/cli/proto/state/v1"
	"github.com/spf13/cobra"
)

// scheduleEntry is the JSON-serialisable representation of a single schedule.
// Fields with omitempty are conditionally omitted per the spec:
//   - description: omit when empty
//   - last_run_at:  omit when there is no prior run (nil proto Timestamp)
type scheduleEntry struct {
	ScheduleName   string `json:"schedule_name"`
	CronExpression string `json:"cron_expression"`
	Description    string `json:"description,omitempty"`
	Timezone       string `json:"timezone"`
	IsRunning      bool   `json:"is_running"`
	LastRunId      string `json:"last_run_id"`
	LastRunStatus  string `json:"last_run_status"`
	LastRunAt      string `json:"last_run_at,omitempty"`
}

// listPayload is the top-level JSON object emitted on success.
// Schedules is initialised to an empty slice so the JSON output is always
// `{"schedules":[]}` rather than `{"schedules":null}`.
type listPayload struct {
	Schedules []scheduleEntry `json:"schedules"`
}

// NewListCommand builds `continuo schedule list`.
func NewListCommand(factory StateClientFactory, cfg *config.Config, stdout, stderr io.Writer) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List all schedules and their last-run status",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx, cancel := context.WithTimeout(cmd.Context(), cfg.Timeout)
			defer cancel()

			c, err := factory(ctx, cfg.StateEndpoint)
			if err != nil {
				return emit(stdout, stderr, cfg.Human, output.FromGRPC(err))
			}
			defer c.Close()

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
func toListPayload(summaries []*statev1.ScheduleSummary) listPayload {
	entries := make([]scheduleEntry, 0, len(summaries))
	for _, s := range summaries {
		e := scheduleEntry{
			ScheduleName:   s.GetScheduleName(),
			CronExpression: s.GetCronExpression(),
			Description:    s.GetDescription(),
			Timezone:       s.GetTimezone(),
			IsRunning:      s.GetIsRunning(),
			LastRunId:      s.GetLastRunId(),
			LastRunStatus:  s.GetLastRunStatus(),
		}
		// Omit last_run_at when the proto Timestamp is nil (never run).
		if ts := s.GetLastRunAt(); ts != nil && ts.IsValid() {
			e.LastRunAt = ts.AsTime().UTC().Format("2006-01-02T15:04:05Z")
		}
		entries = append(entries, e)
	}
	return listPayload{Schedules: entries}
}

// humanList writes one line per schedule to stderr in the format:
//
//	<name>  running  last: <status> <time>
//	<name>  idle     last: <status> <time>
//	<name>  idle
func humanList(stderr io.Writer, summaries []*statev1.ScheduleSummary) error {
	for _, s := range summaries {
		state := "idle"
		if s.GetIsRunning() {
			state = "running"
		}

		lastPart := ""
		if s.GetLastRunStatus() != "" {
			lastPart = "  last: " + s.GetLastRunStatus()
			if ts := s.GetLastRunAt(); ts != nil && ts.IsValid() {
				lastPart += " " + ts.AsTime().UTC().Format("2006-01-02T15:04Z")
			}
		}

		if _, err := fmt.Fprintf(stderr, "%s  %s%s\n", s.GetScheduleName(), state, lastPart); err != nil {
			return err
		}
	}
	return nil
}

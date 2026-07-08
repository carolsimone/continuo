package schedule

import (
	"context"
	"io"
	"time"

	"github.com/carolsimone/continuo/cli/internal/config"
	"github.com/carolsimone/continuo/cli/internal/output"
	"github.com/spf13/cobra"
)

// NewCancelCommand builds `continuo schedule cancel <schedule-name>`. It stops
// the active run of the named schedule via the state service. --reason is
// required and recorded for audit; --by optionally attributes the cancellation
// to a caller identity (the CLI is otherwise an unauthenticated system caller).
func NewCancelCommand(factory StateClientFactory, cfg *config.Config, stdout, stderr io.Writer) *cobra.Command {
	var (
		flagReason string
		flagBy     string
	)
	cmd := &cobra.Command{
		Use:   "cancel <schedule-name>",
		Short: "Cancel the active run of the named schedule",
		Long: `Cancel the active run of the named schedule.

Use when the user asks to stop, cancel, or kill a schedule that is running now.

Arguments:
  <schedule-name>  The schedule whose active run should be cancelled.

Flags:
  --reason  (required) Why the run is being cancelled; recorded for audit.
  --by      (optional) Identity to attribute the cancellation to; defaults to
            the system identity when omitted.

Output (stdout, JSON):
  {"schedule_id":string,"schedule_name":string,"cancelled_at":string}

Errors:
  usage      (exit 2)  wrong number of arguments, or missing --reason
  conflict   (exit 4)  no active run for the schedule, or it already finished
  unavailable(exit 5)  the state service is unreachable
  internal   (exit 6)  unexpected server error`,
		Example: "  continuo schedule cancel daily-revenue --reason \"bad upstream data\"",
		Annotations: map[string]string{
			"output_schema": `{"schedule_id":"string","schedule_name":"string","cancelled_at":"string"}`,
			"exit_codes":    `[0,2,4,5,6]`,
			"mutating":      "true",
		},
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return emit(stdout, stderr, humanOutput(cmd), output.NewUsageError("cancel requires exactly one argument: <schedule-name>"))
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			scheduleName := args[0]
			if flagReason == "" {
				return emit(stdout, stderr, cfg.Human, output.NewUsageError("cancel requires a non-empty --reason"))
			}

			ctx, cancel := context.WithTimeout(cmd.Context(), cfg.Timeout)
			defer cancel()

			c, err := factory(ctx, cfg.StateEndpoint)
			if err != nil {
				return emit(stdout, stderr, cfg.Human, output.FromGRPC(err))
			}
			defer func() { _ = c.Close() }()

			resp, err := c.CancelSchedule(ctx, scheduleName, flagReason, flagBy)
			if err != nil {
				return emit(stdout, stderr, cfg.Human, output.FromGRPC(err))
			}

			if cfg.Human {
				return output.HumanSuccess(stderr, "Cancelled run "+resp.ScheduleId+" for schedule '"+scheduleName+"'")
			}
			payload := map[string]string{
				"schedule_id":   resp.ScheduleId,
				"schedule_name": scheduleName,
				"cancelled_at":  time.Now().UTC().Format(time.RFC3339),
			}
			return output.EmitSuccess(stdout, payload)
		},
	}
	cmd.Flags().StringVar(&flagReason, "reason", "", "why the run is being cancelled (required; recorded for audit)")
	cmd.Flags().StringVar(&flagBy, "by", "", "identity to attribute the cancellation to (optional; defaults to system)")
	// Intercept argument/flag-validation errors so they exit 2 with a CLIError.
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	return cmd
}

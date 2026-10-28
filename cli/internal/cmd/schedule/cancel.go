package schedule

import (
	"context"
	"io"
	"time"

	"github.com/carolsimone/continuo/cli/internal/client"
	"github.com/carolsimone/continuo/cli/internal/config"
	"github.com/carolsimone/continuo/cli/internal/output"
	"github.com/spf13/cobra"
)

// NewCancelCommand builds `continuo schedule cancel <schedule-name> <reason>`.
// It asks the state service to cancel the active run of the named schedule: the
// run becomes cancelling and ends cancelled once its Jobs have stopped. reason is
// a required positional recorded for audit. The cancelling identity is not a
// command argument: it comes from cfg.Actor (the CONTINUO_ACTOR env var) so that
// a caller such as the chat agent stamps a fixed identity without a flag. When
// cfg.Actor is empty the state service records its own system identity.
func NewCancelCommand(factory RunClientFactory, cfg *config.Config, stdout, stderr io.Writer) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "cancel <schedule-name> <reason>",
		Short: "Cancel the active run of the named schedule; it ends cancelled once its Jobs have stopped",
		Long: `Cancel the active run of the named schedule.

Use when the user asks to stop, cancel, or kill a schedule that is running now.

A successful cancel means the run is now cancelling, not that it has stopped:
the state service stops handing out work, and the run ends cancelled once
execution-controller has stopped every Job of it. Until then the run's status
is cancelling and its tasks that were running show as stopping; read
"schedule status <schedule-name>" to see when the run has ended.
A second cancel of a cancelling run succeeds and changes nothing: it returns the same
run and keeps the first cancel's time, author and reason.

Arguments:
  <schedule-name>  The schedule whose active run should be cancelled.
  <reason>         Why the run is being cancelled; recorded for audit. Required
                   and must be non-empty.

The cancelling identity is not an argument: it is taken from the CONTINUO_ACTOR
environment variable when set, otherwise the state service records its own
system identity.

Output (stdout, JSON):
  {"run_id":string,"schedule_name":string,"cancelled_at":string}
  run_id is the id of the run that is cancelling. cancelled_at is the
  time this command received the answer, not the time of the first cancel.

` + client.RequestIDHelp + `

Errors:
  usage      (exit 2)  wrong number of arguments, or a blank reason
  conflict   (exit 4)  the schedule has no pending, running or cancelling run, the run ended before the cancel reached it, or the request id was used before with other arguments
  unavailable(exit 5)  the state service is unreachable
  internal   (exit 6)  unexpected server error`,
		Example: "  continuo schedule cancel daily-revenue \"bad upstream data\"",
		Annotations: map[string]string{
			"output_schema": `{"run_id":"string","schedule_name":"string","cancelled_at":"string"}`,
			"exit_codes":    `[0,2,4,5,6]`,
			"mutating":      "true",
		},
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 2 {
				return emit(stdout, stderr, humanOutput(cmd), output.NewUsageError("cancel requires exactly two arguments: <schedule-name> <reason>"))
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			scheduleName := args[0]
			reason := args[1]
			if reason == "" {
				return emit(stdout, stderr, cfg.Human, output.NewUsageError("cancel requires a non-empty <reason>"))
			}

			ctx, cancel := context.WithTimeout(cmd.Context(), cfg.Timeout)
			defer cancel()

			c, err := factory(ctx, cfg.StateEndpoint)
			if err != nil {
				return emit(stdout, stderr, cfg.Human, output.FromGRPC(err))
			}
			defer func() { _ = c.Close() }()

			resp, err := c.CancelSchedule(ctx, client.RequestID(), scheduleName, reason, cfg.Actor)
			if err != nil {
				return emit(stdout, stderr, cfg.Human, output.FromGRPC(err))
			}

			if cfg.Human {
				return output.HumanSuccess(stderr, "Cancelling run "+resp.GetRunId()+" for schedule '"+scheduleName+"'; it ends cancelled once its Jobs have stopped")
			}
			payload := map[string]string{
				"run_id":        resp.GetRunId(),
				"schedule_name": scheduleName,
				"cancelled_at":  time.Now().UTC().Format(time.RFC3339),
			}
			return output.EmitSuccess(stdout, payload)
		},
	}
	// Intercept argument-validation errors so they exit 2 with a CLIError.
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	return cmd
}

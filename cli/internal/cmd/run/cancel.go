package run

import (
	"context"
	"io"

	"github.com/carolsimone/continuo/cli/internal/config"
	"github.com/carolsimone/continuo/cli/internal/output"
	"github.com/carolsimone/continuo/cli/internal/runview"
	"github.com/spf13/cobra"
)

// NewCancelCommand builds `continuo run cancel <run-id> <reason>`. It asks the
// run-controller service to cancel one run by id. The cancelling identity is not an
// argument: it comes from cfg.Actor (CONTINUO_ACTOR), and the run-controller service
// records its own system identity when that is empty. CancelRun takes no
// request id, because a cancel of a cancelling or cancelled run changes
// nothing, so repeating it is safe without one.
func NewCancelCommand(factory RunClientFactory, cfg *config.Config, stdout, stderr io.Writer) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "cancel <run-id> <reason>",
		Short: "Cancel one run by id; it ends cancelled once its Jobs have stopped",
		Long: `Cancel one run by id.

Use when the user asks to stop, cancel or kill a particular run, for example a
run found with "run show" or "schedule runs". To cancel whatever run a schedule
has active now, use "schedule cancel".

A successful cancel means the run is now cancelling, not that it has stopped:
the run-controller service stops handing out work, and the run ends cancelled once
execution-controller has stopped every Job of it. Until then the run's status is
cancelling and its tasks that were running show as stopping; read "run show
<run-id>" to see when the run has ended. A run with nothing in flight ends
cancelled at once.
A second cancel of a cancelling run succeeds and changes nothing: it returns the same
run and keeps the first cancel's time, author and reason. The same holds for a
run that is already cancelled.

Arguments:
  <run-id>  The UUID of the run to cancel.
  <reason>  Why the run is being cancelled; recorded for audit. Required and
            must be non-empty.

The cancelling identity is not an argument: it is taken from the CONTINUO_ACTOR
environment variable when set, otherwise the run-controller service records its own
system identity. This command sends no request id.

Output (stdout, JSON):
  {"run":{"run_id":string,"schedule_name":string,"kind":string,"operation":string,
          "status":string,"failure_reason":string,"release_id":string,
          "promotion_seq":number,"initiated_by":string,"created_at":string,
          "started_at":string,"completed_at":string,"last_progress_at":string,
          "cancelled_at":string,"cancelled_by":string,"cancellation_reason":string}}
  The run is as the cancel left it: status cancelling while an attempt is still
  being stopped, cancelled when nothing was in flight. Timestamps are RFC3339 in
  UTC; failure_reason, release_id, initiated_by, the timestamps other than
  created_at and the cancel fields are omitted when empty.

Errors:
  usage      (exit 2)  wrong number of arguments, a blank run id or reason, or the run id is not a UUID
  not_found  (exit 3)  no run with that id
  conflict   (exit 4)  the run already ended succeeded, failed or skipped, so there is nothing to cancel
  unavailable(exit 5)  the run-controller service is unreachable
  internal   (exit 6)  unexpected server error`,
		Example: "  continuo run cancel 0b6f4a52-3c1d-4d7e-9a0b-1f2e3d4c5b6a \"bad upstream data\"",
		Annotations: map[string]string{
			"output_schema": `{"run":{"run_id":"string","schedule_name":"string","kind":"string","operation":"string","status":"string","failure_reason":"string","release_id":"string","promotion_seq":"number","initiated_by":"string","created_at":"string","started_at":"string","completed_at":"string","last_progress_at":"string","cancelled_at":"string","cancelled_by":"string","cancellation_reason":"string"}}`,
			"exit_codes":    `[0,2,3,4,5,6]`,
			"mutating":      "true",
		},
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 2 {
				return usageFailure(cmd, stdout, stderr, "cancel requires exactly two arguments: <run-id> <reason>")
			}
			if args[0] == "" {
				return usageFailure(cmd, stdout, stderr, "cancel requires a non-empty <run-id>")
			}
			if args[1] == "" {
				return usageFailure(cmd, stdout, stderr, "cancel requires a non-empty <reason>")
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := context.WithTimeout(cmd.Context(), cfg.Timeout)
			defer cancel()

			c, err := factory(ctx, cfg.RunControllerEndpoint)
			if err != nil {
				return emit(stdout, stderr, cfg.Human, output.FromGRPC(err))
			}
			defer func() { _ = c.Close() }()

			resp, err := c.CancelRun(ctx, args[0], args[1], cfg.Actor)
			if err != nil {
				return emit(stdout, stderr, cfg.Human, output.FromGRPC(err))
			}

			run := runview.ToRun(resp.GetRun())
			if cfg.Human {
				return output.HumanSuccess(stderr, "Run "+run.RunID+" is "+run.Status+"; it ends cancelled once its Jobs have stopped")
			}
			return output.EmitSuccess(stdout, struct {
				Run *runview.Run `json:"run"`
			}{Run: run})
		},
	}
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	return cmd
}

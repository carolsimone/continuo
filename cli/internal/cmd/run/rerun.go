package run

import (
	"context"
	"io"

	"github.com/carolsimone/continuo/cli/internal/client"
	"github.com/carolsimone/continuo/cli/internal/config"
	"github.com/carolsimone/continuo/cli/internal/output"
	"github.com/spf13/cobra"
)

// NewRerunCommand builds `continuo run rerun <run-id>`. It asks the state
// service for a new run that re-executes the source run's tasks that did not
// succeed, and their descendants, on the release the source ran on.
func NewRerunCommand(factory RunClientFactory, cfg *config.Config, stdout, stderr io.Writer) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "rerun <run-id>",
		Short: "Re-execute what did not succeed in a failed or cancelled run, on the release it ran on",
		Long: `Re-execute what did not succeed in a failed or cancelled run.

Use when the user asks to retry, rerun or resume a failed or cancelled run as
it was, on the release it ran on. To run it again on the live release instead
(new nodes, changed code), use "run rebase-preview" and "run rebase".

The new run re-executes the source run's tasks that did not succeed and their
descendants, on the source run's release, and keeps the source's schedule name.
Ready tasks are dispatched when the run is created.

Arguments:
  <run-id>  The UUID of the failed or cancelled run to rerun.

Output (stdout, JSON):
  {"run_id":string,"schedule_name":string}
  run_id is the id of the new run; schedule_name is the source run's schedule.

` + client.RequestIDHelp + `

Errors:
  usage      (exit 2)  wrong number of arguments, a blank run id, or the run id is not a UUID
  not_found  (exit 3)  no run with that id
  conflict   (exit 4)  the run is not failed or cancelled, names no release, is a test run, leaves nothing to re-execute, or its schedule already has an active run; or the request id was used before with other arguments
  unavailable(exit 5)  the state service is unreachable
  maintenance(exit 7)  continuo is in maintenance mode; new work is refused until it is turned off (not retryable)
  internal   (exit 6)  unexpected server error`,
		Example: "  continuo run rerun 0b6f4a52-3c1d-4d7e-9a0b-1f2e3d4c5b6a",
		Annotations: map[string]string{
			"output_schema": `{"run_id":"string","schedule_name":"string"}`,
			"exit_codes":    `[0,2,3,4,5,6,7]`,
			"mutating":      "true",
		},
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return usageFailure(cmd, stdout, stderr, "rerun requires exactly one argument: <run-id>")
			}
			if args[0] == "" {
				return usageFailure(cmd, stdout, stderr, "rerun requires a non-empty <run-id>")
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := context.WithTimeout(cmd.Context(), cfg.Timeout)
			defer cancel()

			c, err := factory(ctx, cfg.StateEndpoint)
			if err != nil {
				return emit(stdout, stderr, cfg.Human, output.FromGRPC(err))
			}
			defer func() { _ = c.Close() }()

			resp, err := c.TriggerRerun(ctx, client.RequestID(), args[0], cfg.Actor)
			if err != nil {
				return emit(stdout, stderr, cfg.Human, output.FromGRPC(err))
			}

			if cfg.Human {
				return output.HumanSuccess(stderr, "Started rerun "+resp.GetRunId()+" of run "+args[0]+" for schedule '"+resp.GetScheduleName()+"'")
			}
			return output.EmitSuccess(stdout, startedRun{RunID: resp.GetRunId(), ScheduleName: resp.GetScheduleName()})
		},
	}
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	return cmd
}

package run

import (
	"context"
	"io"
	"strings"

	"github.com/carolsimone/continuo/cli/internal/client"
	"github.com/carolsimone/continuo/cli/internal/config"
	"github.com/carolsimone/continuo/cli/internal/output"
	"github.com/spf13/cobra"
)

// liveReleaseMoved is the part of the state service's FAILED_PRECONDITION
// message that says the live release is no longer the one the caller previewed.
const liveReleaseMoved = "live release changed"

// NewRebaseCommand builds
// `continuo run rebase <run-id> <release-id> [with-changed-upstream]`. It asks
// the state service for a run that restarts the source run from its failure on
// the live release, provided the live release is still <release-id>, the one
// "run rebase-preview" reported.
func NewRebaseCommand(factory RunClientFactory, cfg *config.Config, stdout, stderr io.Writer) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "rebase <run-id> <release-id> [with-changed-upstream]",
		Short: "Restart a failed or cancelled run from its failure on the live release",
		Long: `Restart a failed or cancelled run from its failure on the live release.

Use when the user asks to restart a failed or cancelled run from its failure on
the newest code. Always run "run rebase-preview" first, show the user its counts
(how many nodes run, how many are new, changed upstream or inherited), ask for
approval, and only then run this command with the release_id the preview printed.
The release id makes the approval binding: if the live release moved since the
preview, the rebase is refused and nothing is started.

The new run keeps the source's schedule name and runs on the live release. It
re-executes the source's tasks that did not succeed, the live schedule's nodes
the source never ran, everything below them and, with with-changed-upstream,
every changed node upstream of those. Every other success is inherited.

Arguments:
  <run-id>                The UUID of the failed or cancelled run to rebase from.
  <release-id>            The release_id "run rebase-preview" printed, which must still
                          be the live release. Required and must be non-empty.
  [with-changed-upstream] Optional: "true" or "false" (default false); pass the value
                          that was previewed. Any other value is refused.

Output (stdout, JSON):
  {"run_id":string,"schedule_name":string}
  run_id is the id of the new run; schedule_name is the source run's schedule.

` + client.RequestIDHelp + `

Errors:
  usage      (exit 2)  wrong number of arguments, a blank run id or release id, [with-changed-upstream] not true or false, or the run id is not a UUID
  not_found  (exit 3)  no run with that id
  conflict   (exit 4)  the live release is not <release-id> (not retryable: the message says to run "continuo run rebase-preview" again and use its new release_id), the run is not failed or cancelled, has no task that did not succeed, names no release, is a test run, leaves nothing to re-execute, or its schedule already has an active run; or the request id was used before with other arguments
  unavailable(exit 5)  the state service is unreachable
  maintenance(exit 7)  continuo is in maintenance mode; new work is refused until it is turned off (not retryable)
  internal   (exit 6)  unexpected server error`,
		Example: "  continuo run rebase 0b6f4a52-3c1d-4d7e-9a0b-1f2e3d4c5b6a rel-2026-10-27-3 true",
		Annotations: map[string]string{
			"output_schema": `{"run_id":"string","schedule_name":"string"}`,
			"exit_codes":    `[0,2,3,4,5,6,7]`,
			"mutating":      "true",
		},
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) < 2 || len(args) > 3 {
				return usageFailure(cmd, stdout, stderr, "rebase requires <run-id> <release-id> and an optional [with-changed-upstream]")
			}
			if args[0] == "" {
				return usageFailure(cmd, stdout, stderr, "rebase requires a non-empty <run-id>")
			}
			if args[1] == "" {
				return usageFailure(cmd, stdout, stderr, "rebase requires a non-empty <release-id>: the release_id that \"continuo run rebase-preview\" printed")
			}
			if _, err := parseChangedUpstream(args, 2); err != nil {
				return usageFailure(cmd, stdout, stderr, "%v", err)
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			runID, releaseID := args[0], args[1]
			withChangedUpstream, _ := parseChangedUpstream(args, 2)
			ctx, cancel := context.WithTimeout(cmd.Context(), cfg.Timeout)
			defer cancel()

			c, err := factory(ctx, cfg.StateEndpoint)
			if err != nil {
				return emit(stdout, stderr, cfg.Human, output.FromGRPC(err))
			}
			defer func() { _ = c.Close() }()

			resp, err := c.TriggerRebase(ctx, client.RequestID(), runID, releaseID, withChangedUpstream, cfg.Actor)
			if err != nil {
				cliErr := output.FromGRPC(err)
				if output.IsFailedPrecondition(err) && strings.Contains(cliErr.Message, liveReleaseMoved) {
					// Resending the same release id is refused again, so the
					// refusal is not retryable until the caller previews anew.
					cliErr.Retryable = false
					cliErr.Message += `: run "continuo run rebase-preview ` + runID + `" again, show the user the new counts for approval, and rebase with the new release_id`
				}
				return emit(stdout, stderr, cfg.Human, cliErr)
			}

			if cfg.Human {
				return output.HumanSuccess(stderr, "Started rebase "+resp.GetRunId()+" of run "+runID+" for schedule '"+resp.GetScheduleName()+"'")
			}
			return output.EmitSuccess(stdout, startedRun{RunID: resp.GetRunId(), ScheduleName: resp.GetScheduleName()})
		},
	}
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	return cmd
}

package run

import (
	"io"

	"github.com/carolsimone/continuo/cli/internal/config"
	"github.com/carolsimone/continuo/cli/internal/output"
	"github.com/carolsimone/continuo/cli/internal/runview"
	"github.com/spf13/cobra"
)

// NewShowCommand builds `continuo run show <run-id>`.
func NewShowCommand(factory RunClientFactory, cfg *config.Config, stdout, stderr io.Writer) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "show <run-id>",
		Short: "Show one run and the status and failure of each of its tasks",
		Long: `Show one run and the status and failure of each of its tasks.

Use when the user asks how a run is going, why it failed, or which of its nodes
ran, failed, waited or were skipped. To find a run id, use "schedule status"
(the latest run of a schedule) or "schedule runs" (its completed runs).

Arguments:
  <run-id>  The UUID of the run.

Output (stdout, JSON):
  {"run":{"run_id":string,"schedule_name":string,"kind":string,"operation":string,
          "status":string,"failure_reason":string,"release_id":string,
          "promotion_seq":number,"initiated_by":string,"created_at":string,
          "started_at":string,"completed_at":string,"last_progress_at":string,
          "cancelled_at":string,"cancelled_by":string,"cancellation_reason":string},
   "tasks":[{"node":string,"status":string,"attempt":number,
             "failure_class":string,"failure_reason":string,
             "wait_reason":string,"waiting_on_run_id":string}]}
  Timestamps are RFC3339 in UTC. failure_reason, release_id, initiated_by, the
  timestamps other than created_at and the cancel fields are omitted when empty.
  kind is one of cron|trigger|rerun|rebase|single_node_run|promote_seed;
  operation is one of run|test|build|full_refresh.
  The run status is one of pending|running|cancelling|succeeded|failed|cancelled|skipped.
  cancelling: a cancel was accepted and the run waits for its Jobs to stop.
  The run's failure_reason (stalled|empty_selection|invalid_node_type) is set only
  when the run failed without a failed task.
  node is "<service>.<schema>.<table>". attempt is the 0-based number of the task's current attempt.
  status is one of: pending|dispatched|queued|running|stopping|succeeded|failed|cancelled|skipped.
  dispatched: an attempt was sent and not yet accepted; queued: the attempt
  was accepted and waits for a slot to run in; stopping: a stop was requested
  and the task waits for its attempt to stop.
  failure_class and failure_reason are those of the task's latest attempt and are
  empty strings when that attempt did not fail or the task has no attempt yet.
  wait_reason is node_lease while the task waits for another run to release its node,
  and waiting_on_run_id is the run writing that node (empty for a moment when the
  node has just been freed); both are empty strings when the task waits for nothing
  besides its upstreams.

Errors:
  not_found  (exit 3)  no run with that id
  usage      (exit 2)  wrong number of arguments, or the run id is not a UUID
  unavailable(exit 5)  the run-controller service is unreachable
  internal   (exit 6)  unexpected server error`,
		Example: "  continuo run show 0b6f4a52-3c1d-4d7e-9a0b-1f2e3d4c5b6a",
		Annotations: map[string]string{
			"output_schema": `{"run":{"run_id":"string","schedule_name":"string","kind":"string","operation":"string","status":"string","failure_reason":"string","release_id":"string","promotion_seq":"number","initiated_by":"string","created_at":"string","started_at":"string","completed_at":"string","last_progress_at":"string","cancelled_at":"string","cancelled_by":"string","cancellation_reason":"string"},"tasks":[{"node":"string","status":"string","attempt":"number","failure_class":"string","failure_reason":"string","wait_reason":"string","waiting_on_run_id":"string"}]}`,
			"exit_codes":    `[0,2,3,5,6]`,
		},
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return emit(stdout, stderr, humanOutput(cmd), output.NewUsageError("show requires exactly one argument: <run-id>"))
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := factory(cmd.Context(), cfg.RunControllerEndpoint)
			if err != nil {
				return emit(stdout, stderr, cfg.Human, output.FromGRPC(err))
			}
			defer func() { _ = c.Close() }()

			view, err := runview.Load(cmd.Context(), c, cfg.Timeout, args[0])
			if err != nil {
				return emit(stdout, stderr, cfg.Human, output.FromGRPC(err))
			}
			if cfg.Human {
				return view.WriteHuman(stderr)
			}
			return output.EmitSuccess(stdout, view)
		},
	}
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	return cmd
}

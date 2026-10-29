package schedule

import (
	"context"
	"io"

	"github.com/carolsimone/continuo/cli/internal/config"
	"github.com/carolsimone/continuo/cli/internal/output"
	"github.com/carolsimone/continuo/cli/internal/runview"
	runv1 "github.com/carolsimone/continuo/cli/proto/run/v1"
	"github.com/spf13/cobra"
)

// NewStatusCommand builds `continuo schedule status <schedule-name>`. It
// resolves the schedule name to its latest run via ListSchedules and prints
// that run exactly as `continuo run show` does.
func NewStatusCommand(factory RunClientFactory, cfg *config.Config, stdout, stderr io.Writer) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "status <schedule-name>",
		Short: "Show a schedule's latest run and the status of each of its tasks",
		Long: `Show a schedule's latest run and the status of each of its tasks.

Use when the user asks where a schedule is, how far its latest run has got, which
of its nodes are running, failed, or succeeded right now, or why the run failed.
It prints what "run show" prints for the schedule's latest run, so use "run show"
with an earlier run id (from "schedule runs") to inspect an older run.

Arguments:
  <schedule-name>  The schedule to inspect (exact match).

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
  pending, running and cancelling mean the run still holds the schedule;
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
  wait_reason is node_lease while the task waits for another run to release its node:
  another run's task is writing the same node, and this pending task starts once
  that write ends. waiting_on_run_id is the run writing the node (empty for a
  moment when the node has just been freed and the task is about to be granted it);
  both are empty strings when the task waits for nothing besides its upstreams.
  A schedule that has never run returns {"run":null,"tasks":[]}.

Errors:
  not_found  (exit 3)  no schedule with that name
  usage      (exit 2)  wrong number of arguments
  unavailable(exit 5)  the run-controller service is unreachable
  internal   (exit 6)  unexpected server error`,
		Example: "  continuo schedule status daily-revenue",
		Annotations: map[string]string{
			"output_schema": `{"run":{"run_id":"string","schedule_name":"string","kind":"string","operation":"string","status":"string","failure_reason":"string","release_id":"string","promotion_seq":"number","initiated_by":"string","created_at":"string","started_at":"string","completed_at":"string","last_progress_at":"string","cancelled_at":"string","cancelled_by":"string","cancellation_reason":"string"},"tasks":[{"node":"string","status":"string","attempt":"number","failure_class":"string","failure_reason":"string","wait_reason":"string","waiting_on_run_id":"string"}]}`,
			"exit_codes":    `[0,2,3,5,6]`,
		},
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return emit(stdout, stderr, humanOutput(cmd), output.NewUsageError("status requires exactly one argument: <schedule-name>"))
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			scheduleName := args[0]

			c, err := factory(cmd.Context(), cfg.RunControllerEndpoint)
			if err != nil {
				return emit(stdout, stderr, cfg.Human, output.FromGRPC(err))
			}
			defer func() { _ = c.Close() }()

			// Each RPC gets its own --timeout deadline, matching the documented
			// per-call contract.
			listCtx, cancelList := context.WithTimeout(cmd.Context(), cfg.Timeout)
			schedules, err := c.ListAllSchedules(listCtx)
			cancelList()
			if err != nil {
				return emit(stdout, stderr, cfg.Human, output.FromGRPC(err))
			}

			var match *runv1.ScheduleSummary
			for _, s := range schedules.GetSchedules() {
				if s.GetScheduleName() == scheduleName {
					match = s
					break
				}
			}
			if match == nil {
				return emit(stdout, stderr, cfg.Human, output.CLIError{
					Code:    output.CodeNotFound,
					Message: "no schedule named '" + scheduleName + "'",
				})
			}

			// Never run: no run to read.
			view := runview.Empty()
			if match.GetLastRunId() != "" {
				view, err = runview.Load(cmd.Context(), c, cfg.Timeout, match.GetLastRunId())
				if err != nil {
					return emit(stdout, stderr, cfg.Human, output.FromGRPC(err))
				}
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

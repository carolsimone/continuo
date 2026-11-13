package node

import (
	"context"
	"io"

	"github.com/carolsimone/continuo/cli/internal/client"
	"github.com/carolsimone/continuo/cli/internal/config"
	"github.com/carolsimone/continuo/cli/internal/output"
	"github.com/spf13/cobra"
)

// NewFullRefreshCommand builds `continuo node full-refresh <service> <schema>
// <table> [source-run-id]`. It rebuilds one dbt model or seed from scratch:
// the node's production table is dropped and recreated. Only that node runs;
// nothing upstream or downstream is triggered. With three arguments the run
// uses the latest topology metadata; with a fourth it reuses the metadata
// snapshot of that past run.
//
// A seed's full refresh also rebuilds the continuo views that read it, in the
// same run. `--acknowledge <schema>.<name>` (repeatable) names an object
// outside continuo that the refresh may drop.
//
// The command reports acceptance, not completion. A node the live release does
// not hold is refused (not_found) and a node that is not a dbt model or seed is
// refused (usage); neither creates a run. A task whose node another run is
// writing waits for that run's write before it is dispatched.
func NewFullRefreshCommand(factory RunClientFactory, cfg *config.Config, stdout, stderr io.Writer) *cobra.Command {
	var acknowledged []string
	cmd := &cobra.Command{
		Use:   "full-refresh <service> <schema> <table> [source-run-id]",
		Short: "Rebuild one dbt model or seed from scratch (dbt --full-refresh)",
		Long: `Rebuild one dbt model or seed from scratch (dbt --full-refresh).

Use when the user asks to fully refresh, rebuild from scratch, or re-create a
specific dbt model or seed — for example an incremental model whose logic or
columns changed, whose normal runs only insert new rows. The node's production
table is dropped and rebuilt. Only this node runs; continuo does not trigger its
upstream or downstream nodes. On Postgres, views that select from a model are
dropped with it until their own models run again. Only dbt models and dbt seeds
can be fully refreshed; snapshots, dbt tests and python nodes are rejected.
A seed's full refresh also rebuilds the continuo views that read it, in the same
run: they appear in "run show" as tasks whose plan_reason starts with "rebuild:".
On Postgres the seed's table is dropped, so the refresh stops before the drop
(the seed's task fails with failure_reason unowned_dependents and lists the
objects) when an object continuo does not own reads the seed, unless each such
object is named with --acknowledge <schema>.<name>; repeat the flag for every
object the stopped load listed. "node history" lists them in blocking_objects
on the seed task that stopped. A seed set to +full_refresh: false on Postgres
is refused (usage, exit 2) and no run is created. A service whose governing
command block has no seed_full_refresh (seed) or full_refresh (model) makes the
task fail with failure_reason seed_full_refresh_undefined or
full_refresh_undefined before any container starts.
By default the run uses the node's current (latest) image and manifest version.
Pass a past run id to rebuild the node exactly as that run saw it.

Arguments:
  <service>  The owning service name.
  <schema>   The schema name.
  <table>    The table (model or seed) name.
` + sourceRunArgDoc + `

Flags:
  --acknowledge <schema>.<name>  Repeatable. An object outside continuo that the
                                 seed's full refresh may drop. Valid only for a
                                 dbt seed; on a model it is a usage error. Names
                                 are trimmed, lowercased and deduplicated by the
                                 server.

The initiating identity is not an argument: it is taken from the CONTINUO_ACTOR
environment variable when set, otherwise the run-controller service records its own
system identity.

This command reports acceptance, not completion. On success the new run is
planned and durably recorded with its first dispatches. A node the live
release does not hold is refused (not_found), and a node that is not a dbt
model or seed is refused (usage); neither creates a run. A seed whose table the
refresh would not rebuild (+full_refresh: false on Postgres) is refused too
(usage, exit 2, "seed_full_refresh_disabled"), and so is --acknowledge on
anything but a seed's full refresh. If the node's service governing command
block defines no full_refresh (model) or seed_full_refresh (seed) command, the
run is accepted and fails when it executes.
A task whose node another run is writing waits for that run to finish
writing it before it is dispatched: "node history" and "schedule status" show
the task pending, with wait_reason "node_lease" and the run it waits on
(waiting_on_run_id).
Check the outcome of an accepted run with "node history".

` + client.RequestIDHelp + `

Output (stdout, JSON):
  {"run_id":string,"schedule_name":string}

Errors:
  usage      (exit 2)  wrong number of arguments, a malformed source run id, the server rejects the identity triple, the seed is set to +full_refresh: false on Postgres, or --acknowledge names an object on a node that is not a seed
` + sourceRunErrorsDoc + `
  unavailable(exit 5)  the run-controller service is unreachable
  maintenance(exit 7)  continuo is in maintenance mode; new work is refused until it is turned off (not retryable)
  internal   (exit 6)  unexpected server error`,
		Example: `  continuo node full-refresh finance analytics orders
  continuo node full-refresh finance analytics orders 3f9e1c2a-7b4d-4e8f-9a1b-2c3d4e5f6a7b
  continuo node full-refresh finance raw customers --acknowledge bi.customer_report --acknowledge bi.churn`,
		Annotations: map[string]string{
			"output_schema": `{"run_id":"string","schedule_name":"string"}`,
			"exit_codes":    nodeRunExitCodes,
			"mutating":      "true",
			// The chat may name the objects a seed's full refresh may drop.
			"chat_flags": "acknowledge",
		},
		Args: nodeTargetArgs("full-refresh", stdout, stderr),
		RunE: func(cmd *cobra.Command, args []string) error {
			target := parseNodeTarget(args)
			ctx, cancel := context.WithTimeout(cmd.Context(), cfg.Timeout)
			defer cancel()

			c, err := factory(ctx, cfg.RunControllerEndpoint)
			if err != nil {
				return emit(stdout, stderr, cfg.Human, output.FromGRPC(err))
			}
			defer func() { _ = c.Close() }()

			resp, err := c.TriggerNodeFullRefresh(ctx, client.RequestID(), target.service, target.schema, target.table, target.sourceRunID, cfg.Actor, acknowledged)
			if err != nil {
				return emit(stdout, stderr, cfg.Human, output.FromGRPC(err))
			}

			if cfg.Human {
				return output.HumanSuccess(stderr, "Triggered single-node full refresh run "+resp.GetRunId()+" for "+target.service+"."+target.schema+"."+target.table)
			}
			return output.EmitSuccess(stdout, map[string]string{
				"run_id":        resp.GetRunId(),
				"schedule_name": resp.GetScheduleName(),
			})
		},
	}
	cmd.Flags().StringArrayVar(&acknowledged, "acknowledge", nil,
		"an object outside continuo that the seed's full refresh may drop, as <schema>.<name>; repeat for each object the stopped load listed")
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	return cmd
}

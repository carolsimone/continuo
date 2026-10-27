package node

import (
	"context"
	"io"

	"github.com/carolsimone/continuo/cli/internal/client"
	"github.com/carolsimone/continuo/cli/internal/config"
	"github.com/carolsimone/continuo/cli/internal/output"
	"github.com/spf13/cobra"
)

// NewTriggerCommand builds `continuo node trigger <service> <schema> <table> [source-run-id]`.
// It triggers a fresh single-node run. With three arguments the run uses the
// latest topology metadata; with a fourth it reuses the metadata snapshot of
// that past run.
//
// The command reports acceptance, not completion: the state service plans the
// run and durably records it with its first dispatches. A node the live release
// does not hold is refused (not_found) and no run is created. A task whose node
// another run is writing waits for that run's write before it is dispatched.
//
// The initiating identity is not an argument: it comes from cfg.Actor (the
// CONTINUO_ACTOR env var) and is forwarded as gRPC metadata. When empty, the
// state service records its own system identity.
func NewTriggerCommand(factory RunClientFactory, cfg *config.Config, stdout, stderr io.Writer) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "trigger <service> <schema> <table> [source-run-id]",
		Short: "Trigger a fresh run of one model node",
		Long: `Trigger a fresh run of one model node.

Use when the user asks to run, re-run, or rebuild a specific dbt model now.
By default the run uses the model's current (latest) image and manifest
version. Pass a past run id to re-run the model exactly as that run saw it.

Arguments:
  <service>  The owning service name.
  <schema>   The schema name.
  <table>    The table (model) name.
` + sourceRunArgDoc + `

The initiating identity is not an argument: it is taken from the CONTINUO_ACTOR
environment variable when set, otherwise the state service records its own
system identity.

This command reports acceptance, not completion. On success the new run is
planned and durably recorded with its first dispatches; a node the live
release does not hold is refused (not_found) and no run is created.
A task whose node another run is writing waits for that run to finish
writing it before it is dispatched: "node history" and "schedule status" show
the task pending, with wait_reason "node_lease" and the run it waits on
(waiting_on_run_id).
Check the outcome of an accepted run with "node history".

` + client.RequestIDHelp + `

Output (stdout, JSON):
  {"run_id":string,"schedule_name":string}

Errors:
  usage      (exit 2)  wrong number of arguments, a malformed source run id, or the server rejects the identity triple
` + sourceRunErrorsDoc + `
  unavailable(exit 5)  the state service is unreachable
  maintenance(exit 7)  continuo is in maintenance mode; new work is refused until it is turned off (not retryable)
  internal   (exit 6)  unexpected server error`,
		Example: `  continuo node trigger finance analytics orders
  continuo node trigger finance analytics orders 3f9e1c2a-7b4d-4e8f-9a1b-2c3d4e5f6a7b`,
		Annotations: map[string]string{
			"output_schema": `{"run_id":"string","schedule_name":"string"}`,
			"exit_codes":    nodeRunExitCodes,
			"mutating":      "true",
		},
		Args: nodeTargetArgs("trigger", stdout, stderr),
		RunE: func(cmd *cobra.Command, args []string) error {
			target := parseNodeTarget(args)
			ctx, cancel := context.WithTimeout(cmd.Context(), cfg.Timeout)
			defer cancel()

			c, err := factory(ctx, cfg.StateEndpoint)
			if err != nil {
				return emit(stdout, stderr, cfg.Human, output.FromGRPC(err))
			}
			defer func() { _ = c.Close() }()

			resp, err := c.TriggerNodeRun(ctx, client.RequestID(), target.service, target.schema, target.table, target.sourceRunID, cfg.Actor)
			if err != nil {
				return emit(stdout, stderr, cfg.Human, output.FromGRPC(err))
			}

			if cfg.Human {
				return output.HumanSuccess(stderr, "Triggered single-node run "+resp.GetRunId()+" for "+target.service+"."+target.schema+"."+target.table)
			}
			payload := map[string]string{
				"run_id":        resp.GetRunId(),
				"schedule_name": resp.GetScheduleName(),
			}
			return output.EmitSuccess(stdout, payload)
		},
	}
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	return cmd
}

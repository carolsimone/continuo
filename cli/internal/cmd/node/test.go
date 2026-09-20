package node

import (
	"context"
	"io"

	"github.com/carolsimone/continuo/cli/internal/config"
	"github.com/carolsimone/continuo/cli/internal/output"
	"github.com/spf13/cobra"
)

// NewTestCommand builds `continuo node test <service> <schema> <table> [source-run-id]`.
// It runs the model's dbt tests (not the model itself). With three arguments
// the run uses the latest topology metadata; with a fourth it reuses the
// metadata snapshot of that past run.
//
// The command reports acceptance, not completion: the state service durably
// records the new test run and its outbox event, but if the node is absent
// from the topology, or the model has no tests, the failure surfaces
// asynchronously downstream.
//
// The initiating identity is not an argument: it comes from cfg.Actor (the
// CONTINUO_ACTOR env var) and is forwarded as gRPC metadata. When empty, the
// state service records its own system identity.
func NewTestCommand(factory StateClientFactory, cfg *config.Config, stdout, stderr io.Writer) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "test <service> <schema> <table> [source-run-id]",
		Short: "Run a model node's dbt tests",
		Long: `Run a model node's dbt tests.

Use when the user asks to test, validate, or check a specific dbt model now,
as opposed to running (building) it. The run dispatches dbt test rather than
dbt run/build for that single node. By default it uses the model's current
(latest) image and manifest version. Pass a past run id to test the model
exactly as that run saw it.

Arguments:
  <service>  The owning service name.
  <schema>   The schema name.
  <table>    The table (model) name.
` + sourceRunArgDoc + `

The initiating identity is not an argument: it is taken from the CONTINUO_ACTOR
environment variable when set, otherwise the state service records its own
system identity.

This command reports acceptance, not completion. On success the new run and its
event are durably recorded. If the node is not in the topology the run fails
asynchronously. If the model has no tests defined (in the latest topology, or
in the source run's snapshot when one is given) nothing is dispatched and the
run finishes with status "skipped". Neither outcome is reported by this
command: check it with "node history".

Output (stdout, JSON):
  {"run_id":string,"schedule_name":string}

Errors:
  usage      (exit 2)  wrong number of arguments, a malformed source run id, or the server rejects the identity triple
` + sourceRunErrorsDoc + `
  unavailable(exit 5)  the state service is unreachable
  internal   (exit 6)  unexpected server error`,
		Example: `  continuo node test finance analytics orders
  continuo node test finance analytics orders 3f9e1c2a-7b4d-4e8f-9a1b-2c3d4e5f6a7b`,
		Annotations: map[string]string{
			"output_schema": `{"run_id":"string","schedule_name":"string"}`,
			"exit_codes":    nodeRunExitCodes,
			"mutating":      "true",
		},
		Args: nodeTargetArgs("test", stdout, stderr),
		RunE: func(cmd *cobra.Command, args []string) error {
			target := parseNodeTarget(args)
			ctx, cancel := context.WithTimeout(cmd.Context(), cfg.Timeout)
			defer cancel()

			c, err := factory(ctx, cfg.StateEndpoint)
			if err != nil {
				return emit(stdout, stderr, cfg.Human, output.FromGRPC(err))
			}
			defer func() { _ = c.Close() }()

			resp, err := c.TriggerNodeTest(ctx, target.service, target.schema, target.table, target.sourceRunID, cfg.Actor)
			if err != nil {
				return emit(stdout, stderr, cfg.Human, output.FromGRPC(err))
			}

			if cfg.Human {
				return output.HumanSuccess(stderr, "Triggered single-node test run "+resp.GetRunId()+" for "+target.service+"."+target.schema+"."+target.table)
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

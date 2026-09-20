package node

import (
	"context"
	"io"

	"github.com/carolsimone/continuo/cli/internal/config"
	"github.com/carolsimone/continuo/cli/internal/output"
	"github.com/spf13/cobra"
)

// NewTriggerCommand builds `continuo node trigger <service> <schema> <table> [source-run-id]`.
// It triggers a fresh single-node run. With three arguments the run uses the
// latest topology metadata; with a fourth it reuses the metadata snapshot of
// that past run.
//
// The command reports acceptance, not completion: the state service durably
// records the new run and its outbox event, but if the node is absent from the
// topology the failure surfaces asynchronously downstream.
//
// The initiating identity is not an argument: it comes from cfg.Actor (the
// CONTINUO_ACTOR env var) and is forwarded as gRPC metadata. When empty, the
// state service records its own system identity.
func NewTriggerCommand(factory StateClientFactory, cfg *config.Config, stdout, stderr io.Writer) *cobra.Command {
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

This command reports acceptance, not completion. On success the new run and its
event are durably recorded; if the node is not in the topology the failure is
surfaced asynchronously downstream, not by this command. Check the outcome
with "node history".

Output (stdout, JSON):
  {"run_id":string,"schedule_name":string}

Errors:
  usage      (exit 2)  wrong number of arguments, a malformed source run id, or the server rejects the identity triple
` + sourceRunErrorsDoc + `
  unavailable(exit 5)  the state service is unreachable
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

			resp, err := c.TriggerNodeRun(ctx, target.service, target.schema, target.table, target.sourceRunID, cfg.Actor)
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

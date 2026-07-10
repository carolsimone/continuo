package node

import (
	"context"
	"io"

	"github.com/carolsimone/continuo/cli/internal/config"
	"github.com/carolsimone/continuo/cli/internal/output"
	"github.com/spf13/cobra"
)

// NewTriggerCommand builds `continuo node trigger <service> <schema> <table>`.
// It triggers a fresh single-node run using the latest topology metadata. The
// snapshot-of-run mode is intentionally not exposed by the CLI.
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
		Use:   "trigger <service> <schema> <table>",
		Short: "Trigger a fresh run of one model node using the latest metadata",
		Long: `Trigger a fresh run of one model node using the latest metadata.

Use when the user asks to run, re-run, or rebuild a specific dbt model now. The
run uses the model's current (latest) image and manifest version.

Arguments:
  <service>  The owning service name.
  <schema>   The schema name.
  <table>    The table (model) name.

The initiating identity is not an argument: it is taken from the CONTINUO_ACTOR
environment variable when set, otherwise the state service records its own
system identity.

This command reports acceptance, not completion. On success the new run and its
event are durably recorded; if the node is not in the topology the failure is
surfaced asynchronously downstream, not by this command.

Output (stdout, JSON):
  {"run_id":string,"schedule_name":string}

Errors:
  usage      (exit 2)  wrong number of arguments, or the server rejects the identity triple
  unavailable(exit 5)  the state service is unreachable
  internal   (exit 6)  unexpected server error`,
		Example: "  continuo node trigger finance analytics orders",
		Annotations: map[string]string{
			"output_schema": `{"run_id":"string","schedule_name":"string"}`,
			"exit_codes":    `[0,2,5,6]`,
			"mutating":      "true",
		},
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 3 {
				return emit(stdout, stderr, humanOutput(cmd), output.NewUsageError("trigger requires exactly three arguments: <service> <schema> <table>"))
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			service, schema, table := args[0], args[1], args[2]
			ctx, cancel := context.WithTimeout(cmd.Context(), cfg.Timeout)
			defer cancel()

			c, err := factory(ctx, cfg.StateEndpoint)
			if err != nil {
				return emit(stdout, stderr, cfg.Human, output.FromGRPC(err))
			}
			defer func() { _ = c.Close() }()

			resp, err := c.TriggerNodeRun(ctx, service, schema, table, cfg.Actor)
			if err != nil {
				return emit(stdout, stderr, cfg.Human, output.FromGRPC(err))
			}

			if cfg.Human {
				return output.HumanSuccess(stderr, "Triggered single-node run "+resp.GetRunId()+" for "+service+"."+schema+"."+table)
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

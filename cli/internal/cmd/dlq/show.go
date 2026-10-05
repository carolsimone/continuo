package dlq

import (
	"context"
	"io"
	"strings"

	"github.com/carolsimone/continuo/cli/internal/config"
	"github.com/carolsimone/continuo/cli/internal/output"
	"github.com/spf13/cobra"
)

// NewShowCommand builds `continuo dlq show <id>`.
func NewShowCommand(factory ClientFactory, cfg *config.Config, stdout, stderr io.Writer) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "show <id>",
		Short: "Show one dead letter with its message fields",
		Long: `Show one dead letter, including the message fields a redrive would publish.

Use when the user asks why a specific message failed or what it contained,
usually with an id from "dlq list".

Arguments:
  <id>  The dead letter id, as listed by "dlq list".

Output (stdout, JSON):
  {"dead_letter":{...same object as dlq list...},"fields":{string:string},
   "original_event_type":string,"failed_outbox_id":string}

Errors:
  usage      (exit 2)  missing id, or an id that is not a dead letter id
  not_found  (exit 3)  no dead letter with this id
  unavailable(exit 5)  dead-letter-controller is unreachable
  internal   (exit 6)  unexpected server error`,
		Example: "  continuo dlq show 7f6c2c4e-1f0a-4f55-9a43-0c1b6f0b9a11",
		Annotations: map[string]string{
			"output_schema": `{"dead_letter":"object","fields":"object","original_event_type":"string","failed_outbox_id":"string"}`,
			"exit_codes":    `[0,2,3,5,6]`,
		},
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 || strings.TrimSpace(args[0]) == "" {
				return emit(stdout, stderr, humanOutput(cmd), output.NewUsageError("show requires exactly one non-empty argument: <id>"))
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := context.WithTimeout(cmd.Context(), cfg.Timeout)
			defer cancel()

			c, err := factory(ctx, cfg.DeadLetterEndpoint)
			if err != nil {
				return emit(stdout, stderr, cfg.Human, output.FromGRPC(err))
			}
			defer func() { _ = c.Close() }()

			resp, err := c.Get(ctx, args[0])
			if err != nil {
				return emit(stdout, stderr, cfg.Human, output.FromGRPC(err))
			}

			if cfg.Human {
				if err := output.HumanSuccess(stderr, humanLine(resp.GetDeadLetter())); err != nil {
					return err
				}
				for k, v := range resp.GetFields() {
					if err := output.HumanSuccess(stderr, "  "+k+"="+v); err != nil {
						return err
					}
				}
				return nil
			}
			fields := resp.GetFields()
			if fields == nil {
				fields = map[string]string{}
			}
			return output.EmitSuccess(stdout, map[string]any{
				"dead_letter":         toJSON(resp.GetDeadLetter()),
				"fields":              fields,
				"original_event_type": resp.GetOriginalEventType(),
				"failed_outbox_id":    resp.GetFailedOutboxId(),
			})
		},
	}
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	return cmd
}

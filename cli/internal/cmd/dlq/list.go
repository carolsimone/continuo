package dlq

import (
	"context"
	"io"
	"strconv"

	"github.com/carolsimone/continuo/cli/internal/config"
	"github.com/carolsimone/continuo/cli/internal/output"
	"github.com/spf13/cobra"
)

// allSources is the source argument that selects every source.
const allSources = "all"

// NewListCommand builds `continuo dlq list [source] [stream]`.
func NewListCommand(factory ClientFactory, cfg *config.Config, stdout, stderr io.Writer) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list [source] [stream]",
		Short: "List open dead letters",
		Long: `List the open dead letters: messages a consumer gave up on, outbox rows a
relay could not publish, and stream entries trimmed before a consumer group
finished them.

Use when the user asks what failed to process, what is in the dead-letter
queue, or whether any messages were lost.

Arguments:
  [source]  Optional. consumer, outbox, quarantine, or all. Omit (or give
            all) to list every source; an empty value is a usage error.
  [stream]  Optional. Only dead letters of this stream, e.g. node.updated:v1.
            Omit to list every stream; an empty value is a usage error. To
            filter by stream alone, give all as the source.

Output (stdout, JSON):
  {"total_open":number,"dead_letters":[{"id":string,"source":string,
   "failure_kind":string,"stream":string,"group":string,
   "original_message_id":string,"producer":string,"error":string,
   "delivery_count":number,"original_at":string,"recorded_at":string,
   "expires_at":string,"status":string,"redrivable":boolean,"redrive":null}]}
  At most 50 dead letters, newest first. total_open counts every open one.

Errors:
  usage      (exit 2)  more than two arguments, a blank argument, or an unknown source
  unavailable(exit 5)  dead-letter-controller is unreachable
  internal   (exit 6)  unexpected server error`,
		Example: "  continuo dlq list\n  continuo dlq list consumer\n  continuo dlq list all query.model:v1",
		Annotations: map[string]string{
			"output_schema": `{"total_open":"number","dead_letters":"array"}`,
			"exit_codes":    `[0,2,5,6]`,
		},
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) > 2 {
				return emit(stdout, stderr, humanOutput(cmd), output.NewUsageError("list takes at most two arguments: [source] [stream]"))
			}
			for i, name := range []string{"source", "stream"} {
				if _, err := optionalArg(args, i, name); err != nil {
					return emit(stdout, stderr, humanOutput(cmd), *err)
				}
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			source, _ := optionalArg(args, 0, "source")
			stream, _ := optionalArg(args, 1, "stream")
			if source == allSources {
				source = ""
			}

			ctx, cancel := context.WithTimeout(cmd.Context(), cfg.Timeout)
			defer cancel()

			c, err := factory(ctx, cfg.DeadLetterEndpoint)
			if err != nil {
				return emit(stdout, stderr, cfg.Human, output.FromGRPC(err))
			}
			defer func() { _ = c.Close() }()

			resp, err := c.List(ctx, source, stream)
			if err != nil {
				return emit(stdout, stderr, cfg.Human, output.FromGRPC(err))
			}

			if cfg.Human {
				for _, d := range resp.GetDeadLetters() {
					if err := output.HumanSuccess(stderr, humanLine(d)); err != nil {
						return err
					}
				}
				return output.HumanSuccess(stderr, strconv.FormatInt(resp.GetTotalOpen(), 10)+" open")
			}
			return output.EmitSuccess(stdout, map[string]any{
				"total_open":   resp.GetTotalOpen(),
				"dead_letters": toJSONList(resp.GetDeadLetters()),
			})
		},
	}
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	return cmd
}

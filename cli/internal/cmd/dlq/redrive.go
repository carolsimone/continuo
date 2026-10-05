package dlq

import (
	"context"
	"io"
	"strconv"
	"strings"

	"github.com/carolsimone/continuo/cli/internal/config"
	"github.com/carolsimone/continuo/cli/internal/output"
	"github.com/spf13/cobra"
)

// NewRedriveCommand builds `continuo dlq redrive <ids> <reason>`. The
// redriving identity is not an argument: it comes from cfg.Actor (the
// CONTINUO_ACTOR env var), so a caller such as the chat agent stamps a fixed
// identity without a flag. When cfg.Actor is empty dead-letter-controller
// records its own system identity.
func NewRedriveCommand(factory ClientFactory, cfg *config.Config, stdout, stderr io.Writer) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "redrive <ids> <reason>",
		Short: "Republish dead letters to their original stream",
		Long: `Republish dead letters to the stream they came from, so the consumer group
that failed them processes them again (an outbox dead letter, never published,
goes to every group of its stream).

Use when the user asks to retry, replay or redrive failed messages, after the
cause was fixed.

Arguments:
  <ids>     One dead letter id, or several separated by commas, as listed by
            "dlq list". Either all are redriven or none is.
  <reason>  Why they are redriven; recorded with the redrive. Required and
            must be non-empty.

The redriving identity is not an argument: it is taken from the
CONTINUO_ACTOR environment variable when set, otherwise dead-letter-controller
records its own system identity. A dead letter already redriven is returned
unchanged.

Output (stdout, JSON):
  {"dead_letters":[{...same object as dlq list, status "redriven" and redrive set...}]}

Errors:
  usage      (exit 2)  wrong number of arguments, no ids, a blank reason, or an id
                       that is not a dead letter id
  not_found  (exit 3)  an id matches no dead letter (nothing was redriven)
  conflict   (exit 4)  a dead letter is older than the 30-day replay horizon, or has
                       no fields to republish (nothing was redriven)
  unavailable(exit 5)  dead-letter-controller is unreachable
  internal   (exit 6)  unexpected server error`,
		Example: "  continuo dlq redrive 7f6c2c4e-1f0a-4f55-9a43-0c1b6f0b9a11 \"upstream fixed\"\n  continuo dlq redrive id1,id2 \"schema migrated\"",
		Annotations: map[string]string{
			"output_schema": `{"dead_letters":"array"}`,
			"exit_codes":    `[0,2,3,4,5,6]`,
			"mutating":      "true",
		},
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 2 {
				return emit(stdout, stderr, humanOutput(cmd), output.NewUsageError("redrive requires exactly two arguments: <ids> <reason>"))
			}
			if len(splitIDs(args[0])) == 0 {
				return emit(stdout, stderr, humanOutput(cmd), output.NewUsageError("redrive requires at least one dead letter id"))
			}
			if strings.TrimSpace(args[1]) == "" {
				return emit(stdout, stderr, humanOutput(cmd), output.NewUsageError("redrive requires a non-empty <reason>"))
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			ids := splitIDs(args[0])

			ctx, cancel := context.WithTimeout(cmd.Context(), cfg.Timeout)
			defer cancel()

			c, err := factory(ctx, cfg.DeadLetterEndpoint)
			if err != nil {
				return emit(stdout, stderr, cfg.Human, output.FromGRPC(err))
			}
			defer func() { _ = c.Close() }()

			resp, err := c.Redrive(ctx, ids, args[1], cfg.Actor)
			if err != nil {
				return emit(stdout, stderr, cfg.Human, output.FromGRPC(err))
			}

			if cfg.Human {
				for _, d := range resp.GetDeadLetters() {
					if err := output.HumanSuccess(stderr, "Redriven "+humanLine(d)); err != nil {
						return err
					}
				}
				return output.HumanSuccess(stderr, strconv.Itoa(len(resp.GetDeadLetters()))+" redriven")
			}
			return output.EmitSuccess(stdout, map[string]any{"dead_letters": toJSONList(resp.GetDeadLetters())})
		},
	}
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	return cmd
}

// splitIDs splits a comma-separated id list, trimming each element and
// dropping empty ones.
func splitIDs(raw string) []string {
	var ids []string
	for _, part := range strings.Split(raw, ",") {
		if id := strings.TrimSpace(part); id != "" {
			ids = append(ids, id)
		}
	}
	return ids
}

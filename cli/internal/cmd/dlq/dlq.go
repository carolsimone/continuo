// Package dlq groups "continuo dlq *" commands: inspecting and redriving the
// dead letters held by dead-letter-controller.
package dlq

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/carolsimone/continuo/cli/internal/client"
	"github.com/carolsimone/continuo/cli/internal/config"
	"github.com/carolsimone/continuo/cli/internal/output"
	deadletterv1 "github.com/carolsimone/continuo/cli/proto/deadletter/v1"
	"github.com/spf13/cobra"
)

// ClientFactory dials and returns a DeadLetterClient. In production this is
// client.NewDeadLetterClient; tests pass a closure returning a fake.
type ClientFactory func(ctx context.Context, endpoint string) (client.DeadLetterClient, error)

// NewCommand builds `continuo dlq` and attaches subcommands. cfg is a pointer
// that root.go fills in via PersistentPreRunE before any subcommand's RunE fires.
func NewCommand(cfg *config.Config, stdout, stderr io.Writer) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "dlq",
		Short: "Inspect and redrive dead letters",
		// A group command does no work itself: bare, it prints its help; with an
		// argument that is not one of its subcommands it reports an unknown
		// command through the usage envelope instead of this help text.
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) > 0 {
				return emit(stdout, stderr, humanOutput(cmd), output.NewUsageError(fmt.Sprintf("unknown command %q for %q", args[0], cmd.CommandPath())))
			}
			return cmd.Help()
		},
	}
	cmd.AddCommand(NewListCommand(defaultFactory, cfg, stdout, stderr))
	cmd.AddCommand(NewShowCommand(defaultFactory, cfg, stdout, stderr))
	cmd.AddCommand(NewRedriveCommand(defaultFactory, cfg, stdout, stderr))
	return cmd
}

func defaultFactory(ctx context.Context, endpoint string) (client.DeadLetterClient, error) {
	return client.NewDeadLetterClient(ctx, endpoint)
}

// emit writes the CLIError envelope (stdout JSON, or stderr in human mode) and
// returns it so cobra's Execute preserves the exit-code contract.
func emit(stdout, stderr io.Writer, human bool, e output.CLIError) error {
	if human {
		_ = output.HumanError(stderr, e)
	} else {
		_ = output.EmitError(stdout, e)
	}
	return e
}

// humanOutput reports whether --human is set, read directly from the command's
// flags. Argument validators run before PersistentPreRunE populates cfg, so a
// pre-RunE usage error must read the flag itself.
func humanOutput(cmd *cobra.Command) bool {
	v, _ := cmd.Flags().GetBool("human")
	return v
}

// deadLetterJSON is one dead letter on stdout.
type deadLetterJSON struct {
	ID                string       `json:"id"`
	Source            string       `json:"source"`
	FailureKind       string       `json:"failure_kind"`
	Stream            string       `json:"stream"`
	Group             string       `json:"group"`
	OriginalMessageID string       `json:"original_message_id"`
	Producer          string       `json:"producer"`
	Error             string       `json:"error"`
	DeliveryCount     int64        `json:"delivery_count"`
	OriginalAt        string       `json:"original_at"`
	RecordedAt        string       `json:"recorded_at"`
	ExpiresAt         string       `json:"expires_at"`
	Status            string       `json:"status"`
	Redrivable        bool         `json:"redrivable"`
	Redrive           *redriveJSON `json:"redrive"`
}

// redriveJSON records who redrove a dead letter, why and when.
type redriveJSON struct {
	Actor  string `json:"actor"`
	Reason string `json:"reason"`
	At     string `json:"at"`
}

func toJSON(d *deadletterv1.DeadLetter) deadLetterJSON {
	out := deadLetterJSON{
		ID: d.GetId(), Source: d.GetSource(), FailureKind: d.GetFailureKind(), Stream: d.GetStream(),
		Group: d.GetConsumerGroup(), OriginalMessageID: d.GetOriginalMessageId(), Producer: d.GetProducer(), Error: d.GetError(),
		DeliveryCount: d.GetDeliveryCount(), OriginalAt: d.GetOriginalAt(), RecordedAt: d.GetRecordedAt(),
		ExpiresAt: d.GetExpiresAt(), Status: d.GetStatus(), Redrivable: d.GetRedrivable(),
	}
	if r := d.GetRedrive(); r != nil {
		out.Redrive = &redriveJSON{Actor: r.GetActor(), Reason: r.GetReason(), At: r.GetAt()}
	}
	return out
}

// toJSONList converts dead letters, never returning nil so an empty list
// encodes as [] rather than null.
func toJSONList(items []*deadletterv1.DeadLetter) []deadLetterJSON {
	out := make([]deadLetterJSON, 0, len(items))
	for _, d := range items {
		out = append(out, toJSON(d))
	}
	return out
}

// humanLine renders one dead letter as a single stderr line.
func humanLine(d *deadletterv1.DeadLetter) string {
	where := d.GetStream()
	if g := d.GetConsumerGroup(); g != "" {
		where += " [" + g + "]"
	}
	return fmt.Sprintf("%s  %s/%s  %s  %s", d.GetId(), d.GetSource(), d.GetFailureKind(), where, d.GetError())
}

// optionalArg returns args[i], "" when it is absent, or a usage error when it
// is present but blank: a filter is omitted, never passed empty.
func optionalArg(args []string, i int, name string) (string, *output.CLIError) {
	if len(args) <= i {
		return "", nil
	}
	if strings.TrimSpace(args[i]) == "" {
		e := output.NewUsageError(name + " must not be empty; omit it to match all")
		return "", &e
	}
	return args[i], nil
}

// Package node groups "continuo node *" commands.
package node

import (
	"context"
	"io"

	"github.com/carolsimone/continuo/cli/internal/client"
	"github.com/carolsimone/continuo/cli/internal/config"
	"github.com/carolsimone/continuo/cli/internal/output"
	"github.com/spf13/cobra"
)

// StateClientFactory dials and returns a StateClient. In production this is
// client.NewStateClient; tests pass a closure returning a fake.
type StateClientFactory func(ctx context.Context, endpoint string) (client.StateClient, error)

// NewCommand builds `continuo node` and attaches subcommands. cfg is a pointer
// that root.go fills in via PersistentPreRunE before any subcommand's RunE fires.
func NewCommand(cfg *config.Config, stdout, stderr io.Writer) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "node",
		Short: "Operate on individual dbt model nodes",
	}
	cmd.AddCommand(NewHistoryCommand(defaultFactory, cfg, stdout, stderr))
	cmd.AddCommand(NewTriggerCommand(defaultFactory, cfg, stdout, stderr))
	cmd.AddCommand(NewTestCommand(defaultFactory, cfg, stdout, stderr))
	cmd.AddCommand(NewBuildCommand(defaultFactory, cfg, stdout, stderr))
	return cmd
}

func defaultFactory(ctx context.Context, endpoint string) (client.StateClient, error) {
	return client.NewStateClient(ctx, endpoint)
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

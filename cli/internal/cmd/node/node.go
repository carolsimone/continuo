// Package node groups "continuo node *" commands.
package node

import (
	"context"
	"fmt"
	"io"

	"github.com/carolsimone/continuo/cli/internal/client"
	"github.com/carolsimone/continuo/cli/internal/config"
	"github.com/carolsimone/continuo/cli/internal/output"
	"github.com/spf13/cobra"
)

// RunClientFactory dials and returns a RunClient. In production this is
// client.NewRunClient; tests pass a closure returning a fake.
type RunClientFactory func(ctx context.Context, endpoint string) (client.RunClient, error)

// CatalogClientFactory dials and returns a CatalogClient. In
// production this is client.NewCatalogClient; tests pass a closure
// returning a fake.
type CatalogClientFactory func(ctx context.Context, endpoint string) (client.CatalogClient, error)

// NewCommand builds `continuo node` and attaches subcommands. cfg is a pointer
// that root.go fills in via PersistentPreRunE before any subcommand's RunE fires.
func NewCommand(cfg *config.Config, stdout, stderr io.Writer) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "node",
		Short: "Operate on individual dbt model nodes",
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
	cmd.AddCommand(NewHistoryCommand(defaultFactory, cfg, stdout, stderr))
	cmd.AddCommand(NewTriggerCommand(defaultFactory, cfg, stdout, stderr))
	cmd.AddCommand(NewTestCommand(defaultFactory, cfg, stdout, stderr))
	cmd.AddCommand(NewBuildCommand(defaultFactory, cfg, stdout, stderr))
	cmd.AddCommand(NewFullRefreshCommand(defaultFactory, cfg, stdout, stderr))
	cmd.AddCommand(NewVersionsCommand(defaultCatalogFactory, cfg, stdout, stderr))
	cmd.AddCommand(NewDiffCommand(defaultCatalogFactory, cfg, stdout, stderr))
	cmd.AddCommand(NewUpstreamChangesCommand(defaultCatalogFactory, cfg, stdout, stderr))
	cmd.AddCommand(NewCodeUnitsCommand(defaultCatalogFactory, cfg, stdout, stderr))
	return cmd
}

func defaultFactory(ctx context.Context, endpoint string) (client.RunClient, error) {
	return client.NewRunClient(ctx, endpoint)
}

func defaultCatalogFactory(ctx context.Context, endpoint string) (client.CatalogClient, error) {
	return client.NewCatalogClient(ctx, endpoint)
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

// Package schedule groups "continuo schedule *" commands.
package schedule

import (
	"context"
	"io"

	"github.com/carolsimone/continuo/cli/internal/client"
	"github.com/carolsimone/continuo/cli/internal/config"
	"github.com/spf13/cobra"
)

// NewCommand builds `continuo schedule` and attaches subcommands.
// cfg is a pointer that root.go fills in via PersistentPreRunE before any
// subcommand's RunE fires.
func NewCommand(cfg *config.Config, stdout, stderr io.Writer) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "schedule",
		Short: "Operate on Continuo schedules",
	}
	cmd.AddCommand(NewTriggerCommand(defaultFactory, cfg, stdout, stderr))
	return cmd
}

func defaultFactory(ctx context.Context, endpoint string) (client.StateClient, error) {
	return client.NewStateClient(ctx, endpoint)
}

// Package schedule groups "continuo schedule *" commands.
package schedule

import (
	"context"
	"io"

	"github.com/carolsimone/continuo/cli/internal/client"
	"github.com/carolsimone/continuo/cli/internal/config"
	"github.com/spf13/cobra"
)

// StateClientFactory dials and returns a StateClient. In production this is
// client.NewStateClient; tests pass a closure returning a fake.
type StateClientFactory func(ctx context.Context, endpoint string) (client.StateClient, error)

// OrchestratorClientFactory dials and returns an OrchestratorClient. In
// production this is client.NewOrchestratorClient; tests pass a closure
// returning a fake.
type OrchestratorClientFactory func(ctx context.Context, endpoint string) (client.OrchestratorClient, error)

// NewCommand builds `continuo schedule` and attaches subcommands.
// cfg is a pointer that root.go fills in via PersistentPreRunE before any
// subcommand's RunE fires.
func NewCommand(cfg *config.Config, stdout, stderr io.Writer) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "schedule",
		Short: "Operate on Continuo schedules",
	}
	cmd.AddCommand(NewTriggerCommand(defaultFactory, cfg, stdout, stderr))
	cmd.AddCommand(NewTestCommand(defaultFactory, cfg, stdout, stderr))
	cmd.AddCommand(NewListCommand(defaultFactory, cfg, stdout, stderr))
	cmd.AddCommand(NewStatusCommand(defaultFactory, cfg, stdout, stderr))
	cmd.AddCommand(NewGraphCommand(defaultOrchestratorFactory, cfg, stdout, stderr))
	cmd.AddCommand(NewCancelCommand(defaultFactory, cfg, stdout, stderr))
	return cmd
}

func defaultFactory(ctx context.Context, endpoint string) (client.StateClient, error) {
	return client.NewStateClient(ctx, endpoint)
}

func defaultOrchestratorFactory(ctx context.Context, endpoint string) (client.OrchestratorClient, error) {
	return client.NewOrchestratorClient(ctx, endpoint)
}

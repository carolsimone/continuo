package node

import (
	"context"
	"io"
	"testing"

	"github.com/carolsimone/continuo/cli/internal/client"
	"github.com/carolsimone/continuo/cli/internal/config"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
)

// The state service refuses a single-node run it cannot plan at the RPC, so the
// help text an LLM reads must say the refusal is the command's own error and no
// run exists, not that the failure shows up later in a run's history.
func TestNodeRunCommands_HelpDescribesTheRefusalsAtTheRPC(t *testing.T) {
	factory := func(context.Context, string) (client.RunClient, error) { return nil, nil }
	cfg := &config.Config{}
	commands := map[string]*cobra.Command{
		"trigger":      NewTriggerCommand(factory, cfg, io.Discard, io.Discard),
		"test":         NewTestCommand(factory, cfg, io.Discard, io.Discard),
		"build":        NewBuildCommand(factory, cfg, io.Discard, io.Discard),
		"full-refresh": NewFullRefreshCommand(factory, cfg, io.Discard, io.Discard),
	}
	for name, cmd := range commands {
		t.Run(name, func(t *testing.T) {
			assert.Contains(t, cmd.Long, "refused (not_found)", "a node the live release does not hold is refused")
			assert.Regexp(t, "no run is created|creates a run", cmd.Long, "a refusal creates no run")
			assert.NotContains(t, cmd.Long, "asynchronously", "no refusal surfaces downstream")
		})
	}
}

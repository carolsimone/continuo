// Package schedule groups "continuo schedule *" commands.
package schedule

import (
	"context"
	"io"
	"time"

	"github.com/carolsimone/continuo/cli/internal/client"
	"github.com/carolsimone/continuo/cli/internal/config"
	"github.com/carolsimone/continuo/cli/internal/output"
	"github.com/spf13/cobra"
)

// StateClientFactory dials and returns a StateClient. In production this is
// client.NewStateClient; tests pass a closure returning a fake.
type StateClientFactory func(ctx context.Context, endpoint string) (client.StateClient, error)

// NewTriggerCommand builds `continuo schedule trigger <schedule-name>`.
// cfg is a pointer because root.go populates it in PersistentPreRunE after
// flags have been parsed — the subcommand reads the up-to-date value at RunE.
// stdout and stderr are injected so tests can capture them; in production
// the root command wires os.Stdout / os.Stderr.
func NewTriggerCommand(factory StateClientFactory, cfg *config.Config, stdout, stderr io.Writer) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "trigger <schedule-name>",
		Short: "Trigger a new run of the named schedule",
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return output.NewUsageError("trigger requires exactly one argument: <schedule-name>")
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			scheduleName := args[0]
			ctx, cancel := context.WithTimeout(cmd.Context(), cfg.Timeout)
			defer cancel()

			c, err := factory(ctx, cfg.StateEndpoint)
			if err != nil {
				return emit(stdout, stderr, cfg.Human, output.FromGRPC(err))
			}
			defer c.Close()

			resp, err := c.TriggerSchedule(ctx, scheduleName)
			if err != nil {
				return emit(stdout, stderr, cfg.Human, output.FromGRPC(err))
			}

			if cfg.Human {
				return output.HumanSuccess(stderr, "Triggered run "+resp.ScheduleId+" for schedule '"+scheduleName+"'")
			}
			payload := map[string]string{
				"schedule_id":   resp.ScheduleId,
				"schedule_name": scheduleName,
				"triggered_at":  time.Now().UTC().Format(time.RFC3339),
			}
			return output.EmitSuccess(stdout, payload)
		},
	}
	// Intercept argument-validation errors so they exit 2 with a CLIError.
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	return cmd
}

// emit returns the CLIError so the caller sees it via cmd.Execute(), and also
// writes the envelope to stdout (or stderr in human mode). Returning the
// CLIError preserves the exit-code contract through cobra's RunE.
func emit(stdout, stderr io.Writer, human bool, e output.CLIError) error {
	if human {
		_ = output.HumanError(stderr, e)
	} else {
		_ = output.EmitError(stdout, e)
	}
	return e
}

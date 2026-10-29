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

// NewTriggerCommand builds `continuo schedule trigger <schedule-name>`.
// cfg is a pointer because root.go populates it in PersistentPreRunE after
// flags have been parsed — the subcommand reads the up-to-date value at RunE.
// stdout and stderr are injected so tests can capture them; in production
// the root command wires os.Stdout / os.Stderr.
func NewTriggerCommand(factory RunClientFactory, cfg *config.Config, stdout, stderr io.Writer) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "trigger <schedule-name>",
		Short: "Trigger a new run of the named schedule",
		Long: `Trigger a new run of the named schedule.

Use when the user asks to start, run, or kick off a schedule now.

Arguments:
  <schedule-name>  The schedule to trigger.

Output (stdout, JSON):
  {"run_id":string,"schedule_name":string,"triggered_at":string}

` + client.RequestIDHelp + `

Errors:
  usage      (exit 2)  wrong number of arguments, or a name longer than 50 characters
  not_found  (exit 3)  no model of the live release has the schedule's tag and the schedule has no cron (see "schedule list")
  conflict   (exit 4)  a run is already active for this schedule, or the request id was used before with other arguments
  unavailable(exit 5)  the run-controller service is unreachable
  maintenance(exit 7)  continuo is in maintenance mode; new work is refused until it is turned off (not retryable)
  internal   (exit 6)  unexpected server error`,
		Example: "  continuo schedule trigger daily-revenue",
		Annotations: map[string]string{
			"output_schema": `{"run_id":"string","schedule_name":"string","triggered_at":"string"}`,
			"exit_codes":    `[0,2,3,4,5,6,7]`,
			"mutating":      "true",
		},
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return emit(stdout, stderr, humanOutput(cmd), output.NewUsageError("trigger requires exactly one argument: <schedule-name>"))
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			scheduleName := args[0]
			ctx, cancel := context.WithTimeout(cmd.Context(), cfg.Timeout)
			defer cancel()

			c, err := factory(ctx, cfg.RunControllerEndpoint)
			if err != nil {
				return emit(stdout, stderr, cfg.Human, output.FromGRPC(err))
			}
			defer func() { _ = c.Close() }()

			resp, err := c.TriggerSchedule(ctx, client.RequestID(), scheduleName, cfg.Actor)
			if err != nil {
				return emit(stdout, stderr, cfg.Human, output.FromGRPC(err))
			}

			if cfg.Human {
				return output.HumanSuccess(stderr, "Triggered run "+resp.GetRunId()+" for schedule '"+scheduleName+"'")
			}
			payload := map[string]string{
				"run_id":        resp.GetRunId(),
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

// humanOutput reports whether --human is set, read directly from the command's
// flags. Argument validators run before PersistentPreRunE populates cfg, so a
// pre-RunE usage error cannot rely on cfg.Human and must read the flag itself.
func humanOutput(cmd *cobra.Command) bool {
	v, _ := cmd.Flags().GetBool("human")
	return v
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

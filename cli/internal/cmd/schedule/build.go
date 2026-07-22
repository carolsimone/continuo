package schedule

import (
	"context"
	"io"
	"time"

	"github.com/carolsimone/continuo/cli/internal/config"
	"github.com/carolsimone/continuo/cli/internal/output"
	"github.com/spf13/cobra"
)

// NewBuildCommand builds `continuo schedule build <schedule-name>`.
// cfg is a pointer because root.go populates it in PersistentPreRunE after
// flags have been parsed — the subcommand reads the up-to-date value at RunE.
// stdout and stderr are injected so tests can capture them; in production
// the root command wires os.Stdout / os.Stderr.
func NewBuildCommand(factory StateClientFactory, cfg *config.Config, stdout, stderr io.Writer) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "build <schedule-name>",
		Short: "Run dbt build for every model in the named schedule",
		Long: `Run dbt build for every model in the named schedule, using the schedule's
current (latest) topology and manifest version.

Use when the user asks to build an entire schedule (its whole DAG of models)
now, as opposed to only testing it. dbt build both materializes (runs) and
tests each node in one invocation. Unlike schedule test's flat fan-out, the
whole-DAG build is dependency-ordered: nodes run in topological order, and
if a node fails, its downstream descendants are cascade-skipped rather than
attempted. A node with no tests defined is still built; there is no
"no_tests" skip for build.

Arguments:
  <schedule-name>  The schedule to build.

Output (stdout, JSON):
  {"schedule_id":string,"schedule_name":string,"triggered_at":string}

Errors:
  usage      (exit 2)  wrong number of arguments
  not_found  (exit 3)  schedule not in the catalog
  conflict   (exit 4)  a run is already active for this schedule
  unavailable(exit 5)  the state service is unreachable
  internal   (exit 6)  unexpected server error`,
		Example: "  continuo schedule build daily",
		Annotations: map[string]string{
			"output_schema": `{"schedule_id":"string","schedule_name":"string","triggered_at":"string"}`,
			"exit_codes":    `[0,2,3,4,5,6]`,
			"mutating":      "true",
		},
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return emit(stdout, stderr, humanOutput(cmd), output.NewUsageError("build requires exactly one argument: <schedule-name>"))
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
			defer func() { _ = c.Close() }()

			resp, err := c.TriggerScheduleBuild(ctx, scheduleName, cfg.Actor)
			if err != nil {
				return emit(stdout, stderr, cfg.Human, output.FromGRPC(err))
			}

			if cfg.Human {
				return output.HumanSuccess(stderr, "Triggered build run "+resp.ScheduleId+" for schedule '"+scheduleName+"'")
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

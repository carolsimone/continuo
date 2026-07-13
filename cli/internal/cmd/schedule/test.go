package schedule

import (
	"context"
	"io"
	"time"

	"github.com/carolsimone/continuo/cli/internal/config"
	"github.com/carolsimone/continuo/cli/internal/output"
	"github.com/spf13/cobra"
)

// NewTestCommand builds `continuo schedule test <schedule-name>`.
// cfg is a pointer because root.go populates it in PersistentPreRunE after
// flags have been parsed — the subcommand reads the up-to-date value at RunE.
// stdout and stderr are injected so tests can capture them; in production
// the root command wires os.Stdout / os.Stderr.
func NewTestCommand(factory StateClientFactory, cfg *config.Config, stdout, stderr io.Writer) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "test <schedule-name>",
		Short: "Run dbt tests for every model in the named schedule",
		Long: `Run dbt tests for every model in the named schedule, using the schedule's
current (latest) topology and manifest version.

Use when the user asks to test, validate, or check an entire schedule (its
whole DAG of models) now, as opposed to running (building) it. Every node in
the schedule that has dbt tests defined runs dbt test; nodes with no tests
defined are skipped rather than failed.

Arguments:
  <schedule-name>  The schedule to test.

Output (stdout, JSON):
  {"schedule_id":string,"schedule_name":string,"triggered_at":string}

Errors:
  usage      (exit 2)  wrong number of arguments
  not_found  (exit 3)  schedule not in the catalog
  conflict   (exit 4)  a run is already active for this schedule
  unavailable(exit 5)  the state service is unreachable
  internal   (exit 6)  unexpected server error`,
		Example: "  continuo schedule test daily",
		Annotations: map[string]string{
			"output_schema": `{"schedule_id":"string","schedule_name":"string","triggered_at":"string"}`,
			"exit_codes":    `[0,2,3,4,5,6]`,
			"mutating":      "true",
		},
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return emit(stdout, stderr, humanOutput(cmd), output.NewUsageError("test requires exactly one argument: <schedule-name>"))
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

			resp, err := c.TriggerScheduleTest(ctx, scheduleName)
			if err != nil {
				return emit(stdout, stderr, cfg.Human, output.FromGRPC(err))
			}

			if cfg.Human {
				return output.HumanSuccess(stderr, "Triggered test run "+resp.ScheduleId+" for schedule '"+scheduleName+"'")
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

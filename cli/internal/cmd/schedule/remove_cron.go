package schedule

import (
	"io"
	"strings"

	"github.com/carolsimone/continuo/cli/internal/config"
	"github.com/carolsimone/continuo/cli/internal/output"
	"github.com/spf13/cobra"
)

// NewRemoveCronCommand builds `continuo schedule remove-cron <name>`. It makes
// one call: removing the cron of a schedule that has none succeeds, so the
// command needs no read first.
func NewRemoveCronCommand(factory RunClientFactory, cfg *config.Config, stdout, stderr io.Writer) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "remove-cron <name>",
		Short: "Remove the cron of a schedule: it then runs only when triggered",
		Long: `Remove the cron of a schedule: it then runs only when triggered.

Use when the user asks to stop a schedule running on its own for good, to make
it manual, or to delete its cron. The schedule itself stays (it is the dbt tag
of its models); only its cron, timezone, description and pause are removed.
A run of it already in progress goes on, and "schedule trigger" still starts it.
Use "schedule pause" instead to keep the cron for later.

Removing the cron of a schedule that has none succeeds and changes nothing, so
a success does not show that a cron existed: names are case-sensitive, and a
name that is not a schedule at all succeeds too. Read "schedule list" to see the
cron before and after.

Arguments:
  <name>  The schedule whose cron to remove, exactly as "schedule list" shows it.

Output (stdout, JSON):
  {"schedule_name":string,"has_cron":bool}
  has_cron is false: the schedule has no cron now.

Errors:
  usage      (exit 2)  wrong number of arguments or a blank name
  unavailable(exit 5)  the state service is unreachable
  maintenance(exit 7)  continuo is in maintenance mode; edits are refused until it is turned off (not retryable)
  internal   (exit 6)  unexpected server error`,
		Example: "  continuo schedule remove-cron daily",
		Annotations: map[string]string{
			"output_schema": `{"schedule_name":"string","has_cron":"bool"}`,
			"exit_codes":    `[0,2,5,6,7]`,
			"mutating":      "true",
		},
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 || strings.TrimSpace(args[0]) == "" {
				return emit(stdout, stderr, humanOutput(cmd), output.NewUsageError("remove-cron requires exactly one argument: <name>"))
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]

			c, err := factory(cmd.Context(), cfg.StateEndpoint)
			if err != nil {
				return emit(stdout, stderr, cfg.Human, output.FromGRPC(err))
			}
			defer func() { _ = c.Close() }()

			ctx, cancel := callContext(cmd, cfg)
			defer cancel()
			if _, err := c.DeleteSchedule(ctx, cfg.Actor, name); err != nil {
				return emit(stdout, stderr, cfg.Human, output.FromGRPC(err))
			}

			if cfg.Human {
				return output.HumanSuccess(stderr, "Removed the cron of schedule '"+name+"'; it now runs only when triggered")
			}
			return output.EmitSuccess(stdout, map[string]any{"schedule_name": name, "has_cron": false})
		},
	}
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	return cmd
}

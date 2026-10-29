package schedule

import (
	"io"

	"github.com/carolsimone/continuo/cli/internal/config"
	"github.com/spf13/cobra"
)

// NewResumeCommand builds `continuo schedule resume <name>`.
func NewResumeCommand(factory RunClientFactory, cfg *config.Config, stdout, stderr io.Writer) *cobra.Command {
	return newPauseResumeCommand(pauseSpec{
		use:   "resume <name>",
		short: "Resume a paused schedule: it runs on its cron again",
		long: `Resume a paused schedule: it runs on its cron again.

Use when the user asks to resume, unpause, restart or re-enable the automatic
runs of a paused schedule. The cron, timezone and description are kept. The next
fire counts from now: fires that came due while the schedule was paused do not
run.

The command reads the schedule and saves it back running, with the version it
read. Resuming a schedule that is not paused succeeds and changes nothing
("changed":false).

Arguments:
  <name>  The schedule to resume, exactly as "schedule list" shows it. Names are
          case-sensitive: "Daily" is not "daily".

Output (stdout, JSON):
  {"schedule_name":string,"paused":bool,"changed":bool,"cron":string,
   "timezone":string,"version":number,"next_fire_at":string}
  paused is false. changed is false when the schedule was not paused.
  version is the schedule's version after the command. next_fire_at is the next
  fire (RFC3339, UTC), omitted when the cron fires no more.

Errors:
  usage      (exit 2)  wrong number of arguments, a blank name, or a name longer than 50 characters (the server refuses it)
  not_found  (exit 3)  no schedule has that name, or the schedule has no cron and so nothing to resume
  conflict   (exit 4)  another edit saved the schedule while this ran ("changed by <who> while this ran; run it again"); nothing was changed, so run the command again
  unavailable(exit 5)  the run-controller service is unreachable
  maintenance(exit 7)  continuo is in maintenance mode; edits are refused until it is turned off (not retryable)
  internal   (exit 6)  unexpected server error`,
		example:      "  continuo schedule resume hourly",
		outputSchema: `{"schedule_name":"string","paused":"bool","changed":"bool","cron":"string","timezone":"string","version":"number","next_fire_at":"string"}`,
		paused:       false,
		verb:         "resume",
		done:         "Resumed",
		already:      "not paused",
	}, factory, cfg, stdout, stderr)
}

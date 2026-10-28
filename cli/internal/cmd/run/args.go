package run

import (
	"fmt"
	"io"

	"github.com/carolsimone/continuo/cli/internal/output"
	"github.com/spf13/cobra"
)

// startedRun is the success payload of the commands that create a run from a
// source run.
type startedRun struct {
	RunID        string `json:"run_id"`
	ScheduleName string `json:"schedule_name"`
}

// usageFailure writes the usage error to stdout (or stderr in human mode) and
// returns it. It serves argument validators, which run before cfg is filled.
func usageFailure(cmd *cobra.Command, stdout, stderr io.Writer, format string, a ...any) error {
	return emit(stdout, stderr, humanOutput(cmd), output.NewUsageError(fmt.Sprintf(format, a...)))
}

// parseChangedUpstream reads the optional bracketed boolean positional
// [with-changed-upstream] of the rebase commands. It is absent (false), "true"
// or "false"; any other spelling, a blank one included, is refused rather than
// read as false, so a mistyped option never silently drops the upstream nodes.
func parseChangedUpstream(args []string, index int) (bool, error) {
	if len(args) <= index {
		return false, nil
	}
	switch args[index] {
	case "true":
		return true, nil
	case "false":
		return false, nil
	default:
		return false, fmt.Errorf("[with-changed-upstream] must be true or false, got %q", args[index])
	}
}

package node

import (
	"io"
	"strings"

	"github.com/carolsimone/continuo/cli/internal/output"
	"github.com/spf13/cobra"
)

// nodeTarget is the positional input shared by node trigger, test and build:
// the node's identity triple plus an optional past run whose metadata
// snapshot the new run should be pinned to.
type nodeTarget struct {
	service, schema, table string
	// sourceRunID is empty when the run should use the latest topology
	// metadata, and a run id when it should reuse the metadata snapshot of
	// that past run.
	sourceRunID string
}

// parseNodeTarget reads a validated 3- or 4-element argument list.
func parseNodeTarget(args []string) nodeTarget {
	t := nodeTarget{service: args[0], schema: args[1], table: args[2]}
	if len(args) == 4 {
		t.sourceRunID = args[3]
	}
	return t
}

// nodeTargetArgs returns the cobra Args validator for a node-run command:
// exactly <service> <schema> <table> with an optional [source-run-id]. Any
// other count is reported through the usage envelope as exit 2.
//
// A fourth argument that is present but blank (typically an unset shell
// variable passed as "$SOURCE_RUN_ID") is also a usage error. Latest-metadata
// mode is selected only by omitting the argument, never by an empty one, so a
// caller who asked for a snapshot cannot silently get a run against current
// code instead.
func nodeTargetArgs(verb string, stdout, stderr io.Writer) cobra.PositionalArgs {
	return func(cmd *cobra.Command, args []string) error {
		if len(args) < 3 || len(args) > 4 {
			return emit(stdout, stderr, humanOutput(cmd), output.NewUsageError(verb+" requires <service> <schema> <table> and an optional [source-run-id]"))
		}
		if len(args) == 4 && strings.TrimSpace(args[3]) == "" {
			return emit(stdout, stderr, humanOutput(cmd), output.NewUsageError(verb+": source-run-id must not be empty; omit it to use the latest metadata"))
		}
		return nil
	}
}

// sourceRunArgDoc is the help text shared by node trigger, test and build for
// the optional fourth positional argument and the errors it can add.
const sourceRunArgDoc = `  [source-run-id]  Optional. The id of a past run of this node, as listed by
                   "node history". When given, the new run reuses the image
                   and manifest version that run used ("snapshot_of_run"
                   metadata) instead of the latest topology. The source run
                   must have finished (succeeded, failed or cancelled) and
                   must have included this node. Omit the argument for
                   latest mode; an empty value is a usage error.`

// sourceRunErrorsDoc lists the extra error codes snapshot mode can return.
const sourceRunErrorsDoc = `  not_found  (exit 3)  the source run does not exist, or it did not include this node
  conflict   (exit 4)  the source run has not finished yet`

// nodeRunExitCodes is the exit_codes annotation for node trigger, test and build.
const nodeRunExitCodes = `[0,2,3,4,5,6]`

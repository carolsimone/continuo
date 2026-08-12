package node

import (
	"context"
	"fmt"
	"io"

	"github.com/carolsimone/continuo/cli/internal/config"
	"github.com/carolsimone/continuo/cli/internal/output"
	orchestratorv1 "github.com/carolsimone/continuo/cli/proto/orchestrator/v1"
	"github.com/spf13/cobra"
)

// upstreamChangeEntry is the JSON-serialisable representation of one
// ancestor of a node together with its most recent code change.
type upstreamChangeEntry struct {
	UniqueID string             `json:"unique_id"`
	Depth    int32              `json:"depth"`
	Diff     versionDiffPayload `json:"diff"`
}

type upstreamChangesPayload struct {
	Changes []upstreamChangeEntry `json:"changes"`
}

// NewUpstreamChangesCommand builds
// `continuo node upstream-changes <service> <schema> <table> [--depth N] [--since RFC3339]`.
func NewUpstreamChangesCommand(factory OrchestratorClientFactory, cfg *config.Config, stdout, stderr io.Writer) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "upstream-changes <service> <schema> <table>",
		Short: "List a model node's ancestors' most recent code changes",
		Long: `List a model node's ancestors' most recent code changes, most-recently-changed first.

Use when the user wants to know what changed upstream of a failing or
suspicious dbt model: which ancestors changed, how recently, and what their
diff looks like.

Arguments:
  <service>  The owning service name.
  <schema>   The schema name.
  <table>    The table (model) name.

The node is addressed by its unique_id, "<schema>.<table>"; <service> is
accepted for consistency with the other node subcommands but does not appear
in the unique_id.

Contract caps (enforced server-side, not adjustable): results are capped at
the 5 most-recently-changed ancestors, and each diff is independently capped
at 8 KiB with its own truncated flag set on the diffs that were cut — size
prompts against both caps, not just the top-level result count.

Flags:
  --depth  How many hops upstream to walk. <= 0 (the default) applies the
           server's default of 3 hops. The server rejects any value above 10
           as invalid — this is a hard cap, not a client-side clamp.
  --since  RFC3339 timestamp; ancestors whose newest version predates it are
           excluded. Empty (the default) applies no time filter.

Output (stdout, JSON):
  {"changes":[{"unique_id":string,"depth":number,
   "diff":{...diff object, same shape as "node diff"...}}]}

Errors:
  usage      (exit 2)  wrong number of arguments, --depth above 10, or --since is not RFC3339
  not_found  (exit 3)  the node's unique_id is not recorded
  unavailable(exit 5)  the orchestrator service is unreachable
  internal   (exit 6)  unexpected server error`,
		Example: "  continuo node upstream-changes finance analytics orders --depth 5 --since 2026-07-01T00:00:00Z",
		Annotations: map[string]string{
			"output_schema": `{"changes":"array"}`,
			"exit_codes":    `[0,2,3,5,6]`,
		},
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 3 {
				return emit(stdout, stderr, humanOutput(cmd), output.NewUsageError("upstream-changes requires exactly three arguments: <service> <schema> <table>"))
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			schema, table := args[1], args[2]
			uniqueID := schema + "." + table
			depth, _ := cmd.Flags().GetInt32("depth")
			since, _ := cmd.Flags().GetString("since")

			ctx, cancel := context.WithTimeout(cmd.Context(), cfg.Timeout)
			defer cancel()

			c, err := factory(ctx, cfg.OrchestratorEndpoint)
			if err != nil {
				return emit(stdout, stderr, cfg.Human, output.FromGRPC(err))
			}
			defer func() { _ = c.Close() }()

			resp, err := c.GetUpstreamChanges(ctx, uniqueID, depth, since)
			if err != nil {
				return emit(stdout, stderr, cfg.Human, output.FromGRPC(err))
			}

			if cfg.Human {
				return humanUpstreamChanges(stderr, resp.GetChanges())
			}
			return output.EmitSuccess(stdout, toUpstreamChangesPayload(resp.GetChanges()))
		},
	}
	cmd.Flags().Int32("depth", 0, "Hops upstream to walk; <= 0 applies the server default of 3, values above 10 are rejected")
	cmd.Flags().String("since", "", "RFC3339 timestamp; excludes ancestors whose newest version predates it (default: no filter)")
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	return cmd
}

func toUpstreamChangesPayload(changes []*orchestratorv1.UpstreamChange) upstreamChangesPayload {
	out := make([]upstreamChangeEntry, 0, len(changes))
	for _, c := range changes {
		out = append(out, upstreamChangeEntry{
			UniqueID: c.GetUniqueId(),
			Depth:    c.GetDepth(),
			Diff:     toVersionDiffPayload(c.GetDiff()),
		})
	}
	return upstreamChangesPayload{Changes: out}
}

// humanUpstreamChanges writes one line per ancestor to stderr:
//
//	<depth>  <unique_id>  truncated=<bool>
func humanUpstreamChanges(stderr io.Writer, changes []*orchestratorv1.UpstreamChange) error {
	for _, c := range changes {
		if _, err := fmt.Fprintf(stderr, "%d  %s  truncated=%t\n", c.GetDepth(), c.GetUniqueId(), c.GetDiff().GetTruncated()); err != nil {
			return err
		}
	}
	return nil
}

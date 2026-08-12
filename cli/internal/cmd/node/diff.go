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

// versionDiffPayload is the JSON-serialisable representation of a server-
// rendered comparison between two versions of one node.
type versionDiffPayload struct {
	UniqueID          string       `json:"unique_id"`
	From              versionEntry `json:"from"`
	To                versionEntry `json:"to"`
	RawCodeDiff       string       `json:"raw_code_diff,omitempty"`
	ConfigDiff        string       `json:"config_diff,omitempty"`
	SourceChanged     bool         `json:"source_changed"`
	SharedCodeChanged bool         `json:"shared_code_changed"`
	ConfigChanged     bool         `json:"config_changed"`
	Truncated         bool         `json:"truncated"`
}

// NewDiffCommand builds `continuo node diff <service> <schema> <table> --from <seq> --to <seq>`.
func NewDiffCommand(factory OrchestratorClientFactory, cfg *config.Config, stdout, stderr io.Writer) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "diff <service> <schema> <table>",
		Short: "Render the diff between two recorded versions of one model node",
		Long: `Render the diff between two recorded versions of one model node.

Use when the user wants to see exactly what changed between two code versions
of a dbt model: the raw source diff, the config diff, and which of source,
shared code, and config actually changed.

Arguments:
  <service>  The owning service name.
  <schema>   The schema name.
  <table>    The table (model) name.

The node is addressed by its unique_id, "<schema>.<table>"; <service> is
accepted for consistency with the other node subcommands but does not appear
in the unique_id.

Flags:
  --from  Required. The version_seq to diff from. version_seq is a stable
          per-node handle used to address a version — it is NOT a
          chronological ordering, and --from need not be the older version.
  --to    Required. The version_seq to diff to. Same handle semantics as
          --from.

Output (stdout, JSON):
  {"unique_id":string,
   "from":{...version object, same shape as "node versions"...},
   "to":{...version object, same shape as "node versions"...},
   "raw_code_diff":string,"config_diff":string,"source_changed":bool,
   "shared_code_changed":bool,"config_changed":bool,"truncated":bool}
  raw_code_diff and config_diff are omitted when that part is unchanged.
  truncated is set when either diff was cut to the server's per-diff 8 KiB cap.

Errors:
  usage      (exit 2)  wrong number of arguments, missing --from/--to, or --from equals --to
  not_found  (exit 3)  the node's unique_id, or one of the two seqs, is not recorded
  unavailable(exit 5)  the orchestrator service is unreachable
  internal   (exit 6)  unexpected server error`,
		Example: "  continuo node diff finance analytics orders --from 1 --to 3",
		Annotations: map[string]string{
			"output_schema": `{"unique_id":"string","from":"object","to":"object","raw_code_diff":"string","config_diff":"string","source_changed":"bool","shared_code_changed":"bool","config_changed":"bool","truncated":"bool"}`,
			"exit_codes":    `[0,2,3,5,6]`,
		},
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 3 {
				return emit(stdout, stderr, humanOutput(cmd), output.NewUsageError("diff requires exactly three arguments: <service> <schema> <table>"))
			}
			if !cmd.Flags().Changed("from") || !cmd.Flags().Changed("to") {
				return emit(stdout, stderr, humanOutput(cmd), output.NewUsageError("diff requires both --from and --to"))
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			schema, table := args[1], args[2]
			uniqueID := schema + "." + table
			from, _ := cmd.Flags().GetInt64("from")
			to, _ := cmd.Flags().GetInt64("to")

			ctx, cancel := context.WithTimeout(cmd.Context(), cfg.Timeout)
			defer cancel()

			c, err := factory(ctx, cfg.OrchestratorEndpoint)
			if err != nil {
				return emit(stdout, stderr, cfg.Human, output.FromGRPC(err))
			}
			defer func() { _ = c.Close() }()

			resp, err := c.GetNodeVersionDiff(ctx, uniqueID, from, to)
			if err != nil {
				return emit(stdout, stderr, cfg.Human, output.FromGRPC(err))
			}

			if cfg.Human {
				return humanDiff(stderr, resp.GetDiff())
			}
			return output.EmitSuccess(stdout, toVersionDiffPayload(resp.GetDiff()))
		},
	}
	cmd.Flags().Int64("from", 0, "Required. The version_seq to diff from (a stable handle, not a chronological position)")
	cmd.Flags().Int64("to", 0, "Required. The version_seq to diff to (a stable handle, not a chronological position)")
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	return cmd
}

func toVersionDiffPayload(d *orchestratorv1.VersionDiff) versionDiffPayload {
	return versionDiffPayload{
		UniqueID:          d.GetUniqueId(),
		From:              toVersionEntry(d.GetFrom()),
		To:                toVersionEntry(d.GetTo()),
		RawCodeDiff:       d.GetRawCodeDiff(),
		ConfigDiff:        d.GetConfigDiff(),
		SourceChanged:     d.GetSourceChanged(),
		SharedCodeChanged: d.GetSharedCodeChanged(),
		ConfigChanged:     d.GetConfigChanged(),
		Truncated:         d.GetTruncated(),
	}
}

// humanDiff writes a one-line summary to stderr:
//
//	<unique_id>: source=<bool> shared_code=<bool> config=<bool> truncated=<bool>
func humanDiff(stderr io.Writer, d *orchestratorv1.VersionDiff) error {
	_, err := fmt.Fprintf(stderr, "%s: source=%t shared_code=%t config=%t truncated=%t\n",
		d.GetUniqueId(), d.GetSourceChanged(), d.GetSharedCodeChanged(), d.GetConfigChanged(), d.GetTruncated())
	return err
}

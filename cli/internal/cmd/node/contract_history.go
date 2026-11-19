package node

import (
	"context"
	"fmt"
	"io"
	"strconv"

	"github.com/carolsimone/continuo/cli/internal/config"
	"github.com/carolsimone/continuo/cli/internal/output"
	catalogv1 "github.com/carolsimone/continuo/cli/proto/catalog/v1"
	"github.com/spf13/cobra"
)

type contractHistoryPayload struct {
	Versions []contractVersionEntry `json:"versions"`
}

// NewContractHistoryCommand builds `continuo node contract-history <unique_id> [limit]`.
func NewContractHistoryCommand(factory CatalogClientFactory, cfg *config.Config, stdout, stderr io.Writer) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "contract-history <unique_id> [limit]",
		Short: "List a node's contract versions, newest first",
		Long: `List a node's contract versions, newest first.

Use when the user wants to see how a node's contract changed over time: one
row for each promotion that changed it, with the release that made it current.
Use "node contract-diff" to see which fields differ between two of the versions.

A version number rises each time a promotion changes the node's contract, so a
revert to an earlier contract is a new version: its same_as_version names the
earlier version with the same contract_hash. is_current marks the version the
node has now; after a node is retired its history stays and none of it is
current. A node with no history, or one the catalog does not know, gives an
empty list: this command never answers not_found.

Arguments:
  <unique_id>  The node's unique_id, "<schema>.<table>" (for example
               analytics.orders, a dbt model, or analytics.py_daily_kpis,
               a Python node).
  [limit]      Optional. The most versions returned, newest first: a whole
               number from 0 up. Default 0, which lists every version.

Output (stdout, JSON):
  {"versions":[{"unique_id":string,"version":number,"contract_hash":string,
   "release_id":string,"promoted_at":string,"content_hash":string,
   "is_current":bool,"same_as_version":number,"continuo_contracts":string}]}
  The rows are the shape of "node contract" without document_yaml. promoted_at
  is RFC 3339, in UTC. is_current and same_as_version are omitted when false
  and 0 (a version that is not current, a version that reverts nothing), and
  release_id, promoted_at and continuo_contracts when the version carries none.

Errors:
  usage      (exit 2)  wrong number of arguments, or a limit that is not a whole number from 0 up
  unavailable(exit 5)  the catalog service is unreachable
  internal   (exit 6)  unexpected server error`,
		Example: "  continuo node contract-history analytics.orders\n  continuo node contract-history analytics.py_daily_kpis 5",
		Annotations: map[string]string{
			"output_schema": `{"versions":[{"unique_id":"string","version":"number","contract_hash":"string","release_id":"string","promoted_at":"string","content_hash":"string","is_current":"boolean","same_as_version":"number","continuo_contracts":"string"}]}`,
			"exit_codes":    `[0,2,5,6]`,
		},
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) < 1 || len(args) > 2 {
				return emit(stdout, stderr, humanOutput(cmd), output.NewUsageError("contract-history requires <unique_id> and an optional [limit]"))
			}
			if len(args) == 2 {
				if _, err := parseContractLimit(args[1]); err != nil {
					return emit(stdout, stderr, humanOutput(cmd), output.NewUsageError(err.Error()))
				}
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			var limit int32
			if len(args) == 2 {
				limit, _ = parseContractLimit(args[1])
			}

			ctx, cancel := context.WithTimeout(cmd.Context(), cfg.Timeout)
			defer cancel()

			c, err := factory(ctx, cfg.CatalogEndpoint)
			if err != nil {
				return emit(stdout, stderr, cfg.Human, output.FromGRPC(err))
			}
			defer func() { _ = c.Close() }()

			resp, err := c.ListNodeContractVersions(ctx, args[0], limit)
			if err != nil {
				return emit(stdout, stderr, cfg.Human, output.FromGRPC(err))
			}

			if cfg.Human {
				return humanContractHistory(stderr, resp.GetVersions())
			}
			return output.EmitSuccess(stdout, toContractHistoryPayload(resp.GetVersions()))
		},
	}
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	return cmd
}

// parseContractLimit reads the [limit] positional: a whole number from 0 up
// that fits the request's int32.
func parseContractLimit(arg string) (int32, error) {
	n, err := strconv.ParseInt(arg, 10, 32)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("limit must be a whole number from 0 up (0 lists every version); got %q", arg)
	}
	return int32(n), nil
}

func toContractHistoryPayload(versions []*catalogv1.ContractVersionView) contractHistoryPayload {
	out := make([]contractVersionEntry, 0, len(versions))
	for _, v := range versions {
		out = append(out, toContractVersionEntry(v))
	}
	return contractHistoryPayload{Versions: out}
}

// humanContractHistory writes one line per version to stderr:
//
//	<version>  <contract_hash>  <promoted_at>  [current]  (same as <n>)
func humanContractHistory(stderr io.Writer, versions []*catalogv1.ContractVersionView) error {
	for _, v := range versions {
		line := fmt.Sprintf("%d  %s  %s", v.GetVersion(), v.GetContractHash(), v.GetPromotedAt())
		if v.GetIsCurrent() {
			line += "  [current]"
		}
		if v.GetSameAsVersion() > 0 {
			line += fmt.Sprintf("  (same as %d)", v.GetSameAsVersion())
		}
		if _, err := fmt.Fprintln(stderr, line); err != nil {
			return err
		}
	}
	return nil
}

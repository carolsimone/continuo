package node

import (
	"context"
	"fmt"
	"io"

	"github.com/carolsimone/continuo/cli/internal/config"
	"github.com/carolsimone/continuo/cli/internal/output"
	catalogv1 "github.com/carolsimone/continuo/cli/proto/catalog/v1"
	"github.com/spf13/cobra"
)

// contractVersionEntry is the JSON-serialisable representation of one version
// of a node's contract: the promotion that made a contract current. The fields
// a version does not carry (a version that reverts nothing has no
// same_as_version, a history row has no document) are omitted, as in
// versionEntry.
type contractVersionEntry struct {
	UniqueID          string `json:"unique_id"`
	Version           int64  `json:"version"`
	ContractHash      string `json:"contract_hash"`
	ReleaseID         string `json:"release_id,omitempty"`
	PromotedAt        string `json:"promoted_at,omitempty"`
	ContentHash       string `json:"content_hash"`
	IsCurrent         bool   `json:"is_current,omitempty"`
	SameAsVersion     int64  `json:"same_as_version,omitempty"`
	ContinuoContracts string `json:"continuo_contracts,omitempty"`
	DocumentYAML      string `json:"document_yaml,omitempty"`
}

type contractPayload struct {
	Contract contractVersionEntry `json:"contract"`
}

// toContractVersionEntry maps a version without its document, the form of a
// history row.
func toContractVersionEntry(v *catalogv1.ContractVersionView) contractVersionEntry {
	return contractVersionEntry{
		UniqueID:          v.GetUniqueId(),
		Version:           v.GetVersion(),
		ContractHash:      v.GetContractHash(),
		ReleaseID:         v.GetReleaseId(),
		PromotedAt:        v.GetPromotedAt(),
		ContentHash:       v.GetContentHash(),
		IsCurrent:         v.GetIsCurrent(),
		SameAsVersion:     v.GetSameAsVersion(),
		ContinuoContracts: v.GetContinuoContracts(),
	}
}

// NewContractCommand builds `continuo node contract <unique_id>`.
func NewContractCommand(factory CatalogClientFactory, cfg *config.Config, stdout, stderr io.Writer) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "contract <unique_id>",
		Short: "Show the contract a node has now, with its ODCS document",
		Long: `Show the contract a node has now, with its ODCS document.

Use when the user wants to know what a node promises: the fields of its table,
their types and descriptions, as the contract the live release carries. The
document is the contract as published, in ODCS (Open Data Contract Standard)
YAML, without the "servers" list. The version is the number of the promotion
that made this contract current; "node contract-history" lists the earlier ones.
Every dbt model, seed and snapshot has a contract, and so has every Python node.
A dbt test and an unknown-external table have none: asking for one is not_found.

Arguments:
  <unique_id>  The node's unique_id, "<schema>.<table>" in lower case (for
               example analytics.orders): the schema_name and table_name
               of a row of "node list" joined by a dot and written in
               lower case.

Output (stdout, JSON):
  {"contract":{"unique_id":string,"version":number,"contract_hash":string,
   "release_id":string,"promoted_at":string,"content_hash":string,
   "is_current":bool,"same_as_version":number,"continuo_contracts":string,
   "document_yaml":string}}
  promoted_at is RFC 3339, in UTC. document_yaml is the whole contract as YAML.
  same_as_version is present only when the version is a revert: the earlier
  version that had the same contract_hash. release_id, promoted_at,
  continuo_contracts and document_yaml are omitted when the version carries no
  value for them.

Errors:
  usage      (exit 2)  wrong number of arguments
  not_found  (exit 3)  the node has no contract, is retired, or its unique_id is not recorded
  unavailable(exit 5)  the catalog service is unreachable
  internal   (exit 6)  unexpected server error`,
		Example: "  continuo node contract analytics.orders\n  continuo node contract analytics.py_daily_kpis",
		Annotations: map[string]string{
			"output_schema": `{"contract":{"unique_id":"string","version":"number","contract_hash":"string","release_id":"string","promoted_at":"string","content_hash":"string","is_current":"boolean","same_as_version":"number","continuo_contracts":"string","document_yaml":"string"}}`,
			"exit_codes":    `[0,2,3,5,6]`,
		},
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return emit(stdout, stderr, humanOutput(cmd), output.NewUsageError("contract requires exactly one argument: <unique_id>"))
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := context.WithTimeout(cmd.Context(), cfg.Timeout)
			defer cancel()

			c, err := factory(ctx, cfg.CatalogEndpoint)
			if err != nil {
				return emit(stdout, stderr, cfg.Human, output.FromGRPC(err))
			}
			defer func() { _ = c.Close() }()

			resp, err := c.GetNodeContract(ctx, args[0], true)
			if err != nil {
				return emit(stdout, stderr, cfg.Human, output.FromGRPC(err))
			}

			if cfg.Human {
				return humanContract(stderr, resp.GetContract())
			}
			entry := toContractVersionEntry(resp.GetContract())
			entry.DocumentYAML = resp.GetContract().GetDocumentYaml()
			return output.EmitSuccess(stdout, contractPayload{Contract: entry})
		},
	}
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	return cmd
}

// humanContract writes the version's summary line to stderr, then its document:
//
//	<unique_id>  version <version>  <contract_hash>  <promoted_at>
func humanContract(stderr io.Writer, v *catalogv1.ContractVersionView) error {
	if _, err := fmt.Fprintf(stderr, "%s  version %d  %s  %s\n", v.GetUniqueId(), v.GetVersion(), v.GetContractHash(), v.GetPromotedAt()); err != nil {
		return err
	}
	if v.GetDocumentYaml() == "" {
		return nil
	}
	_, err := fmt.Fprintf(stderr, "\n%s", v.GetDocumentYaml())
	return err
}

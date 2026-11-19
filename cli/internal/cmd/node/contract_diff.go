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

// contractFieldChange is one path at which two versions of a contract differ.
// before_json and after_json are always present: an added path has an empty
// before_json and a removed one an empty after_json.
type contractFieldChange struct {
	Path       string `json:"path"`
	BeforeJSON string `json:"before_json"`
	AfterJSON  string `json:"after_json"`
}

type contractDiffPayload struct {
	Changes []contractFieldChange `json:"changes"`
}

// NewContractDiffCommand builds `continuo node contract-diff <unique_id> <from> <to>`.
func NewContractDiffCommand(factory CatalogClientFactory, cfg *config.Config, stdout, stderr io.Writer) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "contract-diff <unique_id> <from> <to>",
		Short: "List the fields that differ between two versions of a node's contract",
		Long: `List the fields that differ between two versions of a node's contract.

Use when the user wants to know what changed in a node's contract between two
of its versions, for example the version before a release and the one it made
current. Take the version numbers from "node contract-history". The versions
are handles, not a chronology: <from> need not be the older one.

One row is one changed path of the contract document, in a fixed order. A path
reads like schema[0].properties[2].physicalType. A field or list item that one
version has and the other lacks is one row at its own path, not one row for each
field inside it. before_json and after_json hold the value at that path in
each version as JSON text. An added path has an empty before_json and a removed
path an empty after_json. Two equal versions give an empty list.

Arguments:
  <unique_id>  The node's unique_id, "<schema>.<table>" in lower case (for
               example analytics.orders, a dbt model, or
               analytics.py_daily_kpis, a Python node).
  <from>       The version to diff from: a whole number of 1 or more.
  <to>         The version to diff to: a whole number of 1 or more.

Output (stdout, JSON):
  {"changes":[{"path":string,"before_json":string,"after_json":string}]}

Errors:
  usage      (exit 2)  wrong number of arguments, a version that is not a whole number, or a version below 1
  not_found  (exit 3)  the node has no promotion for one of the two versions, or its unique_id is not recorded
  unavailable(exit 5)  the catalog service is unreachable
  internal   (exit 6)  unexpected server error`,
		Example: "  continuo node contract-diff analytics.orders 1 3\n  continuo node contract-diff analytics.py_daily_kpis 1 3",
		Annotations: map[string]string{
			"output_schema": `{"changes":[{"path":"string","before_json":"string","after_json":"string"}]}`,
			"exit_codes":    `[0,2,3,5,6]`,
		},
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 3 {
				return emit(stdout, stderr, humanOutput(cmd), output.NewUsageError("contract-diff requires exactly three arguments: <unique_id> <from> <to>"))
			}
			if _, err := parseContractVersion("from", args[1]); err != nil {
				return emit(stdout, stderr, humanOutput(cmd), output.NewUsageError(err.Error()))
			}
			if _, err := parseContractVersion("to", args[2]); err != nil {
				return emit(stdout, stderr, humanOutput(cmd), output.NewUsageError(err.Error()))
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			from, _ := parseContractVersion("from", args[1])
			to, _ := parseContractVersion("to", args[2])

			ctx, cancel := context.WithTimeout(cmd.Context(), cfg.Timeout)
			defer cancel()

			c, err := factory(ctx, cfg.CatalogEndpoint)
			if err != nil {
				return emit(stdout, stderr, cfg.Human, output.FromGRPC(err))
			}
			defer func() { _ = c.Close() }()

			resp, err := c.GetNodeContractDiff(ctx, args[0], from, to)
			if err != nil {
				return emit(stdout, stderr, cfg.Human, output.FromGRPC(err))
			}

			if cfg.Human {
				return humanContractDiff(stderr, resp.GetChanges())
			}
			return output.EmitSuccess(stdout, toContractDiffPayload(resp.GetChanges()))
		},
	}
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	return cmd
}

// parseContractVersion reads a version positional as a whole number. Whether
// it names a version the node has, and whether it is at least 1, is the
// catalog service's to say.
func parseContractVersion(name, arg string) (int64, error) {
	n, err := strconv.ParseInt(arg, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("<%s> must be a whole number (a version from node contract-history); got %q", name, arg)
	}
	return n, nil
}

func toContractDiffPayload(changes []*catalogv1.ContractFieldChange) contractDiffPayload {
	out := make([]contractFieldChange, 0, len(changes))
	for _, c := range changes {
		out = append(out, contractFieldChange{Path: c.GetPath(), BeforeJSON: c.GetBeforeJson(), AfterJSON: c.GetAfterJson()})
	}
	return contractDiffPayload{Changes: out}
}

// humanContractDiff writes one line per changed path to stderr:
//
//	<path>: <before> -> <after>
//
// A side the path does not have reads "(absent)".
func humanContractDiff(stderr io.Writer, changes []*catalogv1.ContractFieldChange) error {
	for _, c := range changes {
		if _, err := fmt.Fprintf(stderr, "%s: %s -> %s\n", c.GetPath(), orAbsent(c.GetBeforeJson()), orAbsent(c.GetAfterJson())); err != nil {
			return err
		}
	}
	return nil
}

func orAbsent(s string) string {
	if s == "" {
		return "(absent)"
	}
	return s
}

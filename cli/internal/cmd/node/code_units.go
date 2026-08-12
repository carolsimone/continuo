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

// unitVersionEntry is the JSON-serialisable representation of one recorded
// state of a shared-code unit (a dbt macro today).
type unitVersionEntry struct {
	UnitID     string `json:"unit_id"`
	Checksum   string `json:"checksum"`
	Source     string `json:"source"`
	Repo       string `json:"repo,omitempty"`
	CommitSHA  string `json:"commit_sha,omitempty"`
	ReleaseID  string `json:"release_id,omitempty"`
	PromotedAt string `json:"promoted_at,omitempty"`
	IsCurrent  bool   `json:"is_current,omitempty"`
}

type codeUnitVersionsPayload struct {
	Versions []unitVersionEntry `json:"versions"`
}

// NewCodeUnitsCommand builds
// `continuo node code-units [<unit-id>] [--service <s> --schema <s> --table <t>]`.
func NewCodeUnitsCommand(factory OrchestratorClientFactory, cfg *config.Config, stdout, stderr io.Writer) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "code-units [<unit-id>]",
		Short: "List a shared-code unit's (or a node's units') version chain",
		Long: `List a shared-code unit's version chain, newest first.

Use when the user wants the history of a dbt macro (or other shared-code
unit): every recorded checksum, its source, and the release that promoted it.

This command takes exactly one of two mutually exclusive selectors:

  <unit-id>                              query one unit's chain directly.
  --service <s> --schema <s> --table <t> resolve the named node's CURRENT
                                          units first, then return each of
                                          their chains concatenated in
                                          resolution order. All three flags
                                          are required together; the node is
                                          addressed by its unique_id,
                                          "<schema>.<table>" ( --service is
                                          accepted for consistency with the
                                          other node subcommands but does not
                                          appear in the unique_id).

Supplying both the positional <unit-id> and any of --service/--schema/--table,
or supplying neither, is a usage error — there is no default selector.

Flags:
  --service  Owning service name of the node whose units to list. Must be
             combined with --schema and --table; mutually exclusive with
             <unit-id>.
  --schema   Schema name of the node whose units to list. Must be combined
             with --service and --table; mutually exclusive with <unit-id>.
  --table    Table (model) name of the node whose units to list. Must be
             combined with --service and --schema; mutually exclusive with
             <unit-id>.
  --limit    Maximum number of versions returned per unit, newest first.
             Default 20; the server also applies this default when sent 0,
             and clamps any value above 200 down to 200.

Output (stdout, JSON):
  {"versions":[{"unit_id":string,"checksum":string,"source":string,
   "repo":string,"commit_sha":string,"release_id":string,
   "promoted_at":string,"is_current":bool}]}
  repo, commit_sha, release_id, promoted_at, and is_current are omitted when
  the version carries no value for them.

Errors:
  usage      (exit 2)  wrong number of positional arguments, or the selector is not exactly one of <unit-id> / --service+--schema+--table
  not_found  (exit 3)  the unit_id, or the node's unique_id, is not recorded
  unavailable(exit 5)  the orchestrator service is unreachable
  internal   (exit 6)  unexpected server error`,
		Example: "  continuo node code-units unit_abc123\n  continuo node code-units --service finance --schema analytics --table orders",
		Annotations: map[string]string{
			"output_schema": `{"versions":"array"}`,
			"exit_codes":    `[0,2,3,5,6]`,
		},
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) > 1 {
				return emit(stdout, stderr, humanOutput(cmd), output.NewUsageError("code-units accepts at most one positional argument: <unit-id>"))
			}
			service, _ := cmd.Flags().GetString("service")
			schema, _ := cmd.Flags().GetString("schema")
			table, _ := cmd.Flags().GetString("table")
			nodeSelectorSet := service != "" || schema != "" || table != ""

			if len(args) == 1 {
				if nodeSelectorSet {
					return emit(stdout, stderr, humanOutput(cmd), output.NewUsageError("specify either <unit-id> or --service/--schema/--table, not both"))
				}
				return nil
			}
			if !nodeSelectorSet {
				return emit(stdout, stderr, humanOutput(cmd), output.NewUsageError("code-units requires either <unit-id> or --service, --schema, and --table"))
			}
			if service == "" || schema == "" || table == "" {
				return emit(stdout, stderr, humanOutput(cmd), output.NewUsageError("--service, --schema, and --table must all be set together"))
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			var unitID, uniqueID string
			if len(args) == 1 {
				unitID = args[0]
			} else {
				schema, _ := cmd.Flags().GetString("schema")
				table, _ := cmd.Flags().GetString("table")
				uniqueID = schema + "." + table
			}
			limit, _ := cmd.Flags().GetInt32("limit")

			ctx, cancel := context.WithTimeout(cmd.Context(), cfg.Timeout)
			defer cancel()

			c, err := factory(ctx, cfg.OrchestratorEndpoint)
			if err != nil {
				return emit(stdout, stderr, cfg.Human, output.FromGRPC(err))
			}
			defer func() { _ = c.Close() }()

			resp, err := c.GetCodeUnitVersions(ctx, unitID, uniqueID, limit)
			if err != nil {
				return emit(stdout, stderr, cfg.Human, output.FromGRPC(err))
			}

			if cfg.Human {
				return humanCodeUnits(stderr, resp.GetVersions())
			}
			return output.EmitSuccess(stdout, toCodeUnitVersionsPayload(resp.GetVersions()))
		},
	}
	cmd.Flags().String("service", "", "Owning service name of the node whose units to list (must be combined with --schema and --table)")
	cmd.Flags().String("schema", "", "Schema name of the node whose units to list (must be combined with --service and --table)")
	cmd.Flags().String("table", "", "Table (model) name of the node whose units to list (must be combined with --service and --schema)")
	cmd.Flags().Int32("limit", defaultVersionsLimit, "Maximum number of versions returned per unit, newest first (default 20, server max 200)")
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	return cmd
}

func toCodeUnitVersionsPayload(versions []*orchestratorv1.UnitVersionView) codeUnitVersionsPayload {
	out := make([]unitVersionEntry, 0, len(versions))
	for _, v := range versions {
		out = append(out, unitVersionEntry{
			UnitID:     v.GetUnitId(),
			Checksum:   v.GetChecksum(),
			Source:     v.GetSource(),
			Repo:       v.GetRepo(),
			CommitSHA:  v.GetCommitSha(),
			ReleaseID:  v.GetReleaseId(),
			PromotedAt: v.GetPromotedAt(),
			IsCurrent:  v.GetIsCurrent(),
		})
	}
	return codeUnitVersionsPayload{Versions: out}
}

// humanCodeUnits writes one line per unit version to stderr:
//
//	<unit_id>  <checksum>  <promoted_at>  [current]
func humanCodeUnits(stderr io.Writer, versions []*orchestratorv1.UnitVersionView) error {
	for _, v := range versions {
		current := ""
		if v.GetIsCurrent() {
			current = "  [current]"
		}
		if _, err := fmt.Fprintf(stderr, "%s  %s  %s%s\n", v.GetUnitId(), v.GetChecksum(), v.GetPromotedAt(), current); err != nil {
			return err
		}
	}
	return nil
}

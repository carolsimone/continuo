package node

import (
	"context"
	"fmt"
	"io"
	"strconv"

	"github.com/carolsimone/continuo/cli/internal/config"
	"github.com/carolsimone/continuo/cli/internal/output"
	statev1 "github.com/carolsimone/continuo/cli/proto/state/v1"
	"github.com/spf13/cobra"
)

// nodeSummary is the JSON-serialisable representation of one catalog row.
// The three statistics the state service reports as -1 when it has nothing
// to measure (no terminal run, no timed run) are pointers so that sentinel
// is omitted rather than emitted as a misleading negative number.
type nodeSummary struct {
	ServiceName    string `json:"service_name"`
	SchemaName     string `json:"schema_name"`
	TableName      string `json:"table_name"`
	RunCount       int32  `json:"run_count"`
	SuccessRatePct *int32 `json:"success_rate_pct,omitempty"`
	AvgDurationSec *int32 `json:"avg_duration_sec,omitempty"`
	P95DurationSec *int32 `json:"p95_duration_sec,omitempty"`
	FlakyRatePct   int32  `json:"flaky_rate_pct"`
	LastStatus     string `json:"last_status"`
	LastRunAt      string `json:"last_run_at,omitempty"`
	Operation      string `json:"operation"`
}

type listPayload struct {
	TotalCount int32         `json:"total_count"`
	Nodes      []nodeSummary `json:"nodes"`
}

// defaultListLimit is the page size sent when --limit is not given. The
// server applies the same default to a zero limit; the CLI sends it
// explicitly so --help and describe show a concrete number.
const defaultListLimit int32 = 50

// maxListLimit is the largest page the state service returns. A larger
// --limit is rejected as a usage error rather than silently clamped, so a
// caller never believes it received a bigger page than the server can give.
const maxListLimit int32 = 200

// validListOperations are the values accepted by --operation.
var validListOperations = map[string]bool{"run": true, "test": true, "build": true}

// NewListCommand builds `continuo node list`.
func NewListCommand(factory StateClientFactory, cfg *config.Config, stdout, stderr io.Writer) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List the node catalog with per-node run statistics",
		Long: `List the node catalog with per-node run statistics, one page at a time.

Use when the user names a model but not its service or schema, wants to know
which nodes exist in a service, or asks how reliable or slow a model has been.
Each row carries the (service, schema, table) triple every other node
subcommand takes as arguments, so this is the command that resolves a bare
table name into an addressable node.

Statistics cover each node's most recent 50 runs of the selected operation.

Arguments: none. Every filter is a flag.

Flags:
  --search     Table name to look up. Case-insensitive EXACT match on the
               table name, not a substring: "orders" matches "orders" and
               "Orders", never "orders_daily". Default "" (no filter).
  --service    Exact service name. Default "" (all services).
  --operation  Which operation's runs feed the statistics: run | test |
               build. Default "run". An unrecognized value is a usage error.
  --limit      Page size, 1..200. Default 50. A value outside that range is
               a usage error.
  --offset     Number of matching rows to skip, >= 0. Default 0.

Output (stdout, JSON):
  {"total_count":number,
   "nodes":[{"service_name":string,"schema_name":string,"table_name":string,
   "run_count":number,"success_rate_pct":number,"avg_duration_sec":number,
   "p95_duration_sec":number,"flaky_rate_pct":number,"last_status":string,
   "last_run_at":string,"operation":string}]}
  total_count is the number of nodes matching the filters regardless of
  paging; when it exceeds offset + len(nodes), request the next page with a
  larger --offset. success_rate_pct is omitted when the node has no terminal
  run in the window; avg_duration_sec and p95_duration_sec are omitted when
  no run in the window was timed; last_run_at is omitted when the node has
  never run. last_status is one of succeeded, failed, running, cancelled,
  pending. No match returns {"total_count":0,"nodes":[]}, not an error.

Errors:
  usage      (exit 2)  a positional argument was given, --operation is not
                       run|test|build, --limit is outside 1..200, or --offset
                       is negative
  unavailable(exit 5)  the state service is unreachable
  internal   (exit 6)  unexpected server error`,
		Example: `  continuo node list --search orders
  continuo node list --service finance --operation test --limit 20 --offset 20`,
		Annotations: map[string]string{
			"output_schema": `{"total_count":"number","nodes":"array"}`,
			"exit_codes":    `[0,2,5,6]`,
		},
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 0 {
				return emit(stdout, stderr, humanOutput(cmd), output.NewUsageError("list takes no positional arguments; use --search <table> to look up a node by name"))
			}
			operation, _ := cmd.Flags().GetString("operation")
			if !validListOperations[operation] {
				return emit(stdout, stderr, humanOutput(cmd), output.NewUsageError("--operation must be one of run, test, build; got "+operation))
			}
			limit, _ := cmd.Flags().GetInt32("limit")
			if limit < 1 || limit > maxListLimit {
				return emit(stdout, stderr, humanOutput(cmd), output.NewUsageError("--limit must be between 1 and "+strconv.Itoa(int(maxListLimit))+"; got "+strconv.Itoa(int(limit))))
			}
			offset, _ := cmd.Flags().GetInt32("offset")
			if offset < 0 {
				return emit(stdout, stderr, humanOutput(cmd), output.NewUsageError("--offset must be >= 0; got "+strconv.Itoa(int(offset))))
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			search, _ := cmd.Flags().GetString("search")
			service, _ := cmd.Flags().GetString("service")
			operation, _ := cmd.Flags().GetString("operation")
			limit, _ := cmd.Flags().GetInt32("limit")
			offset, _ := cmd.Flags().GetInt32("offset")

			ctx, cancel := context.WithTimeout(cmd.Context(), cfg.Timeout)
			defer cancel()

			c, err := factory(ctx, cfg.StateEndpoint)
			if err != nil {
				return emit(stdout, stderr, cfg.Human, output.FromGRPC(err))
			}
			defer func() { _ = c.Close() }()

			resp, err := c.ListNodes(ctx, search, service, operation, limit, offset)
			if err != nil {
				return emit(stdout, stderr, cfg.Human, output.FromGRPC(err))
			}

			if cfg.Human {
				return humanList(stderr, resp, offset)
			}
			return output.EmitSuccess(stdout, toListPayload(resp))
		},
	}
	cmd.Flags().String("search", "", "Table name to look up (case-insensitive exact match; default \"\" = all)")
	cmd.Flags().String("service", "", "Exact service name filter (default \"\" = all services)")
	cmd.Flags().String("operation", "run", "Operation whose runs feed the statistics: run | test | build (default \"run\")")
	cmd.Flags().Int32("limit", defaultListLimit, "Page size, 1..200 (default 50)")
	cmd.Flags().Int32("offset", 0, "Number of matching rows to skip (default 0)")
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	return cmd
}

// optionalStat turns the state service's -1 "not measurable" sentinel into a
// nil pointer so the JSON field is omitted; any other value is kept as-is.
func optionalStat(v int32) *int32 {
	if v < 0 {
		return nil
	}
	return &v
}

func toListPayload(resp *statev1.ListNodesResponse) listPayload {
	out := make([]nodeSummary, 0, len(resp.GetNodes()))
	for _, n := range resp.GetNodes() {
		out = append(out, nodeSummary{
			ServiceName:    n.GetServiceName(),
			SchemaName:     n.GetSchemaName(),
			TableName:      n.GetTableName(),
			RunCount:       n.GetRunCount(),
			SuccessRatePct: optionalStat(n.GetSuccessRatePct()),
			AvgDurationSec: optionalStat(n.GetAvgDurationSec()),
			P95DurationSec: optionalStat(n.GetP95DurationSec()),
			FlakyRatePct:   n.GetFlakyRatePct(),
			LastStatus:     n.GetLastStatus(),
			LastRunAt:      n.GetLastRunAt(),
			Operation:      n.GetOperation(),
		})
	}
	return listPayload{TotalCount: resp.GetTotalCount(), Nodes: out}
}

// humanList writes a header, one line per node, and a page summary to stderr:
//
//	NODE  OPERATION  RUNS  SUCCESS_PCT  AVG_SEC  P95_SEC  LAST_STATUS  LAST_RUN_AT
//	<service.schema.table>  <operation>  <runs>  <pct>  <avg>  <p95>  <status>  <at>
//	showing <first>-<last> of <total>
//
// A statistic the server could not measure, and a last_run_at for a node
// that never ran, render as "-". The summary line makes truncation visible:
// first and last are 1-based positions within the full match set, and an
// empty page renders as "showing 0 of <total>".
func humanList(stderr io.Writer, resp *statev1.ListNodesResponse, offset int32) error {
	if _, err := fmt.Fprintf(stderr, "NODE  OPERATION  RUNS  SUCCESS_PCT  AVG_SEC  P95_SEC  LAST_STATUS  LAST_RUN_AT\n"); err != nil {
		return err
	}
	for _, n := range resp.GetNodes() {
		if _, err := fmt.Fprintf(stderr, "%s.%s.%s  %s  %d  %s  %s  %s  %s  %s\n",
			n.GetServiceName(), n.GetSchemaName(), n.GetTableName(), n.GetOperation(), n.GetRunCount(),
			dashIfNegative(n.GetSuccessRatePct()), dashIfNegative(n.GetAvgDurationSec()), dashIfNegative(n.GetP95DurationSec()),
			n.GetLastStatus(), dashIfEmpty(n.GetLastRunAt())); err != nil {
			return err
		}
	}
	count := len(resp.GetNodes())
	if count == 0 {
		_, err := fmt.Fprintf(stderr, "showing 0 of %d\n", resp.GetTotalCount())
		return err
	}
	first := int(offset) + 1
	_, err := fmt.Fprintf(stderr, "showing %d-%d of %d\n", first, first+count-1, resp.GetTotalCount())
	return err
}

func dashIfNegative(v int32) string {
	if v < 0 {
		return "-"
	}
	return strconv.Itoa(int(v))
}

func dashIfEmpty(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

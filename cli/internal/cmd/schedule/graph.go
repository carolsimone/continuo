package schedule

import (
	"context"
	"fmt"
	"io"

	"github.com/carolsimone/continuo/cli/internal/config"
	"github.com/carolsimone/continuo/cli/internal/output"
	runv1 "github.com/carolsimone/continuo/cli/proto/run/v1"
	"github.com/spf13/cobra"
)

// nodeEntry is the JSON-serialisable representation of a single graph node.
type nodeEntry struct {
	TableName   string `json:"table_name"`
	SchemaName  string `json:"schema_name"`
	ServiceName string `json:"service_name"`
	NodeType    string `json:"node_type"`
}

// edgeEntry is the JSON-serialisable representation of a single graph edge.
type edgeEntry struct {
	FromNodeId string `json:"from_node_id"`
	ToNodeId   string `json:"to_node_id"`
}

// graphPayload is the top-level JSON object emitted on success.
// Nodes and Edges are initialised to empty slices so the JSON output always
// contains arrays, never null.
type graphPayload struct {
	ScheduleName string      `json:"schedule_name"`
	Nodes        []nodeEntry `json:"nodes"`
	Edges        []edgeEntry `json:"edges"`
}

// NewGraphCommand builds `continuo schedule graph <schedule-name>`.
// cfg is a pointer because root.go populates it in PersistentPreRunE after
// flags have been parsed — the subcommand reads the up-to-date value at RunE.
// stdout and stderr are injected so tests can capture them; in production
// the root command wires os.Stdout / os.Stderr.
func NewGraphCommand(factory RunClientFactory, cfg *config.Config, stdout, stderr io.Writer) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "graph <schedule-name>",
		Short: "Show the dependency graph for the named schedule",
		Long: `Show the dependency graph for the named schedule.

Use when the user wants the topology (nodes and edges) of a schedule: the nodes
of the live release that belong to it, the nodes of other schedules that feed
them one hop upstream, and the edges among them.

Arguments:
  <schedule-name>  The schedule whose graph to fetch.

Output (stdout, JSON):
  {"schedule_name":string,
   "nodes":[{"table_name":string,"schema_name":string,"service_name":string,
             "node_type":string}],
   "edges":[{"from_node_id":string,"to_node_id":string}]}
  A node id is "service.schema.table"; an edge runs from the dependent node to
  its dependency.

A schedule the live release does not hold returns {"nodes":[],"edges":[]}, not
an error.

Errors:
  usage      (exit 2)  wrong number of arguments
  unavailable(exit 5)  the run-controller service is unreachable
  internal   (exit 6)  unexpected server error`,
		Example: "  continuo schedule graph daily-revenue",
		Annotations: map[string]string{
			"output_schema": `{"schedule_name":"string","nodes":[{"table_name":"string","schema_name":"string","service_name":"string","node_type":"string"}],"edges":[{"from_node_id":"string","to_node_id":"string"}]}`,
			"exit_codes":    `[0,2,5,6]`,
		},
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return emit(stdout, stderr, humanOutput(cmd), output.NewUsageError("graph requires exactly one argument: <schedule-name>"))
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			scheduleName := args[0]
			ctx, cancel := context.WithTimeout(cmd.Context(), cfg.Timeout)
			defer cancel()

			c, err := factory(ctx, cfg.RunControllerEndpoint)
			if err != nil {
				return emit(stdout, stderr, cfg.Human, output.FromGRPC(err))
			}
			defer func() { _ = c.Close() }()

			resp, err := c.GetScheduleGraph(ctx, scheduleName)
			if err != nil {
				return emit(stdout, stderr, cfg.Human, output.FromGRPC(err))
			}

			if cfg.Human {
				return humanGraph(stderr, scheduleName, resp)
			}
			return output.EmitSuccess(stdout, toGraphPayload(scheduleName, resp))
		},
	}
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	return cmd
}

// toGraphPayload converts the proto response into the JSON payload.
func toGraphPayload(scheduleName string, resp *runv1.GetScheduleGraphResponse) graphPayload {
	nodes := make([]nodeEntry, 0, len(resp.GetNodes()))
	for _, n := range resp.GetNodes() {
		// ScheduleGraphNode.ScheduleName is intentionally excluded: it names the
		// schedule an upstream node belongs to, not part of the output contract.
		nodes = append(nodes, nodeEntry{
			TableName:   n.GetTableName(),
			SchemaName:  n.GetSchemaName(),
			ServiceName: n.GetServiceName(),
			NodeType:    n.GetNodeType(),
		})
	}

	edges := make([]edgeEntry, 0, len(resp.GetEdges()))
	for _, edge := range resp.GetEdges() {
		edges = append(edges, edgeEntry{
			FromNodeId: edge.GetFromNodeId(),
			ToNodeId:   edge.GetToNodeId(),
		})
	}

	return graphPayload{
		ScheduleName: scheduleName,
		Nodes:        nodes,
		Edges:        edges,
	}
}

// humanGraph writes a summary line to stderr in the format:
//
//	<schedule-name>: <N> nodes, <M> edges
func humanGraph(stderr io.Writer, scheduleName string, resp *runv1.GetScheduleGraphResponse) error {
	_, err := fmt.Fprintf(stderr, "%s: %d nodes, %d edges\n",
		scheduleName,
		len(resp.GetNodes()),
		len(resp.GetEdges()),
	)
	return err
}

package schedule

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/carolsimone/continuo/cli/internal/config"
	"github.com/carolsimone/continuo/cli/internal/output"
	orchestratorv1 "github.com/carolsimone/continuo/cli/proto/orchestrator/v1"
	"github.com/spf13/cobra"
)

// nodeEntry is the JSON-serialisable representation of a single graph node.
// Fields with omitempty are conditionally omitted per the spec:
//   - owner:          omit when empty
//   - last_updated_at: omit when the proto Timestamp is nil (zero)
//   - created_at:     omit when the proto Timestamp is nil (zero)
type nodeEntry struct {
	TableName     string `json:"table_name"`
	SchemaName    string `json:"schema_name"`
	ServiceName   string `json:"service_name"`
	Owner         string `json:"owner,omitempty"`
	Criticality   string `json:"criticality"`
	NodeType      string `json:"node_type"`
	Status        string `json:"status"`
	LastUpdatedAt string `json:"last_updated_at,omitempty"`
	CreatedAt     string `json:"created_at,omitempty"`
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
func NewGraphCommand(factory OrchestratorClientFactory, cfg *config.Config, stdout, stderr io.Writer) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "graph <schedule-name>",
		Short: "Show the dependency graph for the named schedule",
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return output.NewUsageError("graph requires exactly one argument: <schedule-name>")
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			scheduleName := args[0]
			ctx, cancel := context.WithTimeout(cmd.Context(), cfg.Timeout)
			defer cancel()

			c, err := factory(ctx, cfg.OrchestratorEndpoint)
			if err != nil {
				return emit(stdout, stderr, cfg.Human, output.FromGRPC(err))
			}
			defer c.Close()

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
func toGraphPayload(scheduleName string, resp *orchestratorv1.GetScheduleGraphResponse) graphPayload {
	nodes := make([]nodeEntry, 0, len(resp.GetNodes()))
	for _, n := range resp.GetNodes() {
		e := nodeEntry{
			TableName:   n.GetTableName(),
			SchemaName:  n.GetSchemaName(),
			ServiceName: n.GetServiceName(),
			Owner:       n.GetOwner(),
			Criticality: stripCriticalityPrefix(n.GetCriticality().String()),
			NodeType:    n.GetNodeType(),
			Status:      n.GetStatus(),
		}
		if ts := n.GetLastUpdatedAt(); ts != nil && ts.IsValid() {
			e.LastUpdatedAt = ts.AsTime().UTC().Format("2006-01-02T15:04:05Z")
		}
		if ts := n.GetCreatedAt(); ts != nil && ts.IsValid() {
			e.CreatedAt = ts.AsTime().UTC().Format("2006-01-02T15:04:05Z")
		}
		// TableNode.ScheduleName is intentionally excluded — it is internal denormalisation, not part of the output contract.
		nodes = append(nodes, e)
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

// stripCriticalityPrefix converts a proto enum string like "CRITICALITY_CORE"
// to its lowercase short form "core". The prefix "CRITICALITY_" is stripped
// and the remainder is lowercased.
func stripCriticalityPrefix(s string) string {
	const prefix = "CRITICALITY_"
	if strings.HasPrefix(s, prefix) {
		return strings.ToLower(s[len(prefix):])
	}
	return strings.ToLower(s)
}

// humanGraph writes a summary line to stderr in the format:
//
//	<schedule-name>: <N> nodes, <M> edges
func humanGraph(stderr io.Writer, scheduleName string, resp *orchestratorv1.GetScheduleGraphResponse) error {
	_, err := fmt.Fprintf(stderr, "%s: %d nodes, %d edges\n",
		scheduleName,
		len(resp.GetNodes()),
		len(resp.GetEdges()),
	)
	return err
}

package run

import (
	"context"
	"fmt"
	"io"

	"github.com/carolsimone/continuo/cli/internal/config"
	"github.com/carolsimone/continuo/cli/internal/output"
	runv1 "github.com/carolsimone/continuo/cli/proto/run/v1"
	"github.com/spf13/cobra"
)

type graphNode struct {
	ServiceName  string `json:"service_name"`
	SchemaName   string `json:"schema_name"`
	TableName    string `json:"table_name"`
	ScheduleName string `json:"schedule_name,omitempty"`
	NodeType     string `json:"node_type,omitempty"`
	Status       string `json:"status"`
	TaskID       string `json:"task_id"`
	// WaitReason, WaitingOnRunID and WaitingSince say why a pending task waits
	// besides its upstreams; each is omitted when the run-controller service leaves it empty.
	WaitReason     string `json:"wait_reason,omitempty"`
	WaitingOnRunID string `json:"waiting_on_run_id,omitempty"`
	WaitingSince   string `json:"waiting_since,omitempty"`
}

type graphEdge struct {
	FromNodeID string `json:"from_node_id"`
	ToNodeID   string `json:"to_node_id"`
}

type graphPayload struct {
	Nodes              []graphNode `json:"nodes"`
	Edges              []graphEdge `json:"edges"`
	RunPromotionSeq    int64       `json:"run_promotion_seq"`
	LatestPromotionSeq int64       `json:"latest_promotion_seq"`
}

// NewGraphCommand builds `continuo run graph <run-id>`.
func NewGraphCommand(factory RunClientFactory, cfg *config.Config, stdout, stderr io.Writer) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "graph <run-id>",
		Short: "Show the nodes of a run and the dependencies among them",
		Long: `Show the nodes of a run and the dependencies among them.

Use when the user asks what a run depends on, which nodes sit upstream or
downstream of a failed node, or whether a run still matches the live release.

Arguments:
  <run-id>  The UUID of the run.

Output (stdout, JSON):
  {"nodes":[{"service_name":string,"schema_name":string,"table_name":string,
             "schedule_name":string,"node_type":string,"status":string,"task_id":string,
             "wait_reason":string,"waiting_on_run_id":string,"waiting_since":string}],
   "edges":[{"from_node_id":string,"to_node_id":string}],
   "run_promotion_seq":number,"latest_promotion_seq":number}
  An edge points from the dependent node to its dependency; node ids are
  "<service>.<schema>.<table>". schedule_name and node_type are omitted when
  unknown (node_type is empty for a run that names no release).
  status is the task status: pending|dispatched|queued|running|stopping|succeeded|failed|cancelled|skipped.
  wait_reason is node_lease while the task waits for another run to release its node,
  waiting_on_run_id is the run writing it and waiting_since is when the wait began
  (RFC3339); the three are omitted when the task waits for nothing besides its upstreams.
  run_promotion_seq is the promotion sequence of the release the run executes on and
  latest_promotion_seq is the live one; they differ when a newer release was promoted
  after the run started. 0 means unknown (an unknown run, or a run that names no release).
  An unknown run returns "nodes":[] and "edges":[] and exit 0.

Errors:
  usage      (exit 2)  wrong number of arguments, or the run id is not a UUID
  unavailable(exit 5)  the run-controller service is unreachable
  internal   (exit 6)  unexpected server error`,
		Example: "  continuo run graph 0b6f4a52-3c1d-4d7e-9a0b-1f2e3d4c5b6a",
		Annotations: map[string]string{
			"output_schema": `{"nodes":[{"service_name":"string","schema_name":"string","table_name":"string","schedule_name":"string","node_type":"string","status":"string","task_id":"string","wait_reason":"string","waiting_on_run_id":"string","waiting_since":"string"}],"edges":[{"from_node_id":"string","to_node_id":"string"}],"run_promotion_seq":"number","latest_promotion_seq":"number"}`,
			"exit_codes":    `[0,2,5,6]`,
		},
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return emit(stdout, stderr, humanOutput(cmd), output.NewUsageError("graph requires exactly one argument: <run-id>"))
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := factory(cmd.Context(), cfg.RunControllerEndpoint)
			if err != nil {
				return emit(stdout, stderr, cfg.Human, output.FromGRPC(err))
			}
			defer func() { _ = c.Close() }()

			ctx, cancel := context.WithTimeout(cmd.Context(), cfg.Timeout)
			defer cancel()
			resp, err := c.GetRunGraph(ctx, args[0])
			if err != nil {
				return emit(stdout, stderr, cfg.Human, output.FromGRPC(err))
			}

			payload := toGraphPayload(resp)
			if cfg.Human {
				return humanGraph(stderr, payload)
			}
			return output.EmitSuccess(stdout, payload)
		},
	}
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	return cmd
}

// toGraphPayload converts the response into the JSON payload; the lists are
// never nil, so they print as [] and not null.
func toGraphPayload(resp *runv1.GetRunGraphResponse) graphPayload {
	p := graphPayload{
		Nodes:              make([]graphNode, 0, len(resp.GetNodes())),
		Edges:              make([]graphEdge, 0, len(resp.GetEdges())),
		RunPromotionSeq:    resp.GetRunPromotionSeq(),
		LatestPromotionSeq: resp.GetLatestPromotionSeq(),
	}
	for _, n := range resp.GetNodes() {
		p.Nodes = append(p.Nodes, graphNode{
			ServiceName:    n.GetServiceName(),
			SchemaName:     n.GetSchemaName(),
			TableName:      n.GetTableName(),
			ScheduleName:   n.GetScheduleName(),
			NodeType:       n.GetNodeType(),
			Status:         n.GetStatus(),
			TaskID:         n.GetTaskId(),
			WaitReason:     n.GetWaitReason(),
			WaitingOnRunID: n.GetWaitingOnRunId(),
			WaitingSince:   n.GetWaitingSince(),
		})
	}
	for _, e := range resp.GetEdges() {
		p.Edges = append(p.Edges, graphEdge{FromNodeID: e.GetFromNodeId(), ToNodeID: e.GetToNodeId()})
	}
	return p
}

// humanGraph writes one line per node and one per edge to w.
func humanGraph(w io.Writer, p graphPayload) error {
	if _, err := fmt.Fprintf(w, "%d nodes, %d edges (run promotion %d, live promotion %d)\n",
		len(p.Nodes), len(p.Edges), p.RunPromotionSeq, p.LatestPromotionSeq); err != nil {
		return err
	}
	for _, n := range p.Nodes {
		if _, err := fmt.Fprintf(w, "  %s.%s.%s  %s\n", n.ServiceName, n.SchemaName, n.TableName, n.Status); err != nil {
			return err
		}
	}
	for _, e := range p.Edges {
		if _, err := fmt.Fprintf(w, "  %s -> %s\n", e.FromNodeID, e.ToNodeID); err != nil {
			return err
		}
	}
	return nil
}

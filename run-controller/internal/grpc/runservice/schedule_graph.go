package runservice

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/carolsimone/continuo/pkg/num"
	"github.com/carolsimone/continuo/run-controller/domain/topology"
	runv1 "github.com/carolsimone/continuo/run-controller/proto/run/v1"
)

// GetScheduleGraph returns the live release's view of one schedule: its nodes,
// the nodes they read (of other schedules, dormant or unknown-external), and
// the edges from each node to the nodes it reads. A node is marked dormant
// when it is a node continuo builds that belongs to no schedule
// (topology.Node.IsDormant), and a dormant node that ran carries the creation
// time of its last run, in RFC3339 and UTC.
func (s *Server) GetScheduleGraph(ctx context.Context, req *runv1.GetScheduleGraphRequest) (*runv1.GetScheduleGraphResponse, error) {
	tenantID, err := requestTenant(ctx)
	if err != nil {
		return nil, err
	}
	if req.GetScheduleName() == "" {
		return nil, status.Error(codes.InvalidArgument, "schedule_name is required")
	}
	graph, err := s.reads.ScheduleGraphs.Graph(ctx, tenantID, req.GetScheduleName())
	if err != nil {
		return nil, s.internalError("GetScheduleGraph", err, "schedule", req.GetScheduleName())
	}

	nodes := make([]*runv1.ScheduleGraphNode, 0, len(graph.Nodes))
	ids := make(map[topology.NodeKey]string, len(graph.Nodes))
	for _, n := range graph.Nodes {
		ids[n.Key] = n.ServiceName + "." + n.SchemaName + "." + n.TableName
		node := &runv1.ScheduleGraphNode{
			ServiceName:  n.ServiceName,
			SchemaName:   n.SchemaName,
			TableName:    n.TableName,
			NodeType:     n.NodeType,
			ScheduleName: n.ScheduleName,
			Dormant:      n.IsDormant(),
			LastRunAt:    rfc3339OrEmpty(n.LastRunAt),
		}
		nodes = append(nodes, node)
	}
	// An edge names the dependent node first and the node it reads second; an
	// edge with an end outside the nodes cannot be named and is left out.
	edges := make([]*runv1.ScheduleGraphEdge, 0, len(graph.Edges))
	for _, e := range graph.Edges {
		from, fromOK := ids[e.Downstream]
		to, toOK := ids[e.Upstream]
		if !fromOK || !toOK {
			continue
		}
		edges = append(edges, &runv1.ScheduleGraphEdge{FromNodeId: from, ToNodeId: to})
	}
	return &runv1.GetScheduleGraphResponse{Nodes: nodes, Edges: edges, PromotionSeq: graph.PromotionSeq}, nil
}

// ListScheduleTopologies returns each schedule of the live release with its
// node count and the time that release was loaded, and the number of dormant
// nodes the release holds (0 when no release is live).
func (s *Server) ListScheduleTopologies(ctx context.Context, _ *runv1.ListScheduleTopologiesRequest) (*runv1.ListScheduleTopologiesResponse, error) {
	tenantID, err := requestTenant(ctx)
	if err != nil {
		return nil, err
	}
	summaries, err := s.reads.ScheduleGraphs.Summaries(ctx, tenantID)
	if err != nil {
		return nil, s.internalError("ListScheduleTopologies", err)
	}
	out := make([]*runv1.ScheduleTopologySummary, 0, len(summaries.Schedules))
	for _, sum := range summaries.Schedules {
		item := &runv1.ScheduleTopologySummary{
			ScheduleName: sum.ScheduleName,
			NodeCount:    num.ClampInt32(sum.NodeCount),
		}
		if !sum.LastUpdatedAt.IsZero() {
			item.LastUpdatedAt = timestamppb.New(sum.LastUpdatedAt)
		}
		out = append(out, item)
	}
	return &runv1.ListScheduleTopologiesResponse{Schedules: out, DormantNodeCount: int64(summaries.DormantNodeCount)}, nil
}

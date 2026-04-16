package topology

import "context"

type Repository interface {
	UpsertNode(ctx context.Context, node *TopologyNode) error
	GetScheduleGraph(ctx context.Context, scheduleName string) ([]*Node, []*UpstreamDependency, error)
}

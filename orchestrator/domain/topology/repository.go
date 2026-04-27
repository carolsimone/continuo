package topology

import "context"

type Repository interface {
	ApplySnapshot(ctx context.Context, nodes []*TopologyNode) error
	GetScheduleGraph(ctx context.Context, scheduleName string) ([]*Node, []*UpstreamDependency, error)
}

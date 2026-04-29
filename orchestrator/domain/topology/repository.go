package topology

import "context"

// Repository is the write/read interface for the topology graph.
type Repository interface {
	ApplySnapshot(ctx context.Context, nodes []*TopologyNode, topologyGeneration int64) error
	SetServiceMetadata(ctx context.Context, serviceMetadata map[string]map[string]string, topologyGeneration int64) error
	GetScheduleGraph(ctx context.Context, scheduleName string) ([]*Node, []*UpstreamDependency, error)
}

// TopologyStateRepository tracks the monotonic topology_generation counter.
type TopologyStateRepository interface {
	IncrementGeneration(ctx context.Context) (int64, error)
	GetGeneration(ctx context.Context) (int64, error)
}

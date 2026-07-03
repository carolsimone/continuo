package neo4jinfra

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/carolsimone/continuo/orchestrator/domain/repository"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// TopologyRepository implements repository.TopologyRepository against Neo4j.
type TopologyRepository struct {
	client Neo4jClient
	logger *slog.Logger
}

// Compile-time assertion that the neo4j adapter satisfies the domain port.
var _ repository.TopologyRepository = (*TopologyRepository)(nil)

func NewTopologyRepository(client Neo4jClient, logger *slog.Logger) repository.TopologyRepository {
	return &TopologyRepository{client: client, logger: logger}
}

// SetServiceMetadata writes the per-service metadata map onto the :TopologyRoot singleton node.
func (r *TopologyRepository) SetServiceMetadata(
	ctx context.Context,
	serviceMetadata map[string]map[string]string,
	topologyGeneration int64,
) error {
	session := r.client.NewSession(ctx, neo4j.AccessModeWrite)
	defer func() { _ = session.Close(ctx) }()

	payload, err := json.Marshal(serviceMetadata)
	if err != nil {
		return fmt.Errorf("marshal service_metadata: %w", err)
	}

	_, err = session.Run(ctx, `
		MERGE (root:TopologyRoot {id: 'singleton'})
		SET root.service_metadata = $service_metadata,
		    root.topology_generation = $topology_generation,
		    root.updated_at = datetime()
	`, map[string]interface{}{
		"service_metadata":    string(payload),
		"topology_generation": topologyGeneration,
	})
	return err
}

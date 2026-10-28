package neo4jinfra

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/carolsimone/continuo/orchestrator/domain"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// OrchestratorQueryRepository implements the read-side (CQRS) queries that bypass aggregates.
// Used directly by gRPC handlers.
type OrchestratorQueryRepository struct {
	client Neo4jClient
	logger *slog.Logger
}

func NewOrchestratorQueryRepository(client Neo4jClient, logger *slog.Logger) *OrchestratorQueryRepository {
	return &OrchestratorQueryRepository{client: client, logger: logger}
}

// GetNode returns per-node topology metadata for a single :Table, addressed by
// its (service, schema, table) identity. With domain.ActiveNodesOnly only an
// active :Table matches; with domain.IncludeInactiveNodes a :Table no longer
// active in the topology also matches (reported with Inactive = true) and an
// active match is preferred when both exist; among several inactive matches the
// most recently retired wins, ties broken by the smallest unique_id. Returns domain.ErrNodeNotFound
// when no eligible node matches.
// test_count is read via intFieldPresent so a node predating test_count
// capture reports TestCountKnown = false rather than a misleading zero.
func (r *OrchestratorQueryRepository) GetNode(ctx context.Context, service, schema, table string, scope domain.NodeScope) (*domain.NodeMeta, error) {
	session := r.client.NewSession(ctx, neo4j.AccessModeRead)
	defer func() { _ = session.Close(ctx) }()

	query := `
		MATCH (t:Table {service_name: $service, schema_name: $schema, table_name: $table})
		WHERE $include_inactive OR COALESCE(t.active, true)
		WITH t, NOT COALESCE(t.active, true) AS inactive
		RETURN t.node_type AS node_type, t.test_count AS test_count, inactive
		ORDER BY inactive ASC, t.retired_at DESC, t.unique_id ASC
		LIMIT 1
	`
	result, err := session.Run(ctx, query, map[string]interface{}{
		"service":          service,
		"schema":           schema,
		"table":            table,
		"include_inactive": scope == domain.IncludeInactiveNodes,
	})
	if err != nil {
		return nil, fmt.Errorf("GetNode query failed: %w", err)
	}
	if !result.Next(ctx) {
		if err := result.Err(); err != nil {
			return nil, fmt.Errorf("GetNode query error: %w", err)
		}
		return nil, domain.ErrNodeNotFound
	}
	rec := result.Record()
	nodeType, _ := rec.Get("node_type")
	inactiveVal, _ := rec.Get("inactive")
	tc, tcKnown := intFieldPresent(rec, "test_count")
	nt, _ := nodeType.(string)
	inactive, _ := inactiveVal.(bool)
	return &domain.NodeMeta{
		NodeType:       nt,
		TestCount:      tc,
		TestCountKnown: tcKnown,
		Inactive:       inactive,
	}, nil
}

// GetNodeLocation returns the node's project-relative source path and owning
// service. An unknown or inactive unique_id yields domain.ErrNodeNotFound.
func (r *OrchestratorQueryRepository) GetNodeLocation(ctx context.Context, uniqueID string) (*domain.NodeLocation, error) {
	session := r.client.NewSession(ctx, neo4j.AccessModeRead)
	defer func() { _ = session.Close(ctx) }()

	query := `
		MATCH (t:Table {unique_id: $uid})
		WHERE COALESCE(t.active, true)
		RETURN COALESCE(t.original_file_path, '') AS file_path, t.service_name AS service_name
	`
	result, err := session.Run(ctx, query, map[string]interface{}{"uid": uniqueID})
	if err != nil {
		return nil, fmt.Errorf("GetNodeLocation query failed: %w", err)
	}
	if !result.Next(ctx) {
		if err := result.Err(); err != nil {
			return nil, fmt.Errorf("GetNodeLocation query error: %w", err)
		}
		return nil, domain.ErrNodeNotFound
	}
	rec := result.Record()
	fp, _ := rec.Get("file_path")
	svc, _ := rec.Get("service_name")
	fpStr, _ := fp.(string)
	svcStr, _ := svc.(string)
	return &domain.NodeLocation{FilePath: fpStr, ServiceName: svcStr}, nil
}

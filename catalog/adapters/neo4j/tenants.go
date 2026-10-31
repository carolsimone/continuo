package neo4jinfra

import (
	"context"
	"fmt"

	"github.com/carolsimone/continuo/catalog/service/ports"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// graphTenantsCypher lists the tenants that hold a :Meta node. Every write
// MERGEs its tenant's export lease node, a :Meta, so every tenant the catalog
// has written for is listed. It reads every tenant, so the Cypher tenant
// guard exempts this file.
const graphTenantsCypher = `
MATCH (m:Meta)
WHERE m.tenant_id IS NOT NULL AND m.tenant_id <> ''
RETURN DISTINCT m.tenant_id AS tenant_id
ORDER BY tenant_id`

// graphTenants implements ports.GraphTenants on the catalog graph.
type graphTenants struct {
	client Neo4jClient
}

var _ ports.GraphTenants = (*graphTenants)(nil)

// NewGraphTenants returns the tenant list of client's graph.
func NewGraphTenants(client Neo4jClient) ports.GraphTenants {
	return &graphTenants{client: client}
}

// Tenants returns the tenants that hold a :Meta node, in order.
func (g *graphTenants) Tenants(ctx context.Context) ([]string, error) {
	session := g.client.NewSession(ctx, neo4j.AccessModeRead)
	defer func() { _ = session.Close(ctx) }()
	tenants, err := neo4j.ExecuteRead(ctx, session, func(tx neo4j.ManagedTransaction) ([]string, error) {
		res, err := tx.Run(ctx, graphTenantsCypher, nil)
		if err != nil {
			return nil, err
		}
		recs, err := res.Collect(ctx)
		if err != nil {
			return nil, err
		}
		out := make([]string, 0, len(recs))
		for _, rec := range recs {
			tenantID, err := stringColumn(rec, "tenant_id")
			if err != nil {
				return nil, err
			}
			out = append(out, tenantID)
		}
		return out, nil
	})
	if err != nil {
		return nil, fmt.Errorf("neo4j: list the catalog's tenants: %w", err)
	}
	return tenants, nil
}

package neo4jinfra

import (
	"context"
	"fmt"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// contractHistoryTenantsCypher lists the tenants that hold an edge the sweep
// may delete or a :ContractVersion that no edge and no pointer of the tenant
// reaches. It reads every tenant, so the Cypher tenant guard exempts this
// file; the statements that delete are in contract_history_sweeper.go, under
// the guard.
const contractHistoryTenantsCypher = `
CALL {
  MATCH (n:Node)-[e:CONTRACT_PROMOTED]->(:ContractVersion {tenant_id: n.tenant_id})
  WHERE n.tenant_id IS NOT NULL` + deletableEdgesCypher + `
  RETURN DISTINCT n.tenant_id AS tenant_id
  UNION
  MATCH (v:ContractVersion)
  WHERE v.tenant_id IS NOT NULL
    AND NOT EXISTS { MATCH (:Node {tenant_id: v.tenant_id})-[:CONTRACT_PROMOTED|CURRENT_CONTRACT]->(v) }
  RETURN DISTINCT v.tenant_id AS tenant_id
}
RETURN tenant_id
ORDER BY tenant_id`

// tenantsWithDeletableHistory lists, in order, the tenants that hold history
// the sweep may delete under retentionMs.
func (s *ContractHistorySweeper) tenantsWithDeletableHistory(ctx context.Context, retentionMs int64) ([]string, error) {
	session := s.client.NewSession(ctx, neo4j.AccessModeRead)
	defer func() { _ = session.Close(ctx) }()
	tenants, err := neo4j.ExecuteRead(ctx, session, func(tx neo4j.ManagedTransaction) ([]string, error) {
		res, err := tx.Run(ctx, contractHistoryTenantsCypher, map[string]any{"retention_ms": retentionMs})
		if err != nil {
			return nil, err
		}
		recs, err := res.Collect(ctx)
		if err != nil {
			return nil, err
		}
		out := make([]string, 0, len(recs))
		for _, rec := range recs {
			if tenantID, _ := rec.Values[0].(string); tenantID != "" {
				out = append(out, tenantID)
			}
		}
		return out, nil
	})
	if err != nil {
		return nil, fmt.Errorf("list tenants with deletable contract history: %w", err)
	}
	return tenants, nil
}

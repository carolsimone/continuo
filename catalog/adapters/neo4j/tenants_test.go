//go:build integration

package neo4jinfra

import (
	"context"
	"testing"

	"github.com/carolsimone/continuo/catalog/service/ports"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGraphTenants_ListsEveryTenantWithAMetaNode(t *testing.T) {
	f := newGraphFixture(t)
	writing, live, nodeOnly := f.tenant("tenants-writing"), f.tenant("tenants-live"), f.tenant("tenants-node-only")
	// The export gate's lease node, which every write MERGEs.
	f.takeLease(writing, ports.LeaseExport, "exporter")
	f.exec(live, `CREATE (:Meta {tenant_id: $tenant_id, key: 'current_release', promotion_seq: 1})`, nil)
	f.exec(nodeOnly, `CREATE (:Node {tenant_id: $tenant_id, unique_id: 'a'})`, nil)

	tenants, err := NewGraphTenants(f.client).Tenants(context.Background())
	require.NoError(t, err)
	assert.Contains(t, tenants, writing)
	assert.Contains(t, tenants, live)
	assert.NotContains(t, tenants, nodeOnly)
	assert.NotContains(t, tenants, "")
	assert.IsIncreasing(t, tenants, "tenants in order")
	seen := map[string]bool{}
	for _, id := range tenants {
		assert.False(t, seen[id], "tenant %s listed twice", id)
		seen[id] = true
	}
}

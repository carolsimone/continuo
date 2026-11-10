package handlers

import (
	"testing"

	"github.com/carolsimone/continuo/catalog/domain/topology"
	"github.com/carolsimone/continuo/pkg/topologyartifact"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// An ingested node reaches the swap as it is in the artifact: its type, its
// empty service and content hash, and the readers' edges. A dbt test never
// does.
func TestPromotedNodes_KeepsAnIngestedNode(t *testing.T) {
	doc := topologyartifact.Document{
		SchemaVersion: topologyartifact.SchemaVersion,
		TenantID:      "default",
		ReleaseID:     "rel-1",
		Nodes: []topologyartifact.Node{
			{UniqueID: "raw.ingest_probe", SchemaName: "raw", TableName: "ingest_probe", NodeType: "ingested"},
			{
				UniqueID: "analytics.orders", SchemaName: "analytics", TableName: "orders",
				ServiceName: "service-1", NodeType: "dbt-model", ContentHash: "sha256:orders",
				UpstreamUniqueIDs: []string{"raw.ingest_probe"},
			},
			{UniqueID: "test.service_1.not_null_orders_id.5c1f0b2a3d", NodeType: "dbt-test"},
		},
	}

	got := promotedNodes(doc)

	require.Len(t, got, 2, "the ingested node stays and the dbt test goes")
	assert.Equal(t, topology.ReleasePromotedTopologyNode{
		UniqueID: "raw.ingest_probe", SchemaName: "raw", TableName: "ingest_probe", NodeType: "ingested",
	}, got[0])
	assert.Equal(t, []string{"raw.ingest_probe"}, got[1].UpstreamUniqueIDs, "the reader keeps its edge to the ingested node")
}

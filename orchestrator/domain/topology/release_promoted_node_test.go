package topology_test

import (
	"testing"

	"github.com/carolsimone/continuo/orchestrator/domain/topology"
)

func TestReleasePromotedTopologyNode_Fields(t *testing.T) {
	node := topology.ReleasePromotedTopologyNode{
		UniqueID:          "model.svc_a.table_x",
		SchemaName:        "public",
		TableName:         "table_x",
		ServiceName:       "svc_a",
		ImageTag:          "sha256:aabbcc",
		Schedule:          "0 * * * *",
		UpstreamUniqueIDs: []string{"model.svc_b.table_y", "model.svc_c.table_z"},
	}

	if node.UniqueID != "model.svc_a.table_x" {
		t.Errorf("UniqueID: got %q", node.UniqueID)
	}
	if node.SchemaName != "public" {
		t.Errorf("SchemaName: got %q", node.SchemaName)
	}
	if node.TableName != "table_x" {
		t.Errorf("TableName: got %q", node.TableName)
	}
	if node.ServiceName != "svc_a" {
		t.Errorf("ServiceName: got %q", node.ServiceName)
	}
	if node.ImageTag != "sha256:aabbcc" {
		t.Errorf("ImageTag: got %q", node.ImageTag)
	}
	if node.Schedule != "0 * * * *" {
		t.Errorf("Schedule: got %q", node.Schedule)
	}
	if len(node.UpstreamUniqueIDs) != 2 {
		t.Fatalf("UpstreamUniqueIDs length: got %d, want 2", len(node.UpstreamUniqueIDs))
	}
	if node.UpstreamUniqueIDs[0] != "model.svc_b.table_y" {
		t.Errorf("UpstreamUniqueIDs[0]: got %q", node.UpstreamUniqueIDs[0])
	}
}

func TestReleasePromotedTopologyNode_ZeroValue(t *testing.T) {
	var node topology.ReleasePromotedTopologyNode
	if node.UniqueID != "" {
		t.Errorf("zero UniqueID should be empty string, got %q", node.UniqueID)
	}
	if node.UpstreamUniqueIDs != nil {
		t.Errorf("zero UpstreamUniqueIDs should be nil, got %v", node.UpstreamUniqueIDs)
	}
}

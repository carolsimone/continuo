package grpc_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	orchestratorv1 "github.com/carolsimone/continuo/orchestrator/api/orchestrator/v1"

	grpcadapter "github.com/carolsimone/continuo/remediation-agent/adapters/grpc"
)

func TestMapNodeContext_ReturnsSelfFilePathAndUpstreamAncestors(t *testing.T) {
	ts := time.Date(2024, 3, 15, 10, 0, 0, 0, time.UTC)

	resp := &orchestratorv1.GetNodeAncestryResponse{
		Ancestors: []*orchestratorv1.AncestorNode{
			{
				UniqueId:      "service_a.schema.table_e",
				ServiceName:   "service_a",
				LastCommitSha: "abc123",
				FilePath:      "models/table_e.sql",
				LastChangedAt: timestamppb.New(ts),
				Depth:         0,
			},
			{
				UniqueId:      "service_b.schema.upstream_node",
				ServiceName:   "service_b",
				LastCommitSha: "def456",
				FilePath:      "models/upstream_node.sql",
				LastChangedAt: timestamppb.New(ts),
				Depth:         1,
			},
		},
	}

	filePath, serviceName, ancestors := grpcadapter.MapNodeContext(resp)

	assert.Equal(t, "models/table_e.sql", filePath, "depth-0 node's file path must be returned as filePath")
	assert.Equal(t, "service_a", serviceName, "depth-0 node's service_name must be returned as serviceName")

	require.Len(t, ancestors, 1, "depth-0 self entry must be excluded from ancestors")
	a := ancestors[0]
	assert.Equal(t, "service_b.schema.upstream_node", a.NodeID)
	assert.Equal(t, "service_b", a.ServiceName)
	assert.Equal(t, "def456", a.LastCommitSHA)
	assert.Equal(t, "models/upstream_node.sql", a.FilePath)
	assert.Equal(t, 1, a.Depth)
	assert.Equal(t, ts.Format(time.RFC3339), a.LastChangedAt)
}

func TestMapNodeContext_NilTimestamp(t *testing.T) {
	resp := &orchestratorv1.GetNodeAncestryResponse{
		Ancestors: []*orchestratorv1.AncestorNode{
			{
				UniqueId:      "service_a.schema.self_node",
				ServiceName:   "service_a",
				LastCommitSha: "aaa000",
				FilePath:      "models/self_node.sql",
				LastChangedAt: nil,
				Depth:         0,
			},
			{
				UniqueId:      "service_a.schema.upstream_node",
				ServiceName:   "service_a",
				LastCommitSha: "aaa111",
				FilePath:      "models/upstream_node.sql",
				LastChangedAt: nil,
				Depth:         1,
			},
		},
	}

	filePath, serviceName, ancestors := grpcadapter.MapNodeContext(resp)

	assert.Equal(t, "models/self_node.sql", filePath)
	assert.Equal(t, "service_a", serviceName)
	require.Len(t, ancestors, 1)
	assert.Equal(t, "", ancestors[0].LastChangedAt, "nil timestamp must produce empty string")
}

func TestMapNodeContext_EmptyResponse(t *testing.T) {
	resp := &orchestratorv1.GetNodeAncestryResponse{
		Ancestors: []*orchestratorv1.AncestorNode{},
	}

	filePath, serviceName, ancestors := grpcadapter.MapNodeContext(resp)

	assert.Equal(t, "", filePath, "empty response must return empty filePath")
	assert.Equal(t, "", serviceName, "empty response must return empty serviceName")
	assert.Empty(t, ancestors)
}

func TestMapNodeContext_NoDepthZeroNode(t *testing.T) {
	// Edge case: response has only depth>0 nodes (malformed but must not panic).
	resp := &orchestratorv1.GetNodeAncestryResponse{
		Ancestors: []*orchestratorv1.AncestorNode{
			{
				UniqueId:      "service_b.schema.upstream_node",
				ServiceName:   "service_b",
				LastCommitSha: "def456",
				FilePath:      "models/upstream_node.sql",
				Depth:         1,
			},
		},
	}

	filePath, serviceName, ancestors := grpcadapter.MapNodeContext(resp)

	assert.Equal(t, "", filePath, "no depth-0 node means empty filePath")
	assert.Equal(t, "", serviceName, "no depth-0 node means empty serviceName")
	require.Len(t, ancestors, 1)
	assert.Equal(t, "service_b.schema.upstream_node", ancestors[0].NodeID)
}

func TestMapNodeContext_CarriesLastRepo(t *testing.T) {
	resp := &orchestratorv1.GetNodeAncestryResponse{
		Ancestors: []*orchestratorv1.AncestorNode{
			{UniqueId: "svc_a.self", ServiceName: "service_a", FilePath: "models/self.sql", Depth: 0},
			{
				UniqueId:      "svc_b.up",
				ServiceName:   "service_b",
				LastCommitSha: "def456",
				LastRepo:      "owner/service-b-repo",
				FilePath:      "models/up.sql",
				Depth:         1,
			},
		},
	}

	_, _, ancestors := grpcadapter.MapNodeContext(resp)

	require.Len(t, ancestors, 1)
	assert.Equal(t, "owner/service-b-repo", ancestors[0].LastRepo, "ancestor must carry last_repo for the diff fetch")
}

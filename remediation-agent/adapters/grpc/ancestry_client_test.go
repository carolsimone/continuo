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

func TestMapAncestors_SkipsSelfAndMapsFields(t *testing.T) {
	ts := time.Date(2024, 3, 15, 10, 0, 0, 0, time.UTC)

	resp := &orchestratorv1.GetNodeAncestryResponse{
		Ancestors: []*orchestratorv1.AncestorNode{
			{
				UniqueId:      "service_a.schema.self_node",
				ServiceName:   "service_a",
				LastCommitSha: "abc123",
				FilePath:      "models/self_node.sql",
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

	ancestors := grpcadapter.MapAncestors(resp)

	require.Len(t, ancestors, 1, "depth-0 self entry must be skipped")

	a := ancestors[0]
	assert.Equal(t, "service_b.schema.upstream_node", a.NodeID)
	assert.Equal(t, "service_b", a.ServiceName)
	assert.Equal(t, "def456", a.LastCommitSHA)
	assert.Equal(t, "models/upstream_node.sql", a.FilePath)
	assert.Equal(t, 1, a.Depth)
	assert.Equal(t, ts.Format(time.RFC3339), a.LastChangedAt)
}

func TestMapAncestors_NilTimestamp(t *testing.T) {
	resp := &orchestratorv1.GetNodeAncestryResponse{
		Ancestors: []*orchestratorv1.AncestorNode{
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

	ancestors := grpcadapter.MapAncestors(resp)

	require.Len(t, ancestors, 1)
	assert.Equal(t, "", ancestors[0].LastChangedAt, "nil timestamp must produce empty string")
}

func TestMapAncestors_EmptyResponse(t *testing.T) {
	resp := &orchestratorv1.GetNodeAncestryResponse{
		Ancestors: []*orchestratorv1.AncestorNode{},
	}

	ancestors := grpcadapter.MapAncestors(resp)

	assert.Empty(t, ancestors)
}

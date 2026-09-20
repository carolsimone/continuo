package client

import (
	"context"
	"testing"

	statev1 "github.com/carolsimone/continuo/cli/proto/state/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TriggerSingleNodeRun records the last request so tests can assert on the
// metadata_source / source_run_id mapping the client performs.
func (s *capturingServer) TriggerSingleNodeRun(ctx context.Context, req *statev1.TriggerSingleNodeRunRequest) (*statev1.TriggerSingleNodeRunResponse, error) {
	s.recordUserID(ctx)
	s.gotSingleNode = req
	return &statev1.TriggerSingleNodeRunResponse{RunId: "r1"}, nil
}

func TestTriggerNodeRun_EmptySourceRunSelectsLatest(t *testing.T) {
	c, cap := dialCapturing(t)
	_, err := c.TriggerNodeRun(context.Background(), "finance", "analytics", "orders", "", "")
	require.NoError(t, err)
	assert.Equal(t, "latest", cap.gotSingleNode.GetMetadataSource())
	assert.Equal(t, "", cap.gotSingleNode.GetSourceRunId())
	assert.Equal(t, "", cap.gotSingleNode.GetOperation())
}

func TestTriggerNodeRun_SourceRunSelectsSnapshot(t *testing.T) {
	c, cap := dialCapturing(t)
	_, err := c.TriggerNodeRun(context.Background(), "finance", "analytics", "orders", "3f9e1c2a-7b4d-4e8f-9a1b-2c3d4e5f6a7b", "")
	require.NoError(t, err)
	assert.Equal(t, "snapshot_of_run", cap.gotSingleNode.GetMetadataSource())
	assert.Equal(t, "3f9e1c2a-7b4d-4e8f-9a1b-2c3d4e5f6a7b", cap.gotSingleNode.GetSourceRunId())
}

func TestTriggerNodeTest_SourceRunSelectsSnapshotAndKeepsOperation(t *testing.T) {
	c, cap := dialCapturing(t)
	_, err := c.TriggerNodeTest(context.Background(), "finance", "analytics", "orders", "3f9e1c2a-7b4d-4e8f-9a1b-2c3d4e5f6a7b", "")
	require.NoError(t, err)
	assert.Equal(t, "snapshot_of_run", cap.gotSingleNode.GetMetadataSource())
	assert.Equal(t, "3f9e1c2a-7b4d-4e8f-9a1b-2c3d4e5f6a7b", cap.gotSingleNode.GetSourceRunId())
	assert.Equal(t, "test", cap.gotSingleNode.GetOperation())
}

func TestTriggerNodeBuild_SourceRunSelectsSnapshotAndKeepsOperation(t *testing.T) {
	c, cap := dialCapturing(t)
	_, err := c.TriggerNodeBuild(context.Background(), "finance", "analytics", "orders", "3f9e1c2a-7b4d-4e8f-9a1b-2c3d4e5f6a7b", "")
	require.NoError(t, err)
	assert.Equal(t, "snapshot_of_run", cap.gotSingleNode.GetMetadataSource())
	assert.Equal(t, "3f9e1c2a-7b4d-4e8f-9a1b-2c3d4e5f6a7b", cap.gotSingleNode.GetSourceRunId())
	assert.Equal(t, "build", cap.gotSingleNode.GetOperation())
}

func TestTriggerNodeRun_ForwardsActorMetadata(t *testing.T) {
	c, cap := dialCapturing(t)
	_, err := c.TriggerNodeRun(context.Background(), "finance", "analytics", "orders", "", "okta.example.com|alice")
	require.NoError(t, err)
	assert.Equal(t, "okta.example.com|alice", cap.gotUserID)
}

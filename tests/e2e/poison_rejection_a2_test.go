package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPoisonRejection_A2_DirectPublish verifies the orchestrator's
// IngestTopologyHandler rejects manifest.loaded:v1 batches that carry empty
// image_tag (PR #33 A2). Specifically:
//   - rejected_topology_messages forensics row count goes up by exactly 1.
//   - topology_generation does NOT advance.
//   - Redis XPENDING for manifest.loaded:v1 stays 0 (consumer ACKs the bad batch).
func TestPoisonRejection_A2_DirectPublish(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	clients := setupClients(t, ctx)
	defer clients.close(ctx)

	verifyServicesHealthy(t)

	genBefore := readTopologyGeneration(t, ctx, clients)
	rejectedBefore := countRejectedTopologyMessages(t, ctx, clients)
	t.Logf("baseline: topology_generation=%d, rejected_topology_messages=%d", genBefore, rejectedBefore)

	// Build the poison payload — exactly one node with empty image_tag.
	// The TopologyNodePayload shape mirrors orchestrator/domain/command.go.
	type depPayload struct {
		ServiceName string `json:"service_name"`
		SchemaName  string `json:"schema_name"`
		TableName   string `json:"table_name"`
	}
	type nodePayload struct {
		ServiceName     string       `json:"service_name"`
		SchemaName      string       `json:"schema_name"`
		TableName       string       `json:"table_name"`
		Owner           string       `json:"owner"`
		ScheduleName    string       `json:"schedule_name"`
		Criticality     string       `json:"criticality"`
		NodeType        string       `json:"node_type"`
		ManifestVersion string       `json:"manifest_version"`
		ImageTag        string       `json:"image_tag"`
		Dependencies    []depPayload `json:"dependencies"`
	}
	nodes := []nodePayload{
		{
			ServiceName:     "svc-poison-a2",
			SchemaName:      "raw",
			TableName:       "users",
			Owner:           "test",
			ScheduleName:    "poison-a2",
			Criticality:     "CORE",
			NodeType:        "dbt-model",
			ManifestVersion: "v1",
			ImageTag:        "", // poison
			Dependencies:    []depPayload{},
		},
	}
	payloadBytes, err := json.Marshal(nodes)
	require.NoError(t, err)

	// XADD directly. event_id is required by some consumers; include it for safety.
	msgID, err := clients.redisClient.XAdd(ctx, &goredis.XAddArgs{
		Stream: "manifest.loaded:v1",
		Values: map[string]interface{}{
			"event_id": uuid.NewString(),
			"payload":  string(payloadBytes),
		},
	}).Result()
	require.NoError(t, err, "XADD manifest.loaded:v1 failed")
	t.Logf("Published poison manifest.loaded:v1 id=%s — waiting for A2 to reject...", msgID)

	// Poll: forensics row count goes up by exactly 1 within 30s.
	pollUntil(t, ctx, 30*time.Second, 1*time.Second, func() (bool, error) {
		return countRejectedTopologyMessages(t, ctx, clients) == rejectedBefore+1, nil
	}, fmt.Sprintf("Timeout waiting for rejected_topology_messages count to go from %d to %d", rejectedBefore, rejectedBefore+1))
	t.Log("✅ rejected_topology_messages count incremented by 1")

	// topology_generation must NOT have advanced.
	genAfter := readTopologyGeneration(t, ctx, clients)
	assert.Equal(t, genBefore, genAfter,
		"topology_generation must not advance on A2 rejection (before=%d after=%d)", genBefore, genAfter)
	t.Logf("✅ topology_generation unchanged (%d)", genAfter)

	// Wait an extra 3s for the consumer ACK round-trip then check XPENDING.
	time.Sleep(3 * time.Second)
	pending := xpending(t, ctx, clients, "manifest.loaded:v1", "orchestrator_manifest_loaded")
	assert.Equal(t, int64(0), pending,
		"manifest.loaded:v1 must be ACKed on A2 rejection (got pending=%d)", pending)
	t.Logf("✅ manifest.loaded:v1 ACKed (pending = 0)")
}

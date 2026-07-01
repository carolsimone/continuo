package redis

import (
	"encoding/json"
	"testing"

	"github.com/carolsimone/continuo/release-controller/service/handlers"
	goredis "github.com/redis/go-redis/v9"
)

// TestDecodeCompileCompletedPerNode verifies that the compile.completed:v1
// "payload" field is decoded into HandleCompileResultInput, including the
// per_node array that executor-controller emits.
func TestDecodeCompileCompletedPerNode(t *testing.T) {
	rawPayload := `{
		"release_id": "rel-1",
		"status":     "failed",
		"per_node": [
			{
				"node_id":     "core",
				"status":      "failed",
				"dbt_log_uri": "s3://c.log"
			}
		]
	}`

	payloadJSON, err := json.Marshal(rawPayload)
	if err != nil {
		t.Fatalf("marshal payload string: %v", err)
	}

	msg := goredis.XMessage{
		ID: "0-1",
		Values: map[string]any{
			"payload": string(rawPayload),
		},
	}
	// payloadJSON is not used in msg — suppress unused-variable warning.
	_ = payloadJSON

	var in handlers.HandleCompileResultInput
	if err := decodePayload(msg, &in); err != nil {
		t.Fatalf("decodePayload: %v", err)
	}

	if in.ReleaseID != "rel-1" {
		t.Errorf("ReleaseID: want %q got %q", "rel-1", in.ReleaseID)
	}
	if in.Status != "failed" {
		t.Errorf("Status: want %q got %q", "failed", in.Status)
	}
	if len(in.PerNode) != 1 {
		t.Fatalf("PerNode length: want 1 got %d", len(in.PerNode))
	}
	node := in.PerNode[0]
	if node.NodeID != "core" {
		t.Errorf("PerNode[0].NodeID: want %q got %q", "core", node.NodeID)
	}
	if node.Status != "failed" {
		t.Errorf("PerNode[0].Status: want %q got %q", "failed", node.Status)
	}
	if node.DBTLogURI != "s3://c.log" {
		t.Errorf("PerNode[0].DBTLogURI: want %q got %q", "s3://c.log", node.DBTLogURI)
	}
}

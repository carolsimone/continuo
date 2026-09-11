package redis

import (
	"testing"

	goredis "github.com/redis/go-redis/v9"
)

// TestDecodeCompileCompletedPerNode verifies that the compile.completed:v1
// "payload" field is decoded into compileResultDTO — including the per_node
// array and the failed-container attribution executor-controller emits — and
// that toInput carries every value onto the tag-free handler input.
func TestDecodeCompileCompletedPerNode(t *testing.T) {
	msg := goredis.XMessage{
		ID: "0-1",
		Values: map[string]any{
			"payload": `{
				"release_id":   "rel-1",
				"status":       "failed",
				"error_detail": "Compilation Error",
				"per_node": [
					{
						"node_id":          "core",
						"status":           "failed",
						"dbt_log_uri":      "s3://c.log",
						"run_results_uri":  "s3://c.json",
						"duration_ms":      120,
						"failed_container": "parse-prod"
					}
				]
			}`,
		},
	}

	var dto compileResultDTO
	if err := decodePayload(msg, &dto); err != nil {
		t.Fatalf("decodePayload: %v", err)
	}
	in := dto.toInput()

	if in.ReleaseID != "rel-1" {
		t.Errorf("ReleaseID: want %q got %q", "rel-1", in.ReleaseID)
	}
	if in.Status != "failed" {
		t.Errorf("Status: want %q got %q", "failed", in.Status)
	}
	if in.ErrorDetail != "Compilation Error" {
		t.Errorf("ErrorDetail: want %q got %q", "Compilation Error", in.ErrorDetail)
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
	if node.RunResultsURI != "s3://c.json" {
		t.Errorf("PerNode[0].RunResultsURI: want %q got %q", "s3://c.json", node.RunResultsURI)
	}
	if node.DurationMS != 120 {
		t.Errorf("PerNode[0].DurationMS: want 120 got %d", node.DurationMS)
	}
	if node.FailedContainer != "parse-prod" {
		t.Errorf("PerNode[0].FailedContainer: want %q got %q", "parse-prod", node.FailedContainer)
	}
}

// TestDecodeSeedBuildCompletedPerNode verifies the seed.build.completed:v1
// payload decodes through its own DTO onto the tag-free handler input.
func TestDecodeSeedBuildCompletedPerNode(t *testing.T) {
	msg := goredis.XMessage{
		ID: "0-1",
		Values: map[string]any{
			"payload": `{
				"release_id":   "rel-2",
				"status":       "failed",
				"error_detail": "Database Error in seed s",
				"per_node": [
					{"node_id": "svc.sch.s", "status": "failed", "dbt_log_uri": "s3://s.log"}
				]
			}`,
		},
	}

	var dto seedBuildResultDTO
	if err := decodePayload(msg, &dto); err != nil {
		t.Fatalf("decodePayload: %v", err)
	}
	in := dto.toInput()

	if in.ReleaseID != "rel-2" {
		t.Errorf("ReleaseID: want %q got %q", "rel-2", in.ReleaseID)
	}
	if in.Status != "failed" {
		t.Errorf("Status: want %q got %q", "failed", in.Status)
	}
	if in.ErrorDetail != "Database Error in seed s" {
		t.Errorf("ErrorDetail: want %q got %q", "Database Error in seed s", in.ErrorDetail)
	}
	if len(in.PerNode) != 1 {
		t.Fatalf("PerNode length: want 1 got %d", len(in.PerNode))
	}
	if in.PerNode[0].NodeID != "svc.sch.s" || in.PerNode[0].DBTLogURI != "s3://s.log" {
		t.Errorf("PerNode[0]: got %+v", in.PerNode[0])
	}
}

// A payload with no per_node key leaves the input's slice nil, and one with an
// empty array leaves it non-nil: the rejection body the leg emits repeats that
// difference as null vs [].
func TestStageNodeResultsPreserveNilVersusEmpty(t *testing.T) {
	var absent compileResultDTO
	if err := decodePayload(goredis.XMessage{Values: map[string]any{
		"payload": `{"release_id":"rel-3","status":"failed"}`,
	}}, &absent); err != nil {
		t.Fatalf("decodePayload: %v", err)
	}
	if absent.toInput().PerNode != nil {
		t.Errorf("an absent per_node must map to a nil slice, got %#v", absent.toInput().PerNode)
	}

	var empty compileResultDTO
	if err := decodePayload(goredis.XMessage{Values: map[string]any{
		"payload": `{"release_id":"rel-3","status":"failed","per_node":[]}`,
	}}, &empty); err != nil {
		t.Fatalf("decodePayload: %v", err)
	}
	got := empty.toInput().PerNode
	if got == nil || len(got) != 0 {
		t.Errorf("an empty per_node must map to a non-nil empty slice, got %#v", got)
	}
}

package serialization

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/carolsimone/continuo/release-controller/domain/pipeline"
)

// goldenPerNode is the exact JSON a fully-populated []NodeValidationResult is
// persisted as (per_node_results JSONB) and returned as (per_node_results in the
// HTTP responses). Every tagged field is present so the DTO reproduces each tag,
// name, casing and order byte-for-byte.
const goldenPerNode = `[{"stage":"validation","node_id":"svc.sch.tbl","status":"failed","dbt_log_uri":"s3://l","run_results_uri":"s3://r","duration_ms":42,"file_path":"models/x.sql","node_type":"dbt-model"}]`

func TestNodeValidationResultDTORoundTrip(t *testing.T) {
	var dto []NodeValidationResultDTO
	if err := json.Unmarshal([]byte(goldenPerNode), &dto); err != nil {
		t.Fatalf("unmarshal golden: %v", err)
	}
	got := NodeValidationResultsToDomain(dto)
	want := []pipeline.NodeValidationResult{{
		Stage: "validation", NodeID: "svc.sch.tbl", Status: "failed",
		DBTLogURI: "s3://l", RunResultsURI: "s3://r", DurationMS: 42,
		FilePath: "models/x.sql", NodeType: "dbt-model",
	}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("toDomain:\n got %+v\nwant %+v", got, want)
	}
	out, err := json.Marshal(NodeValidationResultsFromDomain(got))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(out) != goldenPerNode {
		t.Fatalf("bytes changed:\n got %s\nwant %s", out, goldenPerNode)
	}
}

func TestNodeValidationResultOmitemptyStaysOmitted(t *testing.T) {
	out, err := json.Marshal(NodeValidationResultsFromDomain([]pipeline.NodeValidationResult{{NodeID: "n", Status: "ok"}}))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	const want = `[{"node_id":"n","status":"ok"}]`
	if string(out) != want {
		t.Fatalf("omitempty not preserved:\n got %s\nwant %s", out, want)
	}
}

func TestNodeValidationResultsNilRoundTripsAsNull(t *testing.T) {
	out, err := json.Marshal(NodeValidationResultsFromDomain(nil))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(out) != "null" {
		t.Fatalf("nil slice must marshal as null, got %s", out)
	}
	if NodeValidationResultsToDomain(nil) != nil {
		t.Fatal("nil DTO slice must map to nil domain slice")
	}
}

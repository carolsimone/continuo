package serialization

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/carolsimone/continuo/release-controller/domain/pipeline"
	"github.com/carolsimone/continuo/release-controller/domain/release"
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

// goldenTransitions is the exact JSON a []Transition is stored and returned as.
// "to" holds a pipeline.Status value; "at" is an RFC3339 time (json trims
// trailing fractional-second zeros, so a whole-second time has no fraction).
const goldenTransitions = `[{"to":"received","at":"2026-01-02T03:04:05Z"},{"to":"remediation_retry","at":"2026-01-02T03:05:06Z"}]`

func TestTransitionDTORoundTrip(t *testing.T) {
	var dto []TransitionDTO
	if err := json.Unmarshal([]byte(goldenTransitions), &dto); err != nil {
		t.Fatalf("unmarshal golden: %v", err)
	}
	got := TransitionsToDomain(dto)
	want := []pipeline.Transition{
		{To: pipeline.StatusReceived, At: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)},
		{To: pipeline.Status("remediation_retry"), At: time.Date(2026, 1, 2, 3, 5, 6, 0, time.UTC)},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("toDomain:\n got %+v\nwant %+v", got, want)
	}
	out, err := json.Marshal(TransitionsFromDomain(got))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(out) != goldenTransitions {
		t.Fatalf("bytes changed:\n got %s\nwant %s", out, goldenTransitions)
	}
}

func TestTransitionsNilRoundTripsAsNull(t *testing.T) {
	out, err := json.Marshal(TransitionsFromDomain(nil))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(out) != "null" {
		t.Fatalf("nil slice must marshal as null, got %s", out)
	}
	if TransitionsToDomain(nil) != nil {
		t.Fatal("nil DTO slice must map to nil domain slice")
	}
}

// goldenTopology is the exact JSON a release.Topology is stored and carried as.
// Only candidate_artifact_uri is omitempty; upstream_unique_ids has no omitempty
// so a nil slice serialises as null.
const goldenTopology = `[{"unique_id":"svc.model","schema_name":"sch","table_name":"tbl","resolved_relation_id":"sch.tbl","service_name":"svc","node_type":"dbt-model","content_hash":"abc","test_count":2,"image_tag":"v1","upstream_unique_ids":["svc.up"],"schedule":"daily","original_file_path":"models/m.sql","candidate_artifact_uri":"s3://c"}]`

func TestTopologyDTORoundTrip(t *testing.T) {
	var dto TopologyDTO
	if err := json.Unmarshal([]byte(goldenTopology), &dto); err != nil {
		t.Fatalf("unmarshal golden: %v", err)
	}
	got := dto.ToDomain()
	want := release.Topology{{
		UniqueID: "svc.model", SchemaName: "sch", TableName: "tbl",
		ResolvedRelationID: "sch.tbl", ServiceName: "svc", NodeType: "dbt-model",
		ContentHash: "abc", TestCount: 2, ImageTag: "v1",
		UpstreamUniqueIDs: []string{"svc.up"}, Schedule: "daily",
		OriginalFilePath: "models/m.sql", CandidateArtifactURI: "s3://c",
	}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("toDomain:\n got %+v\nwant %+v", got, want)
	}
	out, err := json.Marshal(TopologyFromDomain(got))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(out) != goldenTopology {
		t.Fatalf("bytes changed:\n got %s\nwant %s", out, goldenTopology)
	}
}

// TestTopologyZeroNodeOmitemptyShape pins that candidate_artifact_uri is the
// only omitted key and that a nil upstream slice serialises as null.
func TestTopologyZeroNodeOmitemptyShape(t *testing.T) {
	out, err := json.Marshal(TopologyFromDomain(release.Topology{{}}))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	const want = `[{"unique_id":"","schema_name":"","table_name":"","resolved_relation_id":"","service_name":"","node_type":"","content_hash":"","test_count":0,"image_tag":"","upstream_unique_ids":null,"schedule":"","original_file_path":""}]`
	if string(out) != want {
		t.Fatalf("zero-node shape changed:\n got %s\nwant %s", out, want)
	}
}

// TestNodeCandidateArtifactURIKey pins the candidate-artifact URI to the
// candidate_artifact_uri key and guards against re-emitting the legacy
// candidate_sql / candidate_sql_uri keys — no compatibility alias is written.
func TestNodeCandidateArtifactURIKey(t *testing.T) {
	out, err := json.Marshal(TopologyFromDomain(release.Topology{{
		UniqueID: "analytics.orders", CandidateArtifactURI: "s3://b/candidate_analytics.orders.sql",
	}}))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	s := string(out)
	if !strings.Contains(s, `"candidate_artifact_uri":"s3://b/candidate_analytics.orders.sql"`) {
		t.Fatalf("candidate_artifact_uri key missing: %s", s)
	}
	for _, legacy := range []string{"candidate_sql_uri", `"candidate_sql"`} {
		if strings.Contains(s, legacy) {
			t.Fatalf("legacy key %s must not be emitted: %s", legacy, s)
		}
	}
}

func TestTopologyNilRoundTripsAsNull(t *testing.T) {
	out, err := json.Marshal(TopologyFromDomain(nil))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(out) != "null" {
		t.Fatalf("nil topology must marshal as null, got %s", out)
	}
	if TopologyDTO(nil).ToDomain() != nil {
		t.Fatal("nil DTO must map to nil domain topology")
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

package serialization

import (
	"encoding/json"
	"strings"
	"testing"

	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/release-controller/service/ports"
)

// Each golden below is the exact release.rejected:v1 body one leg emits.
// remediation's binding, the UI and the e2e harness all decode these keys, so
// the goldens pin the key names, the omitempty behaviour and the null vs []
// distinction of every shape.

const goldenParseRejection = `{"release_id":"rel-1","stage":"parse","reason":"invalid_sql","error_detail":"Expecting ). Line 3, Col: 12.",` +
	`"failing_nodes":["svc.sch.a"],` +
	`"per_node":[{"node_id":"svc.sch.a","status":"failed","kind":"invalid_sql","detail":"Expecting ). Line 3, Col: 12.","file_path":"models/a.sql","service":"svc","node_type":"dbt-model"}],` +
	`"repo":"org/svc","commit_sha":"abc123","code_bundle_uri":"s3://bundles/rel-1.tar.gz"}`

func TestEncodeParseRejection(t *testing.T) {
	got := encode(t, ports.ReleaseRejection{
		Shape:        ports.RejectionShapeParse,
		ReleaseID:    "rel-1",
		Reason:       pkg_model.RejectReasonInvalidSQL,
		ErrorDetail:  "Expecting ). Line 3, Col: 12.",
		FailingNodes: []string{"svc.sch.a"},
		PerNode: []ports.RejectedNode{{
			NodeID:   "svc.sch.a",
			Status:   "failed",
			Kind:     "invalid_sql",
			Detail:   "Expecting ). Line 3, Col: 12.",
			FilePath: "models/a.sql",
			Service:  "svc",
			NodeType: "dbt-model",
		}},
		Repo:          "org/svc",
		CommitSHA:     "abc123",
		CodeBundleURI: "s3://bundles/rel-1.tar.gz",
	})
	assertJSON(t, got, goldenParseRejection)
}

// A parse node the topology could not locate still carries every key: the
// parse shape omits nothing, so an empty string says "not known" rather than
// leaving the consumer to distinguish a missing key from an empty one.
func TestEncodeParseRejectionKeepsEmptyPerNodeKeys(t *testing.T) {
	got := encode(t, ports.ReleaseRejection{
		Shape:     ports.RejectionShapeParse,
		ReleaseID: "rel-1",
		Reason:    pkg_model.RejectReasonInternalError,
		PerNode:   []ports.RejectedNode{{NodeID: "svc.sch.a", Status: "failed", Kind: "internal"}},
	})
	const want = `{"release_id":"rel-1","stage":"parse","reason":"internal_error","error_detail":"","failing_nodes":null,` +
		`"per_node":[{"node_id":"svc.sch.a","status":"failed","kind":"internal","detail":"","file_path":"","service":"","node_type":""}],` +
		`"repo":"","commit_sha":"","code_bundle_uri":""}`
	assertJSON(t, got, want)
}

const goldenCompileRejection = `{"release_id":"rel-2","stage":"compile","reason":"compile_failed","error_detail":"Compilation Error in model a",` +
	`"failing_nodes":["svc.sch.a"],` +
	`"per_node":[{"node_id":"svc.sch.a","status":"failed","dbt_log_uri":"s3://logs/a.log","run_results_uri":"s3://logs/a.json"}],` +
	`"repo":"org/svc","commit_sha":"def456","code_bundle_uri":"s3://bundles/rel-2.tar.gz"}`

func TestEncodeCompileRejection(t *testing.T) {
	got := encode(t, ports.ReleaseRejection{
		Shape:        ports.RejectionShapeCompile,
		ReleaseID:    "rel-2",
		Reason:       pkg_model.RejectReasonCompileFailed,
		ErrorDetail:  "Compilation Error in model a",
		FailingNodes: []string{"svc.sch.a"},
		PerNode: []ports.RejectedNode{{
			NodeID:        "svc.sch.a",
			Status:        "failed",
			DBTLogURI:     "s3://logs/a.log",
			RunResultsURI: "s3://logs/a.json",
			// The compile leg resolves no source location; the shape drops
			// these rather than emitting empty keys.
			FilePath: "models/a.sql",
			Service:  "svc",
			NodeType: "dbt-model",
		}},
		Repo:          "org/svc",
		CommitSHA:     "def456",
		CodeBundleURI: "s3://bundles/rel-2.tar.gz",
	})
	assertJSON(t, got, goldenCompileRejection)
}

const goldenSeedBuildRejection = `{"release_id":"rel-3","stage":"seed_build","reason":"seed_build_failed","error_detail":"Database Error in seed s",` +
	`"failing_nodes":["svc.sch.s"],` +
	`"per_node":[{"node_id":"svc.sch.s","status":"failed","dbt_log_uri":"s3://logs/s.log","run_results_uri":"s3://logs/s.json","file_path":"seeds/s.csv","service":"svc"}],` +
	`"repo":"org/svc","commit_sha":"ghi789","code_bundle_uri":"s3://bundles/rel-3.tar.gz","candidate_schema":"_cand_rel_3"}`

func TestEncodeSeedBuildRejection(t *testing.T) {
	got := encode(t, ports.ReleaseRejection{
		Shape:        ports.RejectionShapeSeedBuild,
		ReleaseID:    "rel-3",
		Reason:       pkg_model.RejectReasonSeedBuildFailed,
		ErrorDetail:  "Database Error in seed s",
		FailingNodes: []string{"svc.sch.s"},
		PerNode: []ports.RejectedNode{{
			NodeID:        "svc.sch.s",
			Status:        "failed",
			DBTLogURI:     "s3://logs/s.log",
			RunResultsURI: "s3://logs/s.json",
			FilePath:      "seeds/s.csv",
			Service:       "svc",
		}},
		Repo:            "org/svc",
		CommitSHA:       "ghi789",
		CodeBundleURI:   "s3://bundles/rel-3.tar.gz",
		CandidateSchema: "_cand_rel_3",
	})
	assertJSON(t, got, goldenSeedBuildRejection)
}

const goldenValidationRejection = `{"release_id":"rel-4","stage":"validation","reason":"validation_failed",` +
	`"failing_nodes":["svc.sch.b"],"missing_nodes":[],"aggregate_status":"failed",` +
	`"per_node":[` +
	`{"node_id":"svc.sch.a","status":"ok","dbt_log_uri":"s3://logs/a.log","candidate_artifact_uri":"s3://cand/a.sql","node_type":"dbt-model","file_path":"models/a.sql","service":"svc"},` +
	`{"node_id":"svc.sch.b","status":"failed","dbt_log_uri":"s3://logs/b.log","run_results_uri":"s3://logs/b.json","node_type":"dbt-model","file_path":"models/b.sql","service":"svc",` +
	`"changed_ancestors":[{"node_id":"svc.sch.a","file_path":"models/a.sql","service":"svc","depth":1}]}],` +
	`"repo":"org/svc","commit_sha":"jkl012","code_bundle_uri":"s3://bundles/rel-4.tar.gz"}`

func TestEncodeValidationRejection(t *testing.T) {
	got := encode(t, ports.ReleaseRejection{
		Shape:           ports.RejectionShapeValidation,
		ReleaseID:       "rel-4",
		Reason:          pkg_model.RejectReasonValidationFailed,
		AggregateStatus: "failed",
		FailingNodes:    []string{"svc.sch.b"},
		PerNode: []ports.RejectedNode{
			{
				NodeID:               "svc.sch.a",
				Status:               "ok",
				DBTLogURI:            "s3://logs/a.log",
				CandidateArtifactURI: "s3://cand/a.sql",
				NodeType:             "dbt-model",
				FilePath:             "models/a.sql",
				Service:              "svc",
			},
			{
				NodeID:        "svc.sch.b",
				Status:        "failed",
				DBTLogURI:     "s3://logs/b.log",
				RunResultsURI: "s3://logs/b.json",
				NodeType:      "dbt-model",
				FilePath:      "models/b.sql",
				Service:       "svc",
				ChangedAncestors: []ports.ChangedAncestor{
					{NodeID: "svc.sch.a", FilePath: "models/a.sql", Service: "svc", Depth: 1},
				},
			},
		},
		Repo:          "org/svc",
		CommitSHA:     "jkl012",
		CodeBundleURI: "s3://bundles/rel-4.tar.gz",
	})
	assertJSON(t, got, goldenValidationRejection)
	// The validation shape carries no error_detail: its evidence is per node.
	assertNoKey(t, got, "error_detail")
}

const goldenDuplicateRejection = `{"release_id":"rel-5","reason":"duplicate_table","error_detail":"sch.t claimed by svc_a (models/t.sql) and svc_b (models/t.sql)",` +
	`"failing_nodes":["svc_a.sch.t","svc_b.sch.t"],` +
	`"per_node":[{"node_id":"svc_a.sch.t","status":"failed","service":"svc_a","file_path":"models/t.sql","node_type":"dbt-model",` +
	`"relation_id":"sch.t","other_service":"svc_b","other_file_path":"models/t2.sql"}],` +
	`"repo":"org/svc_a","commit_sha":"mno345","code_bundle_uri":"s3://bundles/rel-5.tar.gz"}`

// The duplicate-table check runs between legs, so its body carries no stage.
func TestEncodeDuplicateTableRejection(t *testing.T) {
	got := encode(t, ports.ReleaseRejection{
		Shape:        ports.RejectionShapeDuplicateTable,
		ReleaseID:    "rel-5",
		Reason:       pkg_model.RejectReasonDuplicateTable,
		ErrorDetail:  "sch.t claimed by svc_a (models/t.sql) and svc_b (models/t.sql)",
		FailingNodes: []string{"svc_a.sch.t", "svc_b.sch.t"},
		PerNode: []ports.RejectedNode{{
			NodeID:        "svc_a.sch.t",
			Status:        "failed",
			Service:       "svc_a",
			FilePath:      "models/t.sql",
			NodeType:      "dbt-model",
			RelationID:    "sch.t",
			OtherService:  "svc_b",
			OtherFilePath: "models/t2.sql",
		}},
		Repo:          "org/svc_a",
		CommitSHA:     "mno345",
		CodeBundleURI: "s3://bundles/rel-5.tar.gz",
	})
	assertJSON(t, got, goldenDuplicateRejection)
	assertNoKey(t, got, "stage")
}

// The detail names the offending node->upstream edges; encoding/json escapes
// > as \u003e, which every JSON decoder reads back as the original character.
const goldenUnbuildableUpstreamRejection = `{"release_id":"rel-6","reason":"unbuildable_cross_service_upstream",` +
	`"error_detail":"svc.sch.a-\u003eother.sch.z; add the missing producing model"}`

// The narrow body names only the release, the reason and the offending edges:
// there is no per-node evidence a fixer could act on.
func TestEncodeUnbuildableUpstreamRejection(t *testing.T) {
	got := encode(t, ports.ReleaseRejection{
		Shape:       ports.RejectionShapeUnbuildableUpstream,
		ReleaseID:   "rel-6",
		Reason:      pkg_model.RejectReasonUnbuildableCrossServiceUpstream,
		ErrorDetail: "svc.sch.a->other.sch.z; add the missing producing model",
		// Values the narrow shape drops.
		FailingNodes:  []string{"svc.sch.a"},
		PerNode:       []ports.RejectedNode{{NodeID: "svc.sch.a"}},
		Repo:          "org/svc",
		CommitSHA:     "pqr678",
		CodeBundleURI: "s3://bundles/rel-6.tar.gz",
	})
	assertJSON(t, got, goldenUnbuildableUpstreamRejection)
}

// A nil per-node slice and an empty one are different bodies (null vs []), and
// the legs rely on the difference: the validation leg leaves per_node null when
// the projection holds no validation row, while the compile and seed-build legs
// always emit an array.
func TestEncodePreservesNilVersusEmptyPerNode(t *testing.T) {
	nilBody := encode(t, ports.ReleaseRejection{Shape: ports.RejectionShapeValidation, ReleaseID: "rel-7"})
	if !strings.Contains(nilBody, `"per_node":null`) {
		t.Fatalf("nil per_node must serialise as null: %s", nilBody)
	}
	emptyBody := encode(t, ports.ReleaseRejection{
		Shape:        ports.RejectionShapeCompile,
		ReleaseID:    "rel-7",
		FailingNodes: []string{},
		PerNode:      []ports.RejectedNode{},
	})
	if !strings.Contains(emptyBody, `"per_node":[]`) || !strings.Contains(emptyBody, `"failing_nodes":[]`) {
		t.Fatalf("empty slices must serialise as []: %s", emptyBody)
	}
}

// An unknown shape is an error: a body silently missing the keys its consumer
// reads would fail far from here.
func TestEncodeRejectsUnknownShape(t *testing.T) {
	if _, err := (ReleaseRejectedJSON{}).Encode(ports.ReleaseRejection{Shape: "no-such-shape"}); err == nil {
		t.Fatal("expected an error for an unknown rejection shape")
	}
}

func encode(t *testing.T, rej ports.ReleaseRejection) string {
	t.Helper()
	b, err := (ReleaseRejectedJSON{}).Encode(rej)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	return string(b)
}

func assertJSON(t *testing.T, got, want string) {
	t.Helper()
	if got != want {
		t.Fatalf("body changed:\n got %s\nwant %s", got, want)
	}
}

func assertNoKey(t *testing.T, body, key string) {
	t.Helper()
	var m map[string]json.RawMessage
	if err := json.Unmarshal([]byte(body), &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if _, ok := m[key]; ok {
		t.Fatalf("key %q must not be present: %s", key, body)
	}
}

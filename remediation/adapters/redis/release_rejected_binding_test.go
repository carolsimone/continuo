package redis

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/remediation/domain/failure"
)

func TestEvidenceFromRejected(t *testing.T) {
	raw := []byte(`{
		"release_id":"r1","reason":"validation_failed",
		"failing_nodes":["s.a","s.b"],"missing_nodes":[],"aggregate_status":"failed",
		"repo":"o/r","commit_sha":"abc",
		"per_node":[
			{"node_id":"s.a","status":"failed","dbt_log_uri":"s3://b/a.log","run_results_uri":"run-results/a.json","candidate_artifact_uri":"s3://b/a.sql"},
			{"node_id":"s.b","status":"failed","dbt_log_uri":"s3://b/b.log","candidate_artifact_uri":"s3://b/b.sql"},
			{"node_id":"s.c","status":"ok"}
		]}`)
	got, err := evidenceFromRejected(raw)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("want 2 failed-node evidences, got %d", len(got))
	}
	a := got[0]
	if a.Source != failure.SourceValidation || a.ReleaseID != "r1" || a.NodeID != "s.a" ||
		a.DBTLogURI != "s3://b/a.log" || a.CandidateArtifactURI != "s3://b/a.sql" ||
		a.Repo != "o/r" || a.CommitSHA != "abc" {
		t.Fatalf("bad evidence: %+v", a)
	}
	if a.RunResultsURI != "run-results/a.json" {
		t.Fatalf("run_results_uri not carried into evidence: %q", a.RunResultsURI)
	}
	if got[1].RunResultsURI != "" {
		t.Fatalf("absent run_results_uri must be empty, got %q", got[1].RunResultsURI)
	}
}

// TestEvidenceFromRejected_Compile verifies that a compile-stage rejection sets
// Source=SourceCompile, and that a seed-stage rejection sets Source=SourceSeed.
// The existing validation path must still yield SourceValidation. The fallback
// path (no stage field, only reason) must also map correctly.
func TestEvidenceFromRejected_Compile(t *testing.T) {
	t.Run("compile stage", func(t *testing.T) {
		raw := []byte(`{"release_id":"rel-1","stage":"compile","reason":"compile_failed",
		  "repo":"o/r","commit_sha":"sha","failing_nodes":["core"],
		  "per_node":[{"node_id":"core","status":"failed","dbt_log_uri":"s3://c.log"}]}`)
		evs, err := evidenceFromRejected(raw)
		if err != nil {
			t.Fatal(err)
		}
		if len(evs) != 1 {
			t.Fatalf("want 1 evidence, got %d", len(evs))
		}
		if evs[0].Source != failure.SourceCompile {
			t.Errorf("Source = %q, want %q", evs[0].Source, failure.SourceCompile)
		}
		if evs[0].NodeID != "core" {
			t.Errorf("NodeID = %q, want %q", evs[0].NodeID, "core")
		}
		if evs[0].DBTLogURI != "s3://c.log" {
			t.Errorf("DBTLogURI = %q, want %q", evs[0].DBTLogURI, "s3://c.log")
		}
	})

	t.Run("seed_build stage", func(t *testing.T) {
		raw := []byte(`{"release_id":"rel-2","stage":"seed_build","reason":"seed_build_failed",
		  "repo":"o/r","commit_sha":"sha","failing_nodes":["seed_node"],
		  "per_node":[{"node_id":"seed_node","status":"failed","dbt_log_uri":"s3://s.log"}]}`)
		evs, err := evidenceFromRejected(raw)
		if err != nil {
			t.Fatal(err)
		}
		if len(evs) != 1 {
			t.Fatalf("want 1 evidence, got %d", len(evs))
		}
		if evs[0].Source != failure.SourceSeed {
			t.Errorf("Source = %q, want %q", evs[0].Source, failure.SourceSeed)
		}
	})

	t.Run("validation stage explicit", func(t *testing.T) {
		raw := []byte(`{"release_id":"rel-3","stage":"validation","reason":"validation_failed",
		  "repo":"o/r","commit_sha":"sha","failing_nodes":["v_node"],
		  "per_node":[{"node_id":"v_node","status":"failed","dbt_log_uri":"s3://v.log"}]}`)
		evs, err := evidenceFromRejected(raw)
		if err != nil {
			t.Fatal(err)
		}
		if len(evs) != 1 {
			t.Fatalf("want 1 evidence, got %d", len(evs))
		}
		if evs[0].Source != failure.SourceValidation {
			t.Errorf("Source = %q, want %q", evs[0].Source, failure.SourceValidation)
		}
	})

	t.Run("fallback reason compile_failed no stage", func(t *testing.T) {
		raw := []byte(`{"release_id":"rel-4","reason":"compile_failed",
		  "repo":"o/r","commit_sha":"sha","failing_nodes":["core2"],
		  "per_node":[{"node_id":"core2","status":"failed","dbt_log_uri":"s3://c2.log"}]}`)
		evs, err := evidenceFromRejected(raw)
		if err != nil {
			t.Fatal(err)
		}
		if len(evs) != 1 {
			t.Fatalf("want 1 evidence, got %d", len(evs))
		}
		if evs[0].Source != failure.SourceCompile {
			t.Errorf("Source = %q, want %q", evs[0].Source, failure.SourceCompile)
		}
	})

	t.Run("parse-phase rejection yields no evidence (not misrouted to validation)", func(t *testing.T) {
		// A parse_failed rejection is not a remediable pipeline leg. Even when it
		// carries per_node entries, it must NOT be classified as a validation
		// source-fix — evidenceFromRejected produces nothing.
		raw := []byte(`{"release_id":"rel-9","reason":"parse_failed",
		  "repo":"o/r","commit_sha":"sha","failing_nodes":["x"],
		  "per_node":[{"node_id":"x","status":"failed","dbt_log_uri":"s3://x.log"}]}`)
		evs, err := evidenceFromRejected(raw)
		if err != nil {
			t.Fatal(err)
		}
		if len(evs) != 0 {
			t.Fatalf("want 0 evidence for parse-phase rejection, got %d", len(evs))
		}
	})
}

// TestEvidenceFromRejected_ParseExportLegRejections verifies that the two
// parse-export-leg rejection reasons — parse_rehearsal_failed and
// artifact_upload_failed — produce no FailureEvidence even though they carry
// stage:"compile" and a failed per_node entry. Neither is fixable by a model
// change (a rehearsal miss is a project property; an upload failure is
// continuo-internal), so no heal trigger must be produced. A compile_failed
// rejection on the same stage must still yield evidence.
func TestEvidenceFromRejected_ParseExportLegRejections(t *testing.T) {
	t.Run("parse_rehearsal_failed yields no evidence", func(t *testing.T) {
		raw := []byte(`{"release_id":"rel-10","stage":"compile","reason":"parse_rehearsal_failed",
		  "repo":"o/r","commit_sha":"sha","failing_nodes":["core"],
		  "per_node":[{"node_id":"core","status":"failed","dbt_log_uri":"s3://c.log"}]}`)
		evs, err := evidenceFromRejected(raw)
		if err != nil {
			t.Fatal(err)
		}
		if evs != nil {
			t.Fatalf("want nil evidence, got %+v", evs)
		}
	})

	t.Run("artifact_upload_failed yields no evidence", func(t *testing.T) {
		raw := []byte(`{"release_id":"rel-11","stage":"compile","reason":"artifact_upload_failed",
		  "repo":"o/r","commit_sha":"sha","failing_nodes":["core"],
		  "per_node":[{"node_id":"core","status":"failed","dbt_log_uri":"s3://c.log"}]}`)
		evs, err := evidenceFromRejected(raw)
		if err != nil {
			t.Fatal(err)
		}
		if evs != nil {
			t.Fatalf("want nil evidence, got %+v", evs)
		}
	})

	t.Run("compile_failed still yields evidence", func(t *testing.T) {
		raw := []byte(`{"release_id":"rel-12","stage":"compile","reason":"compile_failed",
		  "repo":"o/r","commit_sha":"sha","failing_nodes":["core"],
		  "per_node":[{"node_id":"core","status":"failed","dbt_log_uri":"s3://c.log"}]}`)
		evs, err := evidenceFromRejected(raw)
		if err != nil {
			t.Fatal(err)
		}
		if len(evs) != 1 {
			t.Fatalf("want 1 evidence, got %d", len(evs))
		}
		if evs[0].Source != failure.SourceCompile {
			t.Errorf("Source = %q, want %q", evs[0].Source, failure.SourceCompile)
		}
	})
}

// TestEvidenceFromRejected_SeedFilePathAndService verifies that when the
// release.rejected:v1 payload carries file_path and service on a seed_build
// per-node entry, evidenceFromRejected threads them into FailureEvidence so
// the classifier can forward them to the remediation trigger — allowing the
// agent to locate the source file without querying GetNodeLocation.
func TestEvidenceFromRejected_SeedFilePathAndService(t *testing.T) {
	raw := []byte(`{
		"release_id":"rel-seed","stage":"seed_build","reason":"seed_build_failed",
		"repo":"o/r","commit_sha":"sha1",
		"per_node":[
			{"node_id":"seed.svc.customers","status":"failed","dbt_log_uri":"s3://b/c.log",
			 "file_path":"seeds/customers.csv","service":"svc-data"},
			{"node_id":"seed.svc.products","status":"ok","dbt_log_uri":"s3://b/p.log"}
		]}`)
	got, err := evidenceFromRejected(raw)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("want 1 failed evidence, got %d", len(got))
	}
	e := got[0]
	if e.Source != failure.SourceSeed {
		t.Errorf("Source = %q, want %q", e.Source, failure.SourceSeed)
	}
	if e.FilePath != "seeds/customers.csv" {
		t.Errorf("FilePath = %q, want seeds/customers.csv", e.FilePath)
	}
	if e.Service != "svc-data" {
		t.Errorf("Service = %q, want svc-data", e.Service)
	}
}

// TestEvidenceFromRejected_DuplicateTable verifies that a duplicate_table
// rejection — a parse-time gate rejection with no stage field and no dbt log —
// is routed to SourceDuplicateTable and carries the claimant service/file_path
// plus the competing other_service through to FailureEvidence.
func TestEvidenceFromRejected_DuplicateTable(t *testing.T) {
	raw := []byte(`{
	  "release_id": "rel-1",
	  "reason": "duplicate_table",
	  "error_class": "DuplicatedTable",
	  "repo": "owner/repo",
	  "commit_sha": "abc123",
	  "per_node": [{
	    "node_id": "analytics.orders_v2",
	    "relation_id": "analytics.orders",
	    "status": "failed",
	    "service": "marketing",
	    "file_path": "models/orders.sql",
	    "other_service": "finance"
	  }]
	}`)

	evs, err := evidenceFromRejected(raw)
	if err != nil {
		t.Fatal(err)
	}
	if len(evs) != 1 {
		t.Fatalf("want 1 evidence, got %d", len(evs))
	}
	e := evs[0]
	if e.Source != failure.SourceDuplicateTable {
		t.Errorf("Source = %q, want %q", e.Source, failure.SourceDuplicateTable)
	}
	if e.NodeID != "analytics.orders_v2" {
		t.Errorf("NodeID = %q, want analytics.orders_v2", e.NodeID)
	}
	if e.RelationID != "analytics.orders" {
		t.Errorf("RelationID = %q, want analytics.orders", e.RelationID)
	}
	if e.Service != "marketing" {
		t.Errorf("Service = %q, want marketing", e.Service)
	}
	if e.FilePath != "models/orders.sql" {
		t.Errorf("FilePath = %q, want models/orders.sql", e.FilePath)
	}
	if e.OtherService != "finance" {
		t.Errorf("OtherService = %q, want finance", e.OtherService)
	}
	if e.DBTLogURI != "" {
		t.Errorf("DBTLogURI = %q, want empty — a parse-time rejection has no dbt log", e.DBTLogURI)
	}
}

// TestEvidenceFromRejected_ParsePhaseReasonsStillDropped verifies that
// parse_failed and unbuildable_cross_service_upstream — parse-phase reasons
// that are not fixable by a model rename — still yield no evidence now that
// the stage-less branch of sourceFromPayload also matches duplicate_table.
func TestEvidenceFromRejected_ParsePhaseReasonsStillDropped(t *testing.T) {
	for _, reason := range []string{"parse_failed", "unbuildable_cross_service_upstream"} {
		raw := []byte(`{"release_id":"rel-1","reason":"` + reason +
			`","per_node":[{"node_id":"n1","status":"failed"}]}`)

		evs, err := evidenceFromRejected(raw)
		if err != nil {
			t.Fatal(err)
		}
		if len(evs) != 0 {
			t.Errorf("%s: want 0 evidence, got %d — not a model defect a heal proposal could fix", reason, len(evs))
		}
	}
}

// TestEvidenceFromRejected_CarriesCodeBundleURI verifies that a top-level
// code_bundle_uri on the release.rejected:v1 payload is threaded onto each
// failed node's FailureEvidence, so the remediation trigger can point the
// orchestrator's case base at the rejected release's code bundle.
func TestEvidenceFromRejected_CarriesCodeBundleURI(t *testing.T) {
	raw := []byte(`{"release_id":"rel-1","stage":"validation","reason":"validation_failed",
		"repo":"acme/dbt","commit_sha":"abc","code_bundle_uri":"s3://b/code-bundles/rel-1/bundle.json",
		"per_node":[{"node_id":"analytics.orders","status":"failed","dbt_log_uri":"s3://b/l.log"}]}`)
	evs, err := evidenceFromRejected(raw)
	require.NoError(t, err)
	require.Len(t, evs, 1)
	assert.Equal(t, "s3://b/code-bundles/rel-1/bundle.json", evs[0].CodeBundleURI)
}

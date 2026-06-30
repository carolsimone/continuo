package redis

import (
	"testing"

	"github.com/carolsimone/continuo/remediation/domain/failure"
)

func TestEvidenceFromRejected(t *testing.T) {
	raw := []byte(`{
		"release_id":"r1","reason":"validation_failed",
		"failing_nodes":["s.a","s.b"],"missing_nodes":[],"aggregate_status":"failed",
		"repo":"o/r","commit_sha":"abc",
		"per_node":[
			{"node_id":"s.a","status":"failed","dbt_log_uri":"s3://b/a.log","run_results_uri":"run-results/a.json","candidate_sql_uri":"s3://b/a.sql"},
			{"node_id":"s.b","status":"failed","dbt_log_uri":"s3://b/b.log","candidate_sql_uri":"s3://b/b.sql"},
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
		a.DBTLogURI != "s3://b/a.log" || a.CandidateSQLURI != "s3://b/a.sql" ||
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
}

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

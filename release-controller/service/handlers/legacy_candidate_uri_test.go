package handlers

import (
	"encoding/json"
	"testing"

	"github.com/carolsimone/continuo/release-controller/domain/release"
)

// A run from before topology artifacts stored each node's candidate SQL URI
// inline, as topology-controller built it. The upgrade step drops those URIs,
// and validation and rejection derive them per run instead; this pins that
// the derived URI is the one the run stored, for every node type.
func TestCandidateArtifactURI_EqualsTheURIsLegacyRunsStored(t *testing.T) {
	const legacy = `[
	  {"unique_id":"core.orders","node_type":"dbt-model","candidate_artifact_uri":"s3://continuo/candidate-sql/rel-7/candidate_core.orders.sql"},
	  {"unique_id":"core.orders_snap","node_type":"dbt-snapshot","candidate_artifact_uri":"s3://continuo/candidate-sql/rel-7/candidate_core.orders_snap.sql"},
	  {"unique_id":"test.core.not_null_orders_id.5f2a","node_type":"dbt-test","candidate_artifact_uri":"s3://continuo/candidate-sql/rel-7/candidate_test.core.not_null_orders_id.5f2a.sql"},
	  {"unique_id":"core.fx","node_type":"dbt-seed","candidate_artifact_uri":""},
	  {"unique_id":"py.score","node_type":"python-node","candidate_artifact_uri":"s3://continuo/candidate-sql/rel-7/candidate_py.score.json"},
	  {"unique_id":"py.rates_csv","node_type":"python-csv","candidate_artifact_uri":"s3://continuo/candidate-sql/rel-7/candidate_py.rates_csv.json"},
	  {"unique_id":"api.rates","node_type":"python-api","candidate_artifact_uri":"s3://continuo/candidate-sql/rel-7/candidate_api.rates.json"}
	]`
	var nodes []struct {
		UniqueID string `json:"unique_id"`
		NodeType string `json:"node_type"`
		URI      string `json:"candidate_artifact_uri"`
	}
	if err := json.Unmarshal([]byte(legacy), &nodes); err != nil {
		t.Fatal(err)
	}
	for _, n := range nodes {
		got := candidateArtifactURI("continuo", "rel-7", release.Node{UniqueID: n.UniqueID, NodeType: n.NodeType})
		if got != n.URI {
			t.Errorf("%s (%s): derived %q, the run stored %q", n.UniqueID, n.NodeType, got, n.URI)
		}
	}
}

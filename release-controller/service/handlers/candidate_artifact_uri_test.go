package handlers

import (
	"testing"

	"github.com/carolsimone/continuo/release-controller/domain/release"
)

// The URI a validation Job fetches is derived from the run id, the node and
// its type — the key topology-controller writes the object under
// (topologyartifact.CandidateObjectKey, pinned to topology-controller's rule
// by the shared candidate_object_keys fixture). A seed has no object.
func TestCandidateArtifactURI_FollowsTheNodeType(t *testing.T) {
	for _, tc := range []struct{ nodeType, want string }{
		{"dbt-model", "s3://continuo/candidate-sql/r1/candidate_svc.n.sql"},
		{"dbt-snapshot", "s3://continuo/candidate-sql/r1/candidate_svc.n.sql"},
		{"dbt-test", "s3://continuo/candidate-sql/r1/candidate_svc.n.sql"},
		{"python-node", "s3://continuo/candidate-sql/r1/candidate_svc.n.json"},
		{"python-csv", "s3://continuo/candidate-sql/r1/candidate_svc.n.json"},
		{"python-api", "s3://continuo/candidate-sql/r1/candidate_svc.n.json"},
		{"dbt-seed", ""},
	} {
		got := candidateArtifactURI("continuo", "r1", release.Node{UniqueID: "svc.n", NodeType: tc.nodeType})
		if got != tc.want {
			t.Errorf("%s: got %q, want %q", tc.nodeType, got, tc.want)
		}
	}
}

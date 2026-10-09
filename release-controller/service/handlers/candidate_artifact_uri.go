package handlers

import (
	"github.com/carolsimone/continuo/pkg/topologyartifact"
	"github.com/carolsimone/continuo/release-controller/domain/release"
)

// candidateArtifactURI is the S3 URI of the object a node's validation Job
// fetches to build it in the candidate schema. topology-controller writes that
// object under a key derived from the run id and the node, so the URI is
// derived the same way here rather than carried in the topology. A seed has no
// object and gets "".
func candidateArtifactURI(bucket, releaseID string, n release.Node) string {
	key := topologyartifact.CandidateObjectKey(releaseID, n.UniqueID, n.NodeType)
	if key == "" {
		return ""
	}
	return "s3://" + bucket + "/" + key
}

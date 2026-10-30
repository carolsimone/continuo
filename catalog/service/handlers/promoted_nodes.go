package handlers

import (
	"github.com/carolsimone/continuo/catalog/domain/topology"
	pkgModel "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/topologyartifact"
)

// promotedNodes maps the artifact's nodes onto the nodes the swap writes.
// dbt-test nodes are left out: a test is bind-checked during validation and
// never scheduled, so it has no place in the live graph.
func promotedNodes(doc topologyartifact.Document) []topology.ReleasePromotedTopologyNode {
	out := make([]topology.ReleasePromotedTopologyNode, 0, len(doc.Nodes))
	for _, n := range doc.Nodes {
		if n.NodeType == string(pkgModel.NodeTypeDbtTest) {
			continue
		}
		out = append(out, topology.ReleasePromotedTopologyNode{
			UniqueID:          n.UniqueID,
			SchemaName:        n.SchemaName,
			TableName:         n.TableName,
			ServiceName:       n.ServiceName,
			NodeType:          n.NodeType,
			ContentHash:       n.ContentHash,
			TestCount:         n.TestCount,
			ImageTag:          n.ImageTag,
			SecretRef:         n.SecretRef,
			Schedule:          n.Schedule,
			UpstreamUniqueIDs: append([]string(nil), n.UpstreamUniqueIDs...),
			OriginalFilePath:  n.OriginalFilePath,
		})
	}
	return out
}

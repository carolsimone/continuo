package topology

import (
	"time"

	pkgModel "github.com/carolsimone/continuo/pkg/domain/model"
)

// ReleasePromotedTopologyNode is the domain representation of a node in a
// release.promoted:v1 payload. Used by ReleasePromotionRepository.
// Nodes are keyed by unique_id; upstream relationships are expressed as a
// list of unique_id strings rather than (schema_name, table_name) tuples.
//
// Changed marks a node whose dbt content_hash differs from the prior prod.
// LastCommitSHA/LastRepo/LastChangedAt carry the release's provenance,
// denormalized onto each node so the repository stamps only changed nodes
// without a separate scalar parameter.
type ReleasePromotedTopologyNode struct {
	UniqueID          string
	SchemaName        string
	TableName         string
	ServiceName       string
	NodeType          string
	TestCount         int
	ImageTag          string
	Schedule          string
	UpstreamUniqueIDs []string
	Changed           bool
	LastCommitSHA     string
	LastRepo          string
	LastChangedAt     time.Time
	OriginalFilePath  string
	// DBTUniqueID is the node's dbt identity ("model.finance.orders"). It is
	// separate from UniqueID, which keys the graph as "schema.table".
	DBTUniqueID string
	// RuntimeManifestRef names the prebuilt artifact this node executes against.
	// Empty for a release promoted before runtime manifests existed.
	pkgModel.RuntimeManifestRef
}

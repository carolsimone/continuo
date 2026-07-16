package event

import (
	"time"

	pkgModel "github.com/carolsimone/continuo/pkg/domain/model"
)

// ReleasePromotedNode is the wire-format representation of a single node in a
// release.promoted:v1 payload's topology array. Nodes are keyed by unique_id
// and carry upstream relationships as a string-id list. `changed` marks nodes
// whose dbt content_hash differs from the prior prod, scoping provenance writes.
type ReleasePromotedNode struct {
	UniqueID          string   `json:"unique_id"`
	SchemaName        string   `json:"schema_name"`
	TableName         string   `json:"table_name"`
	ServiceName       string   `json:"service_name"`
	NodeType          string   `json:"node_type"`
	TestCount         int      `json:"test_count"`
	ImageTag          string   `json:"image_tag"`
	Schedule          string   `json:"schedule"`
	UpstreamUniqueIDs []string `json:"upstream_unique_ids"`
	Changed           bool     `json:"changed"`
	OriginalFilePath  string   `json:"original_file_path"`
	// DBTUniqueID is the node's dbt identity ("model.finance.orders"). It is
	// separate from UniqueID, which keys the graph as "schema.table".
	DBTUniqueID string `json:"dbt_unique_id,omitempty"`
	// RuntimeManifestRef names the prebuilt artifact this node executes against.
	// A release promoted before runtime manifests existed carries none, which is
	// valid: those nodes execute down the per-node Job path.
	pkgModel.RuntimeManifestRef
}

// ReleasePromoted is the full release.promoted:v1 payload as published by
// release-controller's outbox processor. repo/commit_sha/promoted_at carry the
// source change that this release promoted; they stamp the changed nodes.
type ReleasePromoted struct {
	ReleaseID  string                `json:"release_id"`
	Topology   []ReleasePromotedNode `json:"topology"`
	ImageTags  map[string]string     `json:"image_tags"`
	Repo       string                `json:"repo"`
	CommitSHA  string                `json:"commit_sha"`
	PromotedAt time.Time             `json:"promoted_at"`
}

package event

import "time"

// ReleasePromotedNode is the wire-format representation of a single node in a
// release.promoted:v1 payload's topology array. Nodes are keyed by unique_id
// and carry upstream relationships as a string-id list. `changed` marks nodes
// whose dbt content_hash differs from the prior prod, scoping provenance writes.
type ReleasePromotedNode struct {
	UniqueID    string
	SchemaName  string
	TableName   string
	ServiceName string
	NodeType    string
	// ContentHash is the node's fingerprint over its own source, the shared code
	// it reaches, and its resolved config. It is stored on :Table and is what a
	// release's code bundle is compared against when recording code history.
	ContentHash       string
	TestCount         int
	ImageTag          string
	Schedule          string
	UpstreamUniqueIDs []string
	Changed           bool
	OriginalFilePath  string
}

// ReleasePromoted is the full release.promoted:v1 payload as published by
// release-controller's outbox processor. repo/commit_sha/promoted_at carry the
// source change that this release promoted; they stamp the changed nodes.
type ReleasePromoted struct {
	ReleaseID  string
	Topology   []ReleasePromotedNode
	ImageTags  map[string]string
	Repo       string
	CommitSHA  string
	PromotedAt time.Time
	// CodeBundleURI points at the release's code-bundle contract document in
	// object storage (code-bundles/<release_id>/bundle.json). Empty for releases
	// promoted before topology-controller began writing bundles.
	CodeBundleURI string
	// Bootstrap marks a re-baseline release promoted without validation. Its
	// commit did not author most of the code it carries, so versions recorded
	// from it are stamped as healed rather than exact.
	Bootstrap bool
}

package model

import "time"

// PromoteReleaseInput is the handler input for a release.promoted:v2 message:
// the promotion's identity and seq, the location and checksum of its topology
// artifact, the nodes it changed, and the provenance the versions group stamps.
type PromoteReleaseInput struct {
	ReleaseID string
	// PromotionSeq orders promotions: release-controller allocates one per
	// announcement, strictly increasing, and the topology swap applies a
	// promotion only when its seq is greater than the live one.
	PromotionSeq   int64
	TopologyURI    string
	TopologySHA256 string
	// ChangedNodeIDs are the non-test nodes whose content_hash differs from the
	// previous production topology (every node when there was none). Empty for
	// an announcement, which builds nothing.
	ChangedNodeIDs []string
	Repo           string
	CommitSHA      string
	PromotedAt     time.Time
	// CodeBundleURI locates the release's code-bundle document in object
	// storage; Bootstrap marks a re-baseline release, whose versions are
	// stamped healed. Both are consumed by the version-ingestion path and
	// ignored by the topology swap.
	CodeBundleURI string
	Bootstrap     bool
}

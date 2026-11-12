package events

// ReleasePromoted is the typed form of release.promoted:v2 as run-controller uses it:
// the tenant from the event envelope, the promoted release and its promotion
// seq, the URI and SHA-256 of its topology artifact, the non-test nodes whose
// content changed against the previous prod topology, whether the release is
// the install's first, and whether it rebaselines the install: every live seed
// is recorded as loaded at the release's content and column fingerprint
// instead of being built.
type ReleasePromoted struct {
	TenantID       string
	ReleaseID      string
	PromotionSeq   int64
	TopologyURI    string
	TopologySHA256 string
	ChangedNodeIDs []string
	Bootstrap      bool
	Rebaseline     bool
}

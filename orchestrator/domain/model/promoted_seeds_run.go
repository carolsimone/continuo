package model

// PromotedSeedsNode identifies one node of a promoted-seeds run.
type PromotedSeedsNode struct {
	ServiceName string
	SchemaName  string
	TableName   string
	// NodeType and ImageTag are the values the triggering release pinned to this
	// node. They are used in place of whatever the topology currently holds, so a
	// promotion overtaken by a later one still builds its seeds with its own
	// image rather than the newer release's.
	NodeType string
	ImageTag string
}

// PromotedSeedsRunInput is the parsed trigger.promoted_seeds:v1 message: the run
// state already created to load seeds of the live release, and those seeds.
//
// Membership travels on the event rather than being re-derived from the topology
// here, so the set of seeds a run builds is decided once — by state, which
// planned it — and cannot drift between the service that created the run and
// the one that projects its tasks. PromotionSeq is the live promotion seq state
// planned the run against; the run is projected only once this service's graph
// has reached it. 0 means the trigger carries none and the run is projected at
// once.
type PromotedSeedsRunInput struct {
	RunID        string // == ScheduleID.String()
	ScheduleName string
	ReleaseID    string
	Nodes        []PromotedSeedsNode
	InitiatedBy  string
	PromotionSeq int64
}

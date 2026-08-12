package model

// PromotedSeedsNode identifies one node of a promoted-seeds run.
type PromotedSeedsNode struct {
	ServiceName string `json:"service_name"`
	SchemaName  string `json:"schema_name"`
	TableName   string `json:"table_name"`
	// NodeType and ImageTag are the values the triggering release pinned to this
	// node. They are used in place of whatever the topology currently holds, so a
	// promotion overtaken by a later one still builds its seeds with its own
	// image rather than the newer release's.
	NodeType string `json:"node_type"`
	ImageTag string `json:"image_tag"`
}

// PromotedSeedsRunInput is the parsed trigger.promoted_seeds:v1 message: the run
// state already created for a promoted release, and the seeds that release
// changed.
//
// Membership travels on the event rather than being re-derived from the topology
// here, so the set of seeds a run builds is decided once — by whoever read the
// promoted release — and cannot drift between the service that created the run
// and the one that projects its tasks.
type PromotedSeedsRunInput struct {
	RunID        string // == ScheduleID.String()
	ScheduleName string
	ReleaseID    string
	Nodes        []PromotedSeedsNode
	InitiatedBy  string
}

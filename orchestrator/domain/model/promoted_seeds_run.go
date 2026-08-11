package model

// PromotedSeedsNode identifies one node of a promoted-seeds run.
type PromotedSeedsNode struct {
	ServiceName string `json:"service_name"`
	SchemaName  string `json:"schema_name"`
	TableName   string `json:"table_name"`
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

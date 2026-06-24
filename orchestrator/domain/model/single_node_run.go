package model

// SingleNodeRunInput is the handler input the orchestrator builds in
// response to a trigger.single_node_run:v1 message — everything the
// orchestrator needs to snapshot a one-task run for the identified node.
// The consumer hand-parses the message fields; there is no wire-format
// payload struct.
//
// RunID equals the synthesised scheduler_tracker.schedule_id (string
// UUID); ScheduleName is the "single-node-run-<short-uuid>" identifier
// minted by state when it accepted the gRPC TriggerSingleNodeRun call.
type SingleNodeRunInput struct {
	RunID          string // == ScheduleID.String()
	ScheduleName   string
	ServiceName    string
	SchemaName     string
	TableName      string
	MetadataSource string // "latest" | "snapshot_of_run"
	SourceRunID    string // empty in latest mode
	InitiatedBy    string // user who triggered the run, or "system"
}

package domain

import "github.com/google/uuid"

// SchedulerStarted carries the data from a scheduler.started:v1 stream message.
// This is an event (a fact: state has activated a schedule) — not a command.
type SchedulerStarted struct {
	ScheduleID   uuid.UUID
	ScheduleName string
	Kind         string     // "cron" | "trigger" | "rerun" | "rebase" | "single_node_run"; defaults to "cron" if missing on incoming message
	SourceRunID  *uuid.UUID // populated for rerun, rebase, stale-mode single_node_run; nil otherwise
}

// ScheduleCancelled is the typed form of schedule.cancelled:v1.
type ScheduleCancelled struct {
	ScheduleID uuid.UUID
}

// RunFinalized is the typed form of run.finalized:v1.
// ScheduleID and Status stay as strings — the Neo4j FinalizeRun signature
// already takes strings, so converting to uuid.UUID + model enum would be
// pointless round-tripping.
type RunFinalized struct {
	ScheduleID string
	Status     string
}

type TopologyIngested struct {
	ScheduleNames   []string
	ServiceMetadata map[string]map[string]string
}

type RunInitialized struct {
	RunID     string
	AllNodes  []*TableNode
	RootNodes []*TableNode
	SeedNodes []*TableNode
}

type RerunReady struct {
	RunID       string
	TargetNodes []*TableNode
}

type NodeCompleted struct {
	RunID           string
	ScheduleName    string
	SchemaName      string
	TableName       string
	Status          NodeStatus
	ReadyDownstream []*DownstreamInfo
}

type DownstreamInfo struct {
	ServiceName string
	SchemaName  string
	TableName   string
	NodeType    string
}

type RunCompleted struct {
	RunID          string
	ScheduleName   string
	TerminalStatus string
}

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

package command

import "github.com/google/uuid"

// Command is a marker interface for all commands
type Command interface {
	isCommand()
}

// CheckJobStatus command to check K8s job and handle result
type CheckJobStatus struct {
	OutboxEntryID *uuid.UUID // nil = no dedup (backwards-compat during rollout)
	TaskID        uuid.UUID
	ScheduleID    uuid.UUID
	ScheduleName  string
	ServiceName   string
	SchemaName    string
	TableName     string
	JobName       string
	NodeType      string
}

func (CheckJobStatus) isCommand() {}

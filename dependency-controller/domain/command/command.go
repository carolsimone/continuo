package command

import "github.com/google/uuid"

// Command is a marker interface for all commands
type Command interface {
	isCommand()
}

// ProcessNodeStatus represents a command to process a node status update
type ProcessNodeStatus struct {
	TaskID       uuid.UUID
	ScheduleID   uuid.UUID
	ScheduleName string
	ServiceName  string
	Schema       string
	TableName    string
	Status       string // SUCCEEDED or FAILED
}

func (ProcessNodeStatus) isCommand() {}

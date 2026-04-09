package command

import "github.com/google/uuid"

// RerunNode instructs startup-controller to reset the given node and its
// transitive FAILED downstream in graph and state, then dispatch the target.
type RerunNode struct {
	ScheduleID   uuid.UUID
	ScheduleName string
	Schema       string
	TableName    string
	ServiceName  string
}

func (RerunNode) isCommand() {}

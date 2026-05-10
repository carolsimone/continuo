package model

import "github.com/google/uuid"

// NodeCompletedInput carries the handler-input data derived from a
// node.updated:v1 stream message. The consumer hand-builds it from
// scalar Redis fields; it is not itself a wire-format payload.
type NodeCompletedInput struct {
	TaskID       uuid.UUID
	ScheduleID   uuid.UUID
	ScheduleName string
	ServiceName  string
	SchemaName   string
	TableName    string
	Status       string
}

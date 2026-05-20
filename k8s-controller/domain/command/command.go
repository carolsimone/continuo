package command

import "github.com/google/uuid"

// Command is a marker interface for all commands
type Command interface {
	isCommand()
}

// CheckJobStatus command to check K8s job and handle result
type CheckJobStatus struct {
	// MessageID and StreamName identify the inbound Redis message for dedup via pkg/messageprocessing.
	MessageID  string
	StreamName string
	// Payload is the JSON-serialized Redis message fields map, used as the dedup payload.
	Payload []byte

	TaskID       uuid.UUID
	ScheduleID   uuid.UUID
	ScheduleName string
	ServiceName  string
	SchemaName   string
	TableName    string
	JobName      string
	NodeType     string
	ImageTag     string
	RetryCount   int32 // current task retry count (carried from node.deployed:v1 / check.k8s:v1)
	MaxRetries   int32 // maximum task retries allowed (default from config if absent in message)
}

func (CheckJobStatus) isCommand() {}

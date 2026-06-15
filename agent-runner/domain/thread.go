package domain

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// Role identifies who/what produced a message in a thread.
type Role string

const (
	RoleUser       Role = "user"
	RoleAssistant  Role = "assistant"
	RoleToolCall   Role = "tool_call"
	RoleToolResult Role = "tool_result"
)

// Thread is one conversation, owned by a user.
type Thread struct {
	ID        uuid.UUID
	UserID    string
	CreatedAt time.Time
	UpdatedAt time.Time
}

// Message is one entry in a thread; Content is a role-specific JSON payload.
type Message struct {
	ID        uuid.UUID
	ThreadID  uuid.UUID
	Seq       int
	Role      Role
	Content   json.RawMessage
	CreatedAt time.Time
}

// TextContent is the payload for user and assistant messages.
type TextContent struct {
	Text string `json:"text"`
}

// ToolCallContent is the payload for a tool invocation initiated by the assistant.
type ToolCallContent struct {
	CallID string            `json:"call_id"`
	Tool   string            `json:"tool"`
	Args   map[string]string `json:"args"`
}

// ToolResultContent is the payload returned by a tool after execution.
type ToolResultContent struct {
	CallID  string `json:"call_id"`
	Output  string `json:"output"`
	IsError bool   `json:"is_error"`
}

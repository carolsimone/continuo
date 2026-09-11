package domain

import (
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

// Message is one entry in a thread. Content is the role-specific payload: one
// of TextContent (RoleUser / RoleAssistant), ToolCallContent (RoleToolCall), or
// ToolResultContent (RoleToolResult).
type Message struct {
	ID        uuid.UUID
	ThreadID  uuid.UUID
	Seq       int
	Role      Role
	Content   Content
	CreatedAt time.Time
}

// Content is the typed payload of a Message. Exactly one of the value objects
// below satisfies it; the message's Role selects which. The interface is sealed
// (its marker method is unexported), so no type outside this package can be a
// Content — keeping message payloads a closed, exhaustively switchable set.
type Content interface {
	isContent()
}

// TextContent is the payload for user and assistant messages.
type TextContent struct {
	Text string
}

// ToolCallContent is the payload for a tool invocation initiated by the assistant.
type ToolCallContent struct {
	CallID string
	Tool   string
	Args   map[string]string
}

// ToolResultContent is the payload returned by a tool after execution.
type ToolResultContent struct {
	CallID  string
	Output  string
	IsError bool
}

func (TextContent) isContent()       {}
func (ToolCallContent) isContent()   {}
func (ToolResultContent) isContent() {}

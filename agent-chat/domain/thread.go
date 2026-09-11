package domain

import (
	"fmt"
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

// ValidContentForRole reports whether content is the variant a message with the
// given role must carry: TextContent for user and assistant, ToolCallContent for
// tool_call, ToolResultContent for tool_result. It exists because Role and
// Content carry overlapping information — persistence writes Content and reads it
// back keyed on Role — so a message whose pair disagrees would round-trip as the
// wrong (empty) variant. NewMessage enforces this; the persistence layer relies
// on it holding.
func ValidContentForRole(role Role, content Content) bool {
	switch role {
	case RoleUser, RoleAssistant:
		_, ok := content.(TextContent)
		return ok
	case RoleToolCall:
		_, ok := content.(ToolCallContent)
		return ok
	case RoleToolResult:
		_, ok := content.(ToolResultContent)
		return ok
	default:
		return false
	}
}

// NewMessage assembles a Message, rejecting a role/content pairing that would
// persist as one variant and decode back as another. It is the constructor the
// write path uses so an illegal pairing cannot reach storage.
func NewMessage(id, threadID uuid.UUID, seq int, role Role, content Content, createdAt time.Time) (*Message, error) {
	if !ValidContentForRole(role, content) {
		return nil, fmt.Errorf("domain: %T is not valid content for role %q", content, role)
	}
	return &Message{
		ID:        id,
		ThreadID:  threadID,
		Seq:       seq,
		Role:      role,
		Content:   content,
		CreatedAt: createdAt,
	}, nil
}

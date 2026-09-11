package domain

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestValidContentForRole(t *testing.T) {
	text := TextContent{Text: "hi"}
	call := ToolCallContent{CallID: "c1", Tool: "t"}
	result := ToolResultContent{CallID: "c1", Output: "ok"}

	cases := []struct {
		name    string
		role    Role
		content Content
		want    bool
	}{
		{"user+text", RoleUser, text, true},
		{"assistant+text", RoleAssistant, text, true},
		{"tool_call+call", RoleToolCall, call, true},
		{"tool_result+result", RoleToolResult, result, true},
		{"user+call", RoleUser, call, false},
		{"assistant+result", RoleAssistant, result, false},
		{"tool_call+text", RoleToolCall, text, false},
		{"tool_result+call", RoleToolResult, call, false},
		{"unknown role", Role("bogus"), text, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := ValidContentForRole(c.role, c.content); got != c.want {
				t.Fatalf("ValidContentForRole(%q, %T) = %v, want %v", c.role, c.content, got, c.want)
			}
		})
	}
}

func TestNewMessage_ValidPairing(t *testing.T) {
	id, tid := uuid.New(), uuid.New()
	now := time.Now().UTC()
	m, err := NewMessage(id, tid, 3, RoleToolCall, ToolCallContent{CallID: "c1", Tool: "t"}, now)
	if err != nil {
		t.Fatalf("NewMessage: %v", err)
	}
	if m.ID != id || m.ThreadID != tid || m.Seq != 3 || m.Role != RoleToolCall || m.CreatedAt != now {
		t.Fatalf("NewMessage set unexpected fields: %+v", m)
	}
	if _, ok := m.Content.(ToolCallContent); !ok {
		t.Fatalf("content is %T, want ToolCallContent", m.Content)
	}
}

func TestNewMessage_RejectsMismatch(t *testing.T) {
	if _, err := NewMessage(uuid.New(), uuid.New(), 1, RoleUser, ToolCallContent{CallID: "c1"}, time.Now()); err == nil {
		t.Fatal("expected error pairing RoleUser with ToolCallContent")
	}
}

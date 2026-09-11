package chat

import (
	"strings"
	"testing"

	"github.com/carolsimone/continuo/agent-chat/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func msg(role domain.Role, content domain.Content) domain.Message {
	return domain.Message{Role: role, Content: content}
}

func TestWindow_KeepsEverythingWhenUnderBudget(t *testing.T) {
	msgs := []domain.Message{
		msg(domain.RoleUser, domain.TextContent{Text: "a"}),
		msg(domain.RoleAssistant, domain.TextContent{Text: "b"}),
	}
	assert.Len(t, window(msgs, 1000), 2)
}

func TestWindow_DropsOldestFirstAndNeverStartsOnToolResult(t *testing.T) {
	big := strings.Repeat("x", 400)
	msgs := []domain.Message{
		msg(domain.RoleUser, domain.TextContent{Text: big}),
		msg(domain.RoleToolCall, domain.ToolCallContent{CallID: "1", Tool: "t", Args: map[string]string{}}),
		msg(domain.RoleToolResult, domain.ToolResultContent{CallID: "1", Output: "r"}),
		msg(domain.RoleUser, domain.TextContent{Text: "recent"}),
		msg(domain.RoleAssistant, domain.TextContent{Text: "answer"}),
	}
	got := window(msgs, 30)
	assert.Len(t, got, 2)
	assert.Equal(t, domain.RoleUser, got[0].Role)
}

func TestWindow_TightBudgetWithTrailingToolResult_StartsOnUser(t *testing.T) {
	// Budget only fits the trailing tool_result and tool_call, but not the
	// user message before them. The window must still begin on a RoleUser
	// and must be non-empty.
	msgs := []domain.Message{
		msg(domain.RoleUser, domain.TextContent{Text: "hello"}),
		msg(domain.RoleToolCall, domain.ToolCallContent{CallID: "1", Tool: "t", Args: map[string]string{}}),
		msg(domain.RoleToolResult, domain.ToolResultContent{CallID: "1", Output: "r"}),
	}
	// A very tight budget forces start past the user message into
	// tool_call/tool_result; the window must fall back to the last user message.
	got := window(msgs, 5)
	require.NotEmpty(t, got, "window must never be empty when msgs is non-empty")
	assert.Equal(t, domain.RoleUser, got[0].Role, "window must begin on a user message")
}

func TestWindow_BudgetOfOneWithSingleLargeUserMessage_ReturnsUserMessage(t *testing.T) {
	// A budget of 1 token cannot technically fit the message, but the window
	// must still return the sole user message (non-empty, starts on RoleUser).
	msgs := []domain.Message{
		msg(domain.RoleUser, domain.TextContent{Text: "a very long user message that exceeds one token"}),
	}
	got := window(msgs, 1)
	require.NotEmpty(t, got, "window must never be empty when msgs is non-empty")
	assert.Equal(t, domain.RoleUser, got[0].Role, "window must begin on the user message")
}

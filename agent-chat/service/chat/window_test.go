package chat

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/carolsimone/continuo/agent-chat/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func msg(role domain.Role, payload string) domain.Message {
	return domain.Message{Role: role, Content: json.RawMessage(payload)}
}

func TestWindow_KeepsEverythingWhenUnderBudget(t *testing.T) {
	msgs := []domain.Message{
		msg(domain.RoleUser, `{"text":"a"}`),
		msg(domain.RoleAssistant, `{"text":"b"}`),
	}
	assert.Len(t, window(msgs, 1000), 2)
}

func TestWindow_DropsOldestFirstAndNeverStartsOnToolResult(t *testing.T) {
	big := `{"text":"` + strings.Repeat("x", 400) + `"}`
	msgs := []domain.Message{
		msg(domain.RoleUser, big),
		msg(domain.RoleToolCall, `{"call_id":"1","tool":"t","args":{}}`),
		msg(domain.RoleToolResult, `{"call_id":"1","output":"r"}`),
		msg(domain.RoleUser, `{"text":"recent"}`),
		msg(domain.RoleAssistant, `{"text":"answer"}`),
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
		msg(domain.RoleUser, `{"text":"hello"}`),
		msg(domain.RoleToolCall, `{"call_id":"1","tool":"t","args":{}}`),
		msg(domain.RoleToolResult, `{"call_id":"1","output":"r"}`),
	}
	// budget of 10 tokens fits only the two small trailing messages; the user
	// message above is ~5 bytes/4+4 ≈ 5 tokens, tool_call ~9 tokens, tool_result ~8.
	// A very tight budget of 5 forces start past the user message into tool_call/result;
	// the window must fall back to the last user message.
	got := window(msgs, 5)
	require.NotEmpty(t, got, "window must never be empty when msgs is non-empty")
	assert.Equal(t, domain.RoleUser, got[0].Role, "window must begin on a user message")
}

func TestWindow_BudgetOfOneWithSingleLargeUserMessage_ReturnsUserMessage(t *testing.T) {
	// A budget of 1 token cannot technically fit the message, but the window
	// must still return the sole user message (non-empty, starts on RoleUser).
	msgs := []domain.Message{
		msg(domain.RoleUser, `{"text":"a very long user message that exceeds one token"}`),
	}
	got := window(msgs, 1)
	require.NotEmpty(t, got, "window must never be empty when msgs is non-empty")
	assert.Equal(t, domain.RoleUser, got[0].Role, "window must begin on the user message")
}

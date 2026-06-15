package chat

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/carolsimone/continuo/agent-runner/domain"
	"github.com/stretchr/testify/assert"
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

package anthropic

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/carolsimone/continuo/agent-chat/domain"
)

// mustMessage builds a domain.Message with a JSON-encoded role payload.
func mustMessage(t *testing.T, role domain.Role, payload any) domain.Message {
	t.Helper()
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	return domain.Message{Role: role, Content: raw}
}

// A no-argument tool call (e.g. "continuo schedule list") must still serialize
// the Anthropic-required tool_use.input field as an empty object. When it is
// omitted, the API rejects the follow-up request with
// "messages.N.content.M.tool_use.input: Field required".
func TestToWireMessages_EmptyToolArgsEmitsInputObject(t *testing.T) {
	wire, err := toWireMessages([]domain.Message{
		mustMessage(t, domain.RoleToolCall, domain.ToolCallContent{
			CallID: "call_1",
			Tool:   "schedule_list",
			Args:   map[string]string{}, // no arguments
		}),
	})
	if err != nil {
		t.Fatalf("toWireMessages: %v", err)
	}

	got := mustMarshal(t, wire)
	if !strings.Contains(got, `"input":{}`) {
		t.Errorf("expected tool_use block to carry an empty input object `\"input\":{}`, got: %s", got)
	}
}

// A tool call with arguments serializes them inside input.
func TestToWireMessages_ToolArgsPreserved(t *testing.T) {
	wire, err := toWireMessages([]domain.Message{
		mustMessage(t, domain.RoleToolCall, domain.ToolCallContent{
			CallID: "call_2",
			Tool:   "schedule_status",
			Args:   map[string]string{"name": "daily"},
		}),
	})
	if err != nil {
		t.Fatalf("toWireMessages: %v", err)
	}

	got := mustMarshal(t, wire)
	if !strings.Contains(got, `"input":{"name":"daily"}`) {
		t.Errorf("expected tool args inside input, got: %s", got)
	}
}

// A text block must never carry an input field.
func TestToWireMessages_TextBlockHasNoInput(t *testing.T) {
	wire, err := toWireMessages([]domain.Message{
		mustMessage(t, domain.RoleAssistant, domain.TextContent{Text: "hello"}),
	})
	if err != nil {
		t.Fatalf("toWireMessages: %v", err)
	}

	if got := mustMarshal(t, wire); strings.Contains(got, `"input"`) {
		t.Errorf("text block must not carry an input field, got: %s", got)
	}
}

// A tool_result block must never carry an input field.
func TestToWireMessages_ToolResultHasNoInput(t *testing.T) {
	wire, err := toWireMessages([]domain.Message{
		mustMessage(t, domain.RoleToolResult, domain.ToolResultContent{
			CallID: "call_1",
			Output: "ok",
		}),
	})
	if err != nil {
		t.Fatalf("toWireMessages: %v", err)
	}

	if got := mustMarshal(t, wire); strings.Contains(got, `"input"`) {
		t.Errorf("tool_result block must not carry an input field, got: %s", got)
	}
}

func mustMarshal(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal wire messages: %v", err)
	}
	return string(b)
}

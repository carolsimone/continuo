package anthropic

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/carolsimone/continuo/agent-chat/domain"
	"github.com/carolsimone/continuo/agent-chat/serialization"
	"github.com/carolsimone/continuo/agent-chat/service/ports"
)

// TestStreamTurn_EmptyToolArgsRoundTrip verifies that the Anthropic API accepts
// a follow-up turn whose history contains a no-argument tool_use block (e.g.
// "continuo schedule list") and its matching tool_result. It hits the real API
// and is skipped unless ANTHROPIC_API_KEY is set.
func TestStreamTurn_EmptyToolArgsRoundTrip(t *testing.T) {
	key := os.Getenv("ANTHROPIC_API_KEY")
	if key == "" {
		t.Skip("ANTHROPIC_API_KEY not set; skipping real Anthropic API round-trip")
	}
	model := os.Getenv("ANTHROPIC_MODEL")
	if model == "" {
		model = "claude-sonnet-4-6"
	}

	mustRaw := func(v any) json.RawMessage {
		b, err := json.Marshal(v)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		return b
	}

	// History ends in a no-argument tool_use block and its matching result.
	req := ports.TurnRequest{
		System:    "You are a schedule assistant. Answer briefly.",
		MaxTokens: 64,
		Tools: []ports.ToolDef{{
			Name:        "schedule_list",
			Description: "List all schedules in the system. Takes no arguments.",
		}},
		Messages: []domain.Message{
			{Role: domain.RoleUser, Content: mustRaw(serialization.TextContentDTO{Text: "List the schedules."})},
			{Role: domain.RoleToolCall, Content: mustRaw(serialization.ToolCallContentDTO{
				CallID: "toolu_test_1",
				Tool:   "schedule_list",
				Args:   map[string]string{}, // no arguments — the regression case
			})},
			{Role: domain.RoleToolResult, Content: mustRaw(serialization.ToolResultContentDTO{
				CallID: "toolu_test_1",
				Output: "No schedules found.",
			})},
		},
	}

	p := NewProvider("https://api.anthropic.com", key, model, &http.Client{Timeout: 30 * time.Second})

	res, err := p.StreamTurn(context.Background(), req, func(string) {})
	if err != nil {
		t.Fatalf("StreamTurn returned error (the empty-input bug would surface here as a 400): %v", err)
	}
	if res.Text == "" && len(res.ToolCalls) == 0 {
		t.Fatalf("expected a non-empty response from the model, got empty TurnResult")
	}
	t.Logf("round-trip OK; assistant said: %q (tool calls: %d)", res.Text, len(res.ToolCalls))
}

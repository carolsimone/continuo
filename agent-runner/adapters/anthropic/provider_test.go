package anthropic

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/carolsimone/continuo/agent-runner/domain"
	"github.com/carolsimone/continuo/agent-runner/service/ports"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func sse(event, data string) string {
	return "event: " + event + "\ndata: " + data + "\n\n"
}

func TestStreamTurn_TextAndToolCall(t *testing.T) {
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "k", r.Header.Get("x-api-key"))
		assert.Equal(t, "2023-06-01", r.Header.Get("anthropic-version"))
		body, _ := io.ReadAll(r.Body)
		require.NoError(t, json.Unmarshal(body, &gotBody))
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, sse("message_start", `{"type":"message_start","message":{"usage":{"input_tokens":42,"output_tokens":1}}}`))
		io.WriteString(w, sse("content_block_start", `{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`))
		io.WriteString(w, sse("content_block_delta", `{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Check"}}`))
		io.WriteString(w, sse("content_block_delta", `{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"ing."}}`))
		io.WriteString(w, sse("content_block_stop", `{"type":"content_block_stop","index":0}`))
		io.WriteString(w, sse("content_block_start", `{"type":"content_block_start","index":1,"content_block":{"type":"tool_use","id":"toolu_1","name":"schedule_status","input":{}}}`))
		io.WriteString(w, sse("content_block_delta", `{"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"{\"schedule-na"}}`))
		io.WriteString(w, sse("content_block_delta", `{"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"me\":\"daily\"}"}}`))
		io.WriteString(w, sse("content_block_stop", `{"type":"content_block_stop","index":1}`))
		io.WriteString(w, sse("message_delta", `{"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":17}}`))
		io.WriteString(w, sse("message_stop", `{"type":"message_stop"}`))
	}))
	defer srv.Close()

	p := NewProvider(srv.URL, "k", "claude-sonnet-4-6", srv.Client())
	var deltas []string
	res, err := p.StreamTurn(context.Background(), ports.TurnRequest{
		System: "sys",
		Messages: []domain.Message{
			{Role: domain.RoleUser, Content: json.RawMessage(`{"text":"how is daily?"}`)},
		},
		Tools: []ports.ToolDef{{
			Name: "schedule_status", Description: "d",
			Params: []ports.ToolParam{{Name: "schedule-name", Required: true}},
		}},
		MaxTokens: 1024,
	}, func(s string) { deltas = append(deltas, s) })
	require.NoError(t, err)

	assert.Equal(t, []string{"Check", "ing."}, deltas)
	assert.Equal(t, "Checking.", res.Text)
	require.Len(t, res.ToolCalls, 1)
	assert.Equal(t, "toolu_1", res.ToolCalls[0].ID)
	assert.Equal(t, "schedule_status", res.ToolCalls[0].Name)
	assert.Equal(t, map[string]string{"schedule-name": "daily"}, res.ToolCalls[0].Args)
	assert.Equal(t, 42, res.Usage.InputTokens)
	assert.Equal(t, 17, res.Usage.OutputTokens)

	assert.Equal(t, "sys", gotBody["system"])
	assert.Equal(t, true, gotBody["stream"])
	tools := gotBody["tools"].([]any)
	tool := tools[0].(map[string]any)
	schema := tool["input_schema"].(map[string]any)
	assert.Equal(t, "object", schema["type"])
}

func TestStreamTurn_MapsToolHistoryToAnthropicBlocks(t *testing.T) {
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		require.NoError(t, json.Unmarshal(body, &gotBody))
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, sse("message_delta", `{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":1}}`))
		io.WriteString(w, sse("message_stop", `{"type":"message_stop"}`))
	}))
	defer srv.Close()

	p := NewProvider(srv.URL, "k", "m", srv.Client())
	_, err := p.StreamTurn(context.Background(), ports.TurnRequest{
		Messages: []domain.Message{
			{Role: domain.RoleUser, Content: json.RawMessage(`{"text":"q"}`)},
			{Role: domain.RoleToolCall, Content: json.RawMessage(`{"call_id":"toolu_1","tool":"schedule_status","args":{"schedule-name":"daily"}}`)},
			{Role: domain.RoleToolResult, Content: json.RawMessage(`{"call_id":"toolu_1","output":"{\"ok\":1}","is_error":false}`)},
		},
		MaxTokens: 10,
	}, func(string) {})
	require.NoError(t, err)

	msgs := gotBody["messages"].([]any)
	require.Len(t, msgs, 3)
	m1 := msgs[1].(map[string]any)
	assert.Equal(t, "assistant", m1["role"])
	b1 := m1["content"].([]any)[0].(map[string]any)
	assert.Equal(t, "tool_use", b1["type"])
	assert.Equal(t, "toolu_1", b1["id"])
	m2 := msgs[2].(map[string]any)
	assert.Equal(t, "user", m2["role"])
	b2 := m2["content"].([]any)[0].(map[string]any)
	assert.Equal(t, "tool_result", b2["type"])
	assert.Equal(t, "toolu_1", b2["tool_use_id"])
}

func TestStreamTurn_HTTPErrorIsReturned(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(429)
		io.WriteString(w, `{"error":{"type":"rate_limit_error","message":"slow down"}}`)
	}))
	defer srv.Close()
	p := NewProvider(srv.URL, "k", "m", srv.Client())
	_, err := p.StreamTurn(context.Background(), ports.TurnRequest{MaxTokens: 10}, func(string) {})
	require.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "429"))
}

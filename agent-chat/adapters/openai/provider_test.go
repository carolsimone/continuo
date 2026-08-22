package openai

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/carolsimone/continuo/agent-chat/domain"
	"github.com/carolsimone/continuo/agent-chat/service/ports"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func chunk(data string) string { return "data: " + data + "\n\n" }

func TestStreamTurn_TextAndToolCall(t *testing.T) {
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "Bearer k", r.Header.Get("Authorization"))
		assert.Equal(t, "/v1/chat/completions", r.URL.Path)
		body, _ := io.ReadAll(r.Body)
		require.NoError(t, json.Unmarshal(body, &gotBody))
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, chunk(`{"choices":[{"delta":{"content":"Check"}}]}`))
		io.WriteString(w, chunk(`{"choices":[{"delta":{"content":"ing."}}]}`))
		io.WriteString(w, chunk(`{"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_1","function":{"name":"schedule_status","arguments":"{\"schedule-na"}}]}}]}`))
		io.WriteString(w, chunk(`{"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"me\":\"daily\"}"}}]}}]}`))
		io.WriteString(w, chunk(`{"choices":[{"delta":{},"finish_reason":"tool_calls"}],"usage":{"prompt_tokens":42,"completion_tokens":17}}`))
		io.WriteString(w, "data: [DONE]\n\n")
	}))
	defer srv.Close()

	p := NewProvider(srv.URL, "k", "gpt-test", srv.Client())
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
	assert.Equal(t, "call_1", res.ToolCalls[0].ID)
	assert.Equal(t, map[string]string{"schedule-name": "daily"}, res.ToolCalls[0].Args)
	assert.Equal(t, 42, res.Usage.InputTokens)
	assert.Equal(t, 17, res.Usage.OutputTokens)

	msgs := gotBody["messages"].([]any)
	first := msgs[0].(map[string]any)
	assert.Equal(t, "system", first["role"])
	assert.Equal(t, true, gotBody["stream"])
	tools := gotBody["tools"].([]any)
	fn := tools[0].(map[string]any)["function"].(map[string]any)
	assert.Equal(t, "schedule_status", fn["name"])
}

func TestStreamTurn_MapsToolHistory(t *testing.T) {
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		require.NoError(t, json.Unmarshal(body, &gotBody))
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, chunk(`{"choices":[{"delta":{"content":"ok"},"finish_reason":null}]}`))
		io.WriteString(w, "data: [DONE]\n\n")
	}))
	defer srv.Close()

	p := NewProvider(srv.URL, "k", "m", srv.Client())
	_, err := p.StreamTurn(context.Background(), ports.TurnRequest{
		Messages: []domain.Message{
			{Role: domain.RoleUser, Content: json.RawMessage(`{"text":"q"}`)},
			{Role: domain.RoleToolCall, Content: json.RawMessage(`{"call_id":"call_1","tool":"schedule_status","args":{"schedule-name":"daily"}}`)},
			{Role: domain.RoleToolResult, Content: json.RawMessage(`{"call_id":"call_1","output":"{\"ok\":1}","is_error":false}`)},
		},
		MaxTokens: 10,
	}, func(string) {})
	require.NoError(t, err)

	msgs := gotBody["messages"].([]any)
	require.Len(t, msgs, 3) // user, assistant(tool_calls), tool
	m1 := msgs[1].(map[string]any)
	assert.Equal(t, "assistant", m1["role"])
	tc := m1["tool_calls"].([]any)[0].(map[string]any)
	assert.Equal(t, "call_1", tc["id"])
	m2 := msgs[2].(map[string]any)
	assert.Equal(t, "tool", m2["role"])
	assert.Equal(t, "call_1", m2["tool_call_id"])
}

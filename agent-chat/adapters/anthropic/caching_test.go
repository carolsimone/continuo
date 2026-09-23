package anthropic

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

// cacheServer answers any request with a minimal text response whose
// message_start carries cache usage, and captures the request body.
func cacheServer(t *testing.T, gotBody *map[string]any) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		require.NoError(t, json.Unmarshal(body, gotBody))
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, sse("message_start", `{"type":"message_start","message":{"usage":{"input_tokens":40,"output_tokens":1,"cache_read_input_tokens":9000,"cache_creation_input_tokens":300}}}`))
		io.WriteString(w, sse("content_block_start", `{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`))
		io.WriteString(w, sse("content_block_delta", `{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"ok"}}`))
		io.WriteString(w, sse("content_block_stop", `{"type":"content_block_stop","index":0}`))
		io.WriteString(w, sse("message_delta", `{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":5}}`))
		io.WriteString(w, sse("message_stop", `{"type":"message_stop"}`))
	}))
}

func ephemeral(t *testing.T, block map[string]any) bool {
	t.Helper()
	cc, ok := block["cache_control"].(map[string]any)
	if !ok {
		return false
	}
	return cc["type"] == "ephemeral"
}

// Tools render before system, so one breakpoint on the single system block
// caches the whole static prefix (tool catalog + system prompt).
func TestStreamTurn_SystemIsOneBlockWithCacheBreakpoint(t *testing.T) {
	var gotBody map[string]any
	srv := cacheServer(t, &gotBody)
	defer srv.Close()

	p := NewProvider(srv.URL, "k", "claude-haiku-4-5", srv.Client())
	_, err := p.StreamTurn(context.Background(), ports.TurnRequest{
		System:    "sys",
		Messages:  []domain.Message{{Role: domain.RoleUser, Content: domain.TextContent{Text: "hi"}}},
		MaxTokens: 64,
	}, func(string) {})
	require.NoError(t, err)

	system, ok := gotBody["system"].([]any)
	require.True(t, ok, "system must be a block array, got %T", gotBody["system"])
	require.Len(t, system, 1)
	block := system[0].(map[string]any)
	assert.Equal(t, "text", block["type"])
	assert.Equal(t, "sys", block["text"])
	assert.True(t, ephemeral(t, block))
	_, hasToolMarker := gotBody["cache_control"]
	assert.False(t, hasToolMarker, "no top-level automatic marker; breakpoints are explicit")
}

// The second breakpoint sits on the last content block of the last message
// and nowhere else, so each iteration reads the whole prior thread from cache
// and writes only the newly appended tail.
func TestStreamTurn_LastMessageBlockCarriesCacheBreakpoint(t *testing.T) {
	var gotBody map[string]any
	srv := cacheServer(t, &gotBody)
	defer srv.Close()

	p := NewProvider(srv.URL, "k", "claude-haiku-4-5", srv.Client())
	_, err := p.StreamTurn(context.Background(), ports.TurnRequest{
		System: "sys",
		Messages: []domain.Message{
			{Role: domain.RoleUser, Content: domain.TextContent{Text: "how is daily?"}},
			{Role: domain.RoleToolCall, Content: domain.ToolCallContent{CallID: "toolu_1", Tool: "schedule_status", Args: map[string]string{"schedule-name": "daily"}}},
			{Role: domain.RoleToolResult, Content: domain.ToolResultContent{CallID: "toolu_1", Output: `{"status":"ok"}`}},
		},
		MaxTokens: 64,
	}, func(string) {})
	require.NoError(t, err)

	msgs := gotBody["messages"].([]any)
	require.Len(t, msgs, 3)
	for i, m := range msgs[:2] {
		for _, b := range m.(map[string]any)["content"].([]any) {
			assert.False(t, ephemeral(t, b.(map[string]any)), "message %d must not carry a breakpoint", i)
		}
	}
	last := msgs[2].(map[string]any)["content"].([]any)
	assert.True(t, ephemeral(t, last[len(last)-1].(map[string]any)), "last block of last message carries the breakpoint")
}

// A thread with no messages yet still produces a valid request: only the
// system breakpoint, no dangling marker.
func TestStreamTurn_NoMessagesOnlySystemBreakpoint(t *testing.T) {
	var gotBody map[string]any
	srv := cacheServer(t, &gotBody)
	defer srv.Close()

	p := NewProvider(srv.URL, "k", "claude-haiku-4-5", srv.Client())
	_, err := p.StreamTurn(context.Background(), ports.TurnRequest{System: "sys", MaxTokens: 64}, func(string) {})
	require.NoError(t, err)
	msgs, _ := gotBody["messages"].([]any)
	assert.Empty(t, msgs)
}

func TestStreamTurn_ParsesCacheUsage(t *testing.T) {
	var gotBody map[string]any
	srv := cacheServer(t, &gotBody)
	defer srv.Close()

	p := NewProvider(srv.URL, "k", "claude-haiku-4-5", srv.Client())
	res, err := p.StreamTurn(context.Background(), ports.TurnRequest{
		System:    "sys",
		Messages:  []domain.Message{{Role: domain.RoleUser, Content: domain.TextContent{Text: "hi"}}},
		MaxTokens: 64,
	}, func(string) {})
	require.NoError(t, err)

	assert.Equal(t, 40, res.Usage.InputTokens)
	assert.Equal(t, 5, res.Usage.OutputTokens)
	assert.Equal(t, 9000, res.Usage.CacheReadInputTokens)
	assert.Equal(t, 300, res.Usage.CacheCreationInputTokens)
}

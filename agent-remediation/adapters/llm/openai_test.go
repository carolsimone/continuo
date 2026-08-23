package llm_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/carolsimone/continuo/agent-remediation/adapters/llm"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// openaiRecordedResponse is a minimal recorded /v1/chat/completions response
// containing a tool_calls entry for propose_fix.
const openaiRecordedResponse = `{
  "id": "chatcmpl-abc123",
  "object": "chat.completion",
  "model": "gpt-4o",
  "choices": [
    {
      "index": 0,
      "message": {
        "role": "assistant",
        "content": null,
        "tool_calls": [
          {
            "id": "call_01",
            "type": "function",
            "function": {
              "name": "propose_fix",
              "arguments": "{\"proposed_sql\":\"SELECT id, amount FROM orders WHERE status = 'active'\",\"rationale\":\"Filtered by active status to match schema contract.\",\"confidence\":\"medium\",\"suspected_root_cause_node\":\"model.service_2.orders\"}"
            }
          }
        ]
      },
      "finish_reason": "tool_calls"
    }
  ],
  "usage": {"prompt_tokens": 80, "completion_tokens": 60, "total_tokens": 140}
}`

// TestOpenAI_BuildRequest verifies that the OpenAI adapter sends the correct wire body:
// tool_choice forcing propose_fix, model, max_tokens, stream:false, system+user messages,
// and the propose_fix tool with all four params.
func TestOpenAI_BuildRequest(t *testing.T) {
	var capturedBody []byte

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedBody, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(openaiRecordedResponse))
	}))
	defer srv.Close()

	provider, err := llm.NewProvider("openai", "test-key", "gpt-4o", srv.URL, srv.Client())
	require.NoError(t, err)

	req := buildTestProposeRequest()
	_, err = provider.Propose(context.Background(), req)
	require.NoError(t, err)

	var body map[string]any
	require.NoError(t, json.Unmarshal(capturedBody, &body))

	// model and max_tokens must be present.
	assert.Equal(t, "gpt-4o", body["model"])
	assert.Equal(t, float64(16000), body["max_tokens"])

	// stream must be false.
	assert.Equal(t, false, body["stream"])

	// messages: system first, then user.
	messages, ok := body["messages"].([]any)
	require.True(t, ok, "messages must be an array")
	require.GreaterOrEqual(t, len(messages), 2)

	systemMsg := messages[0].(map[string]any)
	assert.Equal(t, "system", systemMsg["role"])
	assert.Equal(t, "You are a data engineering assistant.", systemMsg["content"])

	userMsg := messages[1].(map[string]any)
	assert.Equal(t, "user", userMsg["role"])
	assert.Equal(t, "Fix the failed model.", userMsg["content"])

	// tool_choice must force propose_fix.
	toolChoice, ok := body["tool_choice"].(map[string]any)
	require.True(t, ok, "tool_choice must be an object")
	assert.Equal(t, "function", toolChoice["type"])
	tcFunc, ok := toolChoice["function"].(map[string]any)
	require.True(t, ok, "tool_choice.function must be an object")
	assert.Equal(t, "propose_fix", tcFunc["name"])

	// tools array must contain propose_fix with all four params.
	tools, ok := body["tools"].([]any)
	require.True(t, ok, "tools must be an array")
	require.Len(t, tools, 1)

	tool := tools[0].(map[string]any)
	assert.Equal(t, "function", tool["type"])

	fn, ok := tool["function"].(map[string]any)
	require.True(t, ok, "tools[0].function must be an object")
	assert.Equal(t, "propose_fix", fn["name"])

	params, ok := fn["parameters"].(map[string]any)
	require.True(t, ok, "parameters must be an object")
	assert.Equal(t, "object", params["type"])

	props, ok := params["properties"].(map[string]any)
	require.True(t, ok, "properties must be an object")
	assert.Contains(t, props, "proposed_sql")
	assert.Contains(t, props, "rationale")
	assert.Contains(t, props, "confidence")
	assert.Contains(t, props, "suspected_root_cause_node")

	required, ok := params["required"].([]any)
	require.True(t, ok, "required must be an array")
	requiredNames := make([]string, len(required))
	for i, r := range required {
		requiredNames[i] = r.(string)
	}
	assert.Contains(t, requiredNames, "proposed_sql")
	assert.Contains(t, requiredNames, "rationale")
	assert.Contains(t, requiredNames, "confidence")
	assert.NotContains(t, requiredNames, "suspected_root_cause_node")
}

// TestOpenAI_ParseResponse verifies that the OpenAI adapter correctly parses
// choices[0].message.tool_calls[0].function.arguments into ProposeResult fields.
func TestOpenAI_ParseResponse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(openaiRecordedResponse))
	}))
	defer srv.Close()

	provider, err := llm.NewProvider("openai", "test-key", "gpt-4o", srv.URL, srv.Client())
	require.NoError(t, err)

	result, err := provider.Propose(context.Background(), buildTestProposeRequest())
	require.NoError(t, err)

	assert.Equal(t, "SELECT id, amount FROM orders WHERE status = 'active'", result.ProposedSQL)
	assert.Equal(t, "Filtered by active status to match schema contract.", result.Rationale)
	assert.Equal(t, "medium", result.Confidence)
	assert.Equal(t, "model.service_2.orders", result.SuspectedRootCauseNode)
	assert.Equal(t, "gpt-4o", result.Model)
}

// TestOpenAI_NonOKStatus verifies that a non-2xx response returns an error
// containing the status code and truncated body.
func TestOpenAI_NonOKStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error": {"message": "Invalid API key"}}`))
	}))
	defer srv.Close()

	provider, err := llm.NewProvider("openai", "bad-key", "gpt-4o", srv.URL, srv.Client())
	require.NoError(t, err)

	_, err = provider.Propose(context.Background(), buildTestProposeRequest())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "401")
}

// TestOpenAI_CorrectHeaders verifies that the OpenAI adapter sends the Bearer token header.
func TestOpenAI_CorrectHeaders(t *testing.T) {
	var capturedHeaders http.Header

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedHeaders = r.Header.Clone()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(openaiRecordedResponse))
	}))
	defer srv.Close()

	provider, err := llm.NewProvider("openai", "sk-test-openai", "gpt-4o", srv.URL, srv.Client())
	require.NoError(t, err)

	_, err = provider.Propose(context.Background(), buildTestProposeRequest())
	require.NoError(t, err)

	assert.Equal(t, "Bearer sk-test-openai", capturedHeaders.Get("Authorization"))
	assert.Equal(t, "application/json", capturedHeaders.Get("Content-Type"))
}

// TestNewProvider_UnsupportedProvider verifies that an unsupported provider name returns an error.
func TestNewProvider_UnsupportedProvider(t *testing.T) {
	_, err := llm.NewProvider("unknown-provider", "key", "model", "", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown-provider")
}

// TestNewProvider_OpenAICompatible verifies that "openai-compatible" uses the provided baseURL.
func TestNewProvider_OpenAICompatible(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(openaiRecordedResponse))
	}))
	defer srv.Close()

	provider, err := llm.NewProvider("openai-compatible", "key", "gpt-4o", srv.URL, srv.Client())
	require.NoError(t, err)
	require.NotNil(t, provider)

	// Calling Propose confirms the adapter reaches the test server (not api.openai.com).
	result, err := provider.Propose(context.Background(), buildTestProposeRequest())
	require.NoError(t, err)
	assert.Equal(t, "medium", result.Confidence)
}

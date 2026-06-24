package llm_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/carolsimone/continuo/remediation-agent/adapters/llm"
	"github.com/carolsimone/continuo/remediation-agent/domain/prompt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// anthropicRecordedResponse is a minimal recorded /v1/messages response
// containing a single tool_use content block for propose_fix.
const anthropicRecordedResponse = `{
  "id": "msg_01abc",
  "type": "message",
  "role": "assistant",
  "content": [
    {
      "type": "tool_use",
      "id": "toolu_01",
      "name": "propose_fix",
      "input": {
        "proposed_sql": "SELECT id, name FROM users WHERE active = true",
        "rationale": "Added missing WHERE clause to filter active users.",
        "confidence": "high",
        "suspected_root_cause_node": "model.service_1.users"
      }
    }
  ],
  "model": "claude-3-5-sonnet-20241022",
  "stop_reason": "tool_use",
  "usage": {"input_tokens": 100, "output_tokens": 50}
}`

func buildTestProposeRequest() prompt.ProposeRequest {
	return prompt.ProposeRequest{
		System:          "You are a data engineering assistant.",
		User:            "Fix the failed model.",
		ToolName:        "propose_fix",
		ToolDescription: "Propose a corrected version of the failed dbt model's SQL.",
		ToolParams: []prompt.ToolParam{
			{Name: "proposed_sql", Type: "string", Description: "The complete corrected SQL.", Required: true},
			{Name: "rationale", Type: "string", Description: "Explanation of the fix.", Required: true},
			{Name: "confidence", Type: "string", Description: "low, medium, or high.", Required: true},
			{Name: "suspected_root_cause_node", Type: "string", Description: "Upstream node id or empty.", Required: false},
		},
	}
}

// TestAnthropic_BuildRequest verifies that the Anthropic adapter sends the correct wire body:
// tool_choice forcing propose_fix, model, max_tokens, and the propose_fix tool with all four params.
func TestAnthropic_BuildRequest(t *testing.T) {
	var capturedBody []byte

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedBody, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(anthropicRecordedResponse))
	}))
	defer srv.Close()

	provider, err := llm.NewProvider("anthropic", "test-key", "claude-3-5-sonnet-20241022", srv.URL, srv.Client())
	require.NoError(t, err)

	req := buildTestProposeRequest()
	_, err = provider.Propose(context.Background(), req)
	require.NoError(t, err)

	// Parse the captured body to assert wire shape.
	var body map[string]any
	require.NoError(t, json.Unmarshal(capturedBody, &body))

	// model and max_tokens must be present.
	assert.Equal(t, "claude-3-5-sonnet-20241022", body["model"])
	assert.Equal(t, float64(16000), body["max_tokens"])

	// system and user message must be set.
	assert.Equal(t, "You are a data engineering assistant.", body["system"])
	messages, ok := body["messages"].([]any)
	require.True(t, ok, "messages must be an array")
	require.Len(t, messages, 1)
	msg := messages[0].(map[string]any)
	assert.Equal(t, "user", msg["role"])
	assert.Equal(t, "Fix the failed model.", msg["content"])

	// tool_choice must force propose_fix.
	toolChoice, ok := body["tool_choice"].(map[string]any)
	require.True(t, ok, "tool_choice must be an object")
	assert.Equal(t, "tool", toolChoice["type"])
	assert.Equal(t, "propose_fix", toolChoice["name"])

	// tools array must contain propose_fix with all four params.
	tools, ok := body["tools"].([]any)
	require.True(t, ok, "tools must be an array")
	require.Len(t, tools, 1)
	tool := tools[0].(map[string]any)
	assert.Equal(t, "propose_fix", tool["name"])

	schema, ok := tool["input_schema"].(map[string]any)
	require.True(t, ok, "input_schema must be an object")
	assert.Equal(t, "object", schema["type"])

	props, ok := schema["properties"].(map[string]any)
	require.True(t, ok, "properties must be an object")
	assert.Contains(t, props, "proposed_sql")
	assert.Contains(t, props, "rationale")
	assert.Contains(t, props, "confidence")
	assert.Contains(t, props, "suspected_root_cause_node")

	// required must include only the required params.
	required, ok := schema["required"].([]any)
	require.True(t, ok, "required must be an array")
	requiredNames := make([]string, len(required))
	for i, r := range required {
		requiredNames[i] = r.(string)
	}
	assert.Contains(t, requiredNames, "proposed_sql")
	assert.Contains(t, requiredNames, "rationale")
	assert.Contains(t, requiredNames, "confidence")
	assert.NotContains(t, requiredNames, "suspected_root_cause_node")

	// No thinking field.
	assert.NotContains(t, body, "thinking")
}

// TestAnthropic_ParseResponse verifies that the Anthropic adapter correctly parses
// a recorded /v1/messages response with a tool_use block into ProposeResult fields.
func TestAnthropic_ParseResponse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(anthropicRecordedResponse))
	}))
	defer srv.Close()

	provider, err := llm.NewProvider("anthropic", "test-key", "claude-3-5-sonnet-20241022", srv.URL, srv.Client())
	require.NoError(t, err)

	result, err := provider.Propose(context.Background(), buildTestProposeRequest())
	require.NoError(t, err)

	assert.Equal(t, "SELECT id, name FROM users WHERE active = true", result.ProposedSQL)
	assert.Equal(t, "Added missing WHERE clause to filter active users.", result.Rationale)
	assert.Equal(t, "high", result.Confidence)
	assert.Equal(t, "model.service_1.users", result.SuspectedRootCauseNode)
	assert.Equal(t, "claude-3-5-sonnet-20241022", result.Model)
}

// TestAnthropic_NonOKStatus verifies that a non-2xx response returns an error
// containing the status code and truncated body.
func TestAnthropic_NonOKStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error": "internal server error"}`))
	}))
	defer srv.Close()

	provider, err := llm.NewProvider("anthropic", "test-key", "claude-3-5-sonnet-20241022", srv.URL, srv.Client())
	require.NoError(t, err)

	_, err = provider.Propose(context.Background(), buildTestProposeRequest())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "500")
}

// TestAnthropic_CorrectHeaders verifies that the Anthropic adapter sends the
// required authentication and version headers, and POSTs to /v1/messages.
func TestAnthropic_CorrectHeaders(t *testing.T) {
	var capturedHeaders http.Header
	var capturedPath string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedHeaders = r.Header.Clone()
		capturedPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(anthropicRecordedResponse))
	}))
	defer srv.Close()

	provider, err := llm.NewProvider("anthropic", "sk-test-key", "claude-3-5-sonnet-20241022", srv.URL, srv.Client())
	require.NoError(t, err)

	_, err = provider.Propose(context.Background(), buildTestProposeRequest())
	require.NoError(t, err)

	assert.Equal(t, "application/json", capturedHeaders.Get("Content-Type"))
	assert.Equal(t, "sk-test-key", capturedHeaders.Get("X-Api-Key"))
	assert.Equal(t, "2023-06-01", capturedHeaders.Get("Anthropic-Version"))

	// Verify the adapter POSTs to the correct API path.
	assert.Equal(t, "/v1/messages", capturedPath)
}

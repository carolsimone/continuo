package llm_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/adapters/llm"
	"github.com/carolsimone/continuo/agent-remediation/domain/prompt"
)

// pythonFixProposeRequest is the multi-file request shape the python contract
// fixer sends: a differently-named forced tool whose single interesting
// parameter is an array of {path, content} objects.
func pythonFixProposeRequest() prompt.ProposeRequest {
	return prompt.ProposeRequest{
		System:          "Fix the contract.",
		User:            "The node failed validation.",
		ToolName:        "propose_python_fix",
		ToolDescription: "Return the complete new content of every contract file that must change.",
		ToolParams: []prompt.ToolParam{
			{
				Name: "updated_files", Type: "array", Description: "Every file you changed.", Required: true,
				Items: []prompt.ToolParam{
					{Name: "path", Type: "string", Description: "The file's repository path."},
					{Name: "content", Type: "string", Description: "The complete new content."},
				},
			},
			{Name: "rationale", Type: "string", Description: "Explanation.", Required: true},
			{Name: "confidence", Type: "string", Description: "low, medium, or high.", Required: true},
		},
	}
}

// anthropicPythonFixResponse is a recorded /v1/messages response whose tool_use
// block is named for the python fix tool and returns two complete files.
const anthropicPythonFixResponse = `{
  "content": [
    {
      "type": "tool_use",
      "name": "propose_python_fix",
      "input": {
        "updated_files": [
          {"path": "contracts/a.yml", "content": "nodes:\n  - schema: analytics\n"},
          {"path": "contracts/b.yml", "content": "nodes: []\n"}
        ],
        "rationale": "Declared the column the script produces.",
        "confidence": "high"
      }
    }
  ]
}`

// TestAnthropic_ParsesUpdatedFiles verifies that the Anthropic adapter reads a
// multi-file answer back off the wire. Without it the python fixer receives an
// empty file list and records every attempt as failed, no matter what the model
// actually proposed.
func TestAnthropic_ParsesUpdatedFiles(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(anthropicPythonFixResponse))
	}))
	defer srv.Close()

	provider, err := llm.NewProvider("anthropic", "k", "claude-test", srv.URL, srv.Client())
	require.NoError(t, err)

	res, err := provider.Propose(context.Background(), pythonFixProposeRequest())
	require.NoError(t, err)
	require.Len(t, res.Files, 2)
	require.Equal(t, "contracts/a.yml", res.Files[0].Path)
	require.Equal(t, "nodes:\n  - schema: analytics\n", res.Files[0].Content)
	require.Equal(t, "contracts/b.yml", res.Files[1].Path)
	require.Equal(t, "Declared the column the script produces.", res.Rationale)
}

// TestAnthropic_BuildRequestRendersArrayItems verifies that an array parameter
// reaches the model with an "items" subschema naming its object properties. A
// bare {"type":"array"} tells the model nothing about what an element is, so
// the file list it returns would be unparseable by shape rather than by
// accident.
func TestAnthropic_BuildRequestRendersArrayItems(t *testing.T) {
	var captured []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		captured, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(anthropicPythonFixResponse))
	}))
	defer srv.Close()

	provider, err := llm.NewProvider("anthropic", "k", "claude-test", srv.URL, srv.Client())
	require.NoError(t, err)
	_, err = provider.Propose(context.Background(), pythonFixProposeRequest())
	require.NoError(t, err)

	var body map[string]any
	require.NoError(t, json.Unmarshal(captured, &body))
	tools := body["tools"].([]any)
	schema := tools[0].(map[string]any)["input_schema"].(map[string]any)
	props := schema["properties"].(map[string]any)
	files := props["updated_files"].(map[string]any)
	require.Equal(t, "array", files["type"])

	items := files["items"].(map[string]any)
	require.Equal(t, "object", items["type"])
	itemProps := items["properties"].(map[string]any)
	require.Contains(t, itemProps, "path")
	require.Contains(t, itemProps, "content")
	require.ElementsMatch(t, []any{"path", "content"}, items["required"])
}

// anthropicWrongToolResponse is a tool_use block for a DIFFERENT tool than the
// one the request forced — the shape a stale or confused response takes.
const anthropicWrongToolResponse = `{
  "content": [
    {
      "type": "tool_use",
      "name": "propose_fix",
      "input": {
        "proposed_sql": "SELECT 1",
        "rationale": "not the answer that was asked for",
        "confidence": "high"
      }
    }
  ]
}`

// TestAnthropic_RejectsAnswerNamingADifferentTool verifies that the adapter
// matches the tool_use block against the tool THIS request forced, rather than
// taking whatever tool_use block comes first. The distinction is the whole
// point of reading the name off the request: a block for another tool carries
// another schema, so accepting it would silently hand the caller a result whose
// fields belong to a different answer — an empty file list for a multi-file
// fix, which the caller can only read as "the model proposed nothing".
func TestAnthropic_RejectsAnswerNamingADifferentTool(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(anthropicWrongToolResponse))
	}))
	defer srv.Close()

	provider, err := llm.NewProvider("anthropic", "k", "claude-test", srv.URL, srv.Client())
	require.NoError(t, err)

	_, err = provider.Propose(context.Background(), pythonFixProposeRequest())
	require.Error(t, err)
	require.Contains(t, err.Error(), "propose_python_fix",
		"the error must name the tool that was forced and not found")
}

// openaiPythonFixResponse is a recorded chat-completions response whose
// propose_python_fix arguments carry the multi-file answer.
const openaiPythonFixResponse = `{
  "choices": [
    {
      "message": {
        "tool_calls": [
          {
            "function": {
              "name": "propose_python_fix",
              "arguments": "{\"updated_files\":[{\"path\":\"contracts/a.yml\",\"content\":\"nodes: []\\n\"}],\"rationale\":\"Fixed.\",\"confidence\":\"medium\"}"
            }
          }
        ]
      }
    }
  ]
}`

// TestOpenAI_ParsesUpdatedFiles verifies the same multi-file answer decodes on
// the OpenAI-compatible adapter, so the python fixer works on either provider.
func TestOpenAI_ParsesUpdatedFiles(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(openaiPythonFixResponse))
	}))
	defer srv.Close()

	provider, err := llm.NewProvider("openai-compatible", "k", "gpt-test", srv.URL, srv.Client())
	require.NoError(t, err)

	res, err := provider.Propose(context.Background(), pythonFixProposeRequest())
	require.NoError(t, err)
	require.Len(t, res.Files, 1)
	require.Equal(t, "contracts/a.yml", res.Files[0].Path)
	require.Equal(t, "nodes: []\n", res.Files[0].Content)
}

// TestOpenAI_BuildRequestRendersArrayItems verifies the OpenAI adapter renders
// the same "items" subschema as the Anthropic one.
func TestOpenAI_BuildRequestRendersArrayItems(t *testing.T) {
	var captured []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		captured, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(openaiPythonFixResponse))
	}))
	defer srv.Close()

	provider, err := llm.NewProvider("openai-compatible", "k", "gpt-test", srv.URL, srv.Client())
	require.NoError(t, err)
	_, err = provider.Propose(context.Background(), pythonFixProposeRequest())
	require.NoError(t, err)

	var body map[string]any
	require.NoError(t, json.Unmarshal(captured, &body))
	tools := body["tools"].([]any)
	params := tools[0].(map[string]any)["function"].(map[string]any)["parameters"].(map[string]any)
	files := params["properties"].(map[string]any)["updated_files"].(map[string]any)
	items := files["items"].(map[string]any)
	require.Equal(t, "object", items["type"])
	require.Contains(t, items["properties"], "path")
	require.Contains(t, items["properties"], "content")
}

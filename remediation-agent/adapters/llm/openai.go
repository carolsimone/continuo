package llm

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/carolsimone/continuo/remediation-agent/service/ports"
)

// compile-time assertion that openaiProvider implements ports.LLMProvider.
var _ ports.LLMProvider = (*openaiProvider)(nil)

// openaiProvider sends a single-shot, non-streaming request to an OpenAI-compatible
// chat-completions API. Setting a custom baseURL enables any compatible endpoint
// (Azure, LiteLLM, Ollama, vLLM, local stub servers).
type openaiProvider struct {
	baseURL string
	apiKey  string
	model   string
	client  *http.Client
}

// newOpenAI creates an openaiProvider with the given base URL, API key, model, and HTTP client.
func newOpenAI(baseURL, apiKey, model string, client *http.Client) *openaiProvider {
	return &openaiProvider{
		baseURL: strings.TrimRight(baseURL, "/"),
		apiKey:  apiKey,
		model:   model,
		client:  client,
	}
}

// openaiRequest is the JSON body sent to POST /v1/chat/completions.
type openaiRequest struct {
	Model      string           `json:"model"`
	MaxTokens  int              `json:"max_tokens"`
	Stream     bool             `json:"stream"`
	Messages   []openaiMessage  `json:"messages"`
	Tools      []openaiTool     `json:"tools"`
	ToolChoice openaiToolChoice `json:"tool_choice"`
}

// openaiMessage is a single message in the chat conversation.
type openaiMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// openaiTool is a tool definition in the OpenAI API format.
type openaiTool struct {
	Type     string             `json:"type"`
	Function openaiToolFunction `json:"function"`
}

// openaiToolFunction holds the function name, description, and parameter schema.
type openaiToolFunction struct {
	Name        string         `json:"name"`
	Description string         `json:"description"`
	Parameters  openaiToolParams `json:"parameters"`
}

// openaiToolParams is the JSON Schema object describing the function's parameters.
type openaiToolParams struct {
	Type       string                       `json:"type"`
	Properties map[string]openaiParamProp   `json:"properties"`
	Required   []string                     `json:"required"`
}

// openaiParamProp is a single parameter definition within the parameter schema.
type openaiParamProp struct {
	Type        string `json:"type"`
	Description string `json:"description,omitempty"`
}

// openaiToolChoice forces the model to call a specific function.
type openaiToolChoice struct {
	Type     string                  `json:"type"`
	Function openaiToolChoiceFunction `json:"function"`
}

// openaiToolChoiceFunction names the function that must be called.
type openaiToolChoiceFunction struct {
	Name string `json:"name"`
}

// openaiResponse is the top-level response from POST /v1/chat/completions.
type openaiResponse struct {
	Choices []openaiChoice `json:"choices"`
}

// openaiChoice is one element of the choices array.
type openaiChoice struct {
	Message openaiResponseMessage `json:"message"`
}

// openaiResponseMessage is the assistant message in a completion choice.
type openaiResponseMessage struct {
	ToolCalls []openaiResponseToolCall `json:"tool_calls"`
}

// openaiResponseToolCall is one tool call in the assistant response.
type openaiResponseToolCall struct {
	Function openaiResponseFunction `json:"function"`
}

// openaiResponseFunction holds the function name and JSON-encoded arguments.
type openaiResponseFunction struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

// openaiToolArgs holds the fields parsed from the propose_fix tool call.
type openaiToolArgs struct {
	ProposedSQL            string `json:"proposed_sql"`
	ProposedContent        string `json:"proposed_content"`
	TargetFile             string `json:"target_file"`
	Rationale              string `json:"rationale"`
	Confidence             string `json:"confidence"`
	SuspectedRootCauseNode string `json:"suspected_root_cause_node"`
}

// Propose sends a single-shot, non-streaming, tool-forced request to an OpenAI-compatible
// chat-completions endpoint and parses the propose_fix function arguments into a ProposeResult.
func (p *openaiProvider) Propose(ctx context.Context, req ports.ProposeRequest) (ports.ProposeResult, error) {
	body, err := p.buildRequest(req)
	if err != nil {
		return ports.ProposeResult{}, fmt.Errorf("openai: build request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, p.baseURL+"/v1/chat/completions", bytes.NewReader(body))
	if err != nil {
		return ports.ProposeResult{}, fmt.Errorf("openai: create http request: %w", err)
	}
	httpReq.Header.Set("content-type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+p.apiKey)

	resp, err := p.client.Do(httpReq)
	if err != nil {
		return ports.ProposeResult{}, fmt.Errorf("openai: http: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		limitedBody, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBodyBytes))
		return ports.ProposeResult{}, fmt.Errorf("openai: %d: %s", resp.StatusCode, strings.TrimSpace(string(limitedBody)))
	}

	return p.parseResponse(resp.Body)
}

// buildRequest marshals the ProposeRequest into the OpenAI chat-completions wire format.
func (p *openaiProvider) buildRequest(req ports.ProposeRequest) ([]byte, error) {
	props := make(map[string]openaiParamProp, len(req.ToolParams))
	required := make([]string, 0, len(req.ToolParams))
	for _, param := range req.ToolParams {
		props[param.Name] = openaiParamProp{
			Type:        param.Type,
			Description: param.Description,
		}
		if param.Required {
			required = append(required, param.Name)
		}
	}

	wireReq := openaiRequest{
		Model:     p.model,
		MaxTokens: 16000,
		Stream:    false,
		Messages: []openaiMessage{
			{Role: "system", Content: req.System},
			{Role: "user", Content: req.User},
		},
		Tools: []openaiTool{
			{
				Type: "function",
				Function: openaiToolFunction{
					Name:        req.ToolName,
					Description: req.ToolDescription,
					Parameters: openaiToolParams{
						Type:       "object",
						Properties: props,
						Required:   required,
					},
				},
			},
		},
		ToolChoice: openaiToolChoice{
			Type: "function",
			Function: openaiToolChoiceFunction{
				Name: req.ToolName,
			},
		},
	}

	return json.Marshal(wireReq)
}

// parseResponse reads the OpenAI chat-completions response and extracts the propose_fix arguments.
func (p *openaiProvider) parseResponse(r io.Reader) (ports.ProposeResult, error) {
	var resp openaiResponse
	if err := json.NewDecoder(r).Decode(&resp); err != nil {
		return ports.ProposeResult{}, fmt.Errorf("openai: decode response: %w", err)
	}

	if len(resp.Choices) == 0 {
		return ports.ProposeResult{}, fmt.Errorf("openai: response contains no choices")
	}
	toolCalls := resp.Choices[0].Message.ToolCalls
	if len(toolCalls) == 0 {
		return ports.ProposeResult{}, fmt.Errorf("openai: response message contains no tool_calls")
	}

	// The adapter forces propose_fix, so the first tool call is always the one we want.
	argsJSON := toolCalls[0].Function.Arguments
	var args openaiToolArgs
	if err := json.Unmarshal([]byte(argsJSON), &args); err != nil {
		return ports.ProposeResult{}, fmt.Errorf("openai: unmarshal tool arguments: %w", err)
	}

	return ports.ProposeResult{
		ProposedSQL:            args.ProposedSQL,
		ProposedContent:        args.ProposedContent,
		TargetFile:             args.TargetFile,
		Rationale:              args.Rationale,
		Confidence:             args.Confidence,
		SuspectedRootCauseNode: args.SuspectedRootCauseNode,
		Model:                  p.model,
	}, nil
}

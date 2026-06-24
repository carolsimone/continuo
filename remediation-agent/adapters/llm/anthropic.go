// Package llm provides LLM provider adapters for the remediation-agent.
// Each adapter performs a single-shot, non-streaming, tool-forced call to its
// respective API, parses the structured propose_fix result, and returns a ProposeResult.
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

// compile-time assertion that anthropicProvider implements ports.LLMProvider.
var _ ports.LLMProvider = (*anthropicProvider)(nil)

const (
	anthropicVersion = "2023-06-01"
	// maxErrorBodyBytes is the maximum number of bytes read from a non-2xx response body
	// when constructing the error message.
	maxErrorBodyBytes = 512
)

// anthropicProvider sends a single-shot, non-streaming request to the Anthropic Messages API.
type anthropicProvider struct {
	baseURL string
	apiKey  string
	model   string
	client  *http.Client
}

// newAnthropic creates an anthropicProvider with the given base URL, API key, model, and HTTP client.
func newAnthropic(baseURL, apiKey, model string, client *http.Client) *anthropicProvider {
	return &anthropicProvider{
		baseURL: strings.TrimRight(baseURL, "/"),
		apiKey:  apiKey,
		model:   model,
		client:  client,
	}
}

// anthropicRequest is the JSON body sent to POST /v1/messages.
type anthropicRequest struct {
	Model     string              `json:"model"`
	MaxTokens int                 `json:"max_tokens"`
	System    string              `json:"system,omitempty"`
	Messages  []anthropicMessage  `json:"messages"`
	Tools     []anthropicTool     `json:"tools"`
	ToolChoice anthropicToolChoice `json:"tool_choice"`
}

// anthropicMessage is a single message in the conversation history.
type anthropicMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// anthropicTool is a tool definition in the Anthropic API format.
type anthropicTool struct {
	Name        string              `json:"name"`
	Description string              `json:"description"`
	InputSchema anthropicInputSchema `json:"input_schema"`
}

// anthropicInputSchema is the JSON Schema object describing the tool's parameters.
type anthropicInputSchema struct {
	Type       string                          `json:"type"`
	Properties map[string]anthropicParamProp   `json:"properties"`
	Required   []string                        `json:"required"`
}

// anthropicParamProp is a single parameter definition within the input schema.
type anthropicParamProp struct {
	Type        string `json:"type"`
	Description string `json:"description,omitempty"`
}

// anthropicToolChoice forces the model to call a specific tool.
type anthropicToolChoice struct {
	Type string `json:"type"`
	Name string `json:"name"`
}

// anthropicResponse is the top-level response from POST /v1/messages.
type anthropicResponse struct {
	Content []anthropicContentBlock `json:"content"`
}

// anthropicContentBlock is one element of the response content array.
type anthropicContentBlock struct {
	Type  string                `json:"type"`
	Name  string                `json:"name,omitempty"`
	Input anthropicToolInput    `json:"input,omitempty"`
}

// anthropicToolInput holds the structured fields returned by the propose_fix tool.
type anthropicToolInput struct {
	ProposedSQL            string `json:"proposed_sql"`
	Rationale              string `json:"rationale"`
	Confidence             string `json:"confidence"`
	SuspectedRootCauseNode string `json:"suspected_root_cause_node"`
}

// Propose sends a single-shot, tool-forced request to the Anthropic Messages API and
// parses the propose_fix tool call result from the response content array.
func (p *anthropicProvider) Propose(ctx context.Context, req ports.ProposeRequest) (ports.ProposeResult, error) {
	body, err := p.buildRequest(req)
	if err != nil {
		return ports.ProposeResult{}, fmt.Errorf("anthropic: build request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, p.baseURL+"/v1/messages", bytes.NewReader(body))
	if err != nil {
		return ports.ProposeResult{}, fmt.Errorf("anthropic: create http request: %w", err)
	}
	httpReq.Header.Set("content-type", "application/json")
	httpReq.Header.Set("x-api-key", p.apiKey)
	httpReq.Header.Set("anthropic-version", anthropicVersion)

	resp, err := p.client.Do(httpReq)
	if err != nil {
		return ports.ProposeResult{}, fmt.Errorf("anthropic: http: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		limitedBody, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBodyBytes))
		return ports.ProposeResult{}, fmt.Errorf("anthropic: %d: %s", resp.StatusCode, strings.TrimSpace(string(limitedBody)))
	}

	return p.parseResponse(resp.Body)
}

// buildRequest marshals the ProposeRequest into the Anthropic wire format.
func (p *anthropicProvider) buildRequest(req ports.ProposeRequest) ([]byte, error) {
	props := make(map[string]anthropicParamProp, len(req.ToolParams))
	required := make([]string, 0, len(req.ToolParams))
	for _, param := range req.ToolParams {
		props[param.Name] = anthropicParamProp{
			Type:        param.Type,
			Description: param.Description,
		}
		if param.Required {
			required = append(required, param.Name)
		}
	}

	wireReq := anthropicRequest{
		Model:     p.model,
		MaxTokens: 16000,
		System:    req.System,
		Messages: []anthropicMessage{
			{Role: "user", Content: req.User},
		},
		Tools: []anthropicTool{
			{
				Name:        req.ToolName,
				Description: req.ToolDescription,
				InputSchema: anthropicInputSchema{
					Type:       "object",
					Properties: props,
					Required:   required,
				},
			},
		},
		ToolChoice: anthropicToolChoice{
			Type: "tool",
			Name: req.ToolName,
		},
	}

	return json.Marshal(wireReq)
}

// parseResponse reads the Anthropic response body and extracts the propose_fix tool input.
func (p *anthropicProvider) parseResponse(r io.Reader) (ports.ProposeResult, error) {
	var resp anthropicResponse
	if err := json.NewDecoder(r).Decode(&resp); err != nil {
		return ports.ProposeResult{}, fmt.Errorf("anthropic: decode response: %w", err)
	}

	for _, block := range resp.Content {
		if block.Type == "tool_use" && block.Name == "propose_fix" {
			return ports.ProposeResult{
				ProposedSQL:            block.Input.ProposedSQL,
				Rationale:              block.Input.Rationale,
				Confidence:             block.Input.Confidence,
				SuspectedRootCauseNode: block.Input.SuspectedRootCauseNode,
				Model:                  p.model,
			}, nil
		}
	}

	return ports.ProposeResult{}, fmt.Errorf("anthropic: propose_fix tool_use block not found in response")
}

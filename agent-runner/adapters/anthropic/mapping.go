package anthropic

import (
	"encoding/json"
	"fmt"

	"github.com/carolsimone/continuo/agent-runner/domain"
	"github.com/carolsimone/continuo/agent-runner/service/ports"
)

// wireMessage is the Anthropic API representation of a single conversation turn.
type wireMessage struct {
	Role    string       `json:"role"`
	Content []wireBlock  `json:"content"`
}

// wireBlock is one content block inside a message (text, tool_use, or tool_result).
type wireBlock struct {
	Type      string         `json:"type"`
	Text      string         `json:"text,omitempty"`
	ID        string         `json:"id,omitempty"`
	Name      string         `json:"name,omitempty"`
	Input     map[string]any `json:"input,omitempty"`
	ToolUseID string         `json:"tool_use_id,omitempty"`
	Content   string         `json:"content,omitempty"`
	IsError   bool           `json:"is_error,omitempty"`
}

// wireTool is the Anthropic API representation of a tool definition.
type wireTool struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	InputSchema wireInputSchema `json:"input_schema"`
}

// wireInputSchema is the JSON Schema object describing a tool's parameters.
type wireInputSchema struct {
	Type       string                       `json:"type"`
	Properties map[string]wireParamProperty `json:"properties"`
	Required   []string                     `json:"required"`
}

// wireParamProperty is a single parameter definition within the input schema.
type wireParamProperty struct {
	Type        string `json:"type"`
	Description string `json:"description,omitempty"`
}

// toWireMessages converts the domain message history to Anthropic wire messages.
// Adjacent messages with the same effective Anthropic role are merged into a single
// message with multiple content blocks, satisfying the API's alternating-role requirement.
func toWireMessages(msgs []domain.Message) ([]wireMessage, error) {
	var result []wireMessage

	for _, msg := range msgs {
		block, role, err := toWireBlock(msg)
		if err != nil {
			return nil, fmt.Errorf("mapping message role=%s: %w", msg.Role, err)
		}

		// Merge into the previous message if it has the same role.
		if len(result) > 0 && result[len(result)-1].Role == role {
			result[len(result)-1].Content = append(result[len(result)-1].Content, block)
		} else {
			result = append(result, wireMessage{
				Role:    role,
				Content: []wireBlock{block},
			})
		}
	}

	return result, nil
}

// toWireBlock converts a single domain message into a content block and its Anthropic role.
func toWireBlock(msg domain.Message) (wireBlock, string, error) {
	switch msg.Role {
	case domain.RoleUser:
		var c domain.TextContent
		if err := json.Unmarshal(msg.Content, &c); err != nil {
			return wireBlock{}, "", fmt.Errorf("unmarshal user content: %w", err)
		}
		return wireBlock{Type: "text", Text: c.Text}, "user", nil

	case domain.RoleAssistant:
		var c domain.TextContent
		if err := json.Unmarshal(msg.Content, &c); err != nil {
			return wireBlock{}, "", fmt.Errorf("unmarshal assistant content: %w", err)
		}
		return wireBlock{Type: "text", Text: c.Text}, "assistant", nil

	case domain.RoleToolCall:
		var c domain.ToolCallContent
		if err := json.Unmarshal(msg.Content, &c); err != nil {
			return wireBlock{}, "", fmt.Errorf("unmarshal tool_call content: %w", err)
		}
		input := make(map[string]any, len(c.Args))
		for k, v := range c.Args {
			input[k] = v
		}
		return wireBlock{
			Type:  "tool_use",
			ID:    c.CallID,
			Name:  c.Tool,
			Input: input,
		}, "assistant", nil

	case domain.RoleToolResult:
		var c domain.ToolResultContent
		if err := json.Unmarshal(msg.Content, &c); err != nil {
			return wireBlock{}, "", fmt.Errorf("unmarshal tool_result content: %w", err)
		}
		return wireBlock{
			Type:      "tool_result",
			ToolUseID: c.CallID,
			Content:   c.Output,
			IsError:   c.IsError,
		}, "user", nil

	default:
		return wireBlock{}, "", fmt.Errorf("unknown role: %q", msg.Role)
	}
}

// toWireTools converts the port tool definitions to the Anthropic API tool format.
func toWireTools(tools []ports.ToolDef) []wireTool {
	result := make([]wireTool, 0, len(tools))
	for _, t := range tools {
		props := make(map[string]wireParamProperty, len(t.Params))
		required := make([]string, 0)
		for _, p := range t.Params {
			props[p.Name] = wireParamProperty{
				Type:        "string",
				Description: p.Description,
			}
			if p.Required {
				required = append(required, p.Name)
			}
		}
		result = append(result, wireTool{
			Name:        t.Name,
			Description: t.Description,
			InputSchema: wireInputSchema{
				Type:       "object",
				Properties: props,
				Required:   required,
			},
		})
	}
	return result
}

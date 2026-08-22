package openai

import (
	"encoding/json"
	"fmt"

	"github.com/carolsimone/continuo/agent-chat/domain"
	"github.com/carolsimone/continuo/agent-chat/service/ports"
)

// wireMessage is the OpenAI chat-completions API representation of a single conversation turn.
// An assistant message may carry both content (text) and tool_calls in the same object.
type wireMessage struct {
	Role       string         `json:"role"`
	Content    string         `json:"content,omitempty"`
	ToolCalls  []wireToolCall `json:"tool_calls,omitempty"`
	ToolCallID string         `json:"tool_call_id,omitempty"`
}

// wireToolCall is one tool invocation inside an assistant message.
type wireToolCall struct {
	ID       string           `json:"id"`
	Type     string           `json:"type"`
	Function wireToolFunction `json:"function"`
}

// wireToolFunction holds the name and JSON-encoded arguments of a tool call.
type wireToolFunction struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

// wireTool is the OpenAI API representation of an available tool.
type wireTool struct {
	Type     string          `json:"type"`
	Function wireToolDetails `json:"function"`
}

// wireToolDetails holds the function metadata and parameter schema.
type wireToolDetails struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	Parameters  wireToolParams  `json:"parameters"`
}

// wireToolParams is the JSON Schema object describing a tool's parameters.
type wireToolParams struct {
	Type       string                       `json:"type"`
	Properties map[string]wireParamProperty `json:"properties"`
	Required   []string                     `json:"required"`
}

// wireParamProperty is a single parameter definition within the parameter schema.
type wireParamProperty struct {
	Type        string `json:"type"`
	Description string `json:"description,omitempty"`
}

// toWireMessages converts the domain message history to OpenAI wire messages.
// System is prepended as role:system if non-empty. RoleToolCall messages become
// assistant messages with tool_calls; if the immediately preceding message is already
// an assistant message it is reused (merging text + tool_calls on one object).
// RoleToolResult becomes role:tool with tool_call_id.
func toWireMessages(system string, msgs []domain.Message) ([]wireMessage, error) {
	var result []wireMessage

	if system != "" {
		result = append(result, wireMessage{Role: "system", Content: system})
	}

	for _, msg := range msgs {
		switch msg.Role {
		case domain.RoleUser:
			var c domain.TextContent
			if err := json.Unmarshal(msg.Content, &c); err != nil {
				return nil, fmt.Errorf("unmarshal user content: %w", err)
			}
			result = append(result, wireMessage{Role: "user", Content: c.Text})

		case domain.RoleAssistant:
			var c domain.TextContent
			if err := json.Unmarshal(msg.Content, &c); err != nil {
				return nil, fmt.Errorf("unmarshal assistant content: %w", err)
			}
			result = append(result, wireMessage{Role: "assistant", Content: c.Text})

		case domain.RoleToolCall:
			var c domain.ToolCallContent
			if err := json.Unmarshal(msg.Content, &c); err != nil {
				return nil, fmt.Errorf("unmarshal tool_call content: %w", err)
			}
			argsJSON, err := json.Marshal(c.Args)
			if err != nil {
				return nil, fmt.Errorf("marshal tool_call args: %w", err)
			}
			tc := wireToolCall{
				ID:   c.CallID,
				Type: "function",
				Function: wireToolFunction{
					Name:      c.Tool,
					Arguments: string(argsJSON),
				},
			}
			// Merge into the preceding assistant message if one exists, otherwise create a new one.
			if len(result) > 0 && result[len(result)-1].Role == "assistant" {
				last := &result[len(result)-1]
				last.ToolCalls = append(last.ToolCalls, tc)
			} else {
				result = append(result, wireMessage{
					Role:      "assistant",
					ToolCalls: []wireToolCall{tc},
				})
			}

		case domain.RoleToolResult:
			var c domain.ToolResultContent
			if err := json.Unmarshal(msg.Content, &c); err != nil {
				return nil, fmt.Errorf("unmarshal tool_result content: %w", err)
			}
			result = append(result, wireMessage{
				Role:       "tool",
				Content:    c.Output,
				ToolCallID: c.CallID,
			})

		default:
			return nil, fmt.Errorf("unknown role: %q", msg.Role)
		}
	}

	return result, nil
}

// toWireTools converts the port tool definitions to the OpenAI API tool format.
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
			Type: "function",
			Function: wireToolDetails{
				Name:        t.Name,
				Description: t.Description,
				Parameters: wireToolParams{
					Type:       "object",
					Properties: props,
					Required:   required,
				},
			},
		})
	}
	return result
}

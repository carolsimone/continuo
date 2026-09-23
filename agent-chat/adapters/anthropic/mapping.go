package anthropic

import (
	"fmt"

	"github.com/carolsimone/continuo/agent-chat/domain"
	"github.com/carolsimone/continuo/agent-chat/service/ports"
)

// wireMessage is the Anthropic API representation of a single conversation turn.
type wireMessage struct {
	Role    string      `json:"role"`
	Content []wireBlock `json:"content"`
}

// wireBlock is one content block inside a message (text, tool_use, or tool_result).
type wireBlock struct {
	Type string `json:"type"`
	Text string `json:"text,omitempty"`
	ID   string `json:"id,omitempty"`
	Name string `json:"name,omitempty"`
	// Input is typed as any (not map[string]any) so that a tool_use block with
	// no arguments still serializes its Anthropic-required "input" field as an
	// empty object. With omitempty, encoding/json drops an empty map but keeps a
	// non-nil interface holding one — and the API rejects a tool_use block
	// without input ("tool_use.input: Field required"). Non-tool_use blocks
	// leave this as a nil interface, which omitempty correctly omits.
	Input     any    `json:"input,omitempty"`
	ToolUseID string `json:"tool_use_id,omitempty"`
	Content   string `json:"content,omitempty"`
	IsError   bool   `json:"is_error,omitempty"`
	// CacheControl marks this block as a prompt-cache breakpoint: the
	// provider caches everything rendered up to and including it, and a later
	// request whose prefix is byte-identical reads that prefix from cache
	// instead of reprocessing it. Nil on every block that is not a breakpoint.
	CacheControl *wireCacheControl `json:"cache_control,omitempty"`
}

// wireCacheControl is the prompt-cache marker the API accepts on a content
// block, a system block, or a tool definition.
type wireCacheControl struct {
	Type string `json:"type"`
}

// ephemeralCache is the only cache_control type the API defines. Entries live
// five minutes from the start of the request that wrote or last read them.
func ephemeralCache() *wireCacheControl { return &wireCacheControl{Type: "ephemeral"} }

// wireSystemBlock is one text block of the request's system field. The
// system prompt is sent as a block array rather than a plain string so the
// block can carry a cache breakpoint.
type wireSystemBlock struct {
	Type         string            `json:"type"`
	Text         string            `json:"text"`
	CacheControl *wireCacheControl `json:"cache_control,omitempty"`
}

// toWireSystem renders the system prompt as a single text block carrying a
// cache breakpoint. Tools render before system in the API's prefix order, so
// this one marker caches the whole static prefix: the tool catalog plus the
// system prompt, which are identical on every iteration of every turn.
func toWireSystem(system string) []wireSystemBlock {
	if system == "" {
		return nil
	}
	return []wireSystemBlock{{Type: "text", Text: system, CacheControl: ephemeralCache()}}
}

// markConversationBreakpoint places the second cache breakpoint on the last
// content block of the last message. Each iteration of a turn appends to the
// thread and moves this marker forward; the previous marker's prefix stays a
// valid read point, so the provider reads the whole prior thread from cache
// and processes only the newly appended tail. A thread with no messages gets
// no marker.
func markConversationBreakpoint(msgs []wireMessage) {
	if len(msgs) == 0 {
		return
	}
	last := &msgs[len(msgs)-1]
	if len(last.Content) == 0 {
		return
	}
	last.Content[len(last.Content)-1].CacheControl = ephemeralCache()
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
		c, ok := msg.Content.(domain.TextContent)
		if !ok {
			return wireBlock{}, "", fmt.Errorf("user message content is %T, want TextContent", msg.Content)
		}
		return wireBlock{Type: "text", Text: c.Text}, "user", nil

	case domain.RoleAssistant:
		c, ok := msg.Content.(domain.TextContent)
		if !ok {
			return wireBlock{}, "", fmt.Errorf("assistant message content is %T, want TextContent", msg.Content)
		}
		return wireBlock{Type: "text", Text: c.Text}, "assistant", nil

	case domain.RoleToolCall:
		c, ok := msg.Content.(domain.ToolCallContent)
		if !ok {
			return wireBlock{}, "", fmt.Errorf("tool_call message content is %T, want ToolCallContent", msg.Content)
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
		c, ok := msg.Content.(domain.ToolResultContent)
		if !ok {
			return wireBlock{}, "", fmt.Errorf("tool_result message content is %T, want ToolResultContent", msg.Content)
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

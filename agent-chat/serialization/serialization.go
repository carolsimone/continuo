// Package serialization holds the persistence-facing DTOs for agent-chat's
// message-content types, keeping the domain thread package free of json struct
// tags. It sits outside domain/ so the tags live away from the domain types.
//
// Encode and Decode are the single crossing between a domain.Content value and
// the JSON bytes stored in the messages.content JSONB column (postgres adapter)
// and the S3 thread archive (s3 adapter). The stored byte shape of every
// content variant is fixed here by the DTO field tags, so persisted threads
// keep deserializing across changes to the domain types.
package serialization

import (
	"encoding/json"
	"fmt"

	"github.com/carolsimone/continuo/agent-chat/domain"
)

// Encode marshals a domain.Content to the JSON bytes stored for its message.
// The concrete variant selects the DTO; an unknown type is a programming error
// (a Content the persistence layer was never taught to store).
func Encode(c domain.Content) ([]byte, error) {
	switch v := c.(type) {
	case domain.TextContent:
		return json.Marshal(TextContentFromDomain(v))
	case domain.ToolCallContent:
		return json.Marshal(ToolCallContentFromDomain(v))
	case domain.ToolResultContent:
		return json.Marshal(ToolResultContentFromDomain(v))
	default:
		return nil, fmt.Errorf("serialization: cannot encode content of type %T", c)
	}
}

// Decode reverses Encode: it unmarshals stored content bytes into the domain
// variant that the message's role selects. An unknown role is rejected rather
// than silently decoded into the wrong variant.
func Decode(role domain.Role, data []byte) (domain.Content, error) {
	switch role {
	case domain.RoleUser, domain.RoleAssistant:
		var dto TextContentDTO
		if err := json.Unmarshal(data, &dto); err != nil {
			return nil, fmt.Errorf("serialization: decode %s content: %w", role, err)
		}
		return dto.ToDomain(), nil
	case domain.RoleToolCall:
		var dto ToolCallContentDTO
		if err := json.Unmarshal(data, &dto); err != nil {
			return nil, fmt.Errorf("serialization: decode %s content: %w", role, err)
		}
		return dto.ToDomain(), nil
	case domain.RoleToolResult:
		var dto ToolResultContentDTO
		if err := json.Unmarshal(data, &dto); err != nil {
			return nil, fmt.Errorf("serialization: decode %s content: %w", role, err)
		}
		return dto.ToDomain(), nil
	default:
		return nil, fmt.Errorf("serialization: cannot decode content for unknown role %q", role)
	}
}

// TextContentDTO is the JSON shape of a text message's content.
type TextContentDTO struct {
	Text string `json:"text"`
}

// TextContentFromDomain maps the domain content to its DTO.
func TextContentFromDomain(c domain.TextContent) TextContentDTO { return TextContentDTO(c) }

// ToDomain maps a decoded DTO back to the domain content.
func (d TextContentDTO) ToDomain() domain.TextContent { return domain.TextContent(d) }

// ToolCallContentDTO is the JSON shape of a tool-invocation message's content.
type ToolCallContentDTO struct {
	CallID string            `json:"call_id"`
	Tool   string            `json:"tool"`
	Args   map[string]string `json:"args"`
}

// ToolCallContentFromDomain maps the domain content to its DTO.
func ToolCallContentFromDomain(c domain.ToolCallContent) ToolCallContentDTO {
	return ToolCallContentDTO(c)
}

// ToDomain maps a decoded DTO back to the domain content.
func (d ToolCallContentDTO) ToDomain() domain.ToolCallContent { return domain.ToolCallContent(d) }

// ToolResultContentDTO is the JSON shape of a tool-result message's content.
type ToolResultContentDTO struct {
	CallID  string `json:"call_id"`
	Output  string `json:"output"`
	IsError bool   `json:"is_error"`
}

// ToolResultContentFromDomain maps the domain content to its DTO.
func ToolResultContentFromDomain(c domain.ToolResultContent) ToolResultContentDTO {
	return ToolResultContentDTO(c)
}

// ToDomain maps a decoded DTO back to the domain content.
func (d ToolResultContentDTO) ToDomain() domain.ToolResultContent {
	return domain.ToolResultContent(d)
}

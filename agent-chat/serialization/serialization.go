// Package serialization holds the wire- and persistence-facing DTOs for
// agent-chat's message-content types, keeping the domain thread package free of
// json struct tags. It sits outside adapters/ so the chat session service (which
// builds and reads the persisted message content) may map through it without
// importing an adapter, and outside domain/ so the tags live away from the
// domain types. The postgres-backed message content (stored as raw JSON), the
// gRPC server, and the openai/anthropic mappers all map through these DTOs, so
// the stored and on-the-wire byte shapes are fixed here.
package serialization

import (
	"github.com/carolsimone/continuo/agent-chat/domain"
)

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

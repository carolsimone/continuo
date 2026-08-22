package ports

import (
	"context"
	"time"

	"github.com/carolsimone/continuo/agent-chat/domain"
	"github.com/google/uuid"
)

// ToolParam is one named string parameter of a tool (CLI positional arg).
type ToolParam struct {
	Name        string
	Description string
	Required    bool
}

// ToolDef is one entry in the closed tool menu offered to the model.
type ToolDef struct {
	Name        string
	Description string
	Params      []ToolParam
	Mutating    bool
	CLIPath     []string
	ParamOrder  []string
}

// ToolCatalog is the closed menu; anything not here does not exist.
type ToolCatalog interface {
	Tools() []ToolDef
	Lookup(name string) (ToolDef, bool)
}

// ToolCall is the model's request to run one tool.
type ToolCall struct {
	ID   string
	Name string
	Args map[string]string
}

// ToolResult is what the model sees back. IsError covers both validation
// rejections and non-zero CLI exits (the CLIError JSON travels in Output).
type ToolResult struct {
	Output  string
	IsError bool
}

// ToolExecutor validates and runs one tool call. It never returns a Go error
// for tool-level failures — those go back to the model inside ToolResult.
type ToolExecutor interface {
	Execute(ctx context.Context, userID string, threadID uuid.UUID, call ToolCall) ToolResult
}

// Usage is the provider-reported token spend of one StreamTurn call.
type Usage struct {
	InputTokens  int
	OutputTokens int
}

// TurnRequest is one provider invocation: history window + tool menu.
type TurnRequest struct {
	System    string
	Messages  []domain.Message
	Tools     []ToolDef
	MaxTokens int
}

// TurnResult is the completed provider response for one invocation.
type TurnResult struct {
	Text      string
	ToolCalls []ToolCall
	Usage     Usage
}

// LLMProvider streams one model response. onDelta receives assistant text
// fragments as they arrive; the returned TurnResult is the complete picture.
type LLMProvider interface {
	StreamTurn(ctx context.Context, req TurnRequest, onDelta func(text string)) (*TurnResult, error)
}

// Clock abstracts time for deterministic tests.
type Clock interface {
	Now() time.Time
}

// Archiver writes a whole thread to cold storage before retention deletes it.
type Archiver interface {
	ArchiveThread(ctx context.Context, t domain.Thread, msgs []domain.Message) error
}

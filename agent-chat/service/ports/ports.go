package ports

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/carolsimone/continuo/agent-chat/domain"
	"github.com/google/uuid"
)

// ToolParam is one named parameter of a tool: a CLI positional argument, or a
// flag the command marks for the chat (Flag), rendered as --<Name> <value>
// after the positionals. A Repeated flag takes a list of strings, which
// ToolCall.Args carries as a JSON array; every other parameter is one string.
type ToolParam struct {
	Name        string
	Description string
	Required    bool
	Flag        bool
	Repeated    bool
}

// Values returns the argv values the parameter takes from its raw value in
// ToolCall.Args. A single-value parameter gives its value, or none when the
// value is blank. A Repeated parameter decodes its JSON array of strings and
// gives every item that is not blank, in order; a blank value or an empty
// array gives none, and anything other than an array of strings is an error.
func (p ToolParam) Values(raw string) ([]string, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	if !p.Repeated {
		return []string{raw}, nil
	}
	var items []string
	if err := json.Unmarshal([]byte(raw), &items); err != nil {
		return nil, fmt.Errorf("parameter %q must be a list of strings", p.Name)
	}
	out := make([]string, 0, len(items))
	for _, item := range items {
		if strings.TrimSpace(item) != "" {
			out = append(out, item)
		}
	}
	return out, nil
}

// ToolDef is one entry in the closed tool menu offered to the model.
// ParamOrder lists the positional parameters in argv order; flag parameters
// are not in it and follow the positionals in the order of Params.
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
	// InputTokens counts prompt tokens processed afresh on this call; tokens
	// served from or written to the provider's prompt cache are reported in
	// the two cache fields instead and are not included here.
	InputTokens  int
	OutputTokens int
	// CacheReadInputTokens is the part of the prompt the provider served from
	// its cache instead of reprocessing.
	CacheReadInputTokens int
	// CacheCreationInputTokens is the part of the prompt the provider
	// processed and wrote into its cache on this call.
	CacheCreationInputTokens int
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

package openai

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"

	"github.com/carolsimone/continuo/agent-chat/service/ports"
)

// compile-time interface assertion.
var _ ports.LLMProvider = (*Provider)(nil)

const (
	sseDataPrefix = "data: "
	sseDone       = "[DONE]"
	// scannerBufSize is the maximum SSE line size (1 MB handles large tool inputs).
	scannerBufSize = 1 << 20
)

// Provider sends requests to an OpenAI-compatible chat-completions API and parses the SSE stream.
// It uses only net/http — no vendor SDK. Setting a custom baseURL enables any compatible endpoint
// (Azure, LiteLLM, Ollama, vLLM, local stub servers).
type Provider struct {
	baseURL string
	apiKey  string
	model   string
	client  *http.Client
}

// NewProvider creates a Provider. client may be nil, in which case http.DefaultClient is used.
// A trailing slash on baseURL is trimmed to avoid double-slash in endpoint paths.
func NewProvider(baseURL, apiKey, model string, client *http.Client) *Provider {
	if client == nil {
		client = http.DefaultClient
	}
	return &Provider{
		baseURL: strings.TrimRight(baseURL, "/"),
		apiKey:  apiKey,
		model:   model,
		client:  client,
	}
}

// requestBody is the JSON body sent to POST /v1/chat/completions.
type requestBody struct {
	Model         string        `json:"model"`
	MaxTokens     int           `json:"max_tokens,omitempty"`
	Stream        bool          `json:"stream"`
	StreamOptions streamOptions `json:"stream_options"`
	Messages      []wireMessage `json:"messages"`
	Tools         []wireTool    `json:"tools,omitempty"`
}

// streamOptions controls SSE stream metadata. include_usage ensures the final
// chunk carries prompt_tokens and completion_tokens.
type streamOptions struct {
	IncludeUsage bool `json:"include_usage"`
}

// StreamTurn sends the conversation to the OpenAI-compatible endpoint, streams the SSE response,
// calls onDelta for each text fragment, and returns the complete TurnResult when the stream ends.
func (p *Provider) StreamTurn(ctx context.Context, req ports.TurnRequest, onDelta func(text string)) (*ports.TurnResult, error) {
	wireMsgs, err := toWireMessages(req.System, req.Messages)
	if err != nil {
		return nil, fmt.Errorf("openai: mapping messages: %w", err)
	}

	body := requestBody{
		Model:         p.model,
		MaxTokens:     req.MaxTokens,
		Stream:        true,
		StreamOptions: streamOptions{IncludeUsage: true},
		Messages:      wireMsgs,
		Tools:         toWireTools(req.Tools),
	}

	encoded, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("openai: marshal request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, p.baseURL+"/v1/chat/completions", bytes.NewReader(encoded))
	if err != nil {
		return nil, fmt.Errorf("openai: build request: %w", err)
	}
	httpReq.Header.Set("content-type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+p.apiKey)

	resp, err := p.client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("openai: http: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("openai: %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	return p.parseStream(resp.Body, onDelta)
}

// streamChunk is one parsed SSE data payload from the chat-completions stream.
type streamChunk struct {
	Choices []streamChoice `json:"choices"`
	Usage   *streamUsage   `json:"usage,omitempty"`
}

// streamChoice is one element of the choices array in a stream chunk.
type streamChoice struct {
	Delta        streamDelta `json:"delta"`
	FinishReason *string     `json:"finish_reason,omitempty"`
}

// streamDelta carries incremental content for text or tool_calls.
type streamDelta struct {
	Content   string              `json:"content,omitempty"`
	ToolCalls []streamToolCallDelta `json:"tool_calls,omitempty"`
}

// streamToolCallDelta is an incremental tool_call fragment. The model sends id and
// function.name only in the first delta for an index; subsequent deltas append to arguments.
type streamToolCallDelta struct {
	Index    int                      `json:"index"`
	ID       string                   `json:"id,omitempty"`
	Function streamToolFunctionDelta  `json:"function"`
}

// streamToolFunctionDelta carries incremental function call data.
type streamToolFunctionDelta struct {
	Name      string `json:"name,omitempty"`
	Arguments string `json:"arguments,omitempty"`
}

// streamUsage is the token usage reported in the final stream chunk.
type streamUsage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
}

// accumulatedToolCall tracks partial state for one in-progress tool call across chunks.
type accumulatedToolCall struct {
	id        string
	name      string
	arguments string
}

// parseStream reads SSE events from r, calls onDelta for text fragments, and builds TurnResult.
func (p *Provider) parseStream(r io.Reader, onDelta func(text string)) (*ports.TurnResult, error) {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, scannerBufSize), scannerBufSize)

	var (
		result  ports.TurnResult
		textBuf strings.Builder
		// toolsByIndex accumulates partial tool call state keyed by the stream's index field.
		toolsByIndex = make(map[int]*accumulatedToolCall)
	)

	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, sseDataPrefix) {
			continue
		}
		raw := strings.TrimPrefix(line, sseDataPrefix)
		if raw == sseDone {
			break
		}

		var chunk streamChunk
		if err := json.Unmarshal([]byte(raw), &chunk); err != nil {
			// Skip unparseable lines; the stream may include comment or keep-alive data lines.
			continue
		}

		// Top-level usage appears in the final chunk (via stream_options.include_usage).
		if chunk.Usage != nil {
			result.Usage.InputTokens = chunk.Usage.PromptTokens
			result.Usage.OutputTokens = chunk.Usage.CompletionTokens
		}

		if len(chunk.Choices) == 0 {
			continue
		}
		delta := chunk.Choices[0].Delta

		// Accumulate text content.
		if delta.Content != "" {
			textBuf.WriteString(delta.Content)
			onDelta(delta.Content)
		}

		// Accumulate tool call fragments by index.
		for _, tc := range delta.ToolCalls {
			acc, ok := toolsByIndex[tc.Index]
			if !ok {
				acc = &accumulatedToolCall{}
				toolsByIndex[tc.Index] = acc
			}
			if tc.ID != "" {
				acc.id = tc.ID
			}
			if tc.Function.Name != "" {
				acc.name = tc.Function.Name
			}
			acc.arguments += tc.Function.Arguments
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("openai: reading stream: %w", err)
	}

	result.Text = textBuf.String()

	// Build final tool calls sorted by index so ordering is deterministic.
	if len(toolsByIndex) > 0 {
		indices := make([]int, 0, len(toolsByIndex))
		for idx := range toolsByIndex {
			indices = append(indices, idx)
		}
		sort.Ints(indices)

		for _, idx := range indices {
			acc := toolsByIndex[idx]
			args, err := parseToolArgs(acc.arguments)
			if err != nil {
				return nil, fmt.Errorf("openai: parse tool args for %s: %w", acc.name, err)
			}
			result.ToolCalls = append(result.ToolCalls, ports.ToolCall{
				ID:   acc.id,
				Name: acc.name,
				Args: args,
			})
		}
	}

	return &result, nil
}

// parseToolArgs unmarshals accumulated JSON arguments into map[string]string.
// An empty or blank string results in an empty map rather than an error.
func parseToolArgs(raw string) (map[string]string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return map[string]string{}, nil
	}
	var generic map[string]any
	if err := json.Unmarshal([]byte(raw), &generic); err != nil {
		return nil, err
	}
	result := make(map[string]string, len(generic))
	for k, v := range generic {
		switch sv := v.(type) {
		case string:
			result[k] = sv
		default:
			b, _ := json.Marshal(v)
			result[k] = string(b)
		}
	}
	return result, nil
}

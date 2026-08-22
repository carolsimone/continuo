package anthropic

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/carolsimone/continuo/agent-chat/service/ports"
)

// compile-time interface assertion.
var _ ports.LLMProvider = (*Provider)(nil)

const (
	anthropicVersion = "2023-06-01"
	sseDataPrefix    = "data: "
	// scannerBufSize is the maximum SSE line size (1 MB handles large tool inputs).
	scannerBufSize = 1 << 20
)

// Provider sends requests to the Anthropic Messages API and parses the SSE stream.
// It uses only net/http — no vendor SDK.
type Provider struct {
	baseURL string
	apiKey  string
	model   string
	client  *http.Client
}

// NewProvider creates a Provider. client may be nil, in which case http.DefaultClient is used.
func NewProvider(baseURL, apiKey, model string, client *http.Client) *Provider {
	if client == nil {
		client = http.DefaultClient
	}
	return &Provider{baseURL: baseURL, apiKey: apiKey, model: model, client: client}
}

// requestBody is the JSON body sent to POST /v1/messages.
type requestBody struct {
	Model     string        `json:"model"`
	MaxTokens int           `json:"max_tokens"`
	Stream    bool          `json:"stream"`
	System    string        `json:"system,omitempty"`
	Messages  []wireMessage `json:"messages"`
	Tools     []wireTool    `json:"tools,omitempty"`
}

// StreamTurn sends the request to Anthropic, streams the SSE response, calls onDelta for
// each text fragment, and returns the complete TurnResult when the stream ends.
func (p *Provider) StreamTurn(ctx context.Context, req ports.TurnRequest, onDelta func(text string)) (*ports.TurnResult, error) {
	wireMsgs, err := toWireMessages(req.Messages)
	if err != nil {
		return nil, fmt.Errorf("anthropic: mapping messages: %w", err)
	}

	body := requestBody{
		Model:     p.model,
		MaxTokens: req.MaxTokens,
		Stream:    true,
		System:    req.System,
		Messages:  wireMsgs,
		Tools:     toWireTools(req.Tools),
	}

	encoded, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("anthropic: marshal request: %w", err)
	}

	//nolint:gosec // G704: p.baseURL is always the hardcoded "https://api.anthropic.com"
	// literal passed by main.go; it is never sourced from operator config or user input.
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, p.baseURL+"/v1/messages", bytes.NewReader(encoded))
	if err != nil {
		return nil, fmt.Errorf("anthropic: build request: %w", err)
	}
	httpReq.Header.Set("content-type", "application/json")
	httpReq.Header.Set("x-api-key", p.apiKey)
	httpReq.Header.Set("anthropic-version", anthropicVersion)

	resp, err := p.client.Do(httpReq) //nolint:gosec // G704: see NewRequestWithContext above; same trusted URL.
	if err != nil {
		return nil, fmt.Errorf("anthropic: http: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("anthropic: %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	return p.parseStream(resp.Body, onDelta)
}

// sseEvent is one parsed event from the stream.
type sseEvent struct {
	Type  string          `json:"type"`
	// message_start fields
	Message *struct {
		Usage *tokenUsage `json:"usage"`
	} `json:"message,omitempty"`
	// content_block_start fields
	Index        int          `json:"index"`
	ContentBlock *contentBlock `json:"content_block,omitempty"`
	// content_block_delta fields
	Delta *delta `json:"delta,omitempty"`
	// message_delta fields
	Usage *tokenUsage `json:"usage,omitempty"`
}

type tokenUsage struct {
	InputTokens  int `json:"input_tokens"`
	OutputTokens int `json:"output_tokens"`
}

type contentBlock struct {
	Type string `json:"type"`
	ID   string `json:"id"`
	Name string `json:"name"`
}

type delta struct {
	Type        string `json:"type"`
	Text        string `json:"text"`
	PartialJSON string `json:"partial_json"`
	StopReason  string `json:"stop_reason"`
}

// toolState tracks accumulated state for a single in-progress tool_use block.
type toolState struct {
	id          string
	name        string
	partialJSON string
}

// parseStream reads SSE events from r and builds the TurnResult.
func (p *Provider) parseStream(r io.Reader, onDelta func(text string)) (*ports.TurnResult, error) {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, scannerBufSize), scannerBufSize)

	var (
		result   ports.TurnResult
		textBuf  strings.Builder
		tools    = make(map[int]*toolState) // keyed by content block index
	)

	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, sseDataPrefix) {
			continue
		}
		raw := strings.TrimPrefix(line, sseDataPrefix)
		if raw == "[DONE]" {
			break
		}

		var ev sseEvent
		if err := json.Unmarshal([]byte(raw), &ev); err != nil {
			// Skip unparseable lines rather than failing; the stream may contain
			// comment or keep-alive lines beyond the data prefix.
			continue
		}

		switch ev.Type {
		case "message_start":
			if ev.Message != nil && ev.Message.Usage != nil {
				result.Usage.InputTokens = ev.Message.Usage.InputTokens
				// output_tokens in message_start is typically 1 (prefill); the
				// authoritative count comes from message_delta.
			}

		case "content_block_start":
			if ev.ContentBlock != nil && ev.ContentBlock.Type == "tool_use" {
				tools[ev.Index] = &toolState{
					id:   ev.ContentBlock.ID,
					name: ev.ContentBlock.Name,
				}
			}

		case "content_block_delta":
			if ev.Delta == nil {
				continue
			}
			switch ev.Delta.Type {
			case "text_delta":
				textBuf.WriteString(ev.Delta.Text)
				onDelta(ev.Delta.Text)
			case "input_json_delta":
				if ts, ok := tools[ev.Index]; ok {
					ts.partialJSON += ev.Delta.PartialJSON
				}
			}

		case "content_block_stop":
			// Finalise a tool_use block: parse accumulated JSON into map[string]string.
			if ts, ok := tools[ev.Index]; ok {
				args, err := parseToolArgs(ts.partialJSON)
				if err != nil {
					return nil, fmt.Errorf("anthropic: parse tool args for %s: %w", ts.name, err)
				}
				result.ToolCalls = append(result.ToolCalls, ports.ToolCall{
					ID:   ts.id,
					Name: ts.name,
					Args: args,
				})
				delete(tools, ev.Index)
			}

		case "message_delta":
			if ev.Usage != nil {
				result.Usage.OutputTokens = ev.Usage.OutputTokens
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("anthropic: reading stream: %w", err)
	}

	result.Text = textBuf.String()
	return &result, nil
}

// parseToolArgs unmarshals accumulated partial JSON into map[string]string.
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

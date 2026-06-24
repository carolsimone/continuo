// Package main is a deterministic OpenAI-compatible server used in e2e tests.
// It listens on :9100 and responds to POST /v1/chat/completions with scripted
// responses so the agent-runner and remediation-agent openai adapters can be
// exercised without a real LLM.
//
// Routing logic (evaluated in order):
//
//  1. propose_fix mode: when the request body contains a tool named "propose_fix"
//     in its tools array, the server returns a NON-STREAMING (stream:false) JSON
//     response with choices[0].message.tool_calls[0] for propose_fix. This is
//     what the remediation-agent's openai adapter expects.
//
//  2. tool-result mode: when the last message role is "tool", respond with a
//     final streaming text chunk ("DONE: ok" or "DONE: error"), finish_reason="stop".
//
//  3. user-message mode: take the last word of the user text as the schedule name.
//     If the text contains "trigger" emit a tool_call for "schedule_trigger", else
//     "schedule_status". finish_reason="tool_calls". Both are SSE streaming.
//
// The SSE chunk shapes match what the agent-runner openai adapter expects:
//   - Text: choices[].delta.content, finish_reason "stop"
//   - Tool call: choices[].delta.tool_calls[{index,id,function:{name,arguments}}]
//   - Stream terminated with "data: [DONE]\n\n"
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
)

func main() {
	http.HandleFunc("/v1/chat/completions", handleChatCompletions)
	log.Println("stub-llm: listening on :9100")
	if err := http.ListenAndServe(":9100", nil); err != nil {
		log.Fatalf("stub-llm: %v", err)
	}
}

// requestPayload is the subset of the OpenAI chat completions request body
// that the stub needs to inspect.
type requestPayload struct {
	Messages []message    `json:"messages"`
	Tools    []toolDef    `json:"tools"`
}

// message is one entry in the messages array.
type message struct {
	Role    string `json:"role"`
	Content any    `json:"content"` // string or []map[string]any for tool results
}

// toolDef is one entry in the tools array.
type toolDef struct {
	Type     string       `json:"type"`
	Function toolFunction `json:"function"`
}

// toolFunction holds the function name from a tool definition.
type toolFunction struct {
	Name string `json:"name"`
}

func handleChatCompletions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req requestPayload
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	// propose_fix mode: when the request tools include propose_fix, return a
	// non-streaming JSON completion. The remediation-agent openai adapter sends
	// stream:false and parses choices[0].message.tool_calls[0].function.
	if hasTool(req.Tools, "propose_fix") {
		writeProposeFixResponse(w)
		return
	}

	// All other requests use SSE streaming (agent-runner chat e2e path).
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)

	if len(req.Messages) == 0 {
		writeSSEDone(w)
		return
	}

	last := req.Messages[len(req.Messages)-1]

	if last.Role == "tool" {
		// A tool result came back — emit a final text response.
		text := extractStringContent(last.Content)
		verdict := "ok"
		lower := strings.ToLower(text)
		if strings.Contains(lower, "denied") || strings.Contains(lower, "error") || strings.Contains(lower, "is_error") {
			verdict = "error"
		}
		writeTextChunk(w, fmt.Sprintf("DONE: %s", verdict))
		writeStopFinish(w)
	} else {
		// A user message — emit a tool call.
		text := extractStringContent(last.Content)
		words := strings.Fields(text)
		scheduleName := "e2e-schedule" // safe default
		if len(words) > 0 {
			scheduleName = words[len(words)-1]
		}

		toolName := "schedule_status"
		if strings.Contains(strings.ToLower(text), "trigger") {
			toolName = "schedule_trigger"
		}

		writeToolCallChunk(w, toolName, scheduleName)
		writeToolCallFinish(w)
	}

	writeSSEDone(w)
	flush(w)
}

// hasTool reports whether any entry in tools has the given function name.
func hasTool(tools []toolDef, name string) bool {
	for _, t := range tools {
		if t.Function.Name == name {
			return true
		}
	}
	return false
}

// writeProposeFixResponse returns a deterministic, non-streaming OpenAI
// chat-completions response that forces the propose_fix tool call. The
// remediation-agent openai adapter reads choices[0].message.tool_calls[0].
func writeProposeFixResponse(w http.ResponseWriter) {
	args, _ := json.Marshal(map[string]string{
		"proposed_sql": "select c.id from e2e_schema.ftable_c c",
		"rationale":    "removed reference to nonexistent relation public.wrong_name",
		"confidence":   "high",
	})
	resp := map[string]any{
		"choices": []map[string]any{
			{
				"message": map[string]any{
					"role": "assistant",
					"tool_calls": []map[string]any{
						{
							"id":   "call_stub_propose_001",
							"type": "function",
							"function": map[string]any{
								"name":      "propose_fix",
								"arguments": string(args),
							},
						},
					},
				},
				"finish_reason": "tool_calls",
			},
		},
	}
	body, err := json.Marshal(resp)
	if err != nil {
		log.Printf("stub-llm: marshal propose_fix response: %v", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}

// extractStringContent coerces a message Content (string or structured) to string.
func extractStringContent(content any) string {
	switch v := content.(type) {
	case string:
		return v
	case []any:
		// Tool result content can be an array of {type, text} blocks.
		var parts []string
		for _, item := range v {
			if m, ok := item.(map[string]any); ok {
				if t, ok := m["text"].(string); ok {
					parts = append(parts, t)
				}
			}
		}
		return strings.Join(parts, " ")
	default:
		b, _ := json.Marshal(content)
		return string(b)
	}
}

// writeTextChunk emits an SSE data line with a text delta chunk.
func writeTextChunk(w http.ResponseWriter, text string) {
	chunk := map[string]any{
		"choices": []map[string]any{
			{
				"delta": map[string]any{
					"content": text,
				},
				"finish_reason": nil,
			},
		},
	}
	writeSSEChunk(w, chunk)
}

// writeStopFinish emits a final chunk with finish_reason "stop".
func writeStopFinish(w http.ResponseWriter) {
	stopReason := "stop"
	chunk := map[string]any{
		"choices": []map[string]any{
			{
				"delta":         map[string]any{},
				"finish_reason": stopReason,
			},
		},
	}
	writeSSEChunk(w, chunk)
}

// writeToolCallChunk emits a single SSE data line with a tool_call delta.
// The agent-runner openai adapter accumulates tool calls by index; sending the
// complete name+arguments in one chunk is sufficient (the adapter appends
// arguments across chunks).
func writeToolCallChunk(w http.ResponseWriter, toolName, scheduleName string) {
	args, _ := json.Marshal(map[string]string{"schedule-name": scheduleName})
	chunk := map[string]any{
		"choices": []map[string]any{
			{
				"delta": map[string]any{
					"tool_calls": []map[string]any{
						{
							"index": 0,
							"id":    "call_stub_001",
							"function": map[string]any{
								"name":      toolName,
								"arguments": string(args),
							},
						},
					},
				},
				"finish_reason": nil,
			},
		},
	}
	writeSSEChunk(w, chunk)
}

// writeToolCallFinish emits a final chunk with finish_reason "tool_calls".
func writeToolCallFinish(w http.ResponseWriter) {
	finishReason := "tool_calls"
	chunk := map[string]any{
		"choices": []map[string]any{
			{
				"delta":         map[string]any{},
				"finish_reason": finishReason,
			},
		},
	}
	writeSSEChunk(w, chunk)
}

// writeSSEChunk marshals the payload and writes it as "data: <json>\n\n".
func writeSSEChunk(w http.ResponseWriter, payload any) {
	b, err := json.Marshal(payload)
	if err != nil {
		log.Printf("stub-llm: marshal chunk: %v", err)
		return
	}
	fmt.Fprintf(w, "data: %s\n\n", b)
	flush(w)
}

// writeSSEDone writes the SSE stream terminator.
func writeSSEDone(w http.ResponseWriter) {
	fmt.Fprintf(w, "data: [DONE]\n\n")
	flush(w)
}

// flush flushes the response writer if it supports http.Flusher.
func flush(w http.ResponseWriter) {
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}
}

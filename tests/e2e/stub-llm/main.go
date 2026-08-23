// Package main is a deterministic OpenAI-compatible server used in e2e tests.
// It listens on :9100 and responds to POST /v1/chat/completions with scripted
// responses so the agent-chat and agent-remediation openai adapters can be
// exercised without a real LLM.
//
// Routing logic (evaluated in order):
//
//  1. propose_fix mode: when the request body contains a tool named "propose_fix"
//     in its tools array, the server returns a NON-STREAMING (stream:false) JSON
//     response with choices[0].message.tool_calls[0] for propose_fix. This is
//     what the agent-remediation's openai adapter expects. The agent-remediation
//     sends three propose_fix variants that share the tool name but differ in
//     their parameter schema, so the stub routes on the declared parameters:
//     compile (target_file) → target_file + corrected proposed_content; seed
//     (proposed_content, no target_file) → corrected CSV proposed_content;
//     validation (proposed_sql) → the two-step candidate/source SQL fix, which
//     further branches on whether the user message contains "Original model
//     source" (Step-2 marker from prompt.AssembleSourceFix): if so it returns the
//     corrected real model source (using {{ ref(...) }} macros), otherwise the
//     Step-1 candidate-SQL fix (compiled SQL, no macros).
//
//  2. propose_python_fix mode: when the request body contains a tool named
//     "propose_python_fix", the server returns a NON-STREAMING response whose
//     tool call carries an updated_files array — the multi-file answer the
//     python-node contract fixer expects. The reply is derived from the prompt
//     rather than canned: the stub reads back the contract file it was shown
//     and returns it with the unbindable relation in its declared read
//     replaced, which is what a model asked to fix that failure would do. See
//     writeProposePythonFixResponse for which replacement each case gets.
//
//  3. tool-result mode: when the last message role is "tool", respond with a
//     final streaming text chunk ("DONE: ok" or "DONE: error"), finish_reason="stop".
//
//  4. user-message mode: take the last word of the user text as the schedule name.
//     If the text contains "trigger" emit a tool_call for "schedule_trigger", else
//     "schedule_status". finish_reason="tool_calls". Both are SSE streaming.
//
// The SSE chunk shapes match what the agent-chat openai adapter expects:
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
	Messages []message `json:"messages"`
	Tools    []toolDef `json:"tools"`
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

// toolFunction holds the function name and parameter schema from a tool
// definition. The parameter property names are how the stub tells the three
// propose_fix variants apart (they all share the tool name propose_fix):
// compile carries target_file, seed carries proposed_content (no target_file),
// and validation carries proposed_sql.
type toolFunction struct {
	Name       string     `json:"name"`
	Parameters toolParams `json:"parameters"`
}

// toolParams is the subset of the JSON-Schema parameter object the stub needs:
// the set of property names.
type toolParams struct {
	Properties map[string]json.RawMessage `json:"properties"`
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
	// non-streaming JSON completion. The agent-remediation openai adapter sends
	// stream:false and parses choices[0].message.tool_calls[0].function.
	// Branch on the user-content marker to distinguish Step-1 (candidate SQL)
	// from Step-2 (real source fix via stub-github).
	// propose_python_fix mode: the python-node contract fixer forces its own
	// tool, whose answer is a list of complete files rather than a single
	// content string. Checked before propose_fix because the two are distinct
	// tools with incompatible answer shapes.
	if hasTool(req.Tools, "propose_python_fix") {
		writeProposePythonFixResponse(w, lastUserContent(req.Messages))
		return
	}

	if hasTool(req.Tools, "propose_fix") {
		userContent := lastUserContent(req.Messages)
		writeProposeFixResponse(w, userContent, proposeFixParams(req.Tools))
		return
	}

	// All other requests use SSE streaming (agent-chat chat e2e path).
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

// proposeFixParams returns the set of parameter property names declared on the
// propose_fix tool. The three fix variants share the tool name but differ in
// their parameters, so this set is how the stub routes: target_file ⇒ compile,
// proposed_content-without-target_file ⇒ seed, proposed_sql ⇒ validation.
func proposeFixParams(tools []toolDef) map[string]bool {
	set := map[string]bool{}
	for _, t := range tools {
		if t.Function.Name != "propose_fix" {
			continue
		}
		for name := range t.Function.Parameters.Properties {
			set[name] = true
		}
	}
	return set
}

// firstShownFile returns the path of the first "File <path>:" block in a
// compile-fix prompt. prompt.AssembleCompileFix renders the offending file
// first, so this is the file the stub targets. Returns "" if none is found, in
// which case the compile fixer defaults target_file to the offending file.
func firstShownFile(userContent string) string {
	const marker = "File "
	for _, line := range strings.Split(userContent, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, marker) && strings.HasSuffix(trimmed, ":") {
			return strings.TrimSuffix(strings.TrimPrefix(trimmed, marker), ":")
		}
	}
	return ""
}

// compileFixContent is the corrected dbt model the stub returns for a compile
// fix: the malformed-Jinja fixture (config() closed too early, tags hoisted
// out) with the config() call closed correctly so dbt can parse it.
const compileFixContent = `{{ config(materialized='table', tags=['daily']) }}
select 1 as id
`

// seedFixContent is the corrected CSV the stub returns for a seed fix: a row
// whose comma-bearing field is quoted so dbt seed can load it.
const seedFixContent = "id,name\n1,\"a,b\"\n"

// step2Marker is the string present in the user message when the
// agent-remediation sends a Step-2 (real-source fix) request. It is produced
// by prompt.AssembleSourceFix, which prefixes the user body with
// "Original model source:".
const step2Marker = "Original model source"

// step2SourceFix is the corrected dbt model source returned by the stub when
// the Step-2 marker is detected. It removes the bad join (public.silly_error /
// public.wrong_name) while preserving the real {{ ref(...) }} macros from the
// stub-github canned source.
const step2SourceFix = `{{ config(materialized='table') }}
select *
from {{ ref('table_b') }}
join {{ ref('table_c') }} using (id)`

// Relation names the python-node contract fixtures declare and the stub
// rewrites between. Only bindingRead names a relation the e2e test actually
// creates in the warehouse; a contract left pointing at any of the others
// fails the validation Job's bind check.
//
// loopBrokenRead deliberately shares no substring with badReadBrokenRead, so
// the two fixtures can be told apart by a plain Contains check.
const (
	badReadBrokenRead = "public.wrong_name"
	loopBrokenRead    = "public.loop_wrong_name"
	stillBrokenRead   = "public.still_wrong_name"
	bindingRead       = "public.right_name"
)

// contractFilePath returns the repository path of the contract file the prompt
// shows, read back from the "Contract file <path> that declares it:" line
// prompt.AssemblePythonContractFix renders. Returning the path from the prompt
// rather than a canned constant keeps the answer valid for whichever fixture
// is being repaired. Returns "" when the prompt shows no contract file.
func contractFilePath(userContent string) string {
	const prefix = "Contract file "
	const suffix = " that declares it:"
	for _, line := range strings.Split(userContent, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, prefix) && strings.HasSuffix(trimmed, suffix) {
			return strings.TrimSuffix(strings.TrimPrefix(trimmed, prefix), suffix)
		}
	}
	return ""
}

// contractYAML returns the verbatim contract file the prompt shows, taken from
// the one ```yaml fenced block prompt.AssemblePythonContractFix renders.
// Returns "" when the prompt carries no such block.
func contractYAML(userContent string) string {
	const open = "```yaml\n"
	start := strings.Index(userContent, open)
	if start < 0 {
		return ""
	}
	rest := userContent[start+len(open):]
	end := strings.Index(rest, "\n```")
	if end < 0 {
		return ""
	}
	return rest[:end]
}

// priorAttemptsHeading opens the section prompt.AssemblePythonContractFix
// renders for the earlier attempts at the same failure. The section exists only
// when an earlier attempt was recorded, so the heading is what tells a retry
// apart from a first attempt.
const priorAttemptsHeading = "Previous fix attempts for this node"

// promptAroundContract returns the request with the contract file it showed cut
// out of it: what the prompt assembler wrote AROUND that file — the failure, the
// runner log, the upstream and precedent sections, the earlier attempts — and
// nothing the team's repository itself holds.
//
// Every decision the stub makes about the request as a whole is read from this
// rather than from the raw request, so no contract fixture can decide which
// answer comes back. A comment inside a fixture naming one of the relations
// below would otherwise flip the answer, and would do so on the FIRST attempt —
// handing back the fix that is supposed to arrive only on a retry, and making a
// test that drives the retry loop pass its first shadow release instead.
func promptAroundContract(userContent, contract string) string {
	if contract == "" {
		return userContent
	}
	return strings.Replace(userContent, contract, "", 1)
}

// isRetryShownTheRejectedRead reports whether this request is a retry that was
// actually shown what the earlier attempt did: the prompt carries the
// prior-attempts section, and that section names the relation the earlier
// attempt declared.
//
// Both halves are required, and both are read from around the contract file.
// The heading alone would answer a retry correctly even when the evidence the
// retry is supposed to learn from went missing, which is the one thing the
// e2e test driving this loop exists to prove. The relation name alone is a
// weaker guard than it looks: it is satisfied by that name appearing anywhere,
// including in text the stub was never meant to read.
func isRetryShownTheRejectedRead(userContent, contract string) bool {
	around := promptAroundContract(userContent, contract)
	return strings.Contains(around, priorAttemptsHeading) && strings.Contains(around, stillBrokenRead)
}

// writeProposePythonFixResponse returns a deterministic, non-streaming answer
// for the python-node contract fixer: the contract file the prompt showed,
// with the relation in its declared read rewritten.
//
// Which rewrite depends on the fixture, so one stub drives both e2e scenarios:
//
//   - the loop fixture (loopBrokenRead) on a FIRST attempt gets
//     stillBrokenRead — a relation that does not exist either, so the shadow
//     release verifying it is rejected and its error becomes the next
//     attempt's evidence;
//   - the loop fixture on a retry gets bindingRead, so the second shadow
//     release validates. A retry is recognised by isRetryShownTheRejectedRead:
//     the prompt's prior-attempts section exists AND it names the relation the
//     earlier attempt declared, which reaches the prompt only through that
//     attempt's recorded verification error or the diff it applied. A retry
//     whose prompt lost that evidence therefore keeps getting the answer that
//     already failed, and the e2e test driving it never goes green;
//   - every other fixture gets bindingRead immediately.
//
// A prompt with no contract file in it yields an empty updated_files list,
// which the fixer records as a failed attempt — a legible failure rather than
// a malformed file written onto a checkout.
func writeProposePythonFixResponse(w http.ResponseWriter, userContent string) {
	path := contractFilePath(userContent)
	original := contractYAML(userContent)

	var files []map[string]string
	if path != "" && original != "" {
		var fixed string
		switch {
		case strings.Contains(original, loopBrokenRead):
			replacement := stillBrokenRead
			if isRetryShownTheRejectedRead(userContent, original) {
				replacement = bindingRead
			}
			fixed = strings.ReplaceAll(original, loopBrokenRead, replacement)
		default:
			fixed = strings.ReplaceAll(original, badReadBrokenRead, bindingRead)
		}
		files = []map[string]string{{"path": path, "content": fixed}}
	}

	args, err := json.Marshal(map[string]any{
		"updated_files": files,
		"rationale":     "pointed the declared read at a relation that exists",
		"confidence":    "high",
	})
	if err != nil {
		log.Printf("stub-llm: marshal propose_python_fix arguments: %v", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	writeToolCallCompletion(w, "propose_python_fix", string(args))
}

// lastUserContent scans messages in reverse to find the most recent user-role
// message and returns its string content. Returns "" if none is found.
func lastUserContent(messages []message) string {
	for i := len(messages) - 1; i >= 0; i-- {
		if messages[i].Role == "user" {
			return extractStringContent(messages[i].Content)
		}
	}
	return ""
}

// writeProposeFixResponse returns a deterministic, non-streaming OpenAI
// chat-completions response that forces the propose_fix tool call. The
// agent-remediation openai adapter reads choices[0].message.tool_calls[0]. The
// response fields are chosen from the tool's parameter set so each fix variant
// gets a valid answer:
//
//   - compile (target_file present): the corrected offending file in
//     proposed_content, targeting the first file shown in the prompt.
//   - seed (proposed_content, no target_file): the corrected CSV in
//     proposed_content.
//   - validation (proposed_sql): the two-step candidate/source SQL fix, branching
//     on step2Marker as before.
func writeProposeFixResponse(w http.ResponseWriter, userContent string, params map[string]bool) {
	var toolArgs map[string]string
	switch {
	case params["target_file"]:
		// Compile fix: return which shown file to change and its corrected content.
		toolArgs = map[string]string{
			"target_file":      firstShownFile(userContent),
			"proposed_content": compileFixContent,
			"rationale":        "closed the config() call and moved tags inside it so dbt can parse the model",
			"confidence":       "high",
		}
	case params["proposed_content"]:
		// Seed fix: return the corrected CSV content.
		toolArgs = map[string]string{
			"proposed_content": seedFixContent,
			"rationale":        "quoted the field containing a comma so the seed row parses",
			"confidence":       "high",
		}
	default:
		// Validation fix: proposed_sql, branching on the Step-2 marker.
		proposedSQL := "select c.id from e2e_schema.ftable_c c"
		rationale := "removed reference to nonexistent relation public.wrong_name"
		if strings.Contains(userContent, step2Marker) {
			// Step-2: corrected real model source (preserves dbt macros).
			proposedSQL = step2SourceFix
			rationale = "removed join to nonexistent relation; preserved {{ ref(...) }} macros"
		}
		toolArgs = map[string]string{
			"proposed_sql": proposedSQL,
			"rationale":    rationale,
			"confidence":   "high",
		}
	}
	args, _ := json.Marshal(toolArgs)
	writeToolCallCompletion(w, "propose_fix", string(args))
}

// writeToolCallCompletion writes a non-streaming chat-completions response
// whose single choice forces one tool call with the given name and
// JSON-encoded arguments — the shape the agent-remediation openai adapter
// parses (choices[0].message.tool_calls[0].function). Every forced-tool answer
// this stub gives goes through here, so the envelope cannot drift between
// them.
func writeToolCallCompletion(w http.ResponseWriter, toolName, argsJSON string) {
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
								"name":      toolName,
								"arguments": argsJSON,
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
		log.Printf("stub-llm: marshal %s response: %v", toolName, err)
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
// The agent-chat openai adapter accumulates tool calls by index; sending the
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

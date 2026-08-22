package e2e

// Full-loop WebSocket chat e2e tests.
//
// These tests drive the complete path:
//   user → ui WebSocket relay → agent-runner → stub-llm → continuo CLI → final reply
//
// They run inside the e2e container (docker exec ... go test ./...) and reach
// the ui by its compose DNS name (http://ui:8090), unchanged from how
// other e2e tests reach the UI_HTTP_BASE.
//
// The stub-llm server (tests/e2e/stub-llm) must be running for these tests.
// It is wired into docker-compose.yml as the "stub-llm" service. The ui
// runs with AUTH_MODE=dev so /ws/chat accepts cookieless clients as the operator
// DEV_USER with no Origin check.
//
// Schedule fixture: the real seeded schedule name "e2e-schedule" (testScheduleName)
// is used so the agent-runner can successfully invoke the continuo CLI commands
// that reference real catalog entries.

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
)

// chatMessage is a generic WebSocket frame for marshaling/unmarshaling.
type chatMessage struct {
	Type     string `json:"type"`
	Text     string `json:"text,omitempty"`
	ThreadID string `json:"threadId,omitempty"`
	Command  string `json:"command,omitempty"`
	ActionID string `json:"actionId,omitempty"`
	Summary  string `json:"summary,omitempty"`
	Approved *bool  `json:"approved,omitempty"`
	Code     string `json:"code,omitempty"`
	Message  string `json:"message,omitempty"`
}

// dialChatWS opens a WebSocket connection to the ui /ws/chat endpoint.
// UI_HTTP_BASE defaults to http://ui:8090 (compose DNS, matching other e2e tests).
func dialChatWS(t *testing.T) *websocket.Conn {
	t.Helper()
	base := getEnv("UI_HTTP_BASE", "http://ui:8090")

	u, err := url.Parse(base)
	require.NoError(t, err, "UI_HTTP_BASE %q is not a valid URL", base)

	// Convert http(s) to ws(s).
	switch u.Scheme {
	case "http":
		u.Scheme = "ws"
	case "https":
		u.Scheme = "wss"
	}
	u.Path = "/ws/chat"

	// AUTH_MODE=dev: no Origin check, no cookie needed.
	dialer := websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
	}
	conn, resp, err := dialer.Dial(u.String(), http.Header{
		"Origin": []string{base},
	})
	require.NoError(t, err, "WebSocket dial to %s failed", u.String())
	if resp != nil {
		resp.Body.Close()
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

// sendJSON writes a JSON-encoded message to the WebSocket connection.
func sendJSON(t *testing.T, conn *websocket.Conn, msg chatMessage) {
	t.Helper()
	b, err := json.Marshal(msg)
	require.NoError(t, err)
	require.NoError(t, conn.WriteMessage(websocket.TextMessage, b))
}

// expectFrame reads WebSocket frames until it receives one of the wanted types,
// returning it. It fails the test immediately if an error frame arrives
// (unless "error" is in wantedTypes) or if the 60s deadline is exceeded.
func expectFrame(t *testing.T, conn *websocket.Conn, wantedTypes ...string) chatMessage {
	t.Helper()
	wantedSet := make(map[string]bool, len(wantedTypes))
	for _, wt := range wantedTypes {
		wantedSet[wt] = true
	}

	conn.SetReadDeadline(time.Now().Add(60 * time.Second)) //nolint:errcheck
	for {
		_, raw, err := conn.ReadMessage()
		require.NoError(t, err, "WebSocket read error while waiting for %v", wantedTypes)

		var msg chatMessage
		require.NoError(t, json.Unmarshal(raw, &msg), "failed to unmarshal WebSocket frame: %s", raw)

		// Fail immediately on error frames unless the caller explicitly expects one.
		if msg.Type == "error" && !wantedSet["error"] {
			t.Fatalf("unexpected error frame: code=%q message=%q (waiting for %v)", msg.Code, msg.Message, wantedTypes)
		}

		if wantedSet[msg.Type] {
			return msg
		}
		// Skip intermediary frames (text, history, tool ticks not yet of interest, etc.)
	}
}

// boolPtr returns a pointer to a bool value.
func boolPtr(b bool) *bool { return &b }

// TestChat_StatusQuestionAnswered verifies that a plain status question flows
// through the full loop and returns a "DONE" final text.
//
// Path: user asks about e2e-schedule status → stub-llm emits schedule_status
// tool_call → agent-runner invokes CLI → stub-llm emits DONE: ok → final frame.
func TestChat_StatusQuestionAnswered(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	conn := dialChatWS(t)

	// The server sends a thread frame on connection.
	thread := expectFrame(t, conn, "thread")
	require.NotEmpty(t, thread.ThreadID, "thread frame must carry a non-empty threadId")

	// Send the status question using the real seeded schedule name.
	sendJSON(t, conn, chatMessage{
		Type: "user_message",
		Text: fmt.Sprintf("how is %s", testScheduleName),
	})

	// Expect a tool frame whose command references schedule status.
	tool := expectFrame(t, conn, "tool")
	require.Contains(t, tool.Command, "schedule status",
		"tool frame command should reference 'schedule status', got: %q", tool.Command)

	// Expect the final text frame containing "DONE".
	final := expectFrame(t, conn, "final")
	require.Contains(t, final.Text, "DONE",
		"final text should contain 'DONE', got: %q", final.Text)
}

// TestChat_TriggerRequiresConfirmAndRuns verifies that a trigger request goes
// through the confirm gate and, when approved, executes the trigger tool and
// returns "DONE: ok".
//
// Path: user asks to trigger → confirm_request gate → user approves →
// stub-llm emits schedule_trigger tool_call → CLI executes → stub-llm emits
// DONE: ok → final frame.
func TestChat_TriggerRequiresConfirmAndRuns(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	conn := dialChatWS(t)

	// Consume the thread frame.
	thread := expectFrame(t, conn, "thread")
	require.NotEmpty(t, thread.ThreadID)

	// Request a trigger.
	sendJSON(t, conn, chatMessage{
		Type: "user_message",
		Text: fmt.Sprintf("please trigger %s", testScheduleName),
	})

	// The agent-runner should gate destructive actions behind a confirm_request.
	confirm := expectFrame(t, conn, "confirm_request")
	require.NotEmpty(t, confirm.ActionID, "confirm_request must carry an actionId")
	require.Contains(t, strings.ToLower(confirm.Summary), "trigger",
		"confirm_request summary should mention 'trigger', got: %q", confirm.Summary)

	// Approve the action.
	sendJSON(t, conn, chatMessage{
		Type:     "confirm_response",
		ActionID: confirm.ActionID,
		Approved: boolPtr(true),
	})

	// Now expect a tool frame for schedule trigger.
	tool := expectFrame(t, conn, "tool")
	require.Contains(t, tool.Command, "schedule trigger",
		"tool frame command should reference 'schedule trigger', got: %q", tool.Command)

	// Final frame should contain "DONE: ok".
	final := expectFrame(t, conn, "final")
	require.Contains(t, final.Text, "DONE: ok",
		"final text should contain 'DONE: ok', got: %q", final.Text)
}

// TestChat_TriggerDenialDoesNotRun verifies that denying a confirm_request feeds
// a "denied" result back to the stub-llm which then emits "DONE: error".
//
// Path: user asks to trigger → confirm_request → user denies →
// agent-runner injects denial tool result → stub-llm emits DONE: error → final.
func TestChat_TriggerDenialDoesNotRun(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping E2E test in short mode")
	}

	conn := dialChatWS(t)

	// Consume the thread frame.
	thread := expectFrame(t, conn, "thread")
	require.NotEmpty(t, thread.ThreadID)

	// Request a trigger.
	sendJSON(t, conn, chatMessage{
		Type: "user_message",
		Text: fmt.Sprintf("please trigger %s", testScheduleName),
	})

	// Gate fires.
	confirm := expectFrame(t, conn, "confirm_request")
	require.NotEmpty(t, confirm.ActionID)

	// Deny the action.
	sendJSON(t, conn, chatMessage{
		Type:     "confirm_response",
		ActionID: confirm.ActionID,
		Approved: boolPtr(false),
	})

	// Final text should indicate "DONE: error" because the denial is fed as an
	// error/denied tool result which the stub-llm maps to "DONE: error".
	final := expectFrame(t, conn, "final")
	require.Contains(t, final.Text, "DONE: error",
		"final text after denial should contain 'DONE: error', got: %q", final.Text)
}

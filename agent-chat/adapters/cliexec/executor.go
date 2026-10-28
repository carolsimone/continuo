package cliexec

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/carolsimone/continuo/agent-chat/service/ports"
	"github.com/google/uuid"
)

const truncationNotice = "\n[output truncated]"

// defaultMaxBytes is used when NewExecutor is called with maxBytes <= 0,
// preventing a misconfigured zero from silently blanking all output.
const defaultMaxBytes = 65536

// stderrCap is the maximum number of stderr bytes included in the agent_failed
// fallback payload (Fix 2).
const stderrCap = 500

// agentChatRequestNamespace is the UUIDv5 namespace of the request ids agent-chat
// gives to mutating tool calls. It is fixed so that the id derived from a thread
// and a tool call is the same in every process and after every restart.
var agentChatRequestNamespace = uuid.MustParse("e8241864-5a00-4b98-8b93-8cb46f940484")

// errorEnvelope is the canonical JSON error shape emitted to the model.
type errorEnvelope struct {
	Error errorBody `json:"error"`
}

type errorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// errorJSON builds a valid JSON error envelope using real JSON encoding so
// that arbitrary message text (control chars, quotes, backslashes, etc.) never
// produces invalid JSON. The %q approach it replaces was unsafe because Go's
// %q emits escape sequences (\a, \v, …) that are not valid JSON string escapes.
func errorJSON(code, message string) string {
	b, _ := json.Marshal(errorEnvelope{Error: errorBody{Code: code, Message: message}})
	return string(b)
}

// Executor runs catalogued tools as direct argv exec of the continuo binary.
// No shell is involved at any point: the argv goes straight to execve.
type Executor struct {
	catalog  ports.ToolCatalog
	cliPath  string
	env      []string
	timeout  time.Duration
	maxBytes int
	logger   *slog.Logger
}

var _ ports.ToolExecutor = (*Executor)(nil)

// NewExecutor constructs an Executor. If maxBytes <= 0 it is set to
// defaultMaxBytes (65536) to prevent a misconfigured zero from silently
// blanking all output via truncation.
func NewExecutor(catalog ports.ToolCatalog, cliPath string, env []string, timeout time.Duration, maxBytes int, logger *slog.Logger) *Executor {
	if maxBytes <= 0 {
		maxBytes = defaultMaxBytes
	}
	return &Executor{catalog: catalog, cliPath: cliPath, env: env, timeout: timeout, maxBytes: maxBytes, logger: logger}
}

func reject(format string, a ...any) ports.ToolResult {
	return ports.ToolResult{
		Output:  errorJSON("usage", fmt.Sprintf(format, a...)),
		IsError: true,
	}
}

// orderedArgValues returns the positional argv values for def in ParamOrder
// sequence, skipping absent or whitespace-only entries.
func orderedArgValues(def ports.ToolDef, args map[string]string) []string {
	var out []string
	for _, name := range def.ParamOrder {
		if v, ok := args[name]; ok && strings.TrimSpace(v) != "" {
			out = append(out, v)
		}
	}
	return out
}

// Execute applies the four checks, then execs. Tool-level failures are
// returned to the model in the result, never as Go errors.
func (e *Executor) Execute(ctx context.Context, userID string, threadID uuid.UUID, call ports.ToolCall) ports.ToolResult {
	// Check 1: the tool must exist in the catalog.
	def, ok := e.catalog.Lookup(call.Name)
	if !ok {
		return reject("unknown tool %q", call.Name)
	}
	// Check 2: args must match the schema (all required present, none unknown).
	known := map[string]ports.ToolParam{}
	for _, p := range def.Params {
		known[p.Name] = p
	}
	for name := range call.Args {
		if _, ok := known[name]; !ok {
			return reject("unknown parameter %q for tool %q", name, call.Name)
		}
	}
	for _, p := range def.Params {
		if p.Required && strings.TrimSpace(call.Args[p.Name]) == "" {
			return reject("missing required parameter %q for tool %q", p.Name, call.Name)
		}
	}
	// Check 3: no value may smuggle a flag into the argv.
	for name, val := range call.Args {
		if strings.HasPrefix(val, "-") {
			return reject("parameter %q must not start with '-'", name)
		}
	}
	// Check 4 (assembly): argv is the catalog's CLIPath + ordered positional values.
	// Whitespace-only optional values are skipped so they don't appear as blank tokens.
	argv := append([]string{}, def.CLIPath...)
	argv = append(argv, orderedArgValues(def, call.Args)...)

	cctx, cancel := context.WithTimeout(ctx, e.timeout)
	defer cancel()
	// G204: argv is not an arbitrary tainted string. Checks 1-3 above already
	// enforced that call.Name resolves to a catalogued tool, that call.Args
	// contains only known/required parameters, and that no value can start
	// with '-' (so it can never be smuggled in as a flag). e.cliPath is our
	// own trusted continuo binary, and there is no shell in this call chain.
	cmd := exec.CommandContext(cctx, e.cliPath, argv...) //nolint:gosec // G204: validated argv, direct execve, no shell.
	// A nil e.env intentionally inherits the parent process environment; the
	// continuo CLI is our own trusted binary and needs PATH plus the
	// CONTINUO_* address vars that main injects.
	//
	// Stamp the initiating identity and the request identity for this call.
	// When userID is set (every mutating chat tool is human-approved before
	// Execute runs, so this is the approving operator), CONTINUO_ACTOR carries
	// it so the CLI forwards it as x-continuo-user-id gRPC metadata. A mutating
	// tool also gets CONTINUO_REQUEST_ID, derived from the thread and the tool
	// call id: the CLI sends it as the request id of the run it starts, so a
	// retry of the same approved call cannot start a second run. When neither
	// applies, e.env is left untouched so the nil-inherits-parent behavior is
	// preserved exactly.
	//
	// Because exec.Cmd only inherits the parent environment when cmd.Env is nil,
	// appending to a nil e.env would replace inheritance with a short env and
	// strip PATH and the inherited CONTINUO_* vars. So when e.env is nil we seed
	// the copy from os.Environ() to keep inheritance while adding the variables.
	env := e.env
	var extra []string
	if userID != "" {
		extra = append(extra, "CONTINUO_ACTOR="+userID)
	}
	if def.Mutating {
		extra = append(extra, "CONTINUO_REQUEST_ID="+requestID(threadID, call.ID))
	}
	if len(extra) > 0 {
		base := e.env
		if base == nil {
			base = os.Environ()
		}
		env = append(append([]string(nil), base...), extra...)
	}
	cmd.Env = env
	start := time.Now()
	out, err := cmd.Output() // stdout only; the CLI emits JSON there for both success and error
	elapsed := time.Since(start)

	exitCode := 0
	isError := false
	if err != nil {
		isError = true
		if ee, ok := err.(*exec.ExitError); ok {
			exitCode = ee.ExitCode()
			// out already holds whatever stdout the process wrote (cmd.Output
			// captures it even on non-zero exit), so the model sees the CLIError JSON.
			// If stdout is empty the process likely panicked or wrote only to stderr;
			// surface the stderr text so the failure is visible to the model.
			if len(out) == 0 {
				stderr := strings.TrimSpace(string(ee.Stderr))
				if len(stderr) > stderrCap {
					stderr = stderr[:stderrCap]
				}
				if stderr == "" {
					stderr = "process exited non-zero with no output"
				}
				out = []byte(errorJSON("agent_failed", fmt.Sprintf("exit %d: %s", exitCode, stderr)))
			}
		} else {
			exitCode = -1
			out = []byte(errorJSON("unavailable", err.Error()))
		}
	}
	if len(out) > e.maxBytes {
		out = append(out[:e.maxBytes], []byte(truncationNotice)...)
	}

	// Audit log: who ran what, outcome, duration. This is what makes mutating
	// tools defensible in production.
	e.logger.Info("tool executed",
		"user_id", userID,
		"thread_id", threadID.String(),
		"argv", strings.Join(argv, " "),
		"exit_code", exitCode,
		"duration_ms", elapsed.Milliseconds(),
	)
	return ports.ToolResult{Output: string(out), IsError: isError}
}

// requestID is the stable request id of one tool call: a UUIDv5 of the thread
// id and the tool call id, so the same call always maps to the same id.
func requestID(threadID uuid.UUID, toolCallID string) string {
	return uuid.NewSHA1(agentChatRequestNamespace, []byte(threadID.String()+"\x00"+toolCallID)).String()
}

package cliexec

import (
	"context"
	"fmt"
	"log/slog"
	"os/exec"
	"strings"
	"time"

	"github.com/carolsimone/continuo/agent-runner/service/ports"
	"github.com/google/uuid"
)

const truncationNotice = "\n[output truncated]"

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

func NewExecutor(catalog ports.ToolCatalog, cliPath string, env []string, timeout time.Duration, maxBytes int, logger *slog.Logger) *Executor {
	return &Executor{catalog: catalog, cliPath: cliPath, env: env, timeout: timeout, maxBytes: maxBytes, logger: logger}
}

func reject(format string, a ...any) ports.ToolResult {
	return ports.ToolResult{Output: fmt.Sprintf(`{"error":{"code":"usage","message":%q}}`, fmt.Sprintf(format, a...)), IsError: true}
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
	// Check 4 (assembly): argv is the catalog's path + ordered positional values.
	argv := append([]string{}, def.CLIPath...)
	for _, name := range def.ParamOrder {
		if v, ok := call.Args[name]; ok && v != "" {
			argv = append(argv, v)
		}
	}

	cctx, cancel := context.WithTimeout(ctx, e.timeout)
	defer cancel()
	cmd := exec.CommandContext(cctx, e.cliPath, argv...)
	cmd.Env = e.env
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
		} else {
			exitCode = -1
			out = []byte(fmt.Sprintf(`{"error":{"code":"unavailable","message":%q}}`, err.Error()))
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

// CommandString renders the argv as displayed to the user in tool ticks.
func CommandString(cliName string, def ports.ToolDef, args map[string]string) string {
	parts := append([]string{cliName}, def.CLIPath...)
	for _, name := range def.ParamOrder {
		if v, ok := args[name]; ok && v != "" {
			parts = append(parts, v)
		}
	}
	return strings.Join(parts, " ")
}

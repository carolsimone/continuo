// Package promptlog provides a logging decorator over the LLMProvider port that
// records the full prompt agent-remediation sends to the model — the system
// prompt, the user content, the forced tool, and (when the driver stamps it on
// the context) the failure it addresses. Nothing else captures this: the
// caching decorator stores only the completion, and only the corrected source
// and its diff are persisted as artifacts, so before this there was no record of
// what was actually fed to the LLM for a given proposal.
//
// The prompt content is already sanitized by each Fixer before it reaches this
// port (the shown files, the dbt log, and the precedents all pass through the
// LogSanitizer during assembly), so logging it exposes nothing the request does
// not already send to the external model.
//
// The decorator is wired inside the caching decorator (it wraps the real
// provider; the cache wraps it), so a prompt is logged exactly when the model is
// actually called — a cache hit reuses a prior completion and logs nothing,
// because nothing is fed to the model. It depends only on ports, so application
// code keeps calling the LLMProvider port unchanged and imports no adapter.
package promptlog

import (
	"context"
	"log/slog"

	"github.com/carolsimone/continuo/agent-remediation/service/ports"
)

// logMessage is the slog message under which every fed prompt is recorded.
const logMessage = "llm prompt"

// Failure identifies the failure a prompt was built for. The driver stamps it on
// the context before dispatching to a Fixer so the logged prompt can be tied
// back to the release, node, source, and attempt it addresses.
type Failure struct {
	Source    string
	ReleaseID string
	NodeID    string
	Attempt   int
}

// failureCtxKey is the unexported context key under which the driver stores the
// failure identity.
type failureCtxKey struct{}

// ContextWithFailure returns a context carrying the failure identity for the
// prompt about to be built. The driver sets it alongside the LLM-cache
// idempotency key.
func ContextWithFailure(ctx context.Context, f Failure) context.Context {
	return context.WithValue(ctx, failureCtxKey{}, f)
}

// failureFromContext returns the stamped failure identity and whether one was
// present.
func failureFromContext(ctx context.Context) (Failure, bool) {
	f, ok := ctx.Value(failureCtxKey{}).(Failure)
	return f, ok
}

// LoggingLLMProvider decorates an LLMProvider, logging each request's full
// prompt before delegating to the wrapped provider.
type LoggingLLMProvider struct {
	provider ports.LLMProvider
	logger   *slog.Logger
}

var _ ports.LLMProvider = (*LoggingLLMProvider)(nil)

// New wraps provider so every Propose call logs the prompt it feeds the model.
func New(provider ports.LLMProvider, logger *slog.Logger) *LoggingLLMProvider {
	return &LoggingLLMProvider{provider: provider, logger: logger}
}

// Propose logs the full prompt — the forced tool, the system prompt, and the
// user content, with the failure identity when the context carries it — then
// delegates to the wrapped provider. The result and any error pass through
// unchanged; logging never alters the call's outcome.
func (p *LoggingLLMProvider) Propose(ctx context.Context, req ports.ProposeRequest) (ports.ProposeResult, error) {
	attrs := []any{
		"tool", req.ToolName,
		"system_bytes", len(req.System),
		"user_bytes", len(req.User),
	}
	if f, ok := failureFromContext(ctx); ok {
		attrs = append(attrs,
			"source", f.Source,
			"release", f.ReleaseID,
			"node", f.NodeID,
			"attempt", f.Attempt,
		)
	}
	// The full prompt goes last so the correlation and size fields stay legible
	// ahead of the (potentially large) system and user bodies.
	attrs = append(attrs, "system", req.System, "user", req.User)
	p.logger.InfoContext(ctx, logMessage, attrs...)

	return p.provider.Propose(ctx, req)
}

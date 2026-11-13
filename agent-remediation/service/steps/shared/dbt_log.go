// Package shared holds the steps every flow can use whatever the kind of
// failure: the context sources that read the dbt log, the failure precedents
// and the upstream changes, and the pre-flight checks that every draft must
// pass. Each step depends only on the ports it reads through.
package shared

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
)

// DBTLog is the context source `dbt_log`: the failing node's dbt log, sanitized.
type DBTLog struct {
	evidence  ports.EvidenceReader
	sanitizer ports.LogSanitizer
	logger    *slog.Logger
}

var _ step.ContextSource = (*DBTLog)(nil)

// NewDBTLog builds the dbt_log step. It calls no port until Gather.
func NewDBTLog(evidence ports.EvidenceReader, sanitizer ports.LogSanitizer, logger *slog.Logger) *DBTLog {
	return &DBTLog{evidence: evidence, sanitizer: sanitizer, logger: loggerOrDefault(logger)}
}

// Meta describes the step.
func (*DBTLog) Meta() step.Meta {
	return step.Meta{
		Name:        "dbt_log",
		Kind:        step.KindContext,
		Description: "Reads the sanitized dbt log of the failing node, or an empty text when the failure has no log.",
		Needs:       []remediation.EvidenceKey{remediation.KeyFailure},
		Provides:    []remediation.EvidenceKey{remediation.KeyDBTLog},
	}
}

// Gather fetches and sanitizes the log the failure's target names. An empty URI
// means the failure happened before any dbt Job ran and has no log, and a log
// that is gone for good is likewise not fatal, since a fix can still be
// proposed from the other evidence: both provide an empty text. Any other fetch
// error is transient and is returned so the work is tried again.
func (s *DBTLog) Gather(ctx context.Context, in step.ContextInput) (step.ContextOutput, error) {
	uri := in.Failure.Target.DBTLogURI
	if uri == "" {
		return logOutput(""), nil
	}
	raw, err := s.evidence.Fetch(ctx, uri)
	if errors.Is(err, ports.ErrNotFound) {
		return logOutput(""), nil
	}
	if err != nil {
		return step.ContextOutput{}, fmt.Errorf("fetch dbt log: %w", err)
	}
	return logOutput(s.sanitizer.Sanitize(raw)), nil
}

func logOutput(text string) step.ContextOutput {
	return step.ContextOutput{Evidence: map[remediation.EvidenceKey]any{
		remediation.KeyDBTLog: remediation.Text(text),
	}}
}

func loggerOrDefault(l *slog.Logger) *slog.Logger {
	if l == nil {
		return slog.Default()
	}
	return l
}

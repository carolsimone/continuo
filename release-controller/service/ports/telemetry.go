package ports

import "context"

// Telemetry emits OTEL spans for release-controller state transitions.
// Implementations live in adapters/observability (added in a later PR).
type Telemetry interface {
	ReleaseReceived(ctx context.Context, releaseID string)
	ReleaseParseRequested(ctx context.Context, releaseID string)
	ReleaseParseCompleted(ctx context.Context, releaseID string, ok bool, durationMS int64)
	ReleaseValidationRequested(ctx context.Context, releaseID string, nodeCount int)
	ReleaseValidationCompleted(ctx context.Context, releaseID string, ok bool, okCount, failCount int, durationMS int64)
	ReleasePromoted(ctx context.Context, releaseID string, nodeCount int)
	ReleaseRejected(ctx context.Context, releaseID, reason string, failingNodes []string)
}

// NoOpTelemetry is the default for tests and bring-up.
type NoOpTelemetry struct{}

func (NoOpTelemetry) ReleaseReceived(context.Context, string)                                   {}
func (NoOpTelemetry) ReleaseParseRequested(context.Context, string)                             {}
func (NoOpTelemetry) ReleaseParseCompleted(context.Context, string, bool, int64)                {}
func (NoOpTelemetry) ReleaseValidationRequested(context.Context, string, int)                   {}
func (NoOpTelemetry) ReleaseValidationCompleted(context.Context, string, bool, int, int, int64) {}
func (NoOpTelemetry) ReleasePromoted(context.Context, string, int)                              {}
func (NoOpTelemetry) ReleaseRejected(context.Context, string, string, []string)                 {}

package ports

import (
	"context"
	"time"
)

// Telemetry records release-controller's release lifecycle: the stage
// completions and the candidate outcomes are the release series. Handlers call
// it after the transaction that made the transition commits.
type Telemetry interface {
	ReleaseReceived(ctx context.Context, releaseID string)
	ReleaseParseRequested(ctx context.Context, releaseID string)
	// ReleaseCompileCompleted reports the end of the compile leg: ok is the
	// leg's own result and took is the time the run spent compiling.
	ReleaseCompileCompleted(ctx context.Context, releaseID string, ok bool, took time.Duration)
	// ReleaseParseCompleted reports the end of the parse leg: ok is the leg's
	// own result and took is the time the run spent parsing.
	ReleaseParseCompleted(ctx context.Context, releaseID string, ok bool, took time.Duration)
	ReleaseValidationRequested(ctx context.Context, releaseID string, nodeCount int)
	// ReleaseValidationCompleted reports the end of the validation leg: ok is
	// the leg's own result and took is the time the run spent validating.
	ReleaseValidationCompleted(ctx context.Context, releaseID string, ok bool, okCount, failCount int, took time.Duration)
	ReleaseSeedBuildRequested(ctx context.Context, releaseID string, seedCount int)
	// ReleaseSeedBuildCompleted reports the end of the seed-build leg: ok is
	// the leg's own result and took is the time the run spent building seeds.
	ReleaseSeedBuildCompleted(ctx context.Context, releaseID string, ok bool, took time.Duration)
	ReleasePromoted(ctx context.Context, releaseID string, nodeCount int)
	ReleaseRejected(ctx context.Context, releaseID, reason string, failingNodes []string)
	// VerificationFinished records a verification run's terminal outcome
	// (passed or failed). A verification never promotes or rejects a release,
	// so its terminal decision gets its own call rather than reusing
	// ReleasePromoted/ReleaseRejected.
	VerificationFinished(ctx context.Context, runID, outcome string, nodeCount int)
}

// NoOpTelemetry is the default for tests and bring-up.
type NoOpTelemetry struct{}

func (NoOpTelemetry) ReleaseReceived(context.Context, string)                              {}
func (NoOpTelemetry) ReleaseParseRequested(context.Context, string)                        {}
func (NoOpTelemetry) ReleaseCompileCompleted(context.Context, string, bool, time.Duration) {}
func (NoOpTelemetry) ReleaseParseCompleted(context.Context, string, bool, time.Duration)   {}
func (NoOpTelemetry) ReleaseValidationRequested(context.Context, string, int)              {}
func (NoOpTelemetry) ReleaseValidationCompleted(context.Context, string, bool, int, int, time.Duration) {
}
func (NoOpTelemetry) ReleaseSeedBuildRequested(context.Context, string, int)                 {}
func (NoOpTelemetry) ReleaseSeedBuildCompleted(context.Context, string, bool, time.Duration) {}
func (NoOpTelemetry) ReleasePromoted(context.Context, string, int)                           {}
func (NoOpTelemetry) ReleaseRejected(context.Context, string, string, []string)              {}
func (NoOpTelemetry) VerificationFinished(context.Context, string, string, int)              {}

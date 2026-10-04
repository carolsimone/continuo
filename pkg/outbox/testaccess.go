package outbox

import (
	"context"
	"log/slog"
)

// NewPostgresRepositoryForTest exposes the concrete repository (with
// ScheduleRetry and CountTerminal) to package-external tests. Production code
// uses NewPostgresRepository (interface) for writers and the processor builds
// the concrete type in-package.
func NewPostgresRepositoryForTest(exec Executor, tableName string, logger *slog.Logger) *postgresRepository {
	return newPostgresRepository(exec, tableName, logger)
}

// ClaimQueryForTest exposes the relay's claim query so tests can check its plan.
func ClaimQueryForTest(table string) string { return claimQuery(table) }

// RefillClaimQueryForTest exposes the relay's later claim within one batch so
// tests can check its plan.
func RefillClaimQueryForTest(table string) string { return refillClaimQuery(table) }

// DrainForTest runs one drain of the processor, as Run does on each wake.
func (p *Processor) DrainForTest(ctx context.Context) { p.drain(ctx) }

// SetAfterDrainHookForTest makes the processor call f each time a drain ends.
// Call it before Run.
func (p *Processor) SetAfterDrainHookForTest(f func()) { p.afterDrain = f }

package outbox

import "log/slog"

// NewPostgresRepositoryForTest exposes the concrete repository (with
// ScheduleRetry and CountTerminal) to package-external tests. Production code
// uses NewPostgresRepository (interface) for writers and the processor builds
// the concrete type in-package.
func NewPostgresRepositoryForTest(exec Executor, tableName string, logger *slog.Logger, opts ...Option) *postgresRepository {
	return newPostgresRepository(exec, tableName, logger, opts...)
}

// ClaimQueryForTest exposes the relay's claim query so tests can check its plan.
func ClaimQueryForTest(table string) string { return claimQuery(table, false) }

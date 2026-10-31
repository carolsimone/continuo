package outbox

import (
	"log/slog"

	"github.com/carolsimone/continuo/pkg/retention"
)

// OutboxRetentionTarget returns a retention.Target that prunes processed rows
// from the given outbox table via the package's Postgres repository.
func OutboxRetentionTarget(exec Executor, tableName string, logger *slog.Logger) retention.Target {
	repo := &postgresRepository{exec: exec, tableName: tableName, logger: logger}
	return retention.Target{
		Name:  tableName,
		Prune: repo.DeleteProcessedOlderThan,
	}
}

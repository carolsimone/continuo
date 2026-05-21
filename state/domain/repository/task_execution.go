package repository

import (
	"context"

	"github.com/carolsimone/continuo/state/domain/events"
)

// TaskExecutionWriter persists a recorded task execution inside the
// repository's bound transaction. The adapter maps the domain event to its
// storage row.
type TaskExecutionWriter interface {
	CreateRecord(ctx context.Context, evt events.TaskExecutionRecorded) error
}

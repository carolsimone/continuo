package repository

import (
	"context"

	"github.com/carolsimone/continuo/state/domain/events"
	"github.com/jmoiron/sqlx"
)

// TaskExecutionWriter persists a recorded task execution inside the caller's
// transaction. The adapter maps the domain event to its storage row.
type TaskExecutionWriter interface {
	CreateRecordTx(ctx context.Context, tx *sqlx.Tx, evt events.TaskExecutionRecorded) error
}

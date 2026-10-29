package ports

import (
	"context"

	"github.com/carolsimone/continuo/run-controller/domain/aggregate/run"
	"github.com/google/uuid"
)

// OutboxPublisher records domain events for publication to their streams inside
// the publisher's bound transaction, so an event is recorded if and only if the
// transaction that raised it commits. The implementation switches on each
// event's concrete type to derive stream_name, event_type, payload,
// aggregate_type, and retry budget.
//
// msgProcID is the message_processing.id that originated the mutation;
// nil-equivalent (uuid.Nil) is acceptable for events not triggered by an
// inbound Redis message (e.g. user-initiated gRPC commands).
type OutboxPublisher interface {
	Append(ctx context.Context, events []run.DomainEvent, msgProcID uuid.UUID) error
}

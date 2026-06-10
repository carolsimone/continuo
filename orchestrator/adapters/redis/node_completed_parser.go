package redis

import (
	"fmt"

	"github.com/carolsimone/continuo/orchestrator/domain/model"
	"github.com/carolsimone/continuo/pkg/events"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
)

// ParseNodeCompleted translates a node.updated:v1 XMessage into a typed
// NodeCompletedInput. Every field is extracted defensively (no panicking type
// assertions, no uuid.MustParse): a missing/non-string field or a malformed
// UUID yields an events.ErrPermanent-wrapped error so the consumer ACKs the
// poison message instead of crash-looping on it.
func ParseNodeCompleted(msg goredis.XMessage) (model.NodeCompletedInput, error) {
	taskID, err := requireUUID(msg, "task_id")
	if err != nil {
		return model.NodeCompletedInput{}, err
	}
	scheduleID, err := requireUUID(msg, "schedule_id")
	if err != nil {
		return model.NodeCompletedInput{}, err
	}
	scheduleName, err := requireString(msg, "schedule_name")
	if err != nil {
		return model.NodeCompletedInput{}, err
	}
	serviceName, err := requireString(msg, "service_name")
	if err != nil {
		return model.NodeCompletedInput{}, err
	}
	schemaName, err := requireString(msg, "schema_name")
	if err != nil {
		return model.NodeCompletedInput{}, err
	}
	tableName, err := requireString(msg, "table_name")
	if err != nil {
		return model.NodeCompletedInput{}, err
	}
	status, err := requireString(msg, "status")
	if err != nil {
		return model.NodeCompletedInput{}, err
	}

	return model.NodeCompletedInput{
		TaskID:       taskID,
		ScheduleID:   scheduleID,
		ScheduleName: scheduleName,
		ServiceName:  serviceName,
		SchemaName:   schemaName,
		TableName:    tableName,
		Status:       status,
	}, nil
}

// requireString extracts a non-empty string field, returning an
// events.ErrPermanent-wrapped error when the field is absent, not a string, or
// empty. Shared by the orchestrator stream parsers.
func requireString(msg goredis.XMessage, field string) (string, error) {
	raw, ok := msg.Values[field].(string)
	if !ok {
		return "", fmt.Errorf("%w: message %s missing or non-string field %q", events.ErrPermanent, msg.ID, field)
	}
	if raw == "" {
		return "", fmt.Errorf("%w: message %s has empty field %q", events.ErrPermanent, msg.ID, field)
	}
	return raw, nil
}

// requireUUID extracts a string field and parses it as a UUID, returning an
// events.ErrPermanent-wrapped error on a missing/non-string/empty/malformed
// value.
func requireUUID(msg goredis.XMessage, field string) (uuid.UUID, error) {
	raw, err := requireString(msg, field)
	if err != nil {
		return uuid.UUID{}, err
	}
	id, err := uuid.Parse(raw)
	if err != nil {
		return uuid.UUID{}, fmt.Errorf("%w: message %s field %q is not a valid UUID %q: %v", events.ErrPermanent, msg.ID, field, raw, err)
	}
	return id, nil
}

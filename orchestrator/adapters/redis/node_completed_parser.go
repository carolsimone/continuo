package redis

import (
	"fmt"

	"github.com/carolsimone/continuo/orchestrator/domain/model"
	"github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/identity"
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

// optionalInitiatedBy extracts the initiating-user provenance from a trigger
// message. The field is optional: messages from a state version predating
// provenance tracking omit it, and the run is then recorded as the "system"
// sentinel rather than rejected.
func optionalInitiatedBy(msg goredis.XMessage) string {
	raw, ok := msg.Values["initiated_by"].(string)
	if !ok || raw == "" {
		return identity.SystemUserID
	}
	return raw
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

// requireUUIDField validates that a required field is a non-empty, well-formed
// UUID and returns it in its original string form. The trigger handler inputs
// keep these identifiers as strings, but they are UUID-valued and the downstream
// handlers uuid.Parse them; validating here ensures a malformed UUID is rejected
// as poison (events.ErrPermanent) at the parser instead of being retried forever
// by the handler's plain parse error.
func requireUUIDField(msg goredis.XMessage, field string) (string, error) {
	raw, err := requireString(msg, field)
	if err != nil {
		return "", err
	}
	if err := validUUIDValue(msg.ID, field, raw); err != nil {
		return "", err
	}
	return raw, nil
}

// validUUIDValue parses an already-extracted UUID string (e.g. an optional field
// validated for presence elsewhere), returning an events.ErrPermanent-wrapped
// error on a malformed value.
func validUUIDValue(msgID, field, raw string) error {
	if _, err := uuid.Parse(raw); err != nil {
		return fmt.Errorf("%w: message %s field %q is not a valid UUID %q: %v", events.ErrPermanent, msgID, field, raw, err)
	}
	return nil
}

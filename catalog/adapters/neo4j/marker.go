package neo4jinfra

import (
	"context"
	"errors"

	"github.com/carolsimone/continuo/catalog/service/uow"
)

// markerRecordedCypher finds a group's marker for a message by its stream
// message id, or by the outbox entry id the message carries, so an outbox row
// re-sent under a fresh message id counts as the same message. A null
// $outbox_entry_id matches no marker.
const markerRecordedCypher = `
RETURN EXISTS { MATCH (:ProcessedMessage {tenant_id: $tenant_id, group: $group, message_id: $message_id}) }
    OR EXISTS { MATCH (:ProcessedMessage {tenant_id: $tenant_id, group: $group, outbox_entry_id: $outbox_entry_id}) }
    AS recorded`

// markerCreateCypher records that the group processed the message. SET leaves
// outbox_entry_id absent when the message carries none.
const markerCreateCypher = `
CREATE (p:ProcessedMessage {tenant_id: $tenant_id, group: $group, message_id: $message_id, processed_at: datetime()})
SET p.outbox_entry_id = $outbox_entry_id`

// validateMarker refuses a marker that cannot name one message of one group
// of one tenant.
func validateMarker(m uow.Marker) error {
	switch {
	case m.TenantID == "":
		return errEmptyTenant
	case m.Group == "":
		return errors.New("neo4j: a processed-message marker needs a consumer group")
	case m.MessageID == "":
		return errors.New("neo4j: a processed-message marker needs a message id")
	}
	return nil
}

// markerParams binds a marker's identity; the tenant comes from the runner.
func markerParams(m uow.Marker) map[string]any {
	var outboxEntryID any
	if m.OutboxEntryID != nil {
		outboxEntryID = m.OutboxEntryID.String()
	}
	return map[string]any{
		"group":           m.Group,
		"message_id":      m.MessageID,
		"outbox_entry_id": outboxEntryID,
	}
}

// markerRecorded reports whether the marker of m exists.
func markerRecorded(ctx context.Context, run TenantRunner, m uow.Marker) (bool, error) {
	return singleBool(ctx, run, markerRecordedCypher, markerParams(m), "recorded")
}

// recordMarker creates the marker of m.
func recordMarker(ctx context.Context, run TenantRunner, m uow.Marker) error {
	return execCypher(ctx, run, markerCreateCypher, markerParams(m))
}

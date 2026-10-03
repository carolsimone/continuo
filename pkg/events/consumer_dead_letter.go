package events

import (
	"strings"
	"time"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/google/uuid"
)

// ConsumerDeadLetterSchemaVersion is the schema_version of consumer.dead_letter:v1 entries.
const ConsumerDeadLetterSchemaVersion = 1

// ConsumerDeadLetter is the payload of a consumer.dead_letter:v1 entry: one
// stream message a consumer group gave up on. The envelope's producer names
// the consuming service and its tenant_id the message's tenant.
type ConsumerDeadLetter struct {
	OriginalStream    string               `json:"original_stream"`
	OriginalGroup     string               `json:"original_group"`
	OriginalMessageID string               `json:"original_message_id"`
	Fields            map[string]string    `json:"fields"`
	FailureKind       model.DeadLetterKind `json:"failure_kind"`
	Error             string               `json:"error"`
	DeliveryCount     int64                `json:"delivery_count"`
}

// DeadLetteredMessage is what a consumer knows about a message it gives up on.
type DeadLetteredMessage struct {
	TenantID      string
	Producer      string
	OccurredAt    time.Time
	Stream        string
	Group         string
	MessageID     string
	Fields        map[string]string
	FailureKind   model.DeadLetterKind
	Error         string
	DeliveryCount int64
}

// ConsumerDeadLetterEventID derives the event id of the dead letter for one
// message in one consumer group. Writing the same message's dead letter twice
// (when the acknowledgement after the first write failed) repeats the id.
func ConsumerDeadLetterEventID(tenantID, group, stream, messageID string) string {
	name := strings.Join([]string{"consumer.dead_letter", tenantID, group, stream, messageID}, "|")
	return uuid.NewSHA1(EventIDNamespace, []byte(name)).String()
}

// ConsumerDeadLetterFields renders m as the Redis fields of one
// consumer.dead_letter:v1 entry.
func ConsumerDeadLetterFields(m DeadLetteredMessage) (map[string]any, error) {
	fields := m.Fields
	if fields == nil {
		fields = map[string]string{}
	}
	env := Envelope{
		EventID:       ConsumerDeadLetterEventID(m.TenantID, m.Group, m.Stream, m.MessageID),
		TenantID:      m.TenantID,
		OccurredAt:    m.OccurredAt,
		Producer:      m.Producer,
		SchemaVersion: ConsumerDeadLetterSchemaVersion,
	}
	return env.Fields(ConsumerDeadLetter{
		OriginalStream:    m.Stream,
		OriginalGroup:     m.Group,
		OriginalMessageID: m.MessageID,
		Fields:            fields,
		FailureKind:       m.FailureKind,
		Error:             m.Error,
		DeliveryCount:     m.DeliveryCount,
	})
}

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
	OriginalStream    string `json:"original_stream"`
	OriginalGroup     string `json:"original_group"`
	OriginalMessageID string `json:"original_message_id"`
	// OriginalKey is the Redis key the message was read from: the stream's
	// prefixed key, or its unprefixed legacy key, whose name is the stream's
	// own. A producer that names no key reads only the legacy key.
	OriginalKey   string               `json:"original_key,omitempty"`
	Fields        map[string]string    `json:"fields"`
	FailureKind   model.DeadLetterKind `json:"failure_kind"`
	Error         string               `json:"error"`
	DeliveryCount int64                `json:"delivery_count"`
}

// DedupMessageID returns the identity of the dead-lettered message within its
// stream and group, by the rule pkgredis.DedupMessageID gives handlers: the
// entry ID when the message was read from the legacy key or the payload names
// no key, and the entry ID followed by "@" and the key for any other key. The
// two keys of a stream number their entries independently, so entries of both
// can share an ID.
func (p ConsumerDeadLetter) DedupMessageID() string {
	if p.OriginalKey == "" || p.OriginalKey == p.OriginalStream {
		return p.OriginalMessageID
	}
	return p.OriginalMessageID + "@" + p.OriginalKey
}

// DeadLetteredMessage is what a consumer knows about a message it gives up on.
type DeadLetteredMessage struct {
	TenantID      string
	Producer      string
	OccurredAt    time.Time
	Stream        string
	Group         string
	MessageID     string
	Key           string // the Redis key the message was read from
	Fields        map[string]string
	FailureKind   model.DeadLetterKind
	Error         string
	DeliveryCount int64
}

// ConsumerDeadLetterEventID derives the event id of the dead letter for one
// message in one consumer group, from the message's dedup identity
// (ConsumerDeadLetter.DedupMessageID). Writing the same message's dead letter
// twice (when the acknowledgement after the first write failed) repeats the id.
func ConsumerDeadLetterEventID(tenantID, group, stream, dedupMessageID string) string {
	name := strings.Join([]string{"consumer.dead_letter", tenantID, group, stream, dedupMessageID}, "|")
	return uuid.NewSHA1(EventIDNamespace, []byte(name)).String()
}

// ConsumerDeadLetterFields renders m as the Redis fields of one
// consumer.dead_letter:v1 entry.
func ConsumerDeadLetterFields(m DeadLetteredMessage) (map[string]any, error) {
	fields := m.Fields
	if fields == nil {
		fields = map[string]string{}
	}
	payload := ConsumerDeadLetter{
		OriginalStream:    m.Stream,
		OriginalGroup:     m.Group,
		OriginalMessageID: m.MessageID,
		OriginalKey:       m.Key,
		Fields:            fields,
		FailureKind:       m.FailureKind,
		Error:             m.Error,
		DeliveryCount:     m.DeliveryCount,
	}
	env := Envelope{
		EventID:       ConsumerDeadLetterEventID(m.TenantID, m.Group, m.Stream, payload.DedupMessageID()),
		TenantID:      m.TenantID,
		OccurredAt:    m.OccurredAt,
		Producer:      m.Producer,
		SchemaVersion: ConsumerDeadLetterSchemaVersion,
	}
	return env.Fields(payload)
}

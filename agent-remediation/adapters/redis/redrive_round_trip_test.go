package redis

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"maps"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/domain/event"
	"github.com/carolsimone/continuo/agent-remediation/service/runner"
	"github.com/carolsimone/continuo/pkg/domain/model"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	"github.com/carolsimone/continuo/pkg/streams"
)

// consumeOne runs the runner engine's consumer of remediation.requested:v3 on
// rc, whose stream holds the consumer's group, until it accepts one message,
// and returns what it accepted.
func consumeOne(t *testing.T, rc *goredis.Client) runner.Accepted {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	got := make(chan runner.Accepted, 1)
	consumer := NewRemediationRequestedRunnerConsumer(rc, func(_ context.Context, a runner.Accepted) error {
		select {
		case got <- a:
		default:
		}
		return nil
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	consumer.SetService("agent-remediation")
	done := make(chan error, 1)
	go func() { done <- consumer.Start(ctx) }()
	var a runner.Accepted
	select {
	case a = <-got:
	case err := <-done:
		require.FailNow(t, "the consumer stopped before it accepted a message", "%v", err)
	case <-ctx.Done():
		require.FailNow(t, "the consumer accepted no message")
	}
	cancel()
	if err := <-done; err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		require.NoError(t, err)
	}
	return a
}

// runnerDeadLetterRow is the outbox row the runner writes when it ends a
// remediation as a system failure (service/runner/stuck.go deadLetterEntry):
// the message the remediation was accepted from, named by its entry id and
// key, with its stream fields and its payload, stored under its tenant as an
// events.DeadLetteredMessage on consumer.dead_letter:v1.
func runnerDeadLetterRow(t *testing.T, a runner.Accepted, remediationID uuid.UUID, now time.Time) *pkgoutbox.Entry {
	t.Helper()
	fields := maps.Clone(a.Fields)
	fields["payload"] = string(a.Payload)
	messageID, key, _ := strings.Cut(a.MessageID, "@")
	body, err := json.Marshal(pkgevents.DeadLetteredMessage{
		TenantID:      a.Trigger.TenantID,
		OccurredAt:    now,
		Stream:        streams.RemediationRequestedV3,
		Group:         streams.AgentRemediationRemediationRequested,
		MessageID:     messageID,
		Key:           key,
		Fields:        fields,
		FailureKind:   model.DeadLetterKindTransientExhausted,
		Error:         "remediation " + remediationID.String() + `, try 1, step "dbt_log": the step failed 5 times`,
		DeliveryCount: 5,
	})
	require.NoError(t, err)
	stored, err := pkgoutbox.WrapTenant(a.Trigger.TenantID, body)
	require.NoError(t, err)
	return &pkgoutbox.Entry{
		ID:            uuid.NewSHA1(uuid.NameSpaceOID, []byte("dead_lettered|"+remediationID.String()+"|"+a.MessageID)),
		AggregateType: event.AggregateType,
		AggregateID:   event.AggregateIDForRelease(a.Trigger.ReleaseID),
		EventType:     event.DeadLetteredEventType,
		Payload:       stored,
		StreamName:    streams.ConsumerDeadLetterV1,
		Status:        "pending",
		CreatedAt:     now,
	}
}

// redriveValues are the fields dead-letter-controller publishes when it
// redrives dl, stored under deadLetterID, through an outbox row whose id is
// redriveRowID: the original message's fields with the row's id as
// outbox_entry_id, the dead letter it came from as redriven_from, and the
// group that gave up on it as redrive_group
// (dead-letter-controller/adapters/redis.RedrivePublisher's Render and
// Publish over the payload serialization.EncodeRedrive writes).
func redriveValues(dl pkgevents.ConsumerDeadLetter, deadLetterID, redriveRowID uuid.UUID) map[string]any {
	values := make(map[string]any, len(dl.Fields)+2)
	for k, v := range dl.Fields {
		values[k] = v
	}
	delete(values, "outbox_entry_id")
	values[pkgredis.RedrivenFromField] = deadLetterID.String()
	values[pkgredis.RedriveGroupField] = dl.OriginalGroup
	values["outbox_entry_id"] = redriveRowID.String()
	return values
}

// TestBinding_ARedrivenDeadLetterReachesTheRunnerAsTheTriggerItWas follows a
// heal trigger through the runner's dead letter and dead-letter-controller's
// redrive back into the runner engine's consumer: the trigger the consumer
// accepted is dead-lettered as the runner stores it, rendered by the outbox
// publisher, decoded as dead-letter-controller stores it, and published again
// with the fields a redrive adds. The consumer accepts the original trigger,
// payload and tenant under a new dedup id and the redrive's outbox entry id,
// with the stream's fields the first delivery had and no redrive field.
func TestBinding_ARedrivenDeadLetterReachesTheRunnerAsTheTriggerItWas(t *testing.T) {
	ctx := context.Background()
	rc := newTestRedis(t)
	key := pkgredis.Key(streams.RemediationRequestedV3)
	require.NoError(t, rc.XGroupCreateMkStream(ctx, key, streams.AgentRemediationRemediationRequested, "0").Err())

	originalEntryID := uuid.New()
	fields, err := pkgevents.RemediationRequestedFields("evt-1", "acme", "remediation",
		time.Date(2026, 11, 14, 9, 0, 0, 0, time.UTC), requestedFixture())
	require.NoError(t, err)
	fields["outbox_entry_id"] = originalEntryID.String()
	require.NoError(t, rc.XAdd(ctx, &goredis.XAddArgs{Stream: key, Values: fields}).Err())
	first := consumeOne(t, rc)
	require.Equal(t, "acme", first.Trigger.TenantID)
	require.NotNil(t, first.OutboxEntryID)
	require.Equal(t, originalEntryID, *first.OutboxEntryID)

	row := runnerDeadLetterRow(t, first, uuid.New(), time.Date(2026, 11, 14, 9, 30, 0, 0, time.UTC))
	rendered, err := (&agentRemediationOutboxPublisher{}).Render(row)
	require.NoError(t, err)
	stored, err := pkgoutbox.StringifyFields(rendered)
	require.NoError(t, err)
	env, dl, err := pkgevents.DecodeConsumerDeadLetter(stored)
	require.NoError(t, err)
	require.Equal(t, "acme", env.TenantID)
	require.Equal(t, streams.AgentRemediationRemediationRequested, dl.OriginalGroup)

	redriveRowID := uuid.New()
	require.NoError(t, rc.XAdd(ctx, &goredis.XAddArgs{
		Stream: key, Values: redriveValues(dl, uuid.New(), redriveRowID),
	}).Err())
	again := consumeOne(t, rc)

	assert.Equal(t, first.Trigger, again.Trigger, "the redrive carries the trigger it dead-lettered")
	assert.Equal(t, "acme", again.Trigger.TenantID)
	assert.Equal(t, first.Payload, again.Payload)
	assert.NotEqual(t, first.MessageID, again.MessageID, "a redrive is a new message")
	assert.True(t, strings.HasSuffix(again.MessageID, "@"+key), "the dedup id names the key it was read from")
	require.NotNil(t, again.OutboxEntryID)
	assert.Equal(t, redriveRowID, *again.OutboxEntryID)
	wantFields := maps.Clone(first.Fields)
	wantFields["outbox_entry_id"] = redriveRowID.String()
	assert.Equal(t, wantFields, again.Fields, "the redrive fields are stripped and the stream's own fields kept")
}

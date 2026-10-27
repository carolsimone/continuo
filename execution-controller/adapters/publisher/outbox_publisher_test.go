package publisher_test

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/carolsimone/continuo/execution-controller/adapters/publisher"
	"github.com/carolsimone/continuo/execution-controller/domain/command"
	"github.com/carolsimone/continuo/execution-controller/domain/event"
	"github.com/carolsimone/continuo/execution-controller/domain/model"
	"github.com/carolsimone/continuo/execution-controller/service/reports"
	"github.com/carolsimone/continuo/execution-controller/service/validation"
	pkgmodel "github.com/carolsimone/continuo/pkg/domain/model"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newRedis(t *testing.T) *goredis.Client {
	t.Helper()
	mr, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(mr.Close)
	return goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
}

func lastEntryFields(t *testing.T, r *goredis.Client, stream string) map[string]interface{} {
	t.Helper()
	res, err := r.XRange(context.Background(), stream, "-", "+").Result()
	require.NoError(t, err)
	require.Len(t, res, 1)
	return res[0].Values
}

// A report row is published as one task.status.updated:v2 entry: the
// envelope names the row, the payload is the report.
func TestPublisher_TaskStatusReport(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	r := newRedis(t)
	pub := publisher.NewOutboxPublisher(r, logger)

	payload, err := json.Marshal(pkgevents.TaskStatusReport{RunID: "s1", TaskID: "t1", Attempt: 1, Status: "running"})
	require.NoError(t, err)

	stored, err := outbox.WrapTenant("acme", payload)
	require.NoError(t, err)

	id := uuid.New()
	require.NoError(t, pub.Publish(context.Background(), &outbox.Entry{
		ID: id, EventType: event.EventTypeTaskStatusReport, StreamName: streams.TaskStatusUpdatedV2, Payload: stored,
		CreatedAt: time.Now(),
	}))

	v := lastEntryFields(t, r, streams.TaskStatusUpdatedV2)
	assert.Equal(t, id.String(), v["event_id"])
	assert.Equal(t, "acme", v["tenant_id"])
	assert.Equal(t, "execution-controller", v["producer"])
	assert.JSONEq(t, string(payload), v["payload"].(string))
	assert.Equal(t, id.String(), v["outbox_entry_id"])
}

func TestPublisher_ValidationCompleted(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	r := newRedis(t)
	pub := publisher.NewOutboxPublisher(r, logger)

	// The aggregate gate stores the validation.completed body as the entry
	// payload; the publisher must re-emit it on the "payload" field so
	// release-controller's HandleValidationResult can decode it.
	body := []byte(`{"release_id":"rel_1","per_node_results":[{"node_id":"public.orders","status":"ok"}],"aggregate_status":"ok"}`)
	id := uuid.New()
	require.NoError(t, pub.Publish(context.Background(), &outbox.Entry{
		ID: id, EventType: "validation_completed", StreamName: streams.ValidationResultV1, Payload: body,
	}))

	v := lastEntryFields(t, r, streams.ValidationResultV1)
	assert.Equal(t, id.String(), v["outbox_entry_id"])
	payloadStr, ok := v["payload"].(string)
	require.True(t, ok, "expected a string payload field")
	assert.JSONEq(t, string(body), payloadStr, "stored aggregate payload re-emitted verbatim")
}

func TestPublisher_SeedBuildCompleted(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	r := newRedis(t)
	pub := publisher.NewOutboxPublisher(r, logger)

	body := []byte(`{"release_id":"rel_1","per_node_results":[{"node_id":"public.seed_x","status":"ok"}],"aggregate_status":"ok"}`)
	id := uuid.New()
	require.NoError(t, pub.Publish(context.Background(), &outbox.Entry{
		ID: id, EventType: "seed_build_completed", StreamName: streams.SeedBuildCompletedV1, Payload: body,
	}))

	v := lastEntryFields(t, r, streams.SeedBuildCompletedV1)
	assert.Equal(t, id.String(), v["outbox_entry_id"])
	payloadStr, ok := v["payload"].(string)
	require.True(t, ok, "expected a string payload field")
	assert.JSONEq(t, string(body), payloadStr, "stored aggregate payload re-emitted verbatim")
}

func TestPublisher_CompileCompleted(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	r := newRedis(t)
	pub := publisher.NewOutboxPublisher(r, logger)

	// Regression: the compile leg's aggregate event must be publishable — a
	// missing switch case stranded releases in `compiling` (no compile.completed:v1).
	body := []byte(`{"release_id":"rel_1","status":"ok"}`)
	id := uuid.New()
	require.NoError(t, pub.Publish(context.Background(), &outbox.Entry{
		ID: id, EventType: "compile_completed", StreamName: streams.CompileCompletedV1, Payload: body,
	}))

	v := lastEntryFields(t, r, streams.CompileCompletedV1)
	assert.Equal(t, id.String(), v["outbox_entry_id"])
	payloadStr, ok := v["payload"].(string)
	require.True(t, ok, "expected a string payload field")
	assert.JSONEq(t, string(body), payloadStr, "stored aggregate payload re-emitted verbatim")
}

func TestPublisher_UnknownEventType(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	r := newRedis(t)
	pub := publisher.NewOutboxPublisher(r, logger)

	err := pub.Publish(context.Background(), &outbox.Entry{
		ID: uuid.New(), EventType: "unknown_type", StreamName: "x:v1", Payload: []byte(`{}`),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown event_type")
}

// TestPublisher_UnknownEventType_IsTransient guards Fix #3: an unknown
// event_type must NOT be classified as pkgevents.ErrPermanent. During a
// rolling deployment an old replica can dequeue a row for an event_type only
// a newer replica knows about; that row must be retried through its budget,
// not dead-lettered on attempt #1.
func TestPublisher_UnknownEventType_IsTransient(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	r := newRedis(t)
	pub := publisher.NewOutboxPublisher(r, logger)

	err := pub.Publish(context.Background(), &outbox.Entry{
		ID: uuid.New(), EventType: "not_yet_known_type", StreamName: "x:v1", Payload: []byte(`{}`),
	})
	require.Error(t, err)
	assert.False(t, errors.Is(err, pkgevents.ErrPermanent),
		"unknown event_type must be transient (plain error), not ErrPermanent")
}

// TestPublisher_ContractAllHandledEventTypes is a regression guard that asserts
// every event_type this service's outbox rows carry — from the dispatcher, the
// observer, and every other emit site — does NOT return "unknown
// event_type". An emit site whose event_type has no matching case in the
// publisher switch strands its row unpublished with no visible error until an
// operator notices the missing downstream event. The event_type constants are
// the single source of truth shared by every producer.
func TestPublisher_ContractAllHandledEventTypes(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	// Minimal valid payloads for each event_type — just enough for the publisher's
	// json.Unmarshal to succeed. Verbatim-passthrough cases ("payload" field) use
	// any valid JSON object.
	cases := []struct {
		eventType  string
		streamName string
		payload    []byte
	}{
		{
			eventType:  event.EventTypeTaskStatusReport,
			streamName: streams.TaskStatusUpdatedV2,
			payload:    storedReport(t, "default", mustMarshal(t, pkgevents.TaskStatusReport{RunID: "s1", TaskID: "t1", Status: "succeeded"})),
		},
		{
			// Uses the shared constant — the single source of truth for this wire string.
			eventType:  validation.EventTypeValidationCompleted,
			streamName: streams.ValidationResultV1,
			payload:    []byte(`{"release_id":"rel1","aggregate_status":"ok"}`),
		},
		{
			eventType:  validation.EventTypeSeedBuildCompleted,
			streamName: streams.SeedBuildCompletedV1,
			payload:    []byte(`{"release_id":"rel1","status":"ok"}`),
		},
		{
			eventType:  validation.EventTypeCompileCompleted,
			streamName: streams.CompileCompletedV1,
			payload:    []byte(`{"release_id":"rel1","status":"ok"}`),
		},
		{
			eventType:  validation.EventTypeValidationNodeResult,
			streamName: streams.ValidationResultV1,
			payload:    []byte(`{"release_id":"rel1","stage":"validation","node_id":"node.a","status":"ok"}`),
		},
	}

	for _, tc := range cases {
		t.Run(tc.eventType, func(t *testing.T) {
			r := newRedis(t)
			pub := publisher.NewOutboxPublisher(r, logger)
			err := pub.Publish(context.Background(), &outbox.Entry{
				ID:         uuid.New(),
				EventType:  tc.eventType,
				StreamName: tc.streamName,
				Payload:    tc.payload,
			})
			// The only acceptable error here is an infrastructure error (e.g. Redis
			// down); "unknown event_type" is never acceptable.
			if err != nil {
				assert.NotContains(t, err.Error(), "unknown event_type",
					"event_type %q must be handled by the publisher switch", tc.eventType)
			}
		})
	}
}

func mustMarshal(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return b
}

// TestPublisher_BadPayloadReturnsError verifies a malformed payload for a known
// event_type surfaces the unmarshal error rather than silently swallowing it.
func TestPublisher_BadPayloadReturnsError(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	r := newRedis(t)
	pub := publisher.NewOutboxPublisher(r, logger)

	err := pub.Publish(context.Background(), &outbox.Entry{
		ID: uuid.New(), EventType: event.EventTypeTaskStatusReport, Payload: storedReport(t, "default", []byte(`"not a report"`)), StreamName: "x:v1",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "unmarshal task_status_report")
}

// TestPublisher_NodeUpdatedRowIsUnknown pins that execution-controller
// publishes nothing to node.updated:v1: a node_updated row left in
// execution_outbox is retried as an unknown event_type and dead-lettered when
// its budget is spent, never written to the retired stream.
func TestPublisher_NodeUpdatedRowIsUnknown(t *testing.T) {
	pub := publisher.NewOutboxPublisher(newRedis(t), slog.New(slog.NewTextHandler(os.Stderr, nil)))
	err := pub.Publish(context.Background(), &outbox.Entry{
		ID: uuid.New(), EventType: "node_updated", StreamName: "node.updated:v1",
		Payload: []byte(`{"task_id":"t1","run_id":"s1","status":"FAILED"}`),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown event_type")
	assert.False(t, errors.Is(err, pkgevents.ErrPermanent), "an unknown event_type is retried, not dead-lettered at once")
}

// recordedOutbox keeps every outbox entry written.
type recordedOutbox struct {
	outbox.Repository
	created []*outbox.Entry
}

func (r *recordedOutbox) Create(_ context.Context, e *outbox.Entry) error {
	r.created = append(r.created, e)
	return nil
}

// A report of every status, stopped included, is published under the tenant of
// the deployment it was written for: the report writer stores the tenant with
// the row and the renderer puts it on the envelope.
func TestReport_CarriesTheDeploymentsTenant(t *testing.T) {
	for _, status := range []pkgmodel.AttemptStatus{
		pkgmodel.AttemptStatusQueued, pkgmodel.AttemptStatusRunning, pkgmodel.AttemptStatusSucceeded,
		pkgmodel.AttemptStatusFailed, pkgmodel.AttemptStatusStopped,
	} {
		t.Run(string(status), func(t *testing.T) {
			cmd := command.DeployTask{TenantID: "acme", TaskID: uuid.NewString(), RunID: uuid.NewString(), Attempt: 1,
				JobName: "svc-sch-tbl-7f3e1a2b-r1"}
			d := model.NewDeployment(cmd, nil, time.Now())
			message := ""
			if status == pkgmodel.AttemptStatusFailed {
				d.Observe(model.AttemptFacts{Reason: pkgmodel.FailureReasonExitCode, Message: "past its deadline"})
				message = "past its deadline"
			}
			out := &recordedOutbox{}
			require.NoError(t, reports.Write(context.Background(), out, d, status, message))
			require.Len(t, out.created, 1)
			entry := out.created[0]
			entry.ID, entry.CreatedAt = uuid.New(), time.Now()

			values, err := publisher.NewOutboxPublisher(nil, nil).Render(entry)

			require.NoError(t, err)
			assert.Equal(t, "acme", values["tenant_id"])
			assert.Contains(t, values["payload"], cmd.TaskID, "the envelope's payload is the report itself")
		})
	}
}

// A report row that stores no tenant cannot be published under a guessed one:
// it is a permanent error.
func TestPublisher_AReportRowWithoutATenantIsPermanent(t *testing.T) {
	entry := &outbox.Entry{ID: uuid.New(), EventType: event.EventTypeTaskStatusReport, StreamName: streams.TaskStatusUpdatedV2,
		Payload: mustMarshal(t, pkgevents.TaskStatusReport{RunID: "s1", TaskID: "t1", Status: "running"})}

	_, err := publisher.NewOutboxPublisher(nil, nil).Render(entry)

	require.Error(t, err)
	assert.ErrorIs(t, err, pkgevents.ErrPermanent)
	assert.ErrorIs(t, err, outbox.ErrNoTenant)
}

// storedReport is the payload of a report row of tenantID: the report stored
// beside its tenant.
func storedReport(t *testing.T, tenantID string, report []byte) []byte {
	t.Helper()
	stored, err := outbox.WrapTenant(tenantID, report)
	require.NoError(t, err)
	return stored
}

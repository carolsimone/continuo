package publisher_test

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"testing"

	"github.com/carolsimone/continuo/executor-controller/adapters/k8s"
	"github.com/carolsimone/continuo/executor-controller/adapters/publisher"
	"github.com/carolsimone/continuo/executor-controller/domain/event"
	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeK8s struct {
	calls int
	err   error
}

func (f *fakeK8s) CreateQueryJob(_ context.Context, _ k8s.JobParams) error {
	f.calls++
	return f.err
}

// TestPublisher_DeployFailureShortCircuits verifies that a K8s error stops
// processing before Redis is called. Redis is nil here; if it were reached the
// test would panic, confirming the short-circuit behavior.
func TestPublisher_DeployFailureShortCircuits(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	k := &fakeK8s{err: errors.New("apiserver down")}

	payload, err := json.Marshal(event.DeployTask{
		TaskID:     uuid.New().String(),
		ScheduleID: uuid.New().String(),
		JobName:    "j",
		ServiceName: "s",
		SchemaName: "sch",
		TableName:  "t",
		NodeType:   "dbt-model",
		ImageTag:   "v1",
	})
	require.NoError(t, err)

	pub := publisher.NewOutboxPublisher(k, nil, "ns", logger)
	pubErr := pub.Publish(context.Background(), &outbox.Entry{
		ID:        uuid.New(),
		EventType: "deploy_task",
		Payload:   payload,
	})
	require.Error(t, pubErr)
	assert.Contains(t, pubErr.Error(), "k8s deployment failed")
	assert.Equal(t, 1, k.calls)
}

// TestPublisher_InvalidPayloadReturnsPermanentError verifies that a payload
// that cannot be parsed triggers a permanent error (so the Processor exhausts
// retries immediately rather than retrying indefinitely).
func TestPublisher_InvalidPayloadReturnsPermanentError(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	k := &fakeK8s{}

	pub := publisher.NewOutboxPublisher(k, nil, "ns", logger)
	pubErr := pub.Publish(context.Background(), &outbox.Entry{
		ID:        uuid.New(),
		EventType: "deploy_task",
		Payload:   []byte(`not json`),
	})
	require.Error(t, pubErr)
	assert.Contains(t, pubErr.Error(), "unmarshal deploy_task payload")
	assert.Equal(t, 0, k.calls, "K8s must not be called when payload is invalid")
}

// TestPublisher_UnknownEventType returns an error for unexpected event types
// without touching K8s or Redis.
func TestPublisher_UnknownEventType(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	k := &fakeK8s{}

	pub := publisher.NewOutboxPublisher(k, nil, "ns", logger)
	pubErr := pub.Publish(context.Background(), &outbox.Entry{
		ID:        uuid.New(),
		EventType: "unknown_type",
		Payload:   []byte(`{}`),
	})
	require.Error(t, pubErr)
	assert.Contains(t, pubErr.Error(), "unknown event_type")
	assert.Equal(t, 0, k.calls)
}

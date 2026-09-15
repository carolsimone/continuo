package redis

import (
	"encoding/json"
	"testing"

	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
)

func msgWith(values map[string]interface{}) goredis.XMessage {
	return goredis.XMessage{ID: "1-0", Values: values}
}

// payloadMsg wraps a typed event as the JSON `payload` field of a Redis message,
// matching the wire shape the producers emit.
func payloadMsg(t *testing.T, evt interface{}) goredis.XMessage {
	t.Helper()
	b, err := json.Marshal(evt)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	return msgWith(map[string]interface{}{"payload": string(b)})
}

func TestParseCheckK8s_DecodesPayloadAndRetryCount(t *testing.T) {
	cmd, err := ParseCheckK8s(payloadMsg(t, pkgevents.CheckK8s{
		TaskID:     uuid.New().String(),
		ScheduleID: uuid.New().String(),
		JobName:    "job-2",
		RetryCount: 4,
	}), 3)
	if err != nil {
		t.Fatalf("ParseCheckK8s: %v", err)
	}
	if cmd.RetryCount != 4 {
		t.Fatalf("expected retry_count 4, got %d", cmd.RetryCount)
	}
}

// TestParseCheckK8s_CarriesRunningAnnounced verifies the re-poll loop preserves the
// per-attempt "already announced RUNNING" flag across check.k8s:v1 hops.
func TestParseCheckK8s_CarriesRunningAnnounced(t *testing.T) {
	cmd, err := ParseCheckK8s(payloadMsg(t, pkgevents.CheckK8s{
		TaskID:           uuid.New().String(),
		ScheduleID:       uuid.New().String(),
		JobName:          "job-ra",
		RunningAnnounced: true,
	}), 3)
	if err != nil {
		t.Fatalf("ParseCheckK8s: %v", err)
	}
	if !cmd.RunningAnnounced {
		t.Fatal("expected RunningAnnounced=true carried from check.k8s:v1 payload")
	}
}

func TestParseCheckK8s_DefaultMaxRetriesWhenAbsent(t *testing.T) {
	cmd, err := ParseCheckK8s(payloadMsg(t, pkgevents.CheckK8s{
		TaskID:     uuid.New().String(),
		ScheduleID: uuid.New().String(),
		JobName:    "job-3",
	}), 7)
	if err != nil {
		t.Fatalf("ParseCheckK8s: %v", err)
	}
	if cmd.MaxRetries != 7 {
		t.Fatalf("expected default max_retries 7, got %d", cmd.MaxRetries)
	}
}

func TestParseCheckK8s_InvalidJSONErrors(t *testing.T) {
	_, err := ParseCheckK8s(msgWith(map[string]interface{}{"payload": "{not json"}), 3)
	if err == nil {
		t.Fatal("expected error for malformed payload JSON")
	}
}

// TestParseCheckK8s_CarriesOperation verifies the re-poll loop preserves the dbt
// verb across check.k8s:v1 hops, so a check that lands after the Job is TTL-reaped
// still retains the verb for retry.
func TestParseCheckK8s_CarriesOperation(t *testing.T) {
	cmd, err := ParseCheckK8s(payloadMsg(t, pkgevents.CheckK8s{
		TaskID:     uuid.New().String(),
		ScheduleID: uuid.New().String(),
		JobName:    "job-op",
		Operation:  "test",
	}), 3)
	if err != nil {
		t.Fatalf("ParseCheckK8s: %v", err)
	}
	if cmd.Operation != "test" {
		t.Fatalf("expected Operation=test carried from check.k8s payload, got %q", cmd.Operation)
	}
}

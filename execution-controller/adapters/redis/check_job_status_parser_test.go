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
	}))
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
	}))
	if err != nil {
		t.Fatalf("ParseCheckK8s: %v", err)
	}
	if !cmd.RunningAnnounced {
		t.Fatal("expected RunningAnnounced=true carried from check.k8s:v1 payload")
	}
}

// TestParseCheckK8s_MaxRetriesAsDecoded verifies the parser carries the ticket's
// budget unchanged and leaves an absent one at 0, which the job-status handler
// replaces with the default budget.
func TestParseCheckK8s_MaxRetriesAsDecoded(t *testing.T) {
	cmd, err := ParseCheckK8s(payloadMsg(t, pkgevents.CheckK8s{
		TaskID:     uuid.New().String(),
		ScheduleID: uuid.New().String(),
		JobName:    "job-3",
		MaxRetries: 5,
	}))
	if err != nil {
		t.Fatalf("ParseCheckK8s: %v", err)
	}
	if cmd.MaxRetries != 5 {
		t.Fatalf("expected max_retries 5, got %d", cmd.MaxRetries)
	}

	cmd, err = ParseCheckK8s(payloadMsg(t, pkgevents.CheckK8s{
		TaskID:     uuid.New().String(),
		ScheduleID: uuid.New().String(),
		JobName:    "job-3",
	}))
	if err != nil {
		t.Fatalf("ParseCheckK8s: %v", err)
	}
	if cmd.MaxRetries != 0 {
		t.Fatalf("expected absent max_retries to stay 0, got %d", cmd.MaxRetries)
	}
}

func TestParseCheckK8s_InvalidJSONErrors(t *testing.T) {
	_, err := ParseCheckK8s(msgWith(map[string]interface{}{"payload": "{not json"}))
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
	}))
	if err != nil {
		t.Fatalf("ParseCheckK8s: %v", err)
	}
	if cmd.Operation != "test" {
		t.Fatalf("expected Operation=test carried from check.k8s payload, got %q", cmd.Operation)
	}
}

// TestParseCheckK8s_CarriesSecretRef verifies the re-poll loop preserves a
// python-api node's Secret name across check.k8s:v1 hops, so a retry rebuilt
// from the ticket still mounts the Secret.
func TestParseCheckK8s_CarriesSecretRef(t *testing.T) {
	cmd, err := ParseCheckK8s(payloadMsg(t, pkgevents.CheckK8s{ //nolint:gosec // G101: continuo-api-* is a Secret name, not a value
		TaskID:     uuid.New().String(),
		ScheduleID: uuid.New().String(),
		JobName:    "job-secret",
		SecretRef:  "continuo-api-fx",
	}))
	if err != nil {
		t.Fatalf("ParseCheckK8s: %v", err)
	}
	if cmd.SecretRef != "continuo-api-fx" {
		t.Fatalf("expected SecretRef carried from check.k8s payload, got %q", cmd.SecretRef)
	}
}

func TestParseCheckK8s_WithoutSecretRefIsEmpty(t *testing.T) {
	cmd, err := ParseCheckK8s(payloadMsg(t, pkgevents.CheckK8s{
		TaskID:     uuid.New().String(),
		ScheduleID: uuid.New().String(),
		JobName:    "job-no-secret",
	}))
	if err != nil {
		t.Fatalf("ParseCheckK8s: %v", err)
	}
	if cmd.SecretRef != "" {
		t.Fatalf("expected empty SecretRef, got %q", cmd.SecretRef)
	}
}

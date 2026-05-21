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

func TestParseNodeDeployed_DecodesPayloadAndTaskRetryCount(t *testing.T) {
	taskID := uuid.New()
	schedID := uuid.New()
	cmd, err := ParseNodeDeployed(payloadMsg(t, pkgevents.NodeDeployed{
		TaskID:         taskID.String(),
		ScheduleID:     schedID.String(),
		ScheduleName:   "daily",
		ServiceName:    "svc",
		SchemaName:     "public",
		TableName:      "orders",
		JobName:        "job-1",
		NodeType:       "model",
		ImageTag:       "sha-abc",
		TaskRetryCount: 2,
		MaxRetries:     5,
	}), 3)
	if err != nil {
		t.Fatalf("ParseNodeDeployed: %v", err)
	}
	if cmd.TaskID != taskID || cmd.ScheduleID != schedID {
		t.Fatalf("ids not mapped: %+v", cmd)
	}
	if cmd.JobName != "job-1" || cmd.ImageTag != "sha-abc" || cmd.NodeType != "model" {
		t.Fatalf("string fields not mapped: %+v", cmd)
	}
	if cmd.RetryCount != 2 {
		t.Fatalf("expected retry_count from task_retry_count=2, got %d", cmd.RetryCount)
	}
	if cmd.MaxRetries != 5 {
		t.Fatalf("expected max_retries 5, got %d", cmd.MaxRetries)
	}
	if cmd.ScheduleName != "daily" || cmd.ServiceName != "svc" || cmd.SchemaName != "public" || cmd.TableName != "orders" {
		t.Fatalf("name/service/schema/table fields not mapped: %+v", cmd)
	}
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

func TestParseNodeDeployed_InvalidTaskIDErrors(t *testing.T) {
	_, err := ParseNodeDeployed(payloadMsg(t, pkgevents.NodeDeployed{
		TaskID:     "not-a-uuid",
		ScheduleID: uuid.New().String(),
	}), 3)
	if err == nil {
		t.Fatal("expected error for invalid task_id")
	}
}

func TestParseNodeDeployed_MissingPayloadErrors(t *testing.T) {
	_, err := ParseNodeDeployed(msgWith(map[string]interface{}{}), 3)
	if err == nil {
		t.Fatal("expected error when payload field is absent")
	}
}

func TestParseCheckK8s_InvalidJSONErrors(t *testing.T) {
	_, err := ParseCheckK8s(msgWith(map[string]interface{}{"payload": "{not json"}), 3)
	if err == nil {
		t.Fatal("expected error for malformed payload JSON")
	}
}

package redis

import (
	"testing"

	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
)

func msgWith(values map[string]interface{}) goredis.XMessage {
	return goredis.XMessage{ID: "1-0", Values: values}
}

func TestParseNodeDeployed_MapsFieldsAndTaskRetryCount(t *testing.T) {
	taskID := uuid.New()
	schedID := uuid.New()
	cmd, err := ParseNodeDeployed(msgWith(map[string]interface{}{
		"task_id":          taskID.String(),
		"schedule_id":      schedID.String(),
		"schedule_name":    "daily",
		"service_name":     "svc",
		"schema_name":      "public",
		"table_name":       "orders",
		"job_name":         "job-1",
		"node_type":        "model",
		"image_tag":        "sha-abc",
		"task_retry_count": "2",
		"max_retries":      "5",
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

func TestParseCheckK8s_UsesRetryCountField(t *testing.T) {
	cmd, err := ParseCheckK8s(msgWith(map[string]interface{}{
		"task_id":     uuid.New().String(),
		"schedule_id": uuid.New().String(),
		"job_name":    "job-2",
		"retry_count": "4",
	}), 3)
	if err != nil {
		t.Fatalf("ParseCheckK8s: %v", err)
	}
	if cmd.RetryCount != 4 {
		t.Fatalf("expected retry_count 4, got %d", cmd.RetryCount)
	}
}

func TestParseCheckK8s_DefaultMaxRetriesWhenAbsent(t *testing.T) {
	cmd, err := ParseCheckK8s(msgWith(map[string]interface{}{
		"task_id":     uuid.New().String(),
		"schedule_id": uuid.New().String(),
		"job_name":    "job-3",
	}), 7)
	if err != nil {
		t.Fatalf("ParseCheckK8s: %v", err)
	}
	if cmd.MaxRetries != 7 {
		t.Fatalf("expected default max_retries 7, got %d", cmd.MaxRetries)
	}
}

func TestParseNodeDeployed_InvalidTaskIDErrors(t *testing.T) {
	_, err := ParseNodeDeployed(msgWith(map[string]interface{}{
		"task_id":     "not-a-uuid",
		"schedule_id": uuid.New().String(),
	}), 3)
	if err == nil {
		t.Fatal("expected error for invalid task_id")
	}
}

package serialization

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/carolsimone/continuo/k8s-controller/domain/event"
)

const goldenJobCheckRequest = `{"task_id":"t","schedule_id":"s","schedule_name":"sn","service_name":"svc","schema_name":"sch","table_name":"tbl","job_name":"j","check_after":123,"node_type":"dbt-model","image_tag":"v1","operation":"test","retry_count":1,"max_retries":3,"running_announced":true}`

func TestJobCheckRequestDTORoundTrip(t *testing.T) {
	var dto JobCheckRequestDTO
	if err := json.Unmarshal([]byte(goldenJobCheckRequest), &dto); err != nil {
		t.Fatalf("unmarshal golden: %v", err)
	}
	got := dto.ToDomain()
	want := event.JobCheckRequest{
		TaskID: "t", ScheduleID: "s", ScheduleName: "sn", ServiceName: "svc",
		SchemaName: "sch", TableName: "tbl", JobName: "j", CheckAfter: 123,
		NodeType: "dbt-model", ImageTag: "v1", Operation: "test", RetryCount: 1,
		MaxRetries: 3, RunningAnnounced: true,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("toDomain:\n got %+v\nwant %+v", got, want)
	}
	out, err := json.Marshal(JobCheckRequestFromDomain(got))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(out) != goldenJobCheckRequest {
		t.Fatalf("bytes changed:\n got %s\nwant %s", out, goldenJobCheckRequest)
	}
}

func TestJobCheckRequestOperationOmitempty(t *testing.T) {
	out, err := json.Marshal(JobCheckRequestFromDomain(event.JobCheckRequest{TaskID: "t"}))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if strings.Contains(string(out), `"operation"`) {
		t.Fatalf("operation must be omitted when empty: %s", out)
	}
}

const goldenTaskFailed = `{"task_id":"t","schedule_id":"s","schedule_name":"sn","service_name":"svc","schema_name":"sch","table_name":"tbl","job_name":"j","error_message":"boom","retry_count":2}`

func TestTaskFailedDTORoundTrip(t *testing.T) {
	var dto TaskFailedDTO
	if err := json.Unmarshal([]byte(goldenTaskFailed), &dto); err != nil {
		t.Fatalf("unmarshal golden: %v", err)
	}
	got := dto.ToDomain()
	want := event.TaskFailed{
		TaskID: "t", ScheduleID: "s", ScheduleName: "sn", ServiceName: "svc",
		SchemaName: "sch", TableName: "tbl", JobName: "j", ErrorMessage: "boom", RetryCount: 2,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("toDomain:\n got %+v\nwant %+v", got, want)
	}
	out, err := json.Marshal(TaskFailedFromDomain(got))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(out) != goldenTaskFailed {
		t.Fatalf("bytes changed:\n got %s\nwant %s", out, goldenTaskFailed)
	}
}

const goldenTaskRetry = `{"task_id":"t","schedule_id":"s","schedule_name":"sn","service_name":"svc","schema_name":"sch","table_name":"tbl","job_name":"j","image_tag":"v1","retry_count":1,"max_retries":3,"node_type":"dbt-model","operation":"test"}`

func TestTaskRetryDTORoundTrip(t *testing.T) {
	var dto TaskRetryDTO
	if err := json.Unmarshal([]byte(goldenTaskRetry), &dto); err != nil {
		t.Fatalf("unmarshal golden: %v", err)
	}
	got := dto.ToDomain()
	want := event.TaskRetry{
		TaskID: "t", ScheduleID: "s", ScheduleName: "sn", ServiceName: "svc",
		SchemaName: "sch", TableName: "tbl", JobName: "j", ImageTag: "v1",
		RetryCount: 1, MaxRetries: 3, NodeType: "dbt-model", Operation: "test",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("toDomain:\n got %+v\nwant %+v", got, want)
	}
	out, err := json.Marshal(TaskRetryFromDomain(got))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(out) != goldenTaskRetry {
		t.Fatalf("bytes changed:\n got %s\nwant %s", out, goldenTaskRetry)
	}
}

func TestTaskRetryOperationOmitempty(t *testing.T) {
	out, err := json.Marshal(TaskRetryFromDomain(event.TaskRetry{TaskID: "t"}))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if strings.Contains(string(out), `"operation"`) {
		t.Fatalf("operation must be omitted when empty: %s", out)
	}
}

const goldenNodeStatusUpdated = `{"task_id":"t","schedule_id":"s","schedule_name":"sn","service_name":"svc","schema_name":"sch","table_name":"tbl","status":"SUCCEEDED"}`

func TestNodeStatusUpdatedDTORoundTrip(t *testing.T) {
	var dto NodeStatusUpdatedDTO
	if err := json.Unmarshal([]byte(goldenNodeStatusUpdated), &dto); err != nil {
		t.Fatalf("unmarshal golden: %v", err)
	}
	got := dto.ToDomain()
	want := event.NodeStatusUpdated{
		TaskID: "t", ScheduleID: "s", ScheduleName: "sn", ServiceName: "svc",
		SchemaName: "sch", TableName: "tbl", Status: "SUCCEEDED",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("toDomain:\n got %+v\nwant %+v", got, want)
	}
	out, err := json.Marshal(NodeStatusUpdatedFromDomain(got))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(out) != goldenNodeStatusUpdated {
		t.Fatalf("bytes changed:\n got %s\nwant %s", out, goldenNodeStatusUpdated)
	}
}

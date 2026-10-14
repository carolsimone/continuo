package events

import "strconv"

// QueryModel — stream: query.model:v1
// One message dispatches one task: execution-controller queues the task's
// Kubernetes Job from it. Fields is the single serializer for this stream, and
// pkg/events/testdata/query_model_v1.json pins the wire shape the producer
// writes and execution-controller parses.
type QueryModel struct {
	ScheduleID   string
	ScheduleName string
	TaskID       string
	ServiceName  string
	SchemaName   string
	TableName    string
	JobName      string
	NodeType     string
	ImageTag     string
	// SecretRef names the continuo-api-* Secret a python-api node's pod
	// receives; empty for every other node.
	SecretRef string
	// Operation is the dbt verb; empty for a plain run.
	Operation string
	// MaxRetries is the task's retry budget; 0 when the dispatch carries none.
	MaxRetries int32
}

// Fields returns the stream fields of q. operation and secret_ref are written
// only when set, and max_retries, as a decimal string, only when positive; the
// consumer reads an absent field as its default.
func (q QueryModel) Fields() map[string]string {
	fields := map[string]string{
		"schedule_id":   q.ScheduleID,
		"schedule_name": q.ScheduleName,
		"task_id":       q.TaskID,
		"service_name":  q.ServiceName,
		"schema_name":   q.SchemaName,
		"table_name":    q.TableName,
		"job_name":      q.JobName,
		"node_type":     q.NodeType,
		"image_tag":     q.ImageTag,
	}
	if q.SecretRef != "" {
		fields["secret_ref"] = q.SecretRef
	}
	if q.Operation != "" {
		fields["operation"] = q.Operation
	}
	if q.MaxRetries > 0 {
		fields["max_retries"] = strconv.FormatInt(int64(q.MaxRetries), 10)
	}
	return fields
}

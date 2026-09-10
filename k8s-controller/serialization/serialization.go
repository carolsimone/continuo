// Package serialization holds the wire- and persistence-facing DTOs for
// k8s-controller's json-tagged event types, keeping the domain event package
// free of struct tags. It sits outside adapters/ so the application layer
// (service/handlers, which writes k8s_outbox payloads) may map through it
// without importing an adapter, and outside domain/ so the tags live away from
// the domain types. The check-status handler (writing outbox payloads) and the
// publisher (reading them, including the delay-queue check ticket) map through
// these DTOs, fixing the payload byte shapes here.
package serialization

import (
	"github.com/carolsimone/continuo/k8s-controller/domain/event"
)

// JobCheckRequestDTO is the JSON shape of event.JobCheckRequest as stored in a
// check_delayed outbox row's payload and the delay-queue ticket.
type JobCheckRequestDTO struct {
	TaskID           string `json:"task_id"`
	ScheduleID       string `json:"schedule_id"`
	ScheduleName     string `json:"schedule_name"`
	ServiceName      string `json:"service_name"`
	SchemaName       string `json:"schema_name"`
	TableName        string `json:"table_name"`
	JobName          string `json:"job_name"`
	CheckAfter       int64  `json:"check_after"`
	NodeType         string `json:"node_type"`
	ImageTag         string `json:"image_tag"`
	Operation        string `json:"operation,omitempty"`
	RetryCount       int    `json:"retry_count"`
	MaxRetries       int    `json:"max_retries"`
	RunningAnnounced bool   `json:"running_announced"`
}

// JobCheckRequestFromDomain maps a domain event to its DTO.
func JobCheckRequestFromDomain(e event.JobCheckRequest) JobCheckRequestDTO {
	return JobCheckRequestDTO{
		TaskID:           e.TaskID,
		ScheduleID:       e.ScheduleID,
		ScheduleName:     e.ScheduleName,
		ServiceName:      e.ServiceName,
		SchemaName:       e.SchemaName,
		TableName:        e.TableName,
		JobName:          e.JobName,
		CheckAfter:       e.CheckAfter,
		NodeType:         e.NodeType,
		ImageTag:         e.ImageTag,
		Operation:        e.Operation,
		RetryCount:       e.RetryCount,
		MaxRetries:       e.MaxRetries,
		RunningAnnounced: e.RunningAnnounced,
	}
}

// ToDomain maps a decoded DTO back to the domain event.
func (d JobCheckRequestDTO) ToDomain() event.JobCheckRequest {
	return event.JobCheckRequest{
		TaskID:           d.TaskID,
		ScheduleID:       d.ScheduleID,
		ScheduleName:     d.ScheduleName,
		ServiceName:      d.ServiceName,
		SchemaName:       d.SchemaName,
		TableName:        d.TableName,
		JobName:          d.JobName,
		CheckAfter:       d.CheckAfter,
		NodeType:         d.NodeType,
		ImageTag:         d.ImageTag,
		Operation:        d.Operation,
		RetryCount:       d.RetryCount,
		MaxRetries:       d.MaxRetries,
		RunningAnnounced: d.RunningAnnounced,
	}
}

// TaskFailedDTO is the JSON shape of event.TaskFailed as stored in a task_failed
// outbox row's payload.
type TaskFailedDTO struct {
	TaskID       string `json:"task_id"`
	ScheduleID   string `json:"schedule_id"`
	ScheduleName string `json:"schedule_name"`
	ServiceName  string `json:"service_name"`
	SchemaName   string `json:"schema_name"`
	TableName    string `json:"table_name"`
	JobName      string `json:"job_name"`
	ErrorMessage string `json:"error_message"`
	RetryCount   int    `json:"retry_count"`
}

// TaskFailedFromDomain maps a domain event to its DTO.
func TaskFailedFromDomain(e event.TaskFailed) TaskFailedDTO {
	return TaskFailedDTO{
		TaskID:       e.TaskID,
		ScheduleID:   e.ScheduleID,
		ScheduleName: e.ScheduleName,
		ServiceName:  e.ServiceName,
		SchemaName:   e.SchemaName,
		TableName:    e.TableName,
		JobName:      e.JobName,
		ErrorMessage: e.ErrorMessage,
		RetryCount:   e.RetryCount,
	}
}

// ToDomain maps a decoded DTO back to the domain event.
func (d TaskFailedDTO) ToDomain() event.TaskFailed {
	return event.TaskFailed{
		TaskID:       d.TaskID,
		ScheduleID:   d.ScheduleID,
		ScheduleName: d.ScheduleName,
		ServiceName:  d.ServiceName,
		SchemaName:   d.SchemaName,
		TableName:    d.TableName,
		JobName:      d.JobName,
		ErrorMessage: d.ErrorMessage,
		RetryCount:   d.RetryCount,
	}
}

// TaskRetryDTO is the JSON shape of event.TaskRetry as stored in a task_retry
// outbox row's payload.
type TaskRetryDTO struct {
	TaskID       string `json:"task_id"`
	ScheduleID   string `json:"schedule_id"`
	ScheduleName string `json:"schedule_name"`
	ServiceName  string `json:"service_name"`
	SchemaName   string `json:"schema_name"`
	TableName    string `json:"table_name"`
	JobName      string `json:"job_name"`
	ImageTag     string `json:"image_tag"`
	RetryCount   int    `json:"retry_count"`
	MaxRetries   int    `json:"max_retries"`
	NodeType     string `json:"node_type"`
	Operation    string `json:"operation,omitempty"`
}

// TaskRetryFromDomain maps a domain event to its DTO.
func TaskRetryFromDomain(e event.TaskRetry) TaskRetryDTO {
	return TaskRetryDTO{
		TaskID:       e.TaskID,
		ScheduleID:   e.ScheduleID,
		ScheduleName: e.ScheduleName,
		ServiceName:  e.ServiceName,
		SchemaName:   e.SchemaName,
		TableName:    e.TableName,
		JobName:      e.JobName,
		ImageTag:     e.ImageTag,
		RetryCount:   e.RetryCount,
		MaxRetries:   e.MaxRetries,
		NodeType:     e.NodeType,
		Operation:    e.Operation,
	}
}

// ToDomain maps a decoded DTO back to the domain event.
func (d TaskRetryDTO) ToDomain() event.TaskRetry {
	return event.TaskRetry{
		TaskID:       d.TaskID,
		ScheduleID:   d.ScheduleID,
		ScheduleName: d.ScheduleName,
		ServiceName:  d.ServiceName,
		SchemaName:   d.SchemaName,
		TableName:    d.TableName,
		JobName:      d.JobName,
		ImageTag:     d.ImageTag,
		RetryCount:   d.RetryCount,
		MaxRetries:   d.MaxRetries,
		NodeType:     d.NodeType,
		Operation:    d.Operation,
	}
}

// NodeStatusUpdatedDTO is the JSON shape of event.NodeStatusUpdated as stored in
// a node_status_updated outbox row's payload.
type NodeStatusUpdatedDTO struct {
	TaskID       string `json:"task_id"`
	ScheduleID   string `json:"schedule_id"`
	ScheduleName string `json:"schedule_name"`
	ServiceName  string `json:"service_name"`
	SchemaName   string `json:"schema_name"`
	TableName    string `json:"table_name"`
	Status       string `json:"status"`
}

// NodeStatusUpdatedFromDomain maps a domain event to its DTO.
func NodeStatusUpdatedFromDomain(e event.NodeStatusUpdated) NodeStatusUpdatedDTO {
	return NodeStatusUpdatedDTO{
		TaskID:       e.TaskID,
		ScheduleID:   e.ScheduleID,
		ScheduleName: e.ScheduleName,
		ServiceName:  e.ServiceName,
		SchemaName:   e.SchemaName,
		TableName:    e.TableName,
		Status:       e.Status,
	}
}

// ToDomain maps a decoded DTO back to the domain event.
func (d NodeStatusUpdatedDTO) ToDomain() event.NodeStatusUpdated {
	return event.NodeStatusUpdated{
		TaskID:       d.TaskID,
		ScheduleID:   d.ScheduleID,
		ScheduleName: d.ScheduleName,
		ServiceName:  d.ServiceName,
		SchemaName:   d.SchemaName,
		TableName:    d.TableName,
		Status:       d.Status,
	}
}

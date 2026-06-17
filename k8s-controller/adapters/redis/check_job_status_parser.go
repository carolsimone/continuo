package redis

import (
	"encoding/json"
	"fmt"

	"github.com/carolsimone/continuo/k8s-controller/domain/command"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
)

// ParseNodeDeployed decodes a node.deployed:v1 message into a CheckJobStatus.
// The typed event travels in the JSON `payload` field; its task-level retry
// count is named task_retry_count.
func ParseNodeDeployed(msg goredis.XMessage, defaultMaxRetries int) (command.CheckJobStatus, error) {
	var wire pkgevents.NodeDeployed
	if err := decodePayload(msg, &wire); err != nil {
		return command.CheckJobStatus{}, err
	}
	return buildCheckJobStatus(checkJobFields{
		taskID:       wire.TaskID,
		scheduleID:   wire.ScheduleID,
		scheduleName: wire.ScheduleName,
		serviceName:  wire.ServiceName,
		schemaName:   wire.SchemaName,
		tableName:    wire.TableName,
		jobName:      wire.JobName,
		nodeType:     wire.NodeType,
		imageTag:     wire.ImageTag,
		retryCount:   wire.TaskRetryCount,
		maxRetries:   wire.MaxRetries,
	}, defaultMaxRetries)
}

// ParseCheckK8s decodes a check.k8s:v1 message into a CheckJobStatus. The typed
// event travels in the JSON `payload` field; its task-level retry count is named
// retry_count.
func ParseCheckK8s(msg goredis.XMessage, defaultMaxRetries int) (command.CheckJobStatus, error) {
	var wire pkgevents.CheckK8s
	if err := decodePayload(msg, &wire); err != nil {
		return command.CheckJobStatus{}, err
	}
	return buildCheckJobStatus(checkJobFields{
		taskID:           wire.TaskID,
		scheduleID:       wire.ScheduleID,
		scheduleName:     wire.ScheduleName,
		serviceName:      wire.ServiceName,
		schemaName:       wire.SchemaName,
		tableName:        wire.TableName,
		jobName:          wire.JobName,
		nodeType:         wire.NodeType,
		imageTag:         wire.ImageTag,
		retryCount:       wire.RetryCount,
		maxRetries:       wire.MaxRetries,
		runningAnnounced: wire.RunningAnnounced,
	}, defaultMaxRetries)
}

// decodePayload reads the `payload` field and unmarshals it into dst.
func decodePayload(msg goredis.XMessage, dst interface{}) error {
	payloadStr, _ := msg.Values["payload"].(string)
	if payloadStr == "" {
		return fmt.Errorf("missing payload field")
	}
	if err := json.Unmarshal([]byte(payloadStr), dst); err != nil {
		return fmt.Errorf("invalid payload JSON: %w", err)
	}
	return nil
}

// checkJobFields holds the common fields both streams decode into a command.
type checkJobFields struct {
	taskID           string
	scheduleID       string
	scheduleName     string
	serviceName      string
	schemaName       string
	tableName        string
	jobName          string
	nodeType         string
	imageTag         string
	retryCount       int32
	maxRetries       int32
	runningAnnounced bool
}

// buildCheckJobStatus validates the decoded fields and assembles the command.
// A non-positive max_retries is treated as absent; it falls back to
// defaultMaxRetries.
func buildCheckJobStatus(f checkJobFields, defaultMaxRetries int) (command.CheckJobStatus, error) {
	taskID, err := uuid.Parse(f.taskID)
	if err != nil {
		return command.CheckJobStatus{}, fmt.Errorf("invalid task_id: %w", err)
	}
	scheduleID, err := uuid.Parse(f.scheduleID)
	if err != nil {
		return command.CheckJobStatus{}, fmt.Errorf("invalid schedule_id: %w", err)
	}

	maxRetries := f.maxRetries
	if maxRetries <= 0 {
		maxRetries = int32(defaultMaxRetries)
	}

	return command.CheckJobStatus{
		TaskID:           taskID,
		ScheduleID:       scheduleID,
		ScheduleName:     f.scheduleName,
		ServiceName:      f.serviceName,
		SchemaName:       f.schemaName,
		TableName:        f.tableName,
		JobName:          f.jobName,
		NodeType:         f.nodeType,
		ImageTag:         f.imageTag,
		RetryCount:       f.retryCount,
		MaxRetries:       maxRetries,
		RunningAnnounced: f.runningAnnounced,
	}, nil
}

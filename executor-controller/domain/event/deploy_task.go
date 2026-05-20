package event

import (
	"github.com/carolsimone/continuo/executor-controller/adapters/k8s"
	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
)

// DeployTask is the payload of an executor_outbox row whose event_type
// is "deploy_task". It carries everything the executor Publisher needs to
// (a) create the K8s Job, (b) publish task.status.updated:v1 RUNNING, and
// (c) publish node.deployed:v1.
type DeployTask struct {
	TaskID         string `json:"task_id"`
	ScheduleID     string `json:"schedule_id"`
	ScheduleName   string `json:"schedule_name"`
	ServiceName    string `json:"service_name"`
	SchemaName     string `json:"schema_name"`
	TableName      string `json:"table_name"`
	JobName        string `json:"job_name"`
	NodeType       string `json:"node_type"`
	ImageTag       string `json:"image_tag"`
	TaskRetryCount int    `json:"task_retry_count"`
	TaskMaxRetries int    `json:"task_max_retries"`
}

// ToJobParams converts DeployTask to k8s.JobParams for CreateQueryJob.
// Namespace is supplied by the Publisher at runtime since it's a deployment
// concern, not part of the event payload.
func (d DeployTask) ToJobParams(namespace string) (k8s.JobParams, error) {
	nodeType, err := pkg_model.ParseNodeType(d.NodeType)
	if err != nil {
		return k8s.JobParams{}, err
	}
	return k8s.JobParams{
		JobName:      d.JobName,
		TaskID:       d.TaskID,
		ScheduleID:   d.ScheduleID,
		ScheduleName: d.ScheduleName,
		ServiceName:  d.ServiceName,
		SchemaName:   d.SchemaName,
		TableName:    d.TableName,
		Namespace:    namespace,
		NodeType:     nodeType,
		ImageTag:     d.ImageTag,
	}, nil
}

// ToNodeDeployedEvent produces the typed node.deployed:v1 payload that the
// Publisher marshals into the message's JSON `payload` field for k8s-controller.
func (d DeployTask) ToNodeDeployedEvent() pkgevents.NodeDeployed {
	return pkgevents.NodeDeployed{
		TaskID:         d.TaskID,
		ScheduleID:     d.ScheduleID,
		ScheduleName:   d.ScheduleName,
		ServiceName:    d.ServiceName,
		SchemaName:     d.SchemaName,
		TableName:      d.TableName,
		JobName:        d.JobName,
		NodeType:       d.NodeType,
		ImageTag:       d.ImageTag,
		TaskRetryCount: int32(d.TaskRetryCount),
		MaxRetries:     int32(d.TaskMaxRetries),
	}
}

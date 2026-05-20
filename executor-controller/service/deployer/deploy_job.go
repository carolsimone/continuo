// Package deployer owns executor-controller's K8s-deploy command queue:
// the DeployJob payload, the Deployment row, the Repository over
// executor_deployments, and the Dispatcher that drains it.
package deployer

import (
	"github.com/carolsimone/continuo/executor-controller/adapters/k8s"
	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
)

// DeployJob is the JSONB payload stored in executor_deployments.job_params.
// It carries everything CreateQueryJob needs plus the identity the dispatcher
// uses to build the RUNNING / node_deployed / FAILED announcement rows.
type DeployJob struct {
	TaskID         string `json:"task_id"`
	ScheduleID     string `json:"schedule_id"`
	ScheduleName   string `json:"schedule_name"`
	ServiceName    string `json:"service_name"`
	SchemaName     string `json:"schema_name"`
	TableName      string `json:"table_name"`
	JobName        string `json:"job_name"`
	NodeType       string `json:"node_type"`
	ImageTag       string `json:"image_tag"`
	// TaskRetryCount and TaskMaxRetries are the task-level retry counters the
	// Dispatcher uses to build the RUNNING/FAILED announcement rows and to decide
	// exhaustion. They are deliberately NOT forwarded into k8s.JobParams by ToJobParams.
	TaskRetryCount int `json:"task_retry_count"`
	TaskMaxRetries int `json:"task_max_retries"`
}

// ToJobParams converts DeployJob to k8s.JobParams for CreateQueryJob.
// Namespace is supplied by the dispatcher at runtime since it is a deployment
// concern, not part of the queued payload.
func (d DeployJob) ToJobParams(namespace string) (k8s.JobParams, error) {
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

// Package command holds executor-controller's domain commands — instructions
// to perform work, distinct from the events that announce work has happened.
package command

import "github.com/carolsimone/continuo/executor-controller/domain/deploy"

// Command is a marker interface for all commands.
type Command interface {
	isCommand()
}

// DeployTask is the command to deploy one task's dbt job. It is the payload of
// a queued deployment and carries everything needed both to perform the deploy
// and to build the RUNNING / node_deployed / FAILED announcements afterwards.
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

func (DeployTask) isCommand() {}

// ValidationDeployTask is the command to deploy one validation node's dbt
// --empty job. Parallel to DeployTask: production fields stay on DeployTask;
// validation-only fields (ReleaseID, NodeID, CandidateSchema, UpstreamNodeIDs)
// live here. The dispatcher branches on which command sits behind the
// executor_deployments row's mode column.
//
// UpstreamNodeIDs lists the dbt unique_ids of intra-service nodes that gate
// dispatch of this node. It is persisted in job_params and read back by the
// dispatcher to evaluate whether all upstreams have completed successfully.
type ValidationDeployTask struct {
	ReleaseID        string   `json:"release_id"`
	NodeID           string   `json:"node_id"`
	ServiceName      string   `json:"service_name"`
	SchemaName       string   `json:"schema_name"`
	TableName        string   `json:"table_name"`
	NodeType         string   `json:"node_type"`
	ImageTag         string   `json:"image_tag"`
	JobName          string   `json:"job_name"`
	CandidateSchema  string   `json:"candidate_schema"`
	CandidateSQLURI  string   `json:"candidate_sql_uri"`
	ValidationOp     string   `json:"validation_op"`
	ProdSchema       string   `json:"prod_schema"`
	UpstreamNodeIDs  []string `json:"upstream_node_ids"`
	ManifestS3URI    string   `json:"manifest_s3_uri"`
}

func (ValidationDeployTask) isCommand() {}

// ToJobSpec projects the command onto the domain deploy.JobSpec the Deployer
// port consumes. The mapping is a pure field copy — no infrastructure concern.
func (c DeployTask) ToJobSpec() deploy.JobSpec {
	return deploy.JobSpec{
		JobName:      c.JobName,
		TaskID:       c.TaskID,
		ScheduleID:   c.ScheduleID,
		ScheduleName: c.ScheduleName,
		ServiceName:  c.ServiceName,
		SchemaName:   c.SchemaName,
		TableName:    c.TableName,
		NodeType:     c.NodeType,
		ImageTag:     c.ImageTag,
	}
}

// ToValidationJobSpec projects the command onto the domain
// deploy.ValidationJobSpec the Deployer port consumes for mode=validation
// rows. The mapping is a pure field copy — no infrastructure concern.
// UpstreamNodeIDs is intentionally not forwarded: gating lives in the
// dispatcher, not in the K8s Job pod.
func (c ValidationDeployTask) ToValidationJobSpec() deploy.ValidationJobSpec {
	return deploy.ValidationJobSpec{
		JobName:         c.JobName,
		ReleaseID:       c.ReleaseID,
		NodeID:          c.NodeID,
		ServiceName:     c.ServiceName,
		SchemaName:      c.SchemaName,
		TableName:       c.TableName,
		NodeType:        c.NodeType,
		ImageTag:        c.ImageTag,
		CandidateSchema: c.CandidateSchema,
		CandidateSQLURI: c.CandidateSQLURI,
		ValidationOp:    c.ValidationOp,
		ProdSchema:      c.ProdSchema,
	}
}

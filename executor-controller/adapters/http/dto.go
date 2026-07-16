package http

import (
	"github.com/carolsimone/continuo/executor-controller/domain/command"
	"github.com/carolsimone/continuo/executor-controller/domain/model"
	"github.com/carolsimone/continuo/executor-controller/service/lease"
)

// runtimeResponse hands a worker the two reads it needs to hydrate: the
// descriptor that says what the artifact is, and the artifact itself.
type runtimeResponse struct {
	DescriptorURL string `json:"descriptor_url"`
	ArtifactURL   string `json:"artifact_url"`
}

// claimRequest is a worker asking for work. The pool it claims for comes from
// its credential, never from this body.
type claimRequest struct {
	// WaitSeconds is how long the worker is willing to wait for work. The
	// executor caps it at its own configured ceiling.
	WaitSeconds int    `json:"wait_seconds"`
	Owner       string `json:"owner"`
	PodName     string `json:"pod_name"`
	PodUID      string `json:"pod_uid"`
}

// leaseResponse is a granted lease. LeaseToken is the raw token, returned here
// exactly once: the executor stores only its digest and cannot reissue it.
type leaseResponse struct {
	DeploymentID  string   `json:"deployment_id"`
	LeaseID       string   `json:"lease_id"`
	LeaseToken    string   `json:"lease_token"`
	Attempt       int      `json:"attempt"`
	ExpiresAt     string   `json:"expires_at"`
	ExecutionPath string   `json:"execution_path"`
	Argv          []string `json:"argv"`
	Task          taskDTO  `json:"task"`
}

// taskDTO is the dbt node a lease covers.
type taskDTO struct {
	TaskID      string `json:"task_id"`
	ScheduleID  string `json:"schedule_id"`
	ServiceName string `json:"service_name"`
	SchemaName  string `json:"schema_name"`
	TableName   string `json:"table_name"`
	DBTUniqueID string `json:"dbt_unique_id"`
}

func newTaskDTO(cmd command.DeployTask) taskDTO {
	return taskDTO{
		TaskID:      cmd.TaskID,
		ScheduleID:  cmd.ScheduleID,
		ServiceName: cmd.ServiceName,
		SchemaName:  cmd.SchemaName,
		TableName:   cmd.TableName,
		DBTUniqueID: cmd.DBTUniqueID,
	}
}

// leaseRequest names the task a report is about. The lease itself is in the
// path; the deployment is the row the lease sits on.
type leaseRequest struct {
	DeploymentID string `json:"deployment_id"`
}

// completeRequest is a worker's terminal report.
type completeRequest struct {
	DeploymentID string             `json:"deployment_id"`
	Result       model.WorkerResult `json:"result"`
}

// signedObject is one object a worker may write, as both the capability to
// write it and the location the executor will record.
type signedObject struct {
	URL   string `json:"url"`
	S3URI string `json:"s3_uri"`
}

// resultURLsResponse hands a worker the two uploads its task may make.
type resultURLsResponse struct {
	Log        signedObject `json:"log"`
	RunResults signedObject `json:"run_results"`
}

// initializationRequest is a worker's account of hydrating its artifact. The
// pool it speaks for comes from its credential, never from this body.
type initializationRequest struct {
	OK               bool    `json:"ok"`
	ErrorCode        string  `json:"error_code"`
	Message          string  `json:"message"`
	HydrationSeconds float64 `json:"hydration_seconds"`
}

// newLeaseResponse renders a granted lease. It is the one place the raw token
// crosses the wire.
func newLeaseResponse(g *lease.Grant, expiresAt string) leaseResponse {
	return leaseResponse{
		DeploymentID:  g.DeploymentID.String(),
		LeaseID:       g.LeaseID.String(),
		LeaseToken:    g.Token,
		Attempt:       g.Attempt,
		ExpiresAt:     expiresAt,
		ExecutionPath: string(g.ExecutionPath),
		Argv:          g.Argv,
		Task:          newTaskDTO(g.Command),
	}
}

// errorResponse is the stable envelope every rejection carries, so a worker can
// branch on Code rather than parse prose.
type errorResponse struct {
	Error errorDetail `json:"error"`
}

type errorDetail struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

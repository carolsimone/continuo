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
	TaskID         string
	ScheduleID     string
	ScheduleName   string
	ServiceName    string
	SchemaName     string
	TableName      string
	JobName        string
	NodeType       string
	ImageTag       string
	TaskRetryCount int
	TaskMaxRetries int
	// Operation selects the dbt verb the executor runs for this node. Empty
	// (pkg_model.OperationRun) is the default: dbt run/seed/snapshot by
	// NodeType. "test" runs `dbt test --select <node>`.
	Operation string
	// Mode carries the legacy promote-seed dispatch mode for deployments queued
	// by an older version. Empty for everything current. See events.ModePromoteSeed.
	Mode string
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
//
// The whole struct is stored in the executor_deployments.job_params JSONB
// column and read back on dispatch; the JSON shape lives on
// serialization.ValidationDeployTaskDTO, which the postgres repository maps to
// and from this type.
type ValidationDeployTask struct {
	ReleaseID            string
	NodeID               string
	ServiceName          string
	SchemaName           string
	TableName            string
	NodeType             string
	ImageTag             string
	JobName              string
	CandidateSchema      string
	CandidateArtifactURI string
	ValidationOp         string
	ProdSchema           string
	UpstreamNodeIDs      []string
	ManifestS3URI        string
	// ParseProdS3URI / ParseCandidateS3URI are the S3 destinations for the
	// compile Job's exported partial-parse artifacts. Empty (older
	// compile.requested messages without candidate_schema) disables the
	// parse-export leg for this release. Their DTO fields are omitempty so an
	// absent value keeps the job_params JSON byte-identical.
	ParseProdS3URI      string
	ParseCandidateS3URI string
	// SourceOverlayURI locates the source-overlay tarball a verification
	// run's compile Job lays over the project before compiling; empty for
	// every production release. Its DTO field is omitempty so an absent value
	// keeps the job_params JSON byte-identical.
	SourceOverlayURI string
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
		Operation:    c.Operation,
		Mode:         c.Mode,
	}
}

// ToValidationJobSpec projects the command onto the domain
// deploy.ValidationJobSpec the Deployer port consumes for mode=validation
// rows. The mapping is a pure field copy — no infrastructure concern.
// UpstreamNodeIDs is intentionally not forwarded: gating lives in the
// dispatcher, not in the K8s Job pod.
func (c ValidationDeployTask) ToValidationJobSpec() deploy.ValidationJobSpec {
	return deploy.ValidationJobSpec{
		JobName:              c.JobName,
		ReleaseID:            c.ReleaseID,
		NodeID:               c.NodeID,
		ServiceName:          c.ServiceName,
		SchemaName:           c.SchemaName,
		TableName:            c.TableName,
		NodeType:             c.NodeType,
		ImageTag:             c.ImageTag,
		CandidateSchema:      c.CandidateSchema,
		CandidateArtifactURI: c.CandidateArtifactURI,
		ValidationOp:         c.ValidationOp,
		ProdSchema:           c.ProdSchema,
		ManifestS3URI:        c.ManifestS3URI,
		ParseProdS3URI:       c.ParseProdS3URI,
		ParseCandidateS3URI:  c.ParseCandidateS3URI,
		SourceOverlayURI:     c.SourceOverlayURI,
	}
}

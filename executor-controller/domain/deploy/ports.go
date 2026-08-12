// Package deploy holds the domain port for executing a K8s deploy and the
// JobSpec value object that describes one. The application service depends on
// this port; the k8s adapter implements it. No infrastructure types leak here.
package deploy

import "context"

// JobSpec is the domain description of a single deploy operation. It carries
// only what the domain knows about the work — never adapter concerns such as
// the Kubernetes namespace or label selector.
type JobSpec struct {
	JobName      string
	TaskID       string
	ScheduleID   string
	ScheduleName string
	ServiceName  string
	SchemaName   string
	TableName    string
	NodeType     string
	ImageTag     string
	// Operation selects the dbt verb the executor runs for this node. Empty
	// is the default: dbt run/seed/snapshot by NodeType. "test" runs
	// `dbt test --select <node>`.
	Operation string
	// Mode carries the legacy promote-seed dispatch mode for queued work; the
	// k8s adapter stamps it as a Job label so k8s-controller keeps suppressing
	// that work's lifecycle events. Empty for everything current.
	Mode string
}

// ValidationJobSpec is the domain description of a single validation deploy.
// It mirrors JobSpec for the production fields a validation node still needs
// (service/schema/table, node type, image tag, job name) and adds the
// validation-only fields (release/node identity, candidate schema). Like
// JobSpec it carries no adapter concerns such as the namespace; NodeType stays
// a string so the domain port does not depend on pkg_model.
type ValidationJobSpec struct {
	JobName              string
	ReleaseID            string
	NodeID               string
	ServiceName          string
	SchemaName           string
	TableName            string
	NodeType             string
	ImageTag             string
	CandidateSchema      string
	CandidateArtifactURI string
	ValidationOp         string
	ProdSchema           string
	// ManifestS3URI is the S3 destination where the compile Job uploads the
	// compiled manifest.json. Populated only for mode=compile dispatches; empty
	// for validation and seed-build dispatches.
	ManifestS3URI string
	// ParseProdS3URI / ParseCandidateS3URI are the S3 destinations for the
	// compile Job's exported partial-parse artifacts. Empty (older
	// compile.requested messages without candidate_schema) disables the
	// parse-export leg for this release.
	ParseProdS3URI      string
	ParseCandidateS3URI string
}

// Deployer is the driven port the dispatcher uses to deploy work and observe
// how many deploys are currently in flight.
type Deployer interface {
	// Deploy executes the job described by spec. Implementations must be
	// idempotent by job name so a redelivery is a no-op.
	Deploy(ctx context.Context, spec JobSpec) error
	// DeployValidation executes the mode=validation job described by spec.
	// Implementations must be idempotent by job name so a redelivery is a
	// no-op.
	DeployValidation(ctx context.Context, spec ValidationJobSpec) error
	// DeploySeedBuild executes the mode=seed_build job described by spec.
	// The job uses the team image and runs `dbt seed --select <TableName>`,
	// materializing into the candidate schema. Implementations must be
	// idempotent by job name so a redelivery is a no-op.
	DeploySeedBuild(ctx context.Context, spec ValidationJobSpec) error
	// DeployCompile executes the mode=compile job described by spec. The job
	// runs `dbt compile` in the team image (init container) and uploads the
	// resulting manifest.json to S3 (main container). Implementations must be
	// idempotent by job name so a redelivery is a no-op.
	DeployCompile(ctx context.Context, spec ValidationJobSpec) error
	// CountActive returns the number of deploys currently running.
	CountActive(ctx context.Context) (int, error)
}

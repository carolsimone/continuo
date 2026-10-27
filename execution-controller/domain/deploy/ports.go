// Package deploy holds the domain port for executing a K8s deploy and the
// JobSpec value object that describes one. The application service depends on
// this port; the k8s adapter implements it. No infrastructure types leak here.
package deploy

import (
	"context"
	"errors"
)

// Errors a Deployer returns for a dispatch it can never run. Each wraps
// pkg/events.ErrPermanent, so no creation try is repeated.
var (
	// ErrInvalidNodeType: the node type is not one the vocabulary declares.
	ErrInvalidNodeType = errors.New("the dispatch names an undeclared node type")
	// ErrMalformedDispatch: another field of the dispatch cannot run (its
	// operation, image, timeout, or a node no command builds).
	ErrMalformedDispatch = errors.New("the dispatch cannot run")
	// ErrJobNameCollision: a Job of the name exists for another task or
	// attempt.
	ErrJobNameCollision = errors.New("a Job of the name exists for another task or attempt")
)

// JobSpec is the domain description of a single deploy operation. It carries
// only what the domain knows about the work — never adapter concerns such as
// the Kubernetes namespace or label selector.
type JobSpec struct {
	JobName      string
	TaskID       string
	RunID        string
	ScheduleName string
	ServiceName  string
	SchemaName   string
	TableName    string
	NodeType     string
	ImageTag     string
	// SecretRef names the continuo-api-* Secret a python-api node's pod
	// receives as env vars; empty for every other node.
	SecretRef string
	// Operation selects the dbt verb the executor runs for this node. Empty
	// is the default: dbt run/seed/snapshot by NodeType. "test" runs
	// `dbt test --select <node>`.
	Operation string
	// Attempt is which attempt of the task the Job runs.
	Attempt int
	// TimeoutSeconds is how long the attempt's container may run once the
	// Job has started its pod.
	TimeoutSeconds int64
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
	// SourceOverlayURI locates the source-overlay tarball a verification
	// run's compile Job lays over the project before compiling. Populated
	// only for mode=compile dispatches of a verification run.
	SourceOverlayURI string
}

// Deployer is the driven port the dispatcher uses to create Jobs.
type Deployer interface {
	// Deploy executes the job described by spec. A Job of the name that
	// carries the same task and attempt counts as created, so a repeated
	// launch is a no-op; one of another task or attempt is
	// ErrJobNameCollision.
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
}

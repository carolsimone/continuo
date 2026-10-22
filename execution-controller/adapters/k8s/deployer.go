package k8s

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/carolsimone/continuo/execution-controller/domain/deploy"
	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
)

// JobSelector matches the label every Job execution-controller creates, and
// every pod of one, carries; the Job watch observes exactly these.
const JobSelector = "app=dbt-job"

// Deployer adapts the K8sClient to the domain deploy.Deployer port. It holds
// the namespace and the deadlines of the Jobs it creates, so those
// infrastructure concerns stay out of the domain and application layers.
type Deployer struct {
	client    *K8sClient
	namespace string
	// startTimeout is the start timeout every Job's deadline includes, since
	// Kubernetes counts it from the Job's start; legTimeout is how long a
	// release leg's container may run.
	startTimeout time.Duration
	legTimeout   time.Duration
}

// NewDeployer wires a domain Deployer to the given K8s client and namespace.
// A task's Job runs up to startTimeout plus the task's own timeout, a release
// leg's up to startTimeout plus legTimeout.
func NewDeployer(client *K8sClient, namespace string, startTimeout, legTimeout time.Duration) *Deployer {
	return &Deployer{client: client, namespace: namespace, startTimeout: startTimeout, legTimeout: legTimeout}
}

// Deploy maps the domain JobSpec to K8s job params and creates the Job of
// the attempt. A node type the vocabulary does not declare is
// deploy.ErrInvalidNodeType and a dispatch without a timeout
// deploy.ErrMalformedDispatch; neither can ever succeed.
func (d *Deployer) Deploy(ctx context.Context, spec deploy.JobSpec) error {
	nodeType, err := pkg_model.ParseNodeType(spec.NodeType)
	if err != nil {
		return fmt.Errorf("%w %q: %w", deploy.ErrInvalidNodeType, spec.NodeType, errors.Join(err, pkgevents.ErrPermanent))
	}
	if spec.TimeoutSeconds <= 0 {
		return fmt.Errorf("%w: timeout_seconds %d for Job %s", errors.Join(deploy.ErrMalformedDispatch, pkgevents.ErrPermanent),
			spec.TimeoutSeconds, spec.JobName)
	}
	params := jobParamsFromSpec(spec, nodeType, d.namespace)
	params.ActiveDeadlineSeconds = d.deadline(time.Duration(spec.TimeoutSeconds) * time.Second)
	return d.client.CreateQueryJob(ctx, params)
}

// deadline is a Job's activeDeadlineSeconds for a container that may run for
// timeout: Kubernetes counts from the Job's start, so the start timeout is
// added.
func (d *Deployer) deadline(timeout time.Duration) int64 {
	return int64((d.startTimeout + timeout).Seconds())
}

// jobParamsFromSpec maps the domain JobSpec onto the K8s query-job params.
func jobParamsFromSpec(spec deploy.JobSpec, nodeType pkg_model.NodeType, namespace string) JobParams {
	return JobParams{
		JobName:      spec.JobName,
		TaskID:       spec.TaskID,
		ScheduleID:   spec.ScheduleID,
		ScheduleName: spec.ScheduleName,
		ServiceName:  spec.ServiceName,
		SchemaName:   spec.SchemaName,
		TableName:    spec.TableName,
		Namespace:    namespace,
		NodeType:     nodeType,
		ImageTag:     spec.ImageTag,
		SecretRef:    spec.SecretRef,
		Operation:    pkg_model.Operation(spec.Operation),
		Attempt:      spec.Attempt,
	}
}

// DeployValidation maps the domain ValidationJobSpec to K8s validation job
// params and creates the Job (idempotent by job name). An unparseable node
// type can never succeed, so it is reported as a permanent error.
func (d *Deployer) DeployValidation(ctx context.Context, spec deploy.ValidationJobSpec) error {
	nodeType, err := pkg_model.ParseNodeType(spec.NodeType)
	if err != nil {
		return fmt.Errorf("invalid node type %q: %w", spec.NodeType, errors.Join(err, pkgevents.ErrPermanent))
	}
	return d.client.CreateValidationJob(ctx, ValidationJobParams{
		JobName:               spec.JobName,
		ReleaseID:             spec.ReleaseID,
		NodeID:                spec.NodeID,
		ServiceName:           spec.ServiceName,
		SchemaName:            spec.SchemaName,
		TableName:             spec.TableName,
		NodeType:              nodeType,
		ImageTag:              spec.ImageTag,
		CandidateSchema:       spec.CandidateSchema,
		CandidateArtifactURI:  spec.CandidateArtifactURI,
		ValidationOp:          spec.ValidationOp,
		ProdSchema:            spec.ProdSchema,
		ActiveDeadlineSeconds: d.deadline(d.legTimeout),
		Namespace:             d.namespace,
	})
}

// DeploySeedBuild maps the domain ValidationJobSpec to K8s seed-build job
// params and creates the Job (idempotent by job name). The job uses the team
// image and runs `dbt seed --select <TableName>` into the candidate schema; a
// verification run's SourceOverlayURI travels with it so the seed is loaded
// from the proposed source rather than the checked-in project.
// An unparseable node type can never succeed, so it is reported as a permanent error.
func (d *Deployer) DeploySeedBuild(ctx context.Context, spec deploy.ValidationJobSpec) error {
	nodeType, err := pkg_model.ParseNodeType(spec.NodeType)
	if err != nil {
		return fmt.Errorf("invalid node type %q: %w", spec.NodeType, errors.Join(err, pkgevents.ErrPermanent))
	}
	return d.client.CreateSeedBuildJob(ctx, ValidationJobParams{
		JobName:               spec.JobName,
		ReleaseID:             spec.ReleaseID,
		NodeID:                spec.NodeID,
		ServiceName:           spec.ServiceName,
		SchemaName:            spec.SchemaName,
		TableName:             spec.TableName,
		NodeType:              nodeType,
		ImageTag:              spec.ImageTag,
		CandidateSchema:       spec.CandidateSchema,
		SourceOverlayURI:      spec.SourceOverlayURI,
		ActiveDeadlineSeconds: d.deadline(d.legTimeout),
		Namespace:             d.namespace,
	})
}

// DeployCompile maps the domain ValidationJobSpec to K8s compile job params
// and creates the Job (idempotent by job name). The compile Job runs `dbt
// compile` in the team image (init container) and uploads the resulting
// manifest.json to S3 (main container).
func (d *Deployer) DeployCompile(ctx context.Context, spec deploy.ValidationJobSpec) error {
	params, err := compileParamsFromSpec(spec, d.namespace)
	if err != nil {
		return err
	}
	params.ActiveDeadlineSeconds = d.deadline(d.legTimeout)
	return d.client.CreateCompileJob(ctx, params)
}

// compileParamsFromSpec maps the domain ValidationJobSpec to K8s
// ValidationJobParams for a compile Job. An unparseable node type is NOT an
// error here — compile Jobs do not use NodeType (they compile the full
// service manifest, not a single dbt node). We still forward the field for
// symmetry. Extracted from DeployCompile so the field-by-field mapping is
// directly unit-testable without a K8s client.
func compileParamsFromSpec(spec deploy.ValidationJobSpec, namespace string) (ValidationJobParams, error) {
	var nodeType pkg_model.NodeType
	if spec.NodeType != "" {
		var err error
		nodeType, err = pkg_model.ParseNodeType(spec.NodeType)
		if err != nil {
			return ValidationJobParams{}, fmt.Errorf("invalid node type %q: %w", spec.NodeType, errors.Join(err, pkgevents.ErrPermanent))
		}
	}
	return ValidationJobParams{
		JobName:             spec.JobName,
		ReleaseID:           spec.ReleaseID,
		NodeID:              spec.NodeID,
		ServiceName:         spec.ServiceName,
		NodeType:            nodeType,
		ImageTag:            spec.ImageTag,
		ManifestS3URI:       spec.ManifestS3URI,
		CandidateSchema:     spec.CandidateSchema,
		ParseProdS3URI:      spec.ParseProdS3URI,
		ParseCandidateS3URI: spec.ParseCandidateS3URI,
		SourceOverlayURI:    spec.SourceOverlayURI,
		Namespace:           namespace,
	}, nil
}

var _ deploy.Deployer = (*Deployer)(nil)

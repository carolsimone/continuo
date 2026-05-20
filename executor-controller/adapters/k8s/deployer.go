package k8s

import (
	"context"
	"errors"
	"fmt"

	"github.com/carolsimone/continuo/executor-controller/domain/deploy"
	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
)

// dbtJobLabelSelector matches the label every executor dbt Job carries.
const dbtJobLabelSelector = "app=dbt-job"

// Deployer adapts the K8sClient to the domain deploy.Deployer port. It holds
// the namespace and label selector so those infrastructure concerns stay out
// of the domain and application layers.
type Deployer struct {
	client    *K8sClient
	namespace string
}

// NewDeployer wires a domain Deployer to the given K8s client and namespace.
func NewDeployer(client *K8sClient, namespace string) *Deployer {
	return &Deployer{client: client, namespace: namespace}
}

// Deploy maps the domain JobSpec to K8s job params and creates the Job
// (idempotent by job name). An unparseable node type can never succeed, so it
// is reported as a permanent error.
func (d *Deployer) Deploy(ctx context.Context, spec deploy.JobSpec) error {
	nodeType, err := pkg_model.ParseNodeType(spec.NodeType)
	if err != nil {
		return fmt.Errorf("invalid node type %q: %w", spec.NodeType, errors.Join(err, pkgevents.ErrPermanent))
	}
	return d.client.CreateQueryJob(ctx, JobParams{
		JobName:      spec.JobName,
		TaskID:       spec.TaskID,
		ScheduleID:   spec.ScheduleID,
		ScheduleName: spec.ScheduleName,
		ServiceName:  spec.ServiceName,
		SchemaName:   spec.SchemaName,
		TableName:    spec.TableName,
		Namespace:    d.namespace,
		NodeType:     nodeType,
		ImageTag:     spec.ImageTag,
	})
}

// CountActive returns the number of executor dbt Jobs currently running.
func (d *Deployer) CountActive(ctx context.Context) (int, error) {
	return d.client.CountActiveJobs(ctx, d.namespace, dbtJobLabelSelector)
}

var _ deploy.Deployer = (*Deployer)(nil)

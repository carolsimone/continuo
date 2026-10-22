package k8s

import (
	"context"
	"testing"
	"time"

	"github.com/carolsimone/continuo/execution-controller/domain/deploy"
	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

func attemptSpec(nodeType pkg_model.NodeType, image string) deploy.JobSpec {
	return deploy.JobSpec{
		JobName: "service-1-e2e-schema-ftable-e-7f3e1a2b-r1", TaskID: "3e8d7c6b-5a49-4382-9170-6f5e4d3c2b1a",
		ScheduleID: "7f3e1a2b-4c5d-4e6f-8a9b-0c1d2e3f4a5b", ScheduleName: "e2e-schedule-failure",
		ServiceName: "service-1", SchemaName: "e2e_schema", TableName: "ftable_e",
		NodeType: string(nodeType), ImageTag: image, Attempt: 1, TimeoutSeconds: 3600,
	}
}

// A production Job names its attempt on the Job and its pod, the pod's
// container reads it as TASK_ATTEMPT, and Kubernetes stops the Job once its pod
// has been alive for the start timeout plus the task's timeout.
func TestDeploy_TheJobCarriesItsAttemptAndItsDeadline(t *testing.T) {
	t.Setenv("VALIDATION_WAREHOUSE_SECRET", "warehouse-conn")
	t.Setenv("DOCKERHUB_USERNAME", "")
	for nodeType, image := range map[pkg_model.NodeType]string{
		pkg_model.NodeTypeDbtModel:   "v42",
		pkg_model.NodeTypePythonNode: "ghcr.io/acme/py:12-abc1234",
	} {
		t.Run(string(nodeType), func(t *testing.T) {
			client := newValidationTestClient()
			d := NewDeployer(client, "default", 15*time.Minute, 24*time.Hour)
			spec := attemptSpec(nodeType, image)

			require.NoError(t, d.Deploy(context.Background(), spec))

			job := fetchJob(t, client, "default", spec.JobName)
			assert.Equal(t, "1", job.Labels["attempt"])
			assert.Equal(t, "1", job.Spec.Template.Labels["attempt"])
			assert.Empty(t, validation.IsValidLabelValue(job.Labels["attempt"]))
			assert.Equal(t, "1", envByName(job.Spec.Template.Spec, "TASK_ATTEMPT"))
			require.NotNil(t, job.Spec.ActiveDeadlineSeconds)
			assert.Equal(t, int64(15*60+3600), *job.Spec.ActiveDeadlineSeconds)
		})
	}
}

// A dispatch without a timeout cannot run: no deadline could be set.
func TestDeploy_RefusesADispatchWithoutATimeout(t *testing.T) {
	client := newValidationTestClient()
	spec := attemptSpec(pkg_model.NodeTypeDbtModel, "v42")
	spec.TimeoutSeconds = 0

	err := NewDeployer(client, "default", 15*time.Minute, 24*time.Hour).Deploy(context.Background(), spec)

	require.ErrorIs(t, err, deploy.ErrMalformedDispatch)
}

// The Job of the same task and attempt, left by an earlier launch, is adopted;
// a Job of the name that belongs to another task or attempt is a collision.
func TestDeploy_AdoptsOnlyTheJobOfTheSameAttempt(t *testing.T) {
	t.Setenv("DOCKERHUB_USERNAME", "")
	spec := attemptSpec(pkg_model.NodeTypeDbtModel, "v42")
	for name, c := range map[string]struct {
		labels map[string]string
		err    error
	}{
		"same task and attempt": {labels: map[string]string{"task-id": spec.TaskID, "attempt": "1"}},
		"another attempt":       {labels: map[string]string{"task-id": spec.TaskID, "attempt": "0"}, err: deploy.ErrJobNameCollision},
		"another task":          {labels: map[string]string{"task-id": "another", "attempt": "1"}, err: deploy.ErrJobNameCollision},
		"no labels":             {err: deploy.ErrJobNameCollision},
	} {
		t.Run(name, func(t *testing.T) {
			existing := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: spec.JobName, Namespace: "default", Labels: c.labels}}
			client := newQueryTestClient()
			client.setClientsetForTest(fake.NewSimpleClientset(existing))

			err := NewDeployer(client, "default", 15*time.Minute, 24*time.Hour).Deploy(context.Background(), spec)

			if c.err == nil {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, c.err)
		})
	}
}

// A Job of the name that another launch creates between the check for one and
// the creation is judged like a Job that was there before: adopted when it is
// this attempt's, a collision when it is another's.
func TestDeploy_AJobCreatedDuringTheCreationIsJudgedLikeAnExistingOne(t *testing.T) {
	t.Setenv("VALIDATION_WAREHOUSE_SECRET", "warehouse-conn")
	t.Setenv("DOCKERHUB_USERNAME", "")
	spec := attemptSpec(pkg_model.NodeTypeDbtModel, "v42")
	for name, c := range map[string]struct {
		labels map[string]string
		err    error
	}{
		"same task and attempt": {labels: map[string]string{"task-id": spec.TaskID, "attempt": "1"}},
		"another task":          {labels: map[string]string{"task-id": "another", "attempt": "1"}, err: deploy.ErrJobNameCollision},
	} {
		t.Run(name, func(t *testing.T) {
			rival := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: spec.JobName, Namespace: "default", Labels: c.labels}}
			cs := fake.NewSimpleClientset(rival)
			checked := false
			cs.PrependReactor("get", "jobs", func(k8stesting.Action) (bool, runtime.Object, error) {
				if checked {
					return false, nil, nil
				}
				checked = true
				return true, nil, k8serrors.NewNotFound(batchv1.Resource("jobs"), spec.JobName)
			})
			client := newValidationTestClient()
			client.setClientsetForTest(cs)

			err := NewDeployer(client, "default", 15*time.Minute, 24*time.Hour).Deploy(context.Background(), spec)

			if c.err == nil {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, c.err)
			require.ErrorIs(t, err, pkgevents.ErrPermanent)
		})
	}
}

// A dispatch of a node type the vocabulary does not declare cannot run.
func TestDeploy_AnUndeclaredNodeTypeIsInvalid(t *testing.T) {
	spec := attemptSpec("dbt-macro", "v42")

	err := NewDeployer(newQueryTestClient(), "default", 15*time.Minute, 24*time.Hour).Deploy(context.Background(), spec)

	require.ErrorIs(t, err, deploy.ErrInvalidNodeType)
}

// Every release-leg Job is stopped once its pod has been alive for the start
// timeout plus the install's task timeout.
func TestDeployLegs_TheJobGetsTheInstallsTaskTimeout(t *testing.T) {
	ctx := context.Background()
	leg := deploy.ValidationJobSpec{
		JobName: "validate-rel1-node", ReleaseID: "rel1", NodeID: "svc.sch.tbl", ServiceName: "svc", SchemaName: "sch",
		TableName: "tbl", NodeType: string(pkg_model.NodeTypeDbtModel), ImageTag: "img", CandidateSchema: "candidate_rel1",
		CandidateArtifactURI: "s3://continuo/candidate-sql/rel1/svc.sch.tbl.sql", ManifestS3URI: "s3://continuo/manifests/svc/rel1/manifest.json",
	}
	want := int64(15*60 + 2*3600)
	for name, deployLeg := range map[string]func(*Deployer) error{
		"validation": func(d *Deployer) error { return d.DeployValidation(ctx, leg) },
		"seed build": func(d *Deployer) error {
			s := leg
			s.NodeType = string(pkg_model.NodeTypeDbtSeed)
			return d.DeploySeedBuild(ctx, s)
		},
		"compile": func(d *Deployer) error { s := leg; s.NodeType = ""; return d.DeployCompile(ctx, s) },
	} {
		t.Run(name, func(t *testing.T) {
			t.Setenv("VALIDATION_WAREHOUSE_SECRET", "warehouse-conn")
			t.Setenv("VALIDATION_IMAGE", "ghcr.io/carolsimone/continuo-python-runtime-postgres:v0.10.0")
			client := newValidationTestClient()

			require.NoError(t, deployLeg(NewDeployer(client, "default", 15*time.Minute, 2*time.Hour)))

			job := fetchJob(t, client, "default", leg.JobName)
			require.NotNil(t, job.Spec.ActiveDeadlineSeconds)
			assert.Equal(t, want, *job.Spec.ActiveDeadlineSeconds)
		})
	}
}

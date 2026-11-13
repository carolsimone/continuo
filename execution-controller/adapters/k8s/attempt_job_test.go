package k8s

import (
	"context"
	"testing"
	"time"

	"github.com/carolsimone/continuo/execution-controller/domain/deploy"
	"github.com/carolsimone/continuo/execution-controller/domain/model"
	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/seedreload"
	"github.com/carolsimone/continuo/pkg/warehouse"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
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
		RunID: "7f3e1a2b-4c5d-4e6f-8a9b-0c1d2e3f4a5b", ScheduleName: "e2e-schedule-failure",
		ServiceName: "service-1", SchemaName: "e2e_schema", TableName: "ftable_e",
		NodeType: string(nodeType), ImageTag: image, Attempt: 1, TimeoutSeconds: 3600,
	}
}

// A production Job names its attempt on the Job and its pod, the pod's
// container reads it as TASK_ATTEMPT, and Kubernetes stops the Job once the
// start timeout plus the task's timeout has passed since the Job started.
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

// A Job of the name that Kubernetes reports as existing when the creation is
// refused, but that is gone when it is read back, fails the creation with a
// plain error: neither a collision nor permanent, so the creation is tried
// again.
func TestDeploy_AJobGoneBetweenTheRefusedCreationAndTheReadIsRetried(t *testing.T) {
	t.Setenv("VALIDATION_WAREHOUSE_SECRET", "warehouse-conn")
	t.Setenv("DOCKERHUB_USERNAME", "")
	spec := attemptSpec(pkg_model.NodeTypeDbtModel, "v42")
	cs := fake.NewSimpleClientset()
	cs.PrependReactor("create", "jobs", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, k8serrors.NewAlreadyExists(batchv1.Resource("jobs"), spec.JobName)
	})
	client := newValidationTestClient()
	client.setClientsetForTest(cs)

	err := NewDeployer(client, "default", 15*time.Minute, 24*time.Hour).Deploy(context.Background(), spec)

	require.Error(t, err)
	assert.Contains(t, err.Error(), spec.JobName)
	assert.NotErrorIs(t, err, deploy.ErrJobNameCollision)
	assert.NotErrorIs(t, err, pkgevents.ErrPermanent)
}

// A dispatch of a node type the vocabulary does not declare cannot run.
func TestDeploy_AnUndeclaredNodeTypeIsInvalid(t *testing.T) {
	spec := attemptSpec("dbt-macro", "v42")

	err := NewDeployer(newQueryTestClient(), "default", 15*time.Minute, 24*time.Hour).Deploy(context.Background(), spec)

	require.ErrorIs(t, err, deploy.ErrInvalidNodeType)
	assert.ErrorIs(t, err, pkgevents.ErrPermanent)
	assert.ErrorContains(t, err, `unknown node_type "dbt-macro"`, "the message names the type once, in the parse error")
	assert.NotContains(t, err.Error(), `run "dbt-macro"`, "the name is not left dangling after the sentinel text")
}

// A dispatch of an unknown-external node cannot run either: nothing builds the table,
// so no command exists for it and the refusal can never succeed.
func TestDeploy_AnUnknownExternalNodeIsInvalid(t *testing.T) {
	spec := attemptSpec(pkg_model.NodeTypeUnknownExternal, "v42")

	err := NewDeployer(newQueryTestClient(), "default", 15*time.Minute, 24*time.Hour).Deploy(context.Background(), spec)

	require.ErrorIs(t, err, deploy.ErrInvalidNodeType)
	assert.ErrorIs(t, err, pkgevents.ErrPermanent)
}

// Every release-leg Job is stopped once the start timeout plus the install's
// task timeout has passed since the Job started.
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
			t.Setenv("VALIDATION_IMAGE", "ghcr.io/carolsimone/continuo-python-runtime-postgres:v0.12.0")
			client := newValidationTestClient()

			require.NoError(t, deployLeg(NewDeployer(client, "default", 15*time.Minute, 2*time.Hour)))

			job := fetchJob(t, client, "default", leg.JobName)
			require.NotNil(t, job.Spec.ActiveDeadlineSeconds)
			assert.Equal(t, want, *job.Spec.ActiveDeadlineSeconds)
		})
	}
}

// seedJobParams are the Job params of a load of the seed
// analytics.seed_fx_rates_eur.
func seedJobParams() JobParams {
	return JobParams{
		JobName: "finance-analytics-seed-fx-rates-eur-5b2e8f10", TaskID: "a1c3e5f7-2b4d-4f60-8a1c-3e5f7b9d1a2c",
		RunID: "5b2e8f10-3a4c-4d7e-9b61-0f8a2c4e6d13", ScheduleName: "seed", ServiceName: "finance",
		SchemaName: "analytics", TableName: "seed_fx_rates_eur", Namespace: "default",
		NodeType: pkg_model.NodeTypeDbtSeed, ImageTag: "v42", Operation: pkg_model.OperationRun,
	}
}

func setPreflightEnv(t *testing.T) {
	t.Helper()
	t.Setenv("VALIDATION_IMAGE", "ghcr.io/carolsimone/continuo-python-runtime-postgres:v0.12.0")
	t.Setenv("VALIDATION_WAREHOUSE_SECRET", "continuo-warehouse-validation")
	t.Setenv("S3_BUCKET", "continuo")
}

// lastInitContainer builds the pod of seedJobParams with pf as its preflight
// and returns its last init container.
func lastInitContainer(t *testing.T, pf seedreload.Preflight) corev1.Container {
	t.Helper()
	setPreflightEnv(t)
	p := seedJobParams()
	p.SeedPreflight = &pf
	spec, err := buildPodSpec(p, []string{"dbt", "seed", "--select", "seed_fx_rates_eur"}, "/project/target/partial_parse.msgpack")
	require.NoError(t, err)
	require.NotEmpty(t, spec.InitContainers)
	return spec.InitContainers[len(spec.InitContainers)-1]
}

func TestBuildPodSpec_ASeedWithAPreflightRunsItLastBeforeDbt(t *testing.T) {
	t.Setenv("VALIDATION_IMAGE", "ghcr.io/carolsimone/continuo-python-runtime-postgres:v0.12.0")
	t.Setenv("VALIDATION_WAREHOUSE_SECRET", "continuo-warehouse-validation")
	t.Setenv("S3_BUCKET", "continuo")
	p := seedJobParams() // dbt-seed, SchemaName "analytics", TableName "seed_fx_rates_eur"
	p.SeedPreflight = &seedreload.Preflight{Mode: seedreload.PreflightCheck, Family: warehouse.SchemaBound, Relation: "analytics.seed_fx_rates_eur",
		Allowed: []string{"analytics.fx_rates_current", "analytics.fx_rates_latest_eur"}, Acknowledged: []string{}}
	spec, err := buildPodSpec(p, []string{"dbt", "seed", "--select", "seed_fx_rates_eur"}, "/project/target/partial_parse.msgpack")
	require.NoError(t, err)
	last := spec.InitContainers[len(spec.InitContainers)-1]
	assert.Equal(t, model.SeedPreflightContainer, last.Name)
	assert.Equal(t, "hydrate-parse-cache", spec.InitContainers[0].Name)
	assert.Subset(t, last.Env, []corev1.EnvVar{
		{Name: "VALIDATION_OP", Value: "seed_preflight"}, {Name: "SEED_PREFLIGHT_MODE", Value: "check"},
		{Name: "SEED_PREFLIGHT_ENGINE_FAMILY", Value: "schema_bound"},
		{Name: "PROD_SCHEMA", Value: "analytics"}, {Name: "TABLE_NAME", Value: "seed_fx_rates_eur"},
		{Name: "SEED_PREFLIGHT_ALLOWED", Value: `["analytics.fx_rates_current","analytics.fx_rates_latest_eur"]`},
		{Name: "SEED_PREFLIGHT_ACKNOWLEDGED", Value: `[]`},
		{Name: "NODE_ID", Value: "analytics.seed_fx_rates_eur"},
	})
	for _, e := range last.Env {
		assert.NotEqual(t, "DBT_TARGET_SCHEMA", e.Name, "R0's seed_preflight reads PROD_SCHEMA, never DBT_TARGET_SCHEMA")
	}
	assert.Equal(t, "continuo-warehouse-validation", last.EnvFrom[0].SecretRef.Name)
	assert.Equal(t, "ghcr.io/carolsimone/continuo-python-runtime-postgres:v0.12.0", last.Image)
	assert.Equal(t, continuoImageSecurityContext(), last.SecurityContext)
	// The runner's exit code is the container's: a stop (1) or a bad input
	// (2) fails the init container, so dbt never starts and nothing is dropped.
	assert.Equal(t, []string{"continuo-runtime", "validation-op"}, last.Command)
	assert.Empty(t, last.Args)
	assert.Equal(t, "dbt-job", spec.Containers[0].Name)
	assert.Equal(t, []string{"dbt", "seed", "--select", "seed_fx_rates_eur"}, spec.Containers[0].Command)
}

// R0 exits 2 in report mode too, on a bad input or missing adapter env. A
// report must never stop the load (N52), so it runs through a shell that exits 0.
func TestBuildPodSpec_AReportPreflightCannotFailThePod(t *testing.T) {
	last := lastInitContainer(t, seedreload.Preflight{Mode: seedreload.PreflightReport, Family: warehouse.LateBinding, Relation: "analytics.seed_fx_rates_eur"}) // Allowed and Acknowledged nil
	assert.Equal(t, []string{"sh", "-c", "continuo-runtime validation-op; exit 0"}, last.Command)
	assert.Subset(t, last.Env, []corev1.EnvVar{{Name: "SEED_PREFLIGHT_MODE", Value: "report"},
		{Name: "SEED_PREFLIGHT_ENGINE_FAMILY", Value: "late_binding"},
		{Name: "SEED_PREFLIGHT_ALLOWED", Value: "[]"}, {Name: "SEED_PREFLIGHT_ACKNOWLEDGED", Value: "[]"}})
}

func TestBuildPodSpec_ATaskWithoutAPreflightHasNoPreflightContainer(t *testing.T) {
	setPreflightEnv(t)
	spec, err := buildPodSpec(seedJobParams(), []string{"dbt", "seed", "--select", "seed_fx_rates_eur"}, "/project/target/partial_parse.msgpack")
	require.NoError(t, err)
	for _, c := range spec.InitContainers {
		assert.NotEqual(t, model.SeedPreflightContainer, c.Name)
	}
}

// Without a parse cache to hydrate, the preflight is the pod's only init
// container.
func TestBuildPodSpec_APreflightRunsWithoutAParseCache(t *testing.T) {
	setPreflightEnv(t)
	t.Setenv("S3_BUCKET", "")
	p := seedJobParams()
	p.SeedPreflight = &seedreload.Preflight{Mode: seedreload.PreflightCheck, Family: warehouse.SchemaBound, Relation: "analytics.seed_fx_rates_eur"}
	spec, err := buildPodSpec(p, []string{"dbt", "seed", "--select", "seed_fx_rates_eur"}, "/project/target/partial_parse.msgpack")
	require.NoError(t, err)
	require.Len(t, spec.InitContainers, 1)
	assert.Equal(t, model.SeedPreflightContainer, spec.InitContainers[0].Name)
}

// A preflight that cannot run never lets the pod start: a relation without a
// schema and a table, or an install without a validation image, is permanent.
func TestBuildPodSpec_APreflightThatCannotRunIsPermanent(t *testing.T) {
	for name, c := range map[string]struct {
		relation string
		image    string
	}{
		"a relation of no dot":    {"seed_fx_rates_eur", "ghcr.io/carolsimone/continuo-python-runtime-postgres:v0.12.0"},
		"a relation of no schema": {".seed", "ghcr.io/carolsimone/continuo-python-runtime-postgres:v0.12.0"},
		"a relation of no name":   {"analytics.", "ghcr.io/carolsimone/continuo-python-runtime-postgres:v0.12.0"},
		"no validation image":     {"analytics.seed_fx_rates_eur", ""},
	} {
		t.Run(name, func(t *testing.T) {
			setPreflightEnv(t)
			t.Setenv("VALIDATION_IMAGE", c.image)
			p := seedJobParams()
			p.SeedPreflight = &seedreload.Preflight{Mode: seedreload.PreflightCheck, Family: warehouse.SchemaBound, Relation: c.relation}
			_, err := buildPodSpec(p, []string{"dbt", "seed", "--select", "seed_fx_rates_eur"}, "/project/target/partial_parse.msgpack")
			require.ErrorIs(t, err, pkgevents.ErrPermanent)
		})
	}
}

// The preflight a seed's dispatch carries reaches the Job the Deployer
// creates, so the check runs before the seed's load.
func TestDeploy_ASeedsPreflightReachesItsJob(t *testing.T) {
	setPreflightEnv(t)
	client := newValidationTestClient()
	spec := attemptSpec(pkg_model.NodeTypeDbtSeed, "v42")
	spec.SchemaName, spec.TableName = "analytics", "seed_fx_rates_eur"
	spec.SeedPreflight = &seedreload.Preflight{Mode: seedreload.PreflightCheck, Family: warehouse.SchemaBound, Relation: "analytics.seed_fx_rates_eur"}

	require.NoError(t, NewDeployer(client, "default", 15*time.Minute, 24*time.Hour).Deploy(context.Background(), spec))

	inits := fetchJob(t, client, "default", spec.JobName).Spec.Template.Spec.InitContainers
	require.NotEmpty(t, inits)
	assert.Equal(t, model.SeedPreflightContainer, inits[len(inits)-1].Name)
}

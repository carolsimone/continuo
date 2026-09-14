package k8s

import (
	"context"
	"fmt"
	"os"
	"time"

	validationmodel "github.com/carolsimone/continuo/execution-controller/domain/model"
	"github.com/carolsimone/continuo/pkg/events"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Candidate-schema lifecycle ops run as one-shot engine-image Jobs: the executor
// schedules them and blocks on the result but never connects to the warehouse itself.
const (
	schemaOpEnsure = "ensure_schema"
	schemaOpDrop   = "drop_schema"

	schemaOpJobPollInterval = 2 * time.Second
	// SchemaOpJobTimeout bounds the wait for a schema-op Job to terminate; the DDL is
	// quick, so a Job still running past this is a failure rather than an infinite
	// block. Exported so main.go can size the handler budget of the stream consumers
	// that block on schema-op Jobs above this wait, not under it.
	SchemaOpJobTimeout = 5 * time.Minute
)

// buildSchemaOpPodSpec constructs the PodSpec for a candidate-schema lifecycle Job. It
// runs the same engine image as validation (VALIDATION_IMAGE) under the same explicit
// validation command, with the operator's warehouse Secret attached via envFrom,
// invoking the ensure_schema/drop_schema op on DBT_TARGET_SCHEMA — a single
// short-lived container that runs one DDL statement through the engine adapter and
// exits. No S3, no candidate SQL, no table.
func buildSchemaOpPodSpec(op, candidateSchema string) (corev1.PodSpec, error) {
	image := os.Getenv("VALIDATION_IMAGE")
	if image == "" {
		return corev1.PodSpec{}, fmt.Errorf("%w: VALIDATION_IMAGE not configured (set it to the matching continuo-python-runtime-<engine> image) for schema op %s",
			events.ErrPermanent, op)
	}
	whFrom, err := warehouseSecretEnvFrom("schema op " + op)
	if err != nil {
		return corev1.PodSpec{}, err
	}
	container := corev1.Container{
		Name:            "schema-op",
		Image:           image,
		ImagePullPolicy: validationImagePullPolicy(),
		// The image's default command runs the python-node harness, so the
		// validation entrypoint is selected explicitly here — the same one every
		// validation pod runs.
		Command: validationmodel.ValidationCommand("", ""),
		Env: []corev1.EnvVar{
			{Name: "DBT_TARGET_SCHEMA", Value: candidateSchema},
			{Name: "VALIDATION_OP", Value: op},
		},
		// Warehouse connection is operator-owned: the whole Secret lands as env.
		EnvFrom:         whFrom,
		SecurityContext: continuoImageSecurityContext(),
	}
	return corev1.PodSpec{
		RestartPolicy:   corev1.RestartPolicyNever,
		SecurityContext: jobPodSecurityContext(),
		Containers:      []corev1.Container{container},
	}, nil
}

// schemaOpJob wraps buildSchemaOpPodSpec in a one-shot Job. It carries a distinct
// app=continuo-schema-op label (never mode=validation/app=dbt-job) so k8s-controller's
// validation watcher ignores it — its lifecycle is owned here, not surfaced as a
// validation node. TTLSecondsAfterFinished is a cleanup backstop; RunSchemaOpJob also
// deletes the Job once it observes a terminal state. ActiveDeadlineSeconds matches
// SchemaOpJobTimeout so a hung DDL pod (e.g. a stuck warehouse lock) is killed and the
// Job goes Failed — a terminal state submitSchemaOpJob can clear on the next retry —
// instead of staying Active forever and livelocking every retry at the wait timeout.
func schemaOpJob(op, candidateSchema, jobName, namespace string) (*batchv1.Job, error) {
	podSpec, err := buildSchemaOpPodSpec(op, candidateSchema)
	if err != nil {
		return nil, err
	}
	backoffLimit := int32(0)
	ttl := int32(120)
	activeDeadline := int64(SchemaOpJobTimeout / time.Second)
	labels := map[string]string{
		"app":              "continuo-schema-op",
		"schema-op":        op,
		"candidate-schema": sanitizeK8sLabel(candidateSchema),
	}
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: jobName, Namespace: namespace, Labels: labels},
		Spec: batchv1.JobSpec{
			BackoffLimit:            &backoffLimit,
			TTLSecondsAfterFinished: &ttl,
			ActiveDeadlineSeconds:   &activeDeadline,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec:       podSpec,
			},
		},
	}, nil
}

// schemaOpJobTerminal reports whether the Job has reached a terminal state. Pod
// counters alone are not enough: a Job killed by ActiveDeadlineSeconds surfaces as a
// JobFailed condition (reason DeadlineExceeded) and may leave the Failed counter at
// zero, so conditions are checked too.
func schemaOpJobTerminal(job *batchv1.Job) (succeeded, failed bool) {
	succeeded = job.Status.Succeeded > 0
	failed = job.Status.Failed > 0
	for _, cond := range job.Status.Conditions {
		if cond.Status != corev1.ConditionTrue {
			continue
		}
		switch cond.Type {
		case batchv1.JobComplete:
			succeeded = true
		case batchv1.JobFailed:
			failed = true
		}
	}
	return succeeded, failed
}

// RunSchemaOpJob schedules a candidate-schema lifecycle Job and blocks until it
// terminates: Succeeded -> nil, Failed (or timeout) -> error. It is idempotent by job
// name — a redelivered trigger waits on the in-flight Job instead of duplicating it —
// and a leftover terminal Job from a prior attempt is cleared first so a retry runs
// clean. The engine adapter runs the DDL; the executor holds no warehouse connection.
func (c *K8sClient) RunSchemaOpJob(ctx context.Context, op, candidateSchema, jobName, namespace string) error {
	if err := c.submitSchemaOpJob(ctx, op, candidateSchema, jobName, namespace); err != nil {
		return err
	}
	return c.waitForSchemaOpJob(ctx, namespace, jobName, op)
}

// submitSchemaOpJob ensures exactly one runnable schema-op Job exists for jobName:
// creates it when absent, clears-and-recreates a leftover terminal Job from a prior
// attempt, and leaves an in-flight Job untouched (a redelivered trigger just waits).
func (c *K8sClient) submitSchemaOpJob(ctx context.Context, op, candidateSchema, jobName, namespace string) error {
	job, err := schemaOpJob(op, candidateSchema, jobName, namespace)
	if err != nil {
		return err
	}

	existing, getErr := c.clientset.BatchV1().Jobs(namespace).Get(ctx, jobName, metav1.GetOptions{})
	switch {
	case getErr == nil:
		succeeded, failed := schemaOpJobTerminal(existing)
		if succeeded || failed {
			if err := c.deleteJob(ctx, namespace, jobName); err != nil {
				return fmt.Errorf("clear stale schema-op job %s: %w", jobName, err)
			}
			return c.createSchemaOpJob(ctx, job)
		}
		c.logger.InfoContext(ctx, "schema-op job already in flight; waiting", "job_name", jobName, "op", op)
		return nil
	case errors.IsNotFound(getErr):
		return c.createSchemaOpJob(ctx, job)
	default:
		return fmt.Errorf("get schema-op job %s: %w", jobName, getErr)
	}
}

// createSchemaOpJob creates the Job, treating a lost create race as success: between
// the caller's Get and this Create, a concurrent redelivery (another replica or
// consumer goroutine) may have created the same name first. The Job is deterministic
// by name and its op is idempotent, so the caller just waits on whichever Job now
// holds the name instead of surfacing AlreadyExists and burning a retry cycle.
func (c *K8sClient) createSchemaOpJob(ctx context.Context, job *batchv1.Job) error {
	err := c.CreateJob(ctx, job)
	if err != nil && errors.IsAlreadyExists(err) {
		c.logger.InfoContext(ctx, "schema-op job created concurrently; waiting", "job_name", job.Name)
		return nil
	}
	return err
}

func (c *K8sClient) waitForSchemaOpJob(ctx context.Context, namespace, jobName, op string) error {
	waitCtx, cancel := context.WithTimeout(ctx, SchemaOpJobTimeout)
	defer cancel()
	ticker := time.NewTicker(schemaOpJobPollInterval)
	defer ticker.Stop()
	for {
		job, err := c.clientset.BatchV1().Jobs(namespace).Get(waitCtx, jobName, metav1.GetOptions{})
		switch {
		case err == nil:
			succeeded, failed := schemaOpJobTerminal(job)
			if succeeded {
				c.logger.InfoContext(ctx, "schema-op job succeeded", "job_name", jobName, "op", op)
				_ = c.deleteJob(ctx, namespace, jobName)
				return nil
			}
			if failed {
				return fmt.Errorf("schema-op job %s (%s) failed", jobName, op)
			}
		case errors.IsNotFound(err):
			// Schema-op Jobs are deleted only after a terminal state: delete-on-success
			// (possibly by a concurrent waiter on the same Job) or the 120s
			// TTLSecondsAfterFinished backstop — both far longer than the 2s poll, so a
			// failure would have been observed here first. Treat the disappearance as
			// success instead of polling into the timeout.
			c.logger.InfoContext(ctx, "schema-op job already completed and was cleaned up",
				"job_name", jobName, "op", op)
			return nil
		}
		select {
		case <-waitCtx.Done():
			return fmt.Errorf("schema-op job %s (%s) did not terminate: %w", jobName, op, waitCtx.Err())
		case <-ticker.C:
		}
	}
}

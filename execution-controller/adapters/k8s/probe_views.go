package k8s

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	validationmodel "github.com/carolsimone/continuo/execution-controller/domain/model"
	"github.com/carolsimone/continuo/execution-controller/service/ports"
	"github.com/carolsimone/continuo/pkg/events"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// probeContainer is the name of the probe Job's only container.
	probeContainer = "probe-views"
	// probeAppLabel is the app label of a probe Job. The Job watch selects
	// JobSelector, which a probe Job does not match, so its lifecycle is owned
	// here and is never mistaken for an attempt.
	probeAppLabel = "continuo-probe"
	// probeDeadline is how long a probe pod may run before Kubernetes ends it.
	// It is shorter than the 40 s the observer allows the whole probe, so a
	// probe that runs to this deadline still leaves time to read its pod's log.
	probeDeadline = 30 * time.Second
	// probeJobTTL is how long a finished probe Job stays if its deletion fails.
	probeJobTTL = int32(120)
	// maxProbeJobNameLen keeps a probe Job name, which Kubernetes copies into
	// the pod's job-name label, well under that label's 63 characters.
	maxProbeJobNameLen = 52
)

// viewProber probes views through a short-lived Job of the validation image.
type viewProber struct {
	client    *K8sClient
	namespace string
	logger    *slog.Logger
}

var _ ports.ViewProber = (*viewProber)(nil)

// NewViewProber returns a ViewProber that runs each probe as a Job in
// namespace and deletes the Job once it has read the probe's log.
func NewViewProber(client *K8sClient, namespace string, logger *slog.Logger) ports.ViewProber {
	return &viewProber{client: client, namespace: namespace, logger: logger}
}

// ProbeViews creates the probe Job, waits for it to end, reads its log and
// deletes it. A Job of the same name already there (CreateJob creates nothing
// then) is waited on instead.
func (p *viewProber) ProbeViews(ctx context.Context, req ports.ProbeRequest) (string, error) {
	job, err := probeJob(req, p.namespace)
	if err != nil {
		return "", err
	}
	defer func() {
		// The caller's context may have ended; the Job is deleted regardless.
		delCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		if err := p.client.deleteJob(delCtx, p.namespace, job.Name); err != nil {
			p.logger.WarnContext(ctx, "Failed to delete the probe Job; its TTL removes it", "job_name", job.Name, "error", err)
		}
	}()
	if err := p.client.CreateJob(ctx, job); err != nil {
		return "", fmt.Errorf("create probe job %s: %w", job.Name, err)
	}
	failed, err := p.wait(ctx, job.Name)
	if err != nil {
		return "", err
	}
	log, err := p.client.GetContainerLog(ctx, p.namespace, job.Name, probeContainer)
	if err != nil {
		return "", fmt.Errorf("read the log of probe job %s: %w", job.Name, err)
	}
	if failed && strings.TrimSpace(log) == "" {
		return "", fmt.Errorf("probe job %s failed and printed nothing", job.Name)
	}
	return log, nil
}

// wait polls the Job until it ends and reports whether it failed.
func (p *viewProber) wait(ctx context.Context, jobName string) (failed bool, err error) {
	ticker := time.NewTicker(schemaOpJobPollInterval)
	defer ticker.Stop()
	for {
		job, err := p.client.clientset.BatchV1().Jobs(p.namespace).Get(ctx, jobName, metav1.GetOptions{})
		switch {
		case err == nil:
			if succeeded, failed := schemaOpJobTerminal(job); succeeded || failed {
				return failed && !succeeded, nil
			}
		case errors.IsNotFound(err):
			return false, fmt.Errorf("probe job %s disappeared before it ended", jobName)
		default:
			return false, fmt.Errorf("get probe job %s: %w", jobName, err)
		}
		select {
		case <-ctx.Done():
			return false, fmt.Errorf("probe job %s did not end: %w", jobName, ctx.Err())
		case <-ticker.C:
		}
	}
}

// probeJob builds the Job that probes req.Views: the validation image's runner
// with VALIDATION_OP=probe_views, the views as PROBE_VIEWS, req.NodeID as
// NODE_ID and the warehouse Secret. The error wraps events.ErrPermanent when
// VALIDATION_IMAGE or the warehouse Secret is not configured.
func probeJob(req ports.ProbeRequest, namespace string) (*batchv1.Job, error) {
	image := os.Getenv("VALIDATION_IMAGE")
	if image == "" {
		return nil, fmt.Errorf("%w: VALIDATION_IMAGE not configured (set it to the matching continuo-python-runtime-<engine> image) for the view probe of node %s",
			events.ErrPermanent, req.NodeID)
	}
	whFrom, err := warehouseSecretEnvFrom("the view probe of node " + req.NodeID)
	if err != nil {
		return nil, err
	}
	container := corev1.Container{
		Name:            probeContainer,
		Image:           image,
		ImagePullPolicy: validationImagePullPolicy(),
		Command:         validationmodel.ValidationCommand("", ""),
		Env: []corev1.EnvVar{
			{Name: "VALIDATION_OP", Value: "probe_views"},
			{Name: "PROBE_VIEWS", Value: jsonNames(req.Views)},
			{Name: "NODE_ID", Value: req.NodeID},
		},
		EnvFrom:         whFrom,
		SecurityContext: continuoImageSecurityContext(),
	}
	backoffLimit := int32(0)
	ttl := probeJobTTL
	deadline := int64(probeDeadline / time.Second)
	labels := map[string]string{"app": probeAppLabel}
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: probeJobName(req.JobName), Namespace: namespace, Labels: labels},
		Spec: batchv1.JobSpec{
			BackoffLimit:            &backoffLimit,
			TTLSecondsAfterFinished: &ttl,
			ActiveDeadlineSeconds:   &deadline,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					RestartPolicy:   corev1.RestartPolicyNever,
					SecurityContext: jobPodSecurityContext(),
					Containers:      []corev1.Container{container},
				},
			},
		},
	}, nil
}

// probeJobName derives the probe Job's name from the attempt's Job name:
// deterministic, DNS-1123-safe and at most maxProbeJobNameLen characters. A
// longer name keeps a readable head and a 10-character digest of the parent,
// so two parents that share a head never share a probe.
func probeJobName(parent string) string {
	name := "probe-" + sanitizeK8sName(parent)
	if len(name) <= maxProbeJobNameLen {
		return name
	}
	digest := sha256.Sum256([]byte(parent))
	suffix := hex.EncodeToString(digest[:])[:10]
	head := strings.TrimRight(name[:maxProbeJobNameLen-len(suffix)-1], "-")
	return head + "-" + suffix
}

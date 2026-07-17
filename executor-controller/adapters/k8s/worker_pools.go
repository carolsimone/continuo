package k8s

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/carolsimone/continuo/executor-controller/service/ports"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
)

const (
	// workerResourcePrefix names every resource a pool owns. Its Deployment and
	// its Secret share the name: they are one pool's two halves, and a pool key
	// identifies both.
	workerResourcePrefix = "dbt-worker-"
	// poolKeyNameBytes is how much of a pool key names its resources. A key is a
	// hex SHA-256, so 16 characters is 64 bits — far more than enough to keep the
	// pools of one cluster apart, and short enough that the name stays inside the
	// object-name limit. The full key travels in an annotation and in the pod's
	// environment; nothing reads identity back out of the name.
	poolKeyNameBytes = 16
	// workerAppLabel marks every worker pod, whichever pool it belongs to.
	workerAppLabel = "continuo-dbt-worker"
	// poolKeyLabel selects one pool's pods. It holds the truncated key, because a
	// label value cannot hold the whole one.
	poolKeyLabel = "pool-key"
	// credentialSecretKey is the one key of a pool's Secret.
	credentialSecretKey = "credential" // #nosec G101 -- a key name, not a credential
	// credentialEnvVar carries the pool credential into the worker, which pops it
	// out of the environment before loading dbt.
	credentialEnvVar = "CONTINUO_POOL_CREDENTIAL" // #nosec G101 -- a variable name
	// workerBinaryPath is where the dbt base image installs the worker.
	workerBinaryPath = "/continuo/bin/continuo-dbt-worker"
	// workerReadyFile is the file a worker touches once it has hydrated its
	// runtime artifact and can claim work.
	workerReadyFile = "/tmp/continuo-worker-ready"
	// workerContainerName names the pod's only container.
	workerContainerName = "worker"
)

// Annotations carrying a pool's full identity. The pool key and the artifact URI
// are both longer than a label value may be, and none of the three is something
// to select on — they are identity to read, so they are annotations.
const (
	annotationWorkerPoolKey         = "continuo.dev/worker-pool-key"
	annotationRuntimeManifestURI    = "continuo.dev/runtime-manifest-uri"
	annotationRuntimeManifestSHA256 = "continuo.dev/runtime-manifest-sha256"
)

// poolResourceName is the name of the resources belonging to poolKey.
func poolResourceName(poolKey string) string {
	if len(poolKey) > poolKeyNameBytes {
		poolKey = poolKey[:poolKeyNameBytes]
	}
	return workerResourcePrefix + poolKey
}

// WorkerPools runs worker pools as Kubernetes Deployments, one per pool, each
// with a Secret holding the credential its pods authenticate with.
//
// A pool gets no Service. A worker dials the executor to claim its work and
// report its outcome; nothing ever dials a worker, and it runs no server to dial.
type WorkerPools struct {
	clientset       kubernetes.Interface
	namespace       string
	controlPlaneURL string
	logger          *slog.Logger
}

var _ ports.WorkerPoolRuntime = (*WorkerPools)(nil)

// NewWorkerPools creates the Kubernetes worker-pool runtime. controlPlaneURL is
// the address the pool's workers call to claim tasks and report outcomes.
func NewWorkerPools(clientset kubernetes.Interface, namespace, controlPlaneURL string, logger *slog.Logger) *WorkerPools {
	return &WorkerPools{
		clientset:       clientset,
		namespace:       namespace,
		controlPlaneURL: controlPlaneURL,
		logger:          logger,
	}
}

// Ensure brings the pool's Secret and Deployment to spec.
//
// The Secret is written first: a pod that starts before its credential exists
// cannot authenticate, and would report an initialization failure that says
// nothing about the pool.
func (w *WorkerPools) Ensure(ctx context.Context, spec ports.WorkerPoolSpec) error {
	if err := w.ensureSecret(ctx, spec); err != nil {
		return err
	}
	return w.ensureDeployment(ctx, spec)
}

// ensureSecret writes the pool's credential, or leaves the Secret exactly as it
// is when the caller holds no credential — which is every reconcile of a pool
// whose Secret is intact.
func (w *WorkerPools) ensureSecret(ctx context.Context, spec ports.WorkerPoolSpec) error {
	if spec.Credential == "" {
		return nil
	}

	name := poolResourceName(spec.PoolKey)
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   w.namespace,
			Labels:      w.poolLabels(spec),
			Annotations: w.poolAnnotations(spec),
		},
		Type: corev1.SecretTypeOpaque,
		Data: map[string][]byte{credentialSecretKey: []byte(spec.Credential)},
	}

	secrets := w.clientset.CoreV1().Secrets(w.namespace)
	_, err := secrets.Create(ctx, secret, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		// The pool already has a Secret and the caller is rotating it, so the
		// stored value must be replaced rather than left as the one nobody holds.
		if _, err = secrets.Update(ctx, secret, metav1.UpdateOptions{}); err != nil {
			return fmt.Errorf("update worker pool secret %s: %w", name, err)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("create worker pool secret %s: %w", name, err)
	}
	return nil
}

// ensureDeployment creates the pool's Deployment or updates it to spec.
func (w *WorkerPools) ensureDeployment(ctx context.Context, spec ports.WorkerPoolSpec) error {
	name := poolResourceName(spec.PoolKey)
	desired := w.buildDeployment(spec)

	deployments := w.clientset.AppsV1().Deployments(w.namespace)
	_, err := deployments.Create(ctx, desired, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		if _, err = deployments.Update(ctx, desired, metav1.UpdateOptions{}); err != nil {
			return fmt.Errorf("update worker pool deployment %s: %w", name, err)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("create worker pool deployment %s: %w", name, err)
	}
	w.logger.Info("created a worker pool deployment",
		"pool_key", spec.PoolKey, "service_name", spec.ServiceName,
		"name", name, "replicas", spec.DesiredReplicas)
	return nil
}

// poolLabels are the labels every resource of a pool carries.
func (w *WorkerPools) poolLabels(spec ports.WorkerPoolSpec) map[string]string {
	key := spec.PoolKey
	if len(key) > poolKeyNameBytes {
		key = key[:poolKeyNameBytes]
	}
	return map[string]string{
		"app":          workerAppLabel,
		poolKeyLabel:   key,
		"service_name": sanitizeK8sLabel(spec.ServiceName),
	}
}

// poolAnnotations carry the pool's full identity, which does not fit in labels.
func (w *WorkerPools) poolAnnotations(spec ports.WorkerPoolSpec) map[string]string {
	return map[string]string{
		annotationWorkerPoolKey:         spec.PoolKey,
		annotationRuntimeManifestURI:    spec.RuntimeManifest.RuntimeManifestURI,
		annotationRuntimeManifestSHA256: spec.RuntimeManifest.RuntimeManifestSHA256,
	}
}

// buildDeployment renders the pool's Deployment.
func (w *WorkerPools) buildDeployment(spec ports.WorkerPoolSpec) *appsv1.Deployment {
	labels := w.poolLabels(spec)
	annotations := w.poolAnnotations(spec)
	replicas := spec.DesiredReplicas
	maxUnavailable := intstr.FromInt32(0)
	maxSurge := intstr.FromInt32(1)

	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:        poolResourceName(spec.PoolKey),
			Namespace:   w.namespace,
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			// A worker holds a lease while it runs dbt, so a replacement must be
			// serving before its predecessor is taken away: maxUnavailable 0 keeps
			// the pool from dipping below its replica count mid-rollout.
			Strategy: appsv1.DeploymentStrategy{
				Type: appsv1.RollingUpdateDeploymentStrategyType,
				RollingUpdate: &appsv1.RollingUpdateDeployment{
					MaxUnavailable: &maxUnavailable,
					MaxSurge:       &maxSurge,
				},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels, Annotations: annotations},
				Spec:       w.buildWorkerPodSpec(spec),
			},
		},
	}
}

// buildWorkerPodSpec renders a worker pod: the team's own dbt image, running the
// worker the base image installs instead of dbt directly.
func (w *WorkerPools) buildWorkerPodSpec(spec ports.WorkerPoolSpec) corev1.PodSpec {
	image := spec.ServiceName + ":" + spec.ImageTag
	if user := os.Getenv("DOCKERHUB_USERNAME"); user != "" {
		image = user + "/" + image
	}

	return corev1.PodSpec{
		RestartPolicy:   corev1.RestartPolicyAlways,
		SecurityContext: jobPodSecurityContext(),
		Containers: []corev1.Container{
			{
				Name:            workerContainerName,
				Image:           image,
				ImagePullPolicy: corev1.PullIfNotPresent,
				Command:         []string{workerBinaryPath},
				Env:             w.workerEnv(spec),
				SecurityContext: baseContainerSecurityContext(),
				// A worker touches its ready file once it has hydrated its runtime
				// artifact. Until then it is running but cannot execute anything, so
				// gating readiness on the worker's own signal is what keeps an
				// unhydrated pod from being counted as part of the pool.
				ReadinessProbe: &corev1.Probe{
					ProbeHandler: corev1.ProbeHandler{
						Exec: &corev1.ExecAction{
							Command: []string{"sh", "-c", "test -f " + workerReadyFile},
						},
					},
				},
			},
		},
	}
}

// workerEnv is what one worker pod needs to know.
//
// CONTINUO_RUNTIME_MANIFEST_SHA256 is the pool's own runtime manifest digest,
// and it is what binds a hydrated manifest to the pool the pod belongs to. The
// worker compares the descriptor it downloads against this value and rejects a
// mismatch. Without it the worker's checks are self-consistent but not
// pool-bound: a descriptor and artifact published for the same service and image
// tag by a DIFFERENT release would satisfy every other one.
//
// The credential arrives by secretKeyRef and never as a literal: a value in the
// pod template is readable by everyone who can read a Deployment, which is a far
// wider audience than those who can read a Secret. The worker pops it out of its
// environment before loading dbt.
//
// The pod's own name and UID come from the downward API, so what a worker
// reports about itself is what Kubernetes actually called it.
func (w *WorkerPools) workerEnv(spec ports.WorkerPoolSpec) []corev1.EnvVar {
	env := []corev1.EnvVar{
		{Name: "CONTINUO_EXECUTOR_URL", Value: w.controlPlaneURL},
		{Name: "CONTINUO_POOL_KEY", Value: spec.PoolKey},
		{Name: "CONTINUO_SERVICE_NAME", Value: spec.ServiceName},
		{Name: "CONTINUO_IMAGE_TAG", Value: spec.ImageTag},
		{Name: "CONTINUO_RUNTIME_MANIFEST_SHA256", Value: spec.RuntimeManifest.RuntimeManifestSHA256},
		{Name: "CONTINUO_RUNTIME_CONTEXT_JSON", Value: spec.ControllerContextJSON},
		{
			Name: credentialEnvVar,
			ValueFrom: &corev1.EnvVarSource{
				SecretKeyRef: &corev1.SecretKeySelector{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: poolResourceName(spec.PoolKey),
					},
					Key: credentialSecretKey,
				},
			},
		},
		{
			Name: "CONTINUO_POD_NAME",
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"},
			},
		},
		{
			Name: "CONTINUO_POD_UID",
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.uid"},
			},
		},
	}

	// The same warehouse connection the Job path forwards to dbt pods: a worker
	// runs the same dbt against the same database, so it needs the same profile.
	return append(env,
		corev1.EnvVar{Name: "DBT_POSTGRES_HOST", Value: os.Getenv("POSTGRES_HOST")},
		corev1.EnvVar{Name: "DBT_POSTGRES_PORT", Value: os.Getenv("POSTGRES_PORT")},
		corev1.EnvVar{Name: "DBT_POSTGRES_DB", Value: os.Getenv("DBT_POSTGRES_DB")},
		corev1.EnvVar{Name: "DBT_POSTGRES_USER", Value: os.Getenv("POSTGRES_USER")},
		corev1.EnvVar{Name: "DBT_POSTGRES_PASSWORD", Value: os.Getenv("POSTGRES_PASSWORD")},
	)
}

// Status reports what the cluster holds for poolKey.
//
// The Secret is looked up regardless of whether the Deployment exists. A pool
// whose Deployment was removed still has a credential its stored digest matches,
// and reporting that Secret as missing would rotate a pool that needs no
// rotation.
func (w *WorkerPools) Status(ctx context.Context, poolKey string) (ports.PoolStatus, bool, error) {
	name := poolResourceName(poolKey)

	secretExists, err := w.secretExists(ctx, name)
	if err != nil {
		return ports.PoolStatus{}, false, err
	}

	dep, err := w.clientset.AppsV1().Deployments(w.namespace).Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return ports.PoolStatus{SecretExists: secretExists}, false, nil
	}
	if err != nil {
		return ports.PoolStatus{}, false, fmt.Errorf("get worker pool deployment %s: %w", name, err)
	}

	desired := 0
	if dep.Spec.Replicas != nil {
		desired = int(*dep.Spec.Replicas)
	}
	return ports.PoolStatus{
		DesiredReplicas: desired,
		ReadyReplicas:   int(dep.Status.ReadyReplicas),
		SecretExists:    secretExists,
	}, true, nil
}

// secretExists reports whether the pool's credential Secret is present.
func (w *WorkerPools) secretExists(ctx context.Context, name string) (bool, error) {
	_, err := w.clientset.CoreV1().Secrets(w.namespace).Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("get worker pool secret %s: %w", name, err)
	}
	return true, nil
}

// DeletePod removes one worker pod, but only while it is still the pod podUID
// names.
//
// The UID precondition is what makes the delete safe to retry. A Deployment
// reuses pod names as it replaces pods, so a delete decided against one pod and
// landing after that pod is gone would otherwise take out its healthy successor.
// With the precondition the late delete is refused, and a refusal is the outcome
// asked for: the pod meant to be gone already is.
func (w *WorkerPools) DeletePod(ctx context.Context, podName, podUID string) error {
	uid := types.UID(podUID)
	err := w.clientset.CoreV1().Pods(w.namespace).Delete(ctx, podName, metav1.DeleteOptions{
		Preconditions: &metav1.Preconditions{UID: &uid},
	})
	if apierrors.IsNotFound(err) || apierrors.IsConflict(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("delete worker pod %s: %w", podName, err)
	}
	return nil
}

package k8s

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/carolsimone/continuo/executor-controller/service/ports"
	pkgmodel "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

const testNamespace = "continuo"

// testPoolKey is a realistic pool key: WorkerPoolKey returns a hex SHA-256, so
// every key is 64 characters and the resource name truncates it.
var testPoolKey = pkgmodel.WorkerPoolKey("finance", "abc123", "deadbeef")

// newTestWorkerPools builds a WorkerPools against a fake cluster preloaded with
// objects.
func newTestWorkerPools(objects ...runtime.Object) (*WorkerPools, *fake.Clientset) {
	cs := fake.NewSimpleClientset(objects...)
	return &WorkerPools{
		clientset:       cs,
		namespace:       testNamespace,
		controlPlaneURL: "http://executor-controller:8084",
		logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
	}, cs
}

// testSpec is a complete pool spec; tweak per test. Its Credential is a fixture
// value the tests trace through the rendered objects, not a real one.
func testSpec() ports.WorkerPoolSpec {
	return ports.WorkerPoolSpec{ // #nosec G101 -- a test fixture, not a credential
		PoolKey:     testPoolKey,
		ServiceName: "finance",
		ImageTag:    "abc123",
		RuntimeManifest: pkgmodel.RuntimeManifestRef{
			RuntimeManifestURI:                "s3://bucket/finance/partial_parse.msgpack",
			RuntimeManifestSHA256:             "deadbeef",
			RuntimeManifestDBTVersion:         "1.12.0b1",
			RuntimeManifestParseContextSHA256: "ctx99",
		},
		ControllerContextJSON: `{"command_dialect_sha256":"x"}`,
		Credential:            "raw-credential-value",
		DesiredReplicas:       2,
	}
}

// getDeployment reads back the pool's Deployment, failing the test when absent.
func getDeployment(t *testing.T, cs *fake.Clientset, poolKey string) *appsv1.Deployment {
	t.Helper()
	dep, err := cs.AppsV1().Deployments(testNamespace).Get(
		context.Background(), poolResourceName(poolKey), metav1.GetOptions{})
	require.NoError(t, err)
	return dep
}

// envOf indexes a container's environment by name.
func envOf(c corev1.Container) map[string]corev1.EnvVar {
	out := make(map[string]corev1.EnvVar, len(c.Env))
	for _, e := range c.Env {
		out[e.Name] = e
	}
	return out
}

// TestPoolResourceNameTruncatesTheKey pins the naming rule: a 64-character pool
// key becomes a Kubernetes name the API server accepts.
func TestPoolResourceNameTruncatesTheKey(t *testing.T) {
	name := poolResourceName(testPoolKey)

	assert.Equal(t, "dbt-worker-"+testPoolKey[:16], name)
	assert.Len(t, name, len("dbt-worker-")+16)
	assert.LessOrEqual(t, len(name), 63, "the name fits a Kubernetes object name")
}

// TestEnsureBindsTheDeploymentToItsPoolsRuntimeManifest is the binding that
// makes a hydrated manifest pool-bound rather than merely well-formed. Without
// this digest on the pod, a descriptor published for the same service and image
// tag by a DIFFERENT release satisfies every other check the worker makes.
func TestEnsureBindsTheDeploymentToItsPoolsRuntimeManifest(t *testing.T) {
	w, cs := newTestWorkerPools()
	spec := testSpec()

	require.NoError(t, w.Ensure(context.Background(), spec))

	env := envOf(getDeployment(t, cs, spec.PoolKey).Spec.Template.Spec.Containers[0])
	got, ok := env["CONTINUO_RUNTIME_MANIFEST_SHA256"]
	require.True(t, ok, "the worker's from_env requires CONTINUO_RUNTIME_MANIFEST_SHA256")
	assert.Equal(t, spec.RuntimeManifest.RuntimeManifestSHA256, got.Value,
		"the pod is pinned to its own pool's runtime manifest digest")
}

// TestEnsureSuppliesEveryVariableTheWorkerRequires pins the pod against the
// worker runtime's from_env contract. Each of these is a hard requirement there,
// so a Deployment missing one fails the pod closed rather than degrading it.
func TestEnsureSuppliesEveryVariableTheWorkerRequires(t *testing.T) {
	w, cs := newTestWorkerPools()
	spec := testSpec()

	require.NoError(t, w.Ensure(context.Background(), spec))

	container := getDeployment(t, cs, spec.PoolKey).Spec.Template.Spec.Containers[0]
	env := envOf(container)

	for name, want := range map[string]string{
		"CONTINUO_EXECUTOR_URL":            "http://executor-controller:8084",
		"CONTINUO_POOL_KEY":                spec.PoolKey,
		"CONTINUO_SERVICE_NAME":            "finance",
		"CONTINUO_IMAGE_TAG":               "abc123",
		"CONTINUO_RUNTIME_MANIFEST_SHA256": "deadbeef",
		"CONTINUO_RUNTIME_CONTEXT_JSON":    `{"command_dialect_sha256":"x"}`,
	} {
		got, ok := env[name]
		require.True(t, ok, "%s must reach the worker", name)
		assert.Equal(t, want, got.Value, "%s", name)
	}

	assert.Equal(t, spec.PoolKey, env["CONTINUO_POOL_KEY"].Value,
		"the pod carries the FULL pool key, not the truncated name")
	assert.Equal(t, []string{workerBinaryPath}, container.Command)
}

// TestEnsureTakesThePodsIdentityFromTheDownwardAPI proves the pod reports the
// name and UID Kubernetes gave it, which is what lets a lease be fenced against
// the exact pod that holds it.
func TestEnsureTakesThePodsIdentityFromTheDownwardAPI(t *testing.T) {
	w, cs := newTestWorkerPools()

	require.NoError(t, w.Ensure(context.Background(), testSpec()))

	env := envOf(getDeployment(t, cs, testPoolKey).Spec.Template.Spec.Containers[0])
	for name, field := range map[string]string{
		"CONTINUO_POD_NAME": "metadata.name",
		"CONTINUO_POD_UID":  "metadata.uid",
	} {
		got := env[name]
		require.NotNil(t, got.ValueFrom, "%s comes from the downward API", name)
		require.NotNil(t, got.ValueFrom.FieldRef)
		assert.Equal(t, field, got.ValueFrom.FieldRef.FieldPath)
		assert.Empty(t, got.Value, "%s is not a literal", name)
	}
}

// TestEnsureNeverPutsTheCredentialInThePodSpec proves the raw credential reaches
// the pod only by secretKeyRef. A literal in the pod template would be readable
// by anyone who can read a Deployment, which is a far wider audience than those
// who can read a Secret.
func TestEnsureNeverPutsTheCredentialInThePodSpec(t *testing.T) {
	w, cs := newTestWorkerPools()
	spec := testSpec()

	require.NoError(t, w.Ensure(context.Background(), spec))

	dep := getDeployment(t, cs, spec.PoolKey)
	env := envOf(dep.Spec.Template.Spec.Containers[0])

	ref := env["CONTINUO_POOL_CREDENTIAL"]
	require.NotNil(t, ref.ValueFrom, "the credential arrives by reference")
	require.NotNil(t, ref.ValueFrom.SecretKeyRef)
	assert.Equal(t, poolResourceName(spec.PoolKey), ref.ValueFrom.SecretKeyRef.Name)
	assert.Equal(t, credentialSecretKey, ref.ValueFrom.SecretKeyRef.Key)
	assert.Empty(t, ref.Value)

	// Nothing anywhere in the rendered Deployment may spell the credential.
	rendered, err := json.Marshal(dep)
	require.NoError(t, err)
	assert.NotContains(t, string(rendered), spec.Credential,
		"the raw credential must not appear anywhere in the Deployment")
}

// TestEnsureCreatesTheCredentialSecret proves the raw credential is written to
// the pool's Secret under the one key the pod reads.
func TestEnsureCreatesTheCredentialSecret(t *testing.T) {
	w, cs := newTestWorkerPools()
	spec := testSpec()

	require.NoError(t, w.Ensure(context.Background(), spec))

	secret, err := cs.CoreV1().Secrets(testNamespace).Get(
		context.Background(), poolResourceName(spec.PoolKey), metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, spec.Credential, string(secret.Data[credentialSecretKey]))
	assert.Len(t, secret.Data, 1, "the Secret holds the credential and nothing else")
}

// TestEnsureWithoutACredentialLeavesTheSecretAlone proves a routine reconcile —
// which never has the credential in hand — cannot blank out the Secret a live
// pool's workers authenticate with.
func TestEnsureWithoutACredentialLeavesTheSecretAlone(t *testing.T) {
	existing := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      poolResourceName(testPoolKey),
			Namespace: testNamespace,
		},
		Data: map[string][]byte{credentialSecretKey: []byte("the-live-credential")},
	}
	w, cs := newTestWorkerPools(existing)

	spec := testSpec()
	spec.Credential = ""
	spec.DesiredReplicas = 4
	require.NoError(t, w.Ensure(context.Background(), spec))

	secret, err := cs.CoreV1().Secrets(testNamespace).Get(
		context.Background(), poolResourceName(testPoolKey), metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, "the-live-credential", string(secret.Data[credentialSecretKey]),
		"a reconcile with no credential in hand leaves the Secret untouched")
	assert.EqualValues(t, 4, *getDeployment(t, cs, testPoolKey).Spec.Replicas,
		"the Deployment is still reconciled")
}

// TestEnsureRewritesASecretWhoseCredentialRotated proves a rotation actually
// replaces the stored value rather than being dropped as "already exists".
func TestEnsureRewritesASecretWhoseCredentialRotated(t *testing.T) {
	existing := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      poolResourceName(testPoolKey),
			Namespace: testNamespace,
		},
		Data: map[string][]byte{credentialSecretKey: []byte("the-old-credential")},
	}
	w, cs := newTestWorkerPools(existing)

	spec := testSpec()
	spec.Credential = "the-new-credential"
	require.NoError(t, w.Ensure(context.Background(), spec))

	secret, err := cs.CoreV1().Secrets(testNamespace).Get(
		context.Background(), poolResourceName(testPoolKey), metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, "the-new-credential", string(secret.Data[credentialSecretKey]))
}

// TestEnsureIsIdempotent proves reconciling twice converges rather than failing
// on the second pass: every tick calls Ensure for every pool.
func TestEnsureIsIdempotent(t *testing.T) {
	w, cs := newTestWorkerPools()
	spec := testSpec()

	require.NoError(t, w.Ensure(context.Background(), spec))
	spec.DesiredReplicas = 7
	require.NoError(t, w.Ensure(context.Background(), spec), "a second reconcile updates in place")

	assert.EqualValues(t, 7, *getDeployment(t, cs, spec.PoolKey).Spec.Replicas)
}

// TestEnsureRollsOutWithoutDroppingCapacity pins the rollout shape: a pool must
// never dip below its replica count while being replaced.
func TestEnsureRollsOutWithoutDroppingCapacity(t *testing.T) {
	w, cs := newTestWorkerPools()

	require.NoError(t, w.Ensure(context.Background(), testSpec()))

	strategy := getDeployment(t, cs, testPoolKey).Spec.Strategy
	require.Equal(t, appsv1.RollingUpdateDeploymentStrategyType, strategy.Type)
	require.NotNil(t, strategy.RollingUpdate)
	assert.Equal(t, int32(0), strategy.RollingUpdate.MaxUnavailable.IntVal,
		"no worker is taken away before its replacement is serving")
	assert.Equal(t, int32(1), strategy.RollingUpdate.MaxSurge.IntVal)
}

// TestEnsureGatesReadinessOnTheWorkersOwnSignal proves a worker is only counted
// ready once it says so, which is what keeps an unhydrated pod out of the pool.
func TestEnsureGatesReadinessOnTheWorkersOwnSignal(t *testing.T) {
	w, cs := newTestWorkerPools()

	require.NoError(t, w.Ensure(context.Background(), testSpec()))

	probe := getDeployment(t, cs, testPoolKey).Spec.Template.Spec.Containers[0].ReadinessProbe
	require.NotNil(t, probe)
	require.NotNil(t, probe.Exec)
	assert.Equal(t, []string{"sh", "-c", "test -f " + workerReadyFile}, probe.Exec.Command)
}

// TestEnsureLabelsThePoolAndAnnotatesItsIdentity pins that the pool is
// selectable by label while its full identity — which does not fit a label —
// stays readable in annotations.
func TestEnsureLabelsThePoolAndAnnotatesItsIdentity(t *testing.T) {
	w, cs := newTestWorkerPools()
	spec := testSpec()

	require.NoError(t, w.Ensure(context.Background(), spec))
	dep := getDeployment(t, cs, spec.PoolKey)

	assert.Equal(t, workerAppLabel, dep.Labels["app"])
	assert.Equal(t, spec.PoolKey[:16], dep.Labels[poolKeyLabel])
	assert.Equal(t, dep.Labels, dep.Spec.Template.Labels, "the pods carry the pool's labels")
	assert.Equal(t, dep.Spec.Selector.MatchLabels, dep.Spec.Template.Labels,
		"the selector matches the pods it owns")

	assert.Equal(t, spec.PoolKey, dep.Annotations[annotationWorkerPoolKey],
		"the FULL key survives in an annotation")
	assert.Equal(t, spec.RuntimeManifest.RuntimeManifestURI, dep.Annotations[annotationRuntimeManifestURI])
	assert.Equal(t, spec.RuntimeManifest.RuntimeManifestSHA256, dep.Annotations[annotationRuntimeManifestSHA256])
}

// TestEnsureGivesTwoPoolsOfOneServiceDistinctResources proves the pool key — not
// the service — names the resources, so a service running two image tags or two
// runtime manifests gets two pools rather than one that overwrites the other.
func TestEnsureGivesTwoPoolsOfOneServiceDistinctResources(t *testing.T) {
	w, cs := newTestWorkerPools()

	newImage := testSpec()
	newImage.ImageTag = "xyz789"
	newImage.PoolKey = pkgmodel.WorkerPoolKey("finance", "xyz789", "deadbeef")

	newManifest := testSpec()
	newManifest.RuntimeManifest.RuntimeManifestSHA256 = "cafebabe"
	newManifest.PoolKey = pkgmodel.WorkerPoolKey("finance", "abc123", "cafebabe")

	require.NoError(t, w.Ensure(context.Background(), testSpec()))
	require.NoError(t, w.Ensure(context.Background(), newImage))
	require.NoError(t, w.Ensure(context.Background(), newManifest))

	deps, err := cs.AppsV1().Deployments(testNamespace).List(context.Background(), metav1.ListOptions{})
	require.NoError(t, err)
	assert.Len(t, deps.Items, 3, "a different image tag or runtime manifest is a different pool")
}

// TestEnsureCreatesNoService proves workers are never addressed: a worker dials
// the executor to claim work, and nothing ever dials a worker. A Service would
// invite inbound traffic to a pod with no server on it.
func TestEnsureCreatesNoService(t *testing.T) {
	w, cs := newTestWorkerPools()

	require.NoError(t, w.Ensure(context.Background(), testSpec()))

	services, err := cs.CoreV1().Services(testNamespace).List(context.Background(), metav1.ListOptions{})
	require.NoError(t, err)
	assert.Empty(t, services.Items, "workers dial out; nothing dials in")
}

// TestStatusReportsAnAbsentPool proves a pool with no Deployment is reported as
// absent rather than as an error: that is the state of every pool before its
// first reconcile.
func TestStatusReportsAnAbsentPool(t *testing.T) {
	w, _ := newTestWorkerPools()

	status, found, err := w.Status(context.Background(), testPoolKey)

	require.NoError(t, err)
	assert.False(t, found)
	assert.Zero(t, status.DesiredReplicas)
}

// TestStatusReportsTheClustersReplicaCounts proves Status reads back what the
// cluster holds, which is the CurrentReplicas the sizing rule works from.
func TestStatusReportsTheClustersReplicaCounts(t *testing.T) {
	w, cs := newTestWorkerPools()
	require.NoError(t, w.Ensure(context.Background(), testSpec()))

	live := getDeployment(t, cs, testPoolKey)
	live.Status.ReadyReplicas = 1
	_, err := cs.AppsV1().Deployments(testNamespace).UpdateStatus(
		context.Background(), live, metav1.UpdateOptions{})
	require.NoError(t, err)

	status, found, err := w.Status(context.Background(), testPoolKey)

	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, 2, status.DesiredReplicas)
	assert.Equal(t, 1, status.ReadyReplicas)
	assert.True(t, status.SecretExists)
}

// TestStatusReportsALostSecret is the signal that drives rotation: the pool's
// digest can no longer be matched by any credential anyone holds, so the pool
// must be given a new one.
func TestStatusReportsALostSecret(t *testing.T) {
	w, cs := newTestWorkerPools()
	require.NoError(t, w.Ensure(context.Background(), testSpec()))
	require.NoError(t, cs.CoreV1().Secrets(testNamespace).Delete(
		context.Background(), poolResourceName(testPoolKey), metav1.DeleteOptions{}))

	status, found, err := w.Status(context.Background(), testPoolKey)

	require.NoError(t, err)
	require.True(t, found, "the Deployment is still there")
	assert.False(t, status.SecretExists, "its Secret is not")
}

// TestStatusReportsASecretWhoseDeploymentIsGone proves the Secret is reported on
// its own. Were it reported only alongside a Deployment, a pool whose Deployment
// was removed would be read as having lost its Secret too and be rotated for no
// reason.
func TestStatusReportsASecretWhoseDeploymentIsGone(t *testing.T) {
	w, cs := newTestWorkerPools()
	require.NoError(t, w.Ensure(context.Background(), testSpec()))
	require.NoError(t, cs.AppsV1().Deployments(testNamespace).Delete(
		context.Background(), poolResourceName(testPoolKey), metav1.DeleteOptions{}))

	status, found, err := w.Status(context.Background(), testPoolKey)

	require.NoError(t, err)
	assert.False(t, found, "the Deployment is gone")
	assert.True(t, status.SecretExists, "the Secret is reported independently of it")
}

// TestDeletePodRemovesTheNamedPod covers the fencing primitive's happy path.
func TestDeletePodRemovesTheNamedPod(t *testing.T) {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: "dbt-worker-abc-1", Namespace: testNamespace, UID: "uid-1",
	}}
	w, cs := newTestWorkerPools(pod)

	require.NoError(t, w.DeletePod(context.Background(), "dbt-worker-abc-1", "uid-1"))

	_, err := cs.CoreV1().Pods(testNamespace).Get(
		context.Background(), "dbt-worker-abc-1", metav1.GetOptions{})
	assert.True(t, apierrors.IsNotFound(err), "the pod is gone")
}

// TestDeletePodAcceptsAPodAlreadyGone proves an absent pod is the outcome asked
// for, not a fault: a fence that arrives after the pod crashed has nothing to do.
func TestDeletePodAcceptsAPodAlreadyGone(t *testing.T) {
	w, _ := newTestWorkerPools()

	assert.NoError(t, w.DeletePod(context.Background(), "dbt-worker-abc-1", "uid-1"))
}

// TestDeletePodNamesTheUIDItIntendsToDelete proves the delete is guarded by UID.
// A pod name is reused as a Deployment replaces its pods, so a fence that lands
// late must not take out the healthy successor wearing the same name.
func TestDeletePodNamesTheUIDItIntendsToDelete(t *testing.T) {
	w, cs := newTestWorkerPools()
	var got *metav1.Preconditions
	cs.PrependReactor("delete", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		got = action.(k8stesting.DeleteActionImpl).DeleteOptions.Preconditions
		return true, nil, nil
	})

	require.NoError(t, w.DeletePod(context.Background(), "dbt-worker-abc-1", "uid-1"))

	require.NotNil(t, got, "the delete carries a precondition")
	require.NotNil(t, got.UID)
	assert.EqualValues(t, "uid-1", *got.UID,
		"only the pod holding this UID is deleted")
}

// TestWorkerPoolsSatisfiesItsPort keeps the adapter honest about the port it
// claims to implement.
func TestWorkerPoolsSatisfiesItsPort(t *testing.T) {
	var _ ports.WorkerPoolRuntime = (*WorkerPools)(nil)
	var _ ports.PodVerifier = (*WorkerPools)(nil)
	assert.True(t, strings.HasPrefix(poolResourceName(testPoolKey), "dbt-worker-"))
}

// workerPod builds a pod carrying the labels a real worker of testPoolKey wears.
func workerPod(name, uid string) *corev1.Pod {
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name:      name,
		Namespace: testNamespace,
		UID:       types.UID(uid),
		Labels: map[string]string{
			"app":        workerAppLabel,
			poolKeyLabel: poolKeyLabelValue(testPoolKey),
		},
	}}
}

// TestVerifyPodAcceptsAPoolsOwnWorker is the confused-deputy guard's accept side:
// a real pod of the authenticated pool, named with its true UID, is bound.
func TestVerifyPodAcceptsAPoolsOwnWorker(t *testing.T) {
	w, _ := newTestWorkerPools(workerPod("dbt-worker-abc-1", "uid-1"))
	assert.NoError(t, w.VerifyPod(context.Background(), testPoolKey, "dbt-worker-abc-1", "uid-1"))
}

// TestVerifyPodRejectsAnythingItCannotVouchFor pins every way a claimed pod
// identity fails to prove itself the pool's own: without this, a caller holding
// the pool credential could bind another pod's identity to its lease and have
// reaping delete it, or omit its identity to escape fencing.
func TestVerifyPodRejectsAnythingItCannotVouchFor(t *testing.T) {
	otherPoolKey := pkgmodel.WorkerPoolKey("finance", "xyz789", "cafebabe")
	otherPoolPod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: "dbt-worker-other-1", Namespace: testNamespace, UID: types.UID("uid-other"),
		Labels: map[string]string{"app": workerAppLabel, poolKeyLabel: poolKeyLabelValue(otherPoolKey)},
	}}
	nonWorkerPod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: "some-other-pod", Namespace: testNamespace, UID: types.UID("uid-x"),
		Labels: map[string]string{"app": "postgres"},
	}}
	w, _ := newTestWorkerPools(workerPod("dbt-worker-abc-1", "uid-1"), otherPoolPod, nonWorkerPod)

	for _, tc := range []struct {
		name, podName, podUID string
	}{
		{"a nonexistent pod", "dbt-worker-ghost", "uid-1"},
		{"a UID that does not match the live pod", "dbt-worker-abc-1", "uid-stale"},
		{"a pod of another pool", "dbt-worker-other-1", "uid-other"},
		{"a pod that is not a worker", "some-other-pod", "uid-x"},
		{"no pod name", "", "uid-1"},
		{"no pod uid", "dbt-worker-abc-1", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Error(t, w.VerifyPod(context.Background(), testPoolKey, tc.podName, tc.podUID))
		})
	}
}

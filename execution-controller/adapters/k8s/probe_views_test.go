package k8s

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/carolsimone/continuo/execution-controller/service/ports"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes/fake"
)

func TestProbeJob_RunsTheProbeOpOnTheOutsideViews(t *testing.T) {
	t.Setenv("VALIDATION_IMAGE", "ghcr.io/carolsimone/continuo-python-runtime-trino:v0.12.0")
	t.Setenv("VALIDATION_WAREHOUSE_SECRET", "continuo-warehouse-validation")
	job, err := probeJob(ports.ProbeRequest{JobName: "finance-analytics-seed-fx-rates-eur-6f1c-0", NodeID: "analytics.seed_fx_rates_eur",
		Views: []string{"bi.fx_rates_report"}}, "default")
	require.NoError(t, err)
	assert.Equal(t, "continuo-probe", job.Labels["app"])
	assert.Equal(t, int64(40), *job.Spec.ActiveDeadlineSeconds)
	assert.Equal(t, int32(120), *job.Spec.TTLSecondsAfterFinished)
	require.Len(t, job.Spec.Template.Spec.Containers, 1)
	assert.Equal(t, "probe-views", job.Spec.Template.Spec.Containers[0].Name)
	env := job.Spec.Template.Spec.Containers[0].Env
	assert.Subset(t, env, []corev1.EnvVar{
		{Name: "VALIDATION_OP", Value: "probe_views"}, {Name: "PROBE_VIEWS", Value: `["bi.fx_rates_report"]`},
		{Name: "NODE_ID", Value: "analytics.seed_fx_rates_eur"}})
	for _, e := range env {
		assert.False(t, strings.HasPrefix(e.Name, "PREFLIGHT_") || strings.HasPrefix(e.Name, "SEED_PREFLIGHT_"), "probe_views reads only PROBE_VIEWS and NODE_ID: %s", e.Name)
	}
	assert.LessOrEqual(t, len(probeJobName(strings.Repeat("x", 63))), 52)
	assert.Equal(t, probeJobName("a"), probeJobName("a"))
}

func TestProbeJobName_KeepsTwoLongParentsApart(t *testing.T) {
	head := strings.Repeat("x", 60)
	a, b := probeJobName(head+"-0"), probeJobName(head+"-1")
	assert.NotEqual(t, a, b)
	assert.LessOrEqual(t, len(a), 52)
	assert.True(t, strings.HasPrefix(probeJobName("Fin_Seed"), "probe-fin-seed"))
}

func TestProbeJob_RefusesAMissingImage(t *testing.T) {
	t.Setenv("VALIDATION_IMAGE", "")
	_, err := probeJob(ports.ProbeRequest{JobName: "j", NodeID: "a.b", Views: []string{"bi.v"}}, "default")
	require.Error(t, err)
}

// The Job watch selects JobSelector; a probe Job and its pods must not match
// it, or the observer would take the probe for an attempt.
func TestProbeJob_IsInvisibleToTheJobWatch(t *testing.T) {
	t.Setenv("VALIDATION_IMAGE", "ghcr.io/x/runtime:v1")
	t.Setenv("VALIDATION_WAREHOUSE_SECRET", "wh")
	sel, err := labels.Parse(JobSelector)
	require.NoError(t, err)
	job, err := probeJob(ports.ProbeRequest{JobName: "j", NodeID: "a.b", Views: []string{"bi.v"}}, "default")
	require.NoError(t, err)
	assert.False(t, sel.Matches(labels.Set(job.Labels)))
	assert.False(t, sel.Matches(labels.Set(job.Spec.Template.Labels)))
}

func probeTestProber(t *testing.T, cs *fake.Clientset) ports.ViewProber {
	t.Helper()
	t.Setenv("VALIDATION_IMAGE", "ghcr.io/x/runtime:v1")
	t.Setenv("VALIDATION_WAREHOUSE_SECRET", "wh")
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	c := &K8sClient{logger: logger}
	c.setClientsetForTest(cs)
	return NewViewProber(c, "ns", logger)
}

var probeReq = ports.ProbeRequest{JobName: "seed-job-0", NodeID: "analytics.seed", Views: []string{"bi.v"}}

func TestViewProber_ReturnsThePodLogOfTheFinishedJobAndDeletesIt(t *testing.T) {
	cs := fake.NewSimpleClientset()
	name := probeJobName(probeReq.JobName)
	_, err := cs.CoreV1().Pods("ns").Create(context.Background(), &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "p1", Namespace: "ns", Labels: map[string]string{"job-name": name}}}, metav1.CreateOptions{})
	require.NoError(t, err)
	go completeJob(t, cs, name)

	log, err := probeTestProber(t, cs).ProbeViews(context.Background(), probeReq)

	require.NoError(t, err)
	assert.Equal(t, "fake logs", log, "the fake clientset serves this body for every pod log")
	_, err = cs.BatchV1().Jobs("ns").Get(context.Background(), name, metav1.GetOptions{})
	assert.True(t, apierrors.IsNotFound(err), "the probe Job is deleted once its log is read")
}

func TestViewProber_ReturnsAnErrorWhenTheContextEndsBeforeTheJobDoes(t *testing.T) {
	cs := fake.NewSimpleClientset()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err := probeTestProber(t, cs).ProbeViews(ctx, probeReq)

	require.Error(t, err)
	assert.True(t, errors.Is(err, context.DeadlineExceeded), "got %v", err)
	_, err = cs.BatchV1().Jobs("ns").Get(context.Background(), probeJobName(probeReq.JobName), metav1.GetOptions{})
	assert.True(t, apierrors.IsNotFound(err), "an unfinished probe Job is deleted too")
}

// completeJob marks the Job of that name complete as soon as it exists.
func completeJob(t *testing.T, cs *fake.Clientset, name string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		job, err := cs.BatchV1().Jobs("ns").Get(context.Background(), name, metav1.GetOptions{})
		if err == nil {
			job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
			_, err = cs.BatchV1().Jobs("ns").UpdateStatus(context.Background(), job, metav1.UpdateOptions{})
			assert.NoError(t, err)
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Error("the probe Job was never created")
}

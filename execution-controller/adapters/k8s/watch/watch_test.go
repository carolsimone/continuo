package watch

import (
	"context"
	"testing"
	"time"

	"github.com/carolsimone/continuo/execution-controller/domain/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

const ns = "jobs"

func job(name string) *batchv1.Job {
	return &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns,
		Labels: map[string]string{"app": "dbt-job", "task-id": "t1"}}}
}

func pod(name, jobName string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns,
			Labels: map[string]string{"app": "dbt-job", batchv1.JobNameLabel: jobName}},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "dbt-job", Image: "svc:v1",
			Env: []corev1.EnvVar{{Name: "AWS_SECRET_ACCESS_KEY", Value: "secret"}}}}},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, ContainerStatuses: []corev1.ContainerStatus{{
			Name: "dbt-job", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.NewTime(time.Unix(100, 0))}}}}},
	}
}

// next waits for the queue's next name.
func next(t *testing.T, q *Queue) string {
	t.Helper()
	got := make(chan string, 1)
	go func() {
		name, _ := q.Get()
		got <- name
	}()
	select {
	case name := <-got:
		q.Done(name)
		return name
	case <-time.After(5 * time.Second):
		t.Fatal("no name queued within 5s")
		return ""
	}
}

func startWatch(t *testing.T, cs *fake.Clientset) (*Watch, *Queue) {
	t.Helper()
	q := NewQueue()
	t.Cleanup(q.ShutDown)
	w, err := New(cs, ns, q)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	w.Start(ctx)
	require.True(t, w.WaitForCacheSync(ctx))
	require.True(t, w.Synced())
	return w, q
}

// The watch queues every Job that exists when it starts, so the observer
// judges every Job, including one whose deployment it does not resync.
func TestWatch_AJobThatExistsWhenItStartsIsQueued(t *testing.T) {
	cs := fake.NewClientset(job("svc-s-t-1a2b3c4d"))
	_, q := startWatch(t, cs)
	assert.Equal(t, "svc-s-t-1a2b3c4d", next(t, q))
}

func TestWatch_AJobAndItsPodsEnqueueTheJobAndAreObserved(t *testing.T) {
	cs := fake.NewClientset()
	w, q := startWatch(t, cs)
	ctx := context.Background()

	_, err := cs.BatchV1().Jobs(ns).Create(ctx, job("svc-s-t-7f3e1a2b"), metav1.CreateOptions{})
	require.NoError(t, err)
	assert.Equal(t, "svc-s-t-7f3e1a2b", next(t, q))

	_, err = cs.CoreV1().Pods(ns).Create(ctx, pod("svc-s-t-7f3e1a2b-x1", "svc-s-t-7f3e1a2b"), metav1.CreateOptions{})
	require.NoError(t, err)
	assert.Equal(t, "svc-s-t-7f3e1a2b", next(t, q), "a pod enqueues its Job's name")

	obs := w.Observe("svc-s-t-7f3e1a2b")
	assert.True(t, obs.JobPresent)
	require.Len(t, obs.Pods, 1)
	assert.Equal(t, model.PodRunning, obs.Pods[0].Phase)
	assert.True(t, obs.MainStarted())

	require.NoError(t, cs.CoreV1().Pods(ns).Delete(ctx, "svc-s-t-7f3e1a2b-x1", metav1.DeleteOptions{}))
	assert.Equal(t, "svc-s-t-7f3e1a2b", next(t, q), "a pod's deletion enqueues its Job's name")
	assert.Empty(t, w.Observe("svc-s-t-7f3e1a2b").Pods)
}

func TestWatch_AJobNotInTheCacheIsObservedAbsent(t *testing.T) {
	w, _ := startWatch(t, fake.NewClientset())
	obs := w.Observe("no-such-job")
	assert.False(t, obs.JobPresent)
	assert.Empty(t, obs.Pods)
	assert.True(t, obs.Ended(), "nothing of it is left in the cache")
}

func TestWatch_OnlyLabelledObjectsAreWatched(t *testing.T) {
	unlabelled := job("schema-op")
	unlabelled.Labels = map[string]string{"app": "continuo-schema-op"}
	w, _ := startWatch(t, fake.NewClientset(unlabelled))
	assert.False(t, w.Observe("schema-op").JobPresent, "a schema-operation Job is not a deployment's attempt")
}

// ObserveLive reads the Job and its pods from the API, past the cache: it needs
// no started informer, and finds pods that carry only the legacy job-name
// label.
func TestWatch_ObserveLiveReadsTheAPIPastTheCache(t *testing.T) {
	legacy := pod("svc-s-old-7f3e1a2b-x1", "")
	legacy.Labels = map[string]string{"app": "dbt-job", "job-name": "svc-s-old-7f3e1a2b"}
	cs := fake.NewClientset(job("svc-s-t-7f3e1a2b"), pod("svc-s-t-7f3e1a2b-x1", "svc-s-t-7f3e1a2b"),
		pod("svc-s-t-7f3e1a2b-x2", "svc-s-t-7f3e1a2b"), pod("svc-s-u-7f3e1a2b-x1", "svc-s-u-7f3e1a2b"),
		job("svc-s-old-7f3e1a2b"), legacy)
	w, err := New(cs, ns, NewQueue())
	require.NoError(t, err)
	ctx := context.Background()

	obs, err := w.ObserveLive(ctx, "svc-s-t-7f3e1a2b")
	require.NoError(t, err)
	assert.True(t, obs.JobPresent)
	assert.Len(t, obs.Pods, 2, "the Job's own pods, not another Job's")

	old, err := w.ObserveLive(ctx, "svc-s-old-7f3e1a2b")
	require.NoError(t, err)
	assert.Len(t, old.Pods, 1)

	gone, err := w.ObserveLive(ctx, "svc-s-gone-7f3e1a2b")
	require.NoError(t, err)
	assert.False(t, gone.JobPresent)
	assert.Empty(t, gone.Pods)
}

// JobNames lists every Job the cache holds, the ones that exist when it
// starts and the ones created since; the observer's resync queues them all.
func TestWatch_JobNamesListsEveryCachedJob(t *testing.T) {
	cs := fake.NewClientset(job("svc-s-t-1a2b3c4d"))
	w, _ := startWatch(t, cs)
	_, err := cs.BatchV1().Jobs(ns).Create(context.Background(), job("svc-s-t-7f3e1a2b"), metav1.CreateOptions{})
	require.NoError(t, err)

	require.Eventually(t, func() bool { return len(w.JobNames()) == 2 }, 5*time.Second, 10*time.Millisecond)
	assert.ElementsMatch(t, []string{"svc-s-t-1a2b3c4d", "svc-s-t-7f3e1a2b"}, w.JobNames())
}

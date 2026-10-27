//go:build integration

package handlers_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/carolsimone/continuo/execution-controller/adapters/k8s/watch"
	"github.com/carolsimone/continuo/execution-controller/adapters/postgres"
	"github.com/carolsimone/continuo/execution-controller/domain/command"
	"github.com/carolsimone/continuo/execution-controller/domain/deploy"
	"github.com/carolsimone/continuo/execution-controller/domain/model"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

const fenceNamespace = "jobs"

// apiJob is the Job of cmd's attempt as the launch creates it: labelled with
// the task, the attempt and the run, on the Job and its pod template alike.
func apiJob(cmd command.DeployTask) *batchv1.Job {
	return &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: cmd.JobName, Namespace: fenceNamespace, Labels: attemptLabels(cmd)}}
}

func attemptLabels(cmd command.DeployTask) map[string]string {
	return map[string]string{"app": "dbt-job", "task-id": cmd.TaskID, "attempt": strconv.Itoa(cmd.Attempt),
		"run-id": cmd.RunID}
}

// apiPod is a running pod of cmd's attempt's Job.
func apiPod(cmd command.DeployTask, started time.Time) *corev1.Pod {
	labels := attemptLabels(cmd)
	labels[batchv1.JobNameLabel], labels["job-name"] = cmd.JobName, cmd.JobName
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: cmd.JobName + "-p0d", Namespace: fenceNamespace, Labels: labels},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "dbt-job", Image: "dbt:sha-abc"}}},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, ContainerStatuses: []corev1.ContainerStatus{{Name: "dbt-job",
			State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.NewTime(started)}}}}},
	}
}

// fenceAPI is a fake Kubernetes API with the watch over it, for the fence's
// reads of a run's Jobs and pods.
func fenceAPI(t *testing.T, objs ...any) (*fake.Clientset, *watch.Watch) {
	t.Helper()
	cs := fake.NewClientset()
	for _, o := range objs {
		switch v := o.(type) {
		case *batchv1.Job:
			_, err := cs.BatchV1().Jobs(fenceNamespace).Create(context.Background(), v, metav1.CreateOptions{})
			require.NoError(t, err)
		case *corev1.Pod:
			_, err := cs.CoreV1().Pods(fenceNamespace).Create(context.Background(), v, metav1.CreateOptions{})
			require.NoError(t, err)
		}
	}
	w, err := watch.New(cs, fenceNamespace, watch.NewQueue())
	require.NoError(t, err)
	return cs, w
}

// A fence over a run with hundreds of rows waiting for their launch reads the
// run's Jobs and pods from the API a bounded number of times, whatever the
// number of rows, and decides every row from that read: a row whose Job
// exists adopts it and moves to terminating, a row whose attempt has a pod
// that has not finished but no Job is left as it is and logged, and a row
// with neither ends stopped.
func TestRunCancelled_ReadsTheRunsJobsOnceForEveryWaitingRow(t *testing.T) {
	f := newFenceFlow(t)
	const perKind = 100
	started := f.now.Add(-time.Minute)
	var withJob, withPod, withNothing []*model.Deployment
	var objs []any
	for i := range 3 * perKind {
		status := model.StatusPending
		if i%2 == 1 {
			status = model.StatusReserved
		}
		d := f.attempt(status)
		switch i % 3 {
		case 0:
			withJob = append(withJob, d)
			objs = append(objs, apiJob(d.Command()), apiPod(d.Command(), started))
		case 1:
			withPod = append(withPod, d)
			objs = append(objs, apiPod(d.Command(), started))
		default:
			withNothing = append(withNothing, d)
		}
	}
	cs, w := fenceAPI(t, objs...)
	cs.ClearActions()
	var logs bytes.Buffer
	h := newRunCancelledHandlerOver(w, f, slog.New(slog.NewTextHandler(&logs, nil)))

	names := f.cancel(h, f.event())

	reads := cs.Actions()
	assert.LessOrEqual(t, len(reads), 3, "the run's Jobs and pods are read a bounded number of times, not once per row")
	for _, a := range reads {
		assert.Contains(t, []string{"list", "get"}, a.GetVerb(), "the fence only reads the API: %s %s", a.GetVerb(), a.GetResource().Resource)
	}
	wantNames := make([]string, 0, perKind)
	for _, d := range withJob {
		wantNames = append(wantNames, d.JobName())
		got := f.reload(d.Command().TaskID, 0)
		assert.Equal(t, model.StatusTerminating, got.Status(), "a row whose Job exists adopts it")
		assert.NotNil(t, got.Facts().StopRequestedAt)
		assert.Empty(t, f.reports(d.Command().TaskID))
	}
	assert.ElementsMatch(t, wantNames, names, "every adopted Job is deleted after the commit")
	for _, d := range withPod {
		got := f.reload(d.Command().TaskID, 0)
		assert.Equal(t, d.Status(), got.Status(), "a row with an unfinished pod and no Job is left as it is")
		assert.Nil(t, got.Facts().StopRequestedAt)
		assert.Empty(t, f.reports(d.Command().TaskID))
	}
	assert.Equal(t, perKind, strings.Count(logs.String(), "level=ERROR"), "each such row is logged")
	for _, d := range withNothing {
		got := f.reload(d.Command().TaskID, 0)
		assert.Equal(t, model.StatusDone, got.Status())
		assert.Equal(t, model.OutcomeStopped, got.Outcome())
		require.Len(t, f.reports(d.Command().TaskID), 1)
	}
	assert.Equal(t, perKind, f.metrics.stopped)
}

// lossyCreate announces on entered that the launch holds the row and is
// calling Kubernetes, waits for release, then creates the Job in the API and
// answers the create with a timeout, as when Kubernetes stored the Job and
// its answer was lost: the launch leaves the row pending with a creation try
// left.
type lossyCreate struct {
	cs      *fake.Clientset
	entered chan struct{}
	release chan struct{}
}

func (l *lossyCreate) Deploy(ctx context.Context, spec deploy.JobSpec) error {
	close(l.entered)
	<-l.release
	cmd := command.DeployTask{TaskID: spec.TaskID, RunID: spec.RunID, JobName: spec.JobName, Attempt: spec.Attempt}
	if _, err := l.cs.BatchV1().Jobs(fenceNamespace).Create(ctx, apiJob(cmd), metav1.CreateOptions{}); err != nil {
		return err
	}
	return errors.New("the server was unable to return a response in the time allotted")
}
func (l *lossyCreate) DeployValidation(context.Context, deploy.ValidationJobSpec) error { return nil }
func (l *lossyCreate) DeploySeedBuild(context.Context, deploy.ValidationJobSpec) error  { return nil }
func (l *lossyCreate) DeployCompile(context.Context, deploy.ValidationJobSpec) error    { return nil }

// A cancel that meets a launch holding the row waits for it. While the fence
// waits, the launch creates the Job but loses the answer, so it leaves the
// row pending with no Job recorded. The fence reads the run's Jobs only once
// it holds the row, so the read shows the Job the launch created: the row
// adopts it, moves to terminating, and the Job is returned for deletion.
func TestRunCancelled_ReadsTheRunsJobsAfterALaunchThatHeldTheRow(t *testing.T) {
	f := newFenceFlow(t)
	d := f.attempt(model.StatusPending)
	cs, w := fenceAPI(t)
	dep := &lossyCreate{cs: cs, entered: make(chan struct{}), release: make(chan struct{})}
	launched := make(chan error, 1)
	go func() { launched <- f.dispatcher(dep).ProcessBatch(context.Background()) }()
	var release sync.Once
	t.Cleanup(func() { release.Do(func() { close(dep.release) }) })
	select {
	case <-dep.entered:
	case err := <-launched:
		t.Fatalf("the launch ended before it created the Job: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("the launch did not create the Job")
	}

	fenced := f.inTx(func(u *postgres.PostgresUnitOfWork) error {
		names, err := newRunCancelledHandlerOver(w, f, f.logger).Handle(context.Background(), u, f.event(), uuid.Nil)
		if err == nil {
			assert.Equal(t, []string{d.JobName()}, names, "the Job the launch created is deleted")
		}
		return err
	})
	f.waitForLockWaiter()
	_, early := waitOrNot(fenced)
	require.False(t, early, "the fence waits for the launch's row lock")

	release.Do(func() { close(dep.release) })
	require.NoError(t, <-launched)
	require.NoError(t, <-fenced)

	got := f.reload(d.Command().TaskID, 0)
	assert.Equal(t, model.StatusTerminating, got.Status(), "the fence's read shows the Job the launch created")
	assert.NotNil(t, got.DeployedAt())
	assert.NotNil(t, got.Facts().StopRequestedAt)
	assert.Empty(t, f.reports(d.Command().TaskID), "stopped is reported once the pods are gone")
}

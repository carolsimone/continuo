package k8s

import (
	"context"
	"log/slog"
	"os"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestCountActiveJobs_CountsOnlyActivePods(t *testing.T) {
	cs := fake.NewSimpleClientset(
		jobWithActive("running-1", "default", map[string]string{"app": "dbt-job"}, 1),
		jobWithActive("running-2", "default", map[string]string{"app": "dbt-job"}, 1),
		jobWithActive("completed", "default", map[string]string{"app": "dbt-job"}, 0), // not active
		jobWithActive("other-app", "default", map[string]string{"app": "something"}, 1), // wrong label
	)
	c := &K8sClient{logger: slog.New(slog.NewTextHandler(os.Stderr, nil))}
	c.setClientsetForTest(cs)

	n, err := c.CountActiveJobs(context.Background(), "default", "app=dbt-job")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 2 {
		t.Fatalf("want 2 active dbt-job Jobs, got %d", n)
	}
}

func jobWithActive(name, ns string, labels map[string]string, active int32) *batchv1.Job {
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, Labels: labels},
		Status:     batchv1.JobStatus{Active: active},
	}
}

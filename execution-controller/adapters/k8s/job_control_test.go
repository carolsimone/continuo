package k8s

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

func newJobControl(t *testing.T, objs ...runtime.Object) (*JobControl, *fake.Clientset) {
	t.Helper()
	cs := fake.NewClientset(objs...)
	c := &K8sClient{logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	c.setClientsetForTest(cs)
	return NewJobControl(c, "jobs"), cs
}

func TestJobControl_DeleteJobIsForegroundAndToleratesAGoneJob(t *testing.T) {
	jc, cs := newJobControl(t, &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "j", Namespace: "jobs"}})
	var policy metav1.DeletionPropagation
	cs.PrependReactor("delete", "jobs", func(a k8stesting.Action) (bool, runtime.Object, error) {
		policy = *a.(k8stesting.DeleteActionImpl).DeleteOptions.PropagationPolicy
		return false, nil, nil
	})
	require.NoError(t, jc.DeleteJob(context.Background(), "j"))
	assert.Equal(t, metav1.DeletePropagationForeground, policy, "the Job stays until its pods are gone")
	require.NoError(t, jc.DeleteJob(context.Background(), "j"), "deleting a Job that is gone is not an error")
}

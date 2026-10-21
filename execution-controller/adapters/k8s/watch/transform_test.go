package watch

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestTrim_KeepsWhatTheObservationReadsAndNothingElse(t *testing.T) {
	deleting := metav1.NewTime(time.Unix(200, 0))
	p := pod("p", "j")
	p.DeletionTimestamp = &deleting
	p.Status.InitContainerStatuses = []corev1.ContainerStatus{{Name: "hydrate-parse-cache"}}

	out, err := trim(p)
	require.NoError(t, err)
	got := out.(*corev1.Pod)
	assert.Equal(t, p.Name, got.Name)
	assert.Equal(t, p.Labels, got.Labels)
	assert.Equal(t, &deleting, got.DeletionTimestamp)
	assert.Equal(t, p.Status.Phase, got.Status.Phase)
	assert.Equal(t, p.Status.ContainerStatuses, got.Status.ContainerStatuses)
	assert.Equal(t, p.Status.InitContainerStatuses, got.Status.InitContainerStatuses)
	assert.Empty(t, got.Spec.Containers, "the pod spec, with its env, is not kept")

	again, err := trim(got)
	require.NoError(t, err)
	assert.Equal(t, got, again, "trimming a trimmed pod changes nothing")

	j := job("j")
	j.Annotations = map[string]string{"continuo.io/release-id": "r1"}
	j.Spec.Template.Spec.Containers = []corev1.Container{{Name: "dbt-job"}}
	j.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: "DeadlineExceeded"}}
	out, err = trim(j)
	require.NoError(t, err)
	gotJob := out.(*batchv1.Job)
	assert.Equal(t, j.Annotations, gotJob.Annotations)
	assert.Equal(t, j.Status.Conditions, gotJob.Status.Conditions)
	assert.Empty(t, gotJob.Spec.Template.Spec.Containers)
}

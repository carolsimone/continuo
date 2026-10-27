package k8s

import (
	"context"
	"testing"

	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// A task's Job and its pod template both carry the id of the run the attempt
// belongs to under the run-id label, the key the watch selects a run's Jobs by.
func TestJobsCreate_LabelsTheJobAndItsPodsWithRunID(t *testing.T) {
	t.Setenv("DOCKERHUB_USERNAME", "")
	c := newQueryTestClient()
	ctx := context.Background()
	params := JobParams{
		JobName: "prod-svc-analytics-orders-abc456", TaskID: "task-1", RunID: "run-1",
		ScheduleName: "daily", ServiceName: "svc", SchemaName: "analytics", TableName: "orders",
		Namespace: "default", NodeType: pkg_model.NodeTypeDbtModel, ImageTag: "v1",
	}

	require.NoError(t, c.CreateQueryJob(ctx, params))

	job, err := c.clientset.BatchV1().Jobs("default").Get(ctx, params.JobName, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, "run-id", LabelRunID)
	assert.Equal(t, "run-1", job.Labels[LabelRunID])
	assert.Equal(t, "run-1", job.Spec.Template.Labels[LabelRunID])
}

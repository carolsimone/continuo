package k8s

import (
	"testing"

	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildPodSpec_CommandPerNodeType(t *testing.T) {
	tests := []struct {
		nodeType    pkg_model.NodeType
		tableName   string
		wantCommand []string
	}{
		{pkg_model.NodeTypeDbtModel, "orders", []string{"dbt", "run", "--select", "orders"}},
		{pkg_model.NodeTypeDbtSeed, "my_seed", []string{"dbt", "seed", "--select", "my_seed"}},
		{pkg_model.NodeTypeDbtSnapshot, "my_snap", []string{"dbt", "snapshot", "--select", "my_snap"}},
	}

	for _, tt := range tests {
		params := JobParams{
			NodeType:  tt.nodeType,
			TableName: tt.tableName,
			ImageTag:  "test-tag",
		}
		spec, err := buildPodSpec(params)
		require.NoError(t, err)
		require.Len(t, spec.Containers, 1)
		assert.Equal(t, tt.wantCommand, spec.Containers[0].Command,
			"NodeType %q should produce command %v", tt.nodeType, tt.wantCommand)
	}
}

func TestBuildPodSpec_ImageRef(t *testing.T) {
	t.Run("no DOCKERHUB_USERNAME uses service name directly", func(t *testing.T) {
		t.Setenv("DOCKERHUB_USERNAME", "")
		spec, err := buildPodSpec(JobParams{ServiceName: "service-1", ImageTag: "some-tag", NodeType: pkg_model.NodeTypeDbtModel, TableName: "t"})
		require.NoError(t, err)
		require.Len(t, spec.Containers, 1)
		assert.Equal(t, "service-1:some-tag", spec.Containers[0].Image)
	})

	t.Run("with DOCKERHUB_USERNAME each service gets its own image", func(t *testing.T) {
		t.Setenv("DOCKERHUB_USERNAME", "carolsimone")
		specA, err := buildPodSpec(JobParams{ServiceName: "service-1", ImageTag: "latest", NodeType: pkg_model.NodeTypeDbtModel, TableName: "t"})
		require.NoError(t, err)
		specB, err := buildPodSpec(JobParams{ServiceName: "service-2", ImageTag: "latest", NodeType: pkg_model.NodeTypeDbtModel, TableName: "t"})
		require.NoError(t, err)
		require.Len(t, specA.Containers, 1)
		require.Len(t, specB.Containers, 1)
		assert.Equal(t, "carolsimone/service-1:latest", specA.Containers[0].Image)
		assert.Equal(t, "carolsimone/service-2:latest", specB.Containers[0].Image)
	})

	t.Run("with DOCKERHUB_USERNAME uses params.ImageTag", func(t *testing.T) {
		t.Setenv("DOCKERHUB_USERNAME", "carolsimone")
		spec, err := buildPodSpec(JobParams{ServiceName: "service-1", ImageTag: "v1.2.3", NodeType: pkg_model.NodeTypeDbtModel, TableName: "t"})
		require.NoError(t, err)
		require.Len(t, spec.Containers, 1)
		assert.Equal(t, "carolsimone/service-1:v1.2.3", spec.Containers[0].Image)
	})
}

func TestBuildPodSpec_UsesImageTagFromParams(t *testing.T) {
	t.Setenv("DOCKERHUB_USERNAME", "")
	params := JobParams{
		ServiceName: "service-1",
		ImageTag:    "abc123-1714300000",
		NodeType:    pkg_model.NodeTypeDbtModel,
		TableName:   "users",
	}
	spec, err := buildPodSpec(params)
	require.NoError(t, err)
	require.Len(t, spec.Containers, 1)
	assert.Equal(t, "service-1:abc123-1714300000", spec.Containers[0].Image)
}

func TestBuildPodSpec_RefusesEmptyImageTag(t *testing.T) {
	params := JobParams{
		ServiceName: "service-1",
		ImageTag:    "",
		NodeType:    pkg_model.NodeTypeDbtModel,
		TableName:   "users",
	}
	_, err := buildPodSpec(params)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "image_tag missing")
}

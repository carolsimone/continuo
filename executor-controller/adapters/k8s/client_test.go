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
		}
		spec := buildPodSpec(params)
		require.Len(t, spec.Containers, 1)
		assert.Equal(t, tt.wantCommand, spec.Containers[0].Command,
			"NodeType %q should produce command %v", tt.nodeType, tt.wantCommand)
	}
}

func TestBuildPodSpec_ImageRef(t *testing.T) {
	t.Run("no DOCKERHUB_USERNAME uses service name directly", func(t *testing.T) {
		t.Setenv("DOCKERHUB_USERNAME", "")
		spec := buildPodSpec(JobParams{ServiceName: "service-1"})
		assert.Equal(t, "service-1", spec.Containers[0].Image)
	})

	t.Run("with DOCKERHUB_USERNAME each service gets its own image", func(t *testing.T) {
		t.Setenv("DOCKERHUB_USERNAME", "carolsimone")
		specA := buildPodSpec(JobParams{ServiceName: "service-1"})
		specB := buildPodSpec(JobParams{ServiceName: "service-2"})
		assert.Equal(t, "carolsimone/service-1:latest", specA.Containers[0].Image)
		assert.Equal(t, "carolsimone/service-2:latest", specB.Containers[0].Image)
	})
}

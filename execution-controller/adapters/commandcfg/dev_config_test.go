package commandcfg

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// The dev/docker-compose config must satisfy the same completeness contract as
// the deployed one, so the executor boots against it.
func TestDevConfigLoads(t *testing.T) {
	path := filepath.Join("..", "..", "..", "config", "dbt-commands.yaml")
	_, err := Load(path, testLogger())
	require.NoError(t, err)
}

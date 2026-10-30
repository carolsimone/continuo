package neo4jinfra

import (
	"context"
	"errors"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// recordingRunner records each statement it is asked to run instead of
// sending it to Neo4j.
type recordingRunner struct {
	cypher string
	params map[string]any
	calls  int
}

func (r *recordingRunner) Run(_ context.Context, cypher string, params map[string]any) (neo4j.ResultWithContext, error) {
	r.cypher = cypher
	r.params = params
	r.calls++
	return nil, nil
}

func TestTenantRunner_RefusesEmptyTenant(t *testing.T) {
	_, err := NewTenantRunner("", &recordingRunner{})
	require.Error(t, err)
	assert.True(t, errors.Is(err, errEmptyTenant))

	var zero TenantRunner
	_, err = zero.Run(context.Background(), "RETURN 1", nil)
	require.Error(t, err, "the zero TenantRunner carries no tenant and must refuse to run")
	assert.True(t, errors.Is(err, errEmptyTenant))
}

func TestTenantRunner_RefusesNilRunner(t *testing.T) {
	_, err := NewTenantRunner("default", nil)
	require.Error(t, err)
}

func TestTenantRunner_BindsTenantAndRefusesAnOverride(t *testing.T) {
	rec := &recordingRunner{}
	run, err := NewTenantRunner("tenant-a", rec)
	require.NoError(t, err)

	params := map[string]any{"unique_id": "analytics.table_a"}
	_, err = run.Run(context.Background(), "MATCH (n:Node {tenant_id: $tenant_id, unique_id: $unique_id}) RETURN n", params)
	require.NoError(t, err)
	assert.Equal(t, 1, rec.calls)
	assert.Equal(t, map[string]any{"tenant_id": "tenant-a", "unique_id": "analytics.table_a"}, rec.params)
	assert.Equal(t, map[string]any{"unique_id": "analytics.table_a"}, params, "the caller's map is not mutated")

	_, err = run.Run(context.Background(), "RETURN $tenant_id", nil)
	require.NoError(t, err, "nil params bind the tenant alone")
	assert.Equal(t, map[string]any{"tenant_id": "tenant-a"}, rec.params)

	override := map[string]any{"tenant_id": "tenant-b"}
	_, err = run.Run(context.Background(), "MATCH (n:Node {tenant_id: $tenant_id}) RETURN n", override)
	require.Error(t, err, "a params map that carries tenant_id is refused")
	assert.True(t, errors.Is(err, errTenantParamOverride))
	assert.Equal(t, 2, rec.calls, "a refused statement is never sent")
	assert.Equal(t, map[string]any{"tenant_id": "tenant-b"}, override, "the refused map is not mutated")
}

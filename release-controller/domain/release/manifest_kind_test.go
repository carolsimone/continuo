package release_test

import (
	"testing"

	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseManifestKind_AcceptsDbt(t *testing.T) {
	k, err := release.ParseManifestKind("dbt")
	require.NoError(t, err)
	assert.Equal(t, release.ManifestKindDbt, k)
}

// A Python project ships its odcs-bundle.json under the kind python.
func TestParseManifestKind_AcceptsPython(t *testing.T) {
	k, err := release.ParseManifestKind("python")
	require.NoError(t, err)
	assert.Equal(t, release.ManifestKindPython, k)
	assert.Equal(t, "python", string(k))
}

func TestParseManifestKind_AcceptsOntology(t *testing.T) {
	k, err := release.ParseManifestKind("ontology")
	require.NoError(t, err)
	assert.Equal(t, release.ManifestKindOntology, k)
	assert.Equal(t, "ontology", string(k))
}

// Only a dbt project is compiled before it is parsed: a Python project ships a
// contract bundle and the ontology project ships its ontology file, both
// uploaded before the release is requested.
func TestHasCompileLeg_OnlyDbt(t *testing.T) {
	assert.True(t, release.ManifestKindDbt.HasCompileLeg())
	assert.False(t, release.ManifestKindPython.HasCompileLeg())
	assert.False(t, release.ManifestKindOntology.HasCompileLeg())
}

func TestParseManifestKind_RejectsEmptyAndUnknown(t *testing.T) {
	_, err := release.ParseManifestKind("")
	assert.Error(t, err, "empty-to-dbt defaulting is the API boundary's job, not the domain's")

	_, err = release.ParseManifestKind("r")
	assert.EqualError(t, err, `unknown manifest kind "r" (expected "dbt", "python" or "ontology")`)
}

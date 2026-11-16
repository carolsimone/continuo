package release_test

import (
	"testing"

	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseManifestKind_AcceptsDbtAndPython(t *testing.T) {
	k, err := release.ParseManifestKind("dbt")
	require.NoError(t, err)
	assert.Equal(t, release.ManifestKindDbt, k)

	k, err = release.ParseManifestKind("python")
	require.NoError(t, err)
	assert.Equal(t, release.ManifestKindPython, k)
}

func TestParseManifestKind_AcceptsPythonODCS(t *testing.T) {
	k, err := release.ParseManifestKind("python-odcs")
	require.NoError(t, err)
	assert.Equal(t, release.ManifestKindPythonODCS, k)
	assert.Equal(t, "python-odcs", string(k))
}

func TestManifestKind_IsPythonCoversBothPythonKinds(t *testing.T) {
	assert.True(t, release.ManifestKindPython.IsPython())
	assert.True(t, release.ManifestKindPythonODCS.IsPython())
	assert.False(t, release.ManifestKindDbt.IsPython())
}

func TestParseManifestKind_RejectsEmptyAndUnknown(t *testing.T) {
	_, err := release.ParseManifestKind("")
	assert.Error(t, err, "empty-to-dbt defaulting is the API boundary's job, not the domain's")

	_, err = release.ParseManifestKind("r")
	assert.Error(t, err)
}

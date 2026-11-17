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

// The python kind names a contract.yaml artifact no parser reads: a
// new release or verification that names it is refused with the kind to use
// instead.
func TestParseManifestKind_RefusesPythonNamingPythonODCS(t *testing.T) {
	k, err := release.ParseManifestKind("python")
	require.Error(t, err)
	assert.Empty(t, k)
	assert.EqualError(t, err,
		`kind "python" is no longer accepted: release the service as "python-odcs", one ODCS contract per node (docs/contracts.md)`)
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

// A stored row may still carry the python kind, so the constant stays: it is
// the value an old service_prod or run row reads back as, and it is still a
// python kind (no compile leg, no overlay).
func TestManifestKindPython_IsStillAKnownStoredValue(t *testing.T) {
	assert.Equal(t, "python", string(release.ManifestKindPython))
}

func TestParseManifestKind_RejectsEmptyAndUnknown(t *testing.T) {
	_, err := release.ParseManifestKind("")
	assert.Error(t, err, "empty-to-dbt defaulting is the API boundary's job, not the domain's")

	_, err = release.ParseManifestKind("r")
	assert.EqualError(t, err, `unknown manifest kind "r" (expected "dbt" or "python-odcs")`)
}

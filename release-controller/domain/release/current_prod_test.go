package release_test

import (
	"testing"
	"time"

	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/stretchr/testify/assert"
)

func TestCurrentProd_NewIsEmpty(t *testing.T) {
	cp := release.NewCurrentProd()
	assert.Empty(t, cp.ReleaseID())
}

func TestCurrentProd_Update(t *testing.T) {
	cp := release.NewCurrentProd()
	cp.Update("sha-xyz", "s3://continuo/releases/sha-xyz/manifests/", release.Topology{{UniqueID: "n"}}, time.Unix(10, 0))
	assert.Equal(t, "sha-xyz", cp.ReleaseID())
	assert.Equal(t, "s3://continuo/releases/sha-xyz/manifests/", cp.ManifestsURI())
	assert.Equal(t, 1, len(cp.TopologySnapshot()))
	assert.Equal(t, time.Unix(10, 0).UTC(), cp.UpdatedAt().UTC())
}

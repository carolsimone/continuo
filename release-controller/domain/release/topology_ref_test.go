package release_test

import (
	"testing"

	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/stretchr/testify/assert"
)

func TestTopologyRef_IsZeroOnlyWithoutURI(t *testing.T) {
	assert.True(t, release.TopologyRef{}.IsZero())
	assert.True(t, release.TopologyRef{SHA256: "abc", NodeCount: 3}.IsZero(), "a ref without a URI names no artifact")
	assert.False(t, release.TopologyRef{URI: "s3://continuo/tenants/default/topologies/r1/topology.json.gz", SHA256: "abc"}.IsZero())
}

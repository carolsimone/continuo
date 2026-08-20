package release_test

import (
	"testing"
	"time"

	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/stretchr/testify/assert"
)

func TestNew_BootstrapFlag(t *testing.T) {
	now := time.Unix(100, 0).UTC()

	plain := release.New("r1", "svc-a", "sha-a", false, false, "acme/demo", "deadbeef", release.ManifestKindDbt, now)
	assert.False(t, plain.IsBootstrap())

	boot := release.New("r2", "svc-a", "sha-a", true, false, "acme/demo", "deadbeef", release.ManifestKindDbt, now)
	assert.True(t, boot.IsBootstrap())
}

func TestRehydrate_PreservesBootstrap(t *testing.T) {
	r := release.Rehydrate(release.RehydrateInput{
		ID:        "r3",
		Status:    release.StatusReceived,
		Bootstrap: true,
	})
	assert.True(t, r.IsBootstrap())
}

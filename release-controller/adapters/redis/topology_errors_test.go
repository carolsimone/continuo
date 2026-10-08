package redis

import (
	"errors"
	"fmt"
	"testing"

	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/release-controller/service/ports"
	"github.com/stretchr/testify/assert"
)

func TestPermanentOnCorruptTopology(t *testing.T) {
	corrupt := fmt.Errorf("load candidate topology: %w", ports.ErrTopologyArtifactCorrupt)
	got := permanentOnCorruptTopology(corrupt)
	assert.ErrorIs(t, got, pkgevents.ErrPermanent, "a corrupt artifact can never be read: dead-letter")
	assert.ErrorIs(t, got, ports.ErrTopologyArtifactCorrupt)

	notFound := fmt.Errorf("load: %w", ports.ErrTopologyArtifactNotFound)
	assert.False(t, errors.Is(permanentOnCorruptTopology(notFound), pkgevents.ErrPermanent),
		"a missing object stays transient: it is retried within the delivery budget")

	other := errors.New("dial tcp: connection refused")
	assert.Equal(t, other, permanentOnCorruptTopology(other))
	assert.NoError(t, permanentOnCorruptTopology(nil))
}

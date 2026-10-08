package redis

import (
	"errors"
	"fmt"

	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/release-controller/service/ports"
)

// permanentOnCorruptTopology marks a handler error caused by a corrupt
// topology artifact as permanent: reading the artifact again cannot succeed,
// so the message is dead-lettered (and shows in continuo dlq) instead of being
// retried through its delivery budget. Every other error is returned
// unchanged, so an unreachable object store still pauses the consumer.
func permanentOnCorruptTopology(err error) error {
	if errors.Is(err, ports.ErrTopologyArtifactCorrupt) && !errors.Is(err, pkgevents.ErrPermanent) {
		return fmt.Errorf("%w: %w", pkgevents.ErrPermanent, err)
	}
	return err
}

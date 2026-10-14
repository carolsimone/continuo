package handlers

import (
	"context"
	"fmt"

	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/service/uow"
)

// liveStamp reads the default tenant's live release through u, inside the
// transaction u has open, so a run names the release that was live when it
// was created. With no live release it returns the zero stamp.
//
// It takes the topology lock before the read. A promotion moves the live
// release and the retention pruner deletes releases no committed run names,
// both under that lock; holding it until the creating transaction ends means
// the stamped release cannot be pruned between this read and the commit of the
// run that names it.
func liveStamp(ctx context.Context, u uow.UnitOfWork) (run.ReleaseStamp, error) {
	if err := u.Topology().LockTopology(ctx); err != nil {
		return run.ReleaseStamp{}, fmt.Errorf("lock the topology for the live release read: %w", err)
	}
	p, err := u.Topology().LivePointer(ctx, pkgevents.DefaultTenantID)
	if err != nil {
		return run.ReleaseStamp{}, fmt.Errorf("read the live release: %w", err)
	}
	return run.ReleaseStamp{ReleaseID: p.ReleaseID, PromotionSeq: p.PromotionSeq}, nil
}

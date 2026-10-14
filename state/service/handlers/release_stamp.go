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
func liveStamp(ctx context.Context, u uow.UnitOfWork) (run.ReleaseStamp, error) {
	p, err := u.Topology().LivePointer(ctx, pkgevents.DefaultTenantID)
	if err != nil {
		return run.ReleaseStamp{}, fmt.Errorf("read the live release: %w", err)
	}
	return run.ReleaseStamp{ReleaseID: p.ReleaseID, PromotionSeq: p.PromotionSeq}, nil
}

package handlers

import (
	"context"
	"fmt"
	"time"

	"github.com/carolsimone/continuo/release-controller/domain/releasecheck"
)

// ActiveCheck describes the check currently holding the release queue's one slot.
type ActiveCheck struct {
	CheckID           string
	Kind              releasecheck.Kind
	Status            releasecheck.Status
	Project           string
	Since             time.Time
	VerifiesReleaseID string // fix only
	Round             int    // fix only
	Try               int    // fix only
}

// ReleaseQueue is what the release queue is doing right now: the active
// check of either kind, or nothing.
type ReleaseQueue struct {
	Active *ActiveCheck
}

// GetReleaseQueue reads the active check. It is the one read that spans both
// kinds, so an operator can see a fix's release check holding the slot that
// candidate releases are queued behind.
func GetReleaseQueue(ctx context.Context, d *Deps) (ReleaseQueue, error) {
	u := d.NewUoW()
	r, err := u.ReleaseCheckRepo().Active(ctx)
	if err != nil {
		return ReleaseQueue{}, fmt.Errorf("active check: %w", err)
	}
	if r == nil {
		return ReleaseQueue{}, nil
	}
	since, _ := r.ActivatedAt()
	active := &ActiveCheck{CheckID: r.ID(), Kind: r.Kind(), Status: r.Status(), Project: r.ChangedProject(), Since: since}
	if r.Kind() == releasecheck.KindFix {
		active.VerifiesReleaseID = r.VerifiesReleaseID()
		active.Round = r.Round()
		active.Try = r.Try()
	}
	return ReleaseQueue{Active: active}, nil
}

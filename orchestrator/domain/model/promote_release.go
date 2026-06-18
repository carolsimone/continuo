package model

import (
	"time"

	"github.com/carolsimone/continuo/orchestrator/domain/event"
)

// PromoteReleaseInput is the handler-input wrapper for a release.promoted:v1
// message. Built by the binding after unmarshaling the wire payload.
type PromoteReleaseInput struct {
	ReleaseID  string
	Topology   []event.ReleasePromotedNode
	ImageTags  map[string]string
	Repo       string
	CommitSHA  string
	PromotedAt time.Time
}

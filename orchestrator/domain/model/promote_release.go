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
	// CodeBundleURI locates the release's code-bundle document in object
	// storage; Bootstrap marks a re-baseline release. Both are consumed by the
	// version-ingestion path and ignored by the topology swap.
	CodeBundleURI string
	Bootstrap     bool
}

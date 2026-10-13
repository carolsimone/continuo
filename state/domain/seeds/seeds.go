// Package seeds decides which dbt seeds of the live release need building. A
// seed is built when the warehouse does not hold the content its live node
// carries; at most one build per seed runs at a time, and a build that failed
// at a content hash is not retried at that hash.
package seeds

import (
	"cmp"
	"slices"
	"strconv"
	"time"

	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/state/domain/topology"
	"github.com/google/uuid"
)

// promotedSeedsNamespace seeds the deterministic ids of a release's
// promote-seed runs.
//
// IMMUTABLE: changing it re-keys every promote-seed run id, so a release
// planned again across the change would mint a second run for seeds already
// requested.
var promotedSeedsNamespace = uuid.MustParse("3e7b1a2f-8c4d-4e9a-b5f0-1d2c3a4e5f60")

// Desired is a dbt seed of the live release: the content its live node carries
// and the image that builds it.
type Desired struct {
	Key         topology.NodeKey
	ServiceName string
	SchemaName  string
	TableName   string
	NodeType    string
	ContentHash string
	ImageTag    string
}

// AttemptState is how the run of a seed's latest build request stands.
type AttemptState int

const (
	// AttemptNone: no build was requested, or its run no longer exists.
	AttemptNone AttemptState = iota
	// AttemptInFlight: the run is pending or running.
	AttemptInFlight
	// AttemptFailed: the run ended failed, cancelled or skipped.
	AttemptFailed
	// AttemptSucceeded: the run ended succeeded.
	AttemptSucceeded
)

// Materialization is what state knows about one node in the warehouse: the
// content last loaded ("" when none is recorded) and its latest build request.
type Materialization struct {
	Key                topology.NodeKey
	ContentHash        string
	AttemptContentHash string
	AttemptRunID       uuid.UUID
	AttemptState       AttemptState
}

// Decision is what a reconcile pass does about one desired seed.
type Decision int

const (
	// DecisionNothing: the warehouse holds the desired content.
	DecisionNothing Decision = iota
	// DecisionSkipUnknownHash: the live node carries no content hash, so there
	// is nothing to compare.
	DecisionSkipUnknownHash
	// DecisionWait: a build of this seed is still pending or running.
	DecisionWait
	// DecisionNoRetry: the build of the desired content failed; it is not
	// retried until the content changes or an operator reruns it.
	DecisionNoRetry
	// DecisionBuild: the seed joins this pass's promote-seed run.
	DecisionBuild
)

// String names the decision for logs.
func (d Decision) String() string {
	switch d {
	case DecisionNothing:
		return "nothing"
	case DecisionSkipUnknownHash:
		return "skip-unknown-hash"
	case DecisionWait:
		return "wait"
	case DecisionNoRetry:
		return "no-retry"
	case DecisionBuild:
		return "build"
	default:
		return "unknown"
	}
}

// Decide applies the seed rules to one desired seed and what state knows about
// it (found is false when no row exists). The rules apply in order: no hash →
// skip; built at the desired hash → nothing; a build in flight → wait; the
// build of the desired hash failed → no retry; otherwise → build. The image
// is not compared: it changes on every release of its service.
func Decide(d Desired, m Materialization, found bool) Decision {
	switch {
	case d.ContentHash == "":
		return DecisionSkipUnknownHash
	case found && m.ContentHash == d.ContentHash:
		return DecisionNothing
	case found && m.AttemptState == AttemptInFlight:
		return DecisionWait
	case found && m.AttemptContentHash == d.ContentHash && m.AttemptState == AttemptFailed:
		return DecisionNoRetry
	default:
		return DecisionBuild
	}
}

// DesiredFrom returns the dbt-seed nodes of a release as desired seeds, in key
// order, whatever their content hash.
func DesiredFrom(nodes []topology.Node) []Desired {
	out := make([]Desired, 0, len(nodes))
	for _, n := range nodes {
		if n.NodeType == string(model.NodeTypeDbtSeed) {
			out = append(out, desiredOf(n))
		}
	}
	sortByKey(out)
	return out
}

// Plan returns the desired seeds Decide says to build, in key order.
func Plan(desired []Desired, built map[topology.NodeKey]Materialization) []Desired {
	out := make([]Desired, 0, len(desired))
	for _, d := range desired {
		m, found := built[d.Key]
		if Decide(d, m, found) == DecisionBuild {
			out = append(out, d)
		}
	}
	sortByKey(out)
	return out
}

// BuiltSource says how a recorded version got into the warehouse.
type BuiltSource string

const (
	// SourceBuild: a run loaded it.
	SourceBuild BuiltSource = "build"
	// SourceAdopted: it was recorded as already loaded when state applied its
	// first promotion.
	SourceAdopted BuiltSource = "adopted"
)

// Built is the version of one node last loaded into the warehouse. RunID and
// TaskID are uuid.Nil for an adopted version.
type Built struct {
	Key         topology.NodeKey
	ContentHash string
	ImageTag    string
	ReleaseID   string
	RunID       uuid.UUID
	TaskID      uuid.UUID
	At          time.Time
	Source      BuiltSource
}

// Adopted records d as loaded at the live release's content, without a run.
func Adopted(d Desired, releaseID string, at time.Time) Built {
	return Built{
		Key:         d.Key,
		ContentHash: d.ContentHash,
		ImageTag:    d.ImageTag,
		ReleaseID:   releaseID,
		At:          at,
		Source:      SourceAdopted,
	}
}

// Adoptions returns the seeds to record as already loaded when state applies
// its first promotion, in key order. An install's first release (bootstrap)
// adopts nothing. Otherwise every seed whose unique id is not in
// changedUniqueIDs is adopted, and every seed when the release's first
// promote-seed run already exists. A seed without a content hash is never
// adopted.
func Adoptions(live []topology.Node, changedUniqueIDs []string, bootstrap, firstRunExists bool) []Desired {
	if bootstrap {
		return nil
	}
	changed := make(map[string]bool, len(changedUniqueIDs))
	for _, id := range changedUniqueIDs {
		changed[id] = true
	}
	var out []Desired
	for _, n := range live {
		if n.NodeType != string(model.NodeTypeDbtSeed) || n.ContentHash == "" {
			continue
		}
		if !firstRunExists && changed[n.UniqueID] {
			continue
		}
		out = append(out, desiredOf(n))
	}
	sortByKey(out)
	return out
}

// RunID is the id of the n-th promote-seed run planned for a release, counting
// from 1. The first is the UUIDv5 of the release id alone and every later one
// also carries its count; n below 1 names the first.
func RunID(releaseID string, n int) uuid.UUID {
	name := "schedule:" + releaseID
	if n >= 2 {
		name += "#" + strconv.Itoa(n)
	}
	return uuid.NewSHA1(promotedSeedsNamespace, []byte(name))
}

func desiredOf(n topology.Node) Desired {
	return Desired{
		Key:         n.Key,
		ServiceName: n.ServiceName,
		SchemaName:  n.SchemaName,
		TableName:   n.TableName,
		NodeType:    n.NodeType,
		ContentHash: n.ContentHash,
		ImageTag:    n.ImageTag,
	}
}

func sortByKey(ds []Desired) {
	slices.SortStableFunc(ds, func(a, b Desired) int { return cmp.Compare(a.Key, b.Key) })
}

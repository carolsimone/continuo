package ports

import "time"

// The result labels of RemediationMetrics.Try: how a cluster ended in the try
// that worked it.
const (
	// TryPassed: the try's verification runs passed and the fix was handed
	// over for review.
	TryPassed = "passed"
	// TryGateFailed: a verification run did not pass the fix, or never answered.
	TryGateFailed = "gate_failed"
	// TryChecksFailed: a cross-cluster check refused the try's drafts, or the
	// gate refused them at submit.
	TryChecksFailed = "checks_failed"
	// TryPreflightSpent: every pre-flight round of the cluster's type refused
	// the draft.
	TryPreflightSpent = "preflight_spent"
	// TryDeclined: a step declined the cluster, or its draft made no edit.
	TryDeclined = "declined"
)

// The result labels of RemediationMetrics.Round: how one pre-flight pass of a
// cluster ended.
const (
	// RoundPassed: no check of the pass found anything.
	RoundPassed = "passed"
	// RoundRevised: a check found something and the cluster has a round left,
	// so the model revises the draft.
	RoundRevised = "revised"
	// RoundSpent: a check found something in the cluster's last round.
	RoundSpent = "spent"
)

// The outcome labels of RemediationMetrics.Outcome: how a node ended when its
// remediation did.
const (
	// OutcomeFixed: a verified fix was handed over for review.
	OutcomeFixed = "fixed"
	// OutcomeHandedOver: the node was left to a human, with a reason.
	OutcomeHandedOver = "handed_over"
)

// The stage labels of RemediationMetrics.Stage: the kind of step a committed
// step was.
const (
	StageContext      = "context"
	StagePropose      = "propose"
	StagePreflight    = "preflight"
	StageCrossCluster = "cross_cluster"
	StageGate         = "gate"
	StageOutput       = "output"
)

// The direction labels of RemediationMetrics.Tokens.
const (
	TokensIn  = "in"
	TokensOut = "out"
)

// TypeNone is the type label of a node whose cluster no type matched.
const TypeNone = "none"

// RemediationMetrics counts what the remediation runner does, by the flow it
// follows and the type of the cluster it works. flow and typ are the names of
// loaded files, so their values are as many as the install has files; typ is
// empty for a step of the flow itself and TypeNone for a node no type matched.
type RemediationMetrics interface {
	// Try counts one cluster ended by a try, with the Try* result.
	Try(flow, typ, result string)
	// Round counts one pre-flight pass, with the Round* result.
	Round(flow, typ, result string)
	// Outcome counts one node of a remediation that ended, with the Outcome*
	// label and, for a node handed over, the reason the node carries.
	Outcome(flow, typ, outcome, reason string)
	// SystemFailure counts one remediation that stopped for a failure of the
	// runner, and waits to be redriven, by the kind of that failure.
	SystemFailure(flow, reason string)
	// Stage observes how long one committed step took.
	Stage(flow, typ, stage string, d time.Duration)
	// Tokens adds the n tokens one answered model call used, in the
	// direction TokensIn or TokensOut.
	Tokens(flow, typ, direction string, n int)
}

// NopRemediationMetrics counts nothing.
type NopRemediationMetrics struct{}

var _ RemediationMetrics = NopRemediationMetrics{}

func (NopRemediationMetrics) Try(string, string, string)                  {}
func (NopRemediationMetrics) Round(string, string, string)                {}
func (NopRemediationMetrics) Outcome(string, string, string, string)      {}
func (NopRemediationMetrics) SystemFailure(string, string)                {}
func (NopRemediationMetrics) Stage(string, string, string, time.Duration) {}
func (NopRemediationMetrics) Tokens(string, string, string, int)          {}

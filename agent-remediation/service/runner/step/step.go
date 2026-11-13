// Package step defines the contract between the runner and the steps it runs.
// A step is a small unit of work at one stage of a remediation: it gathers
// evidence, builds a model request, checks a draft, checks every cluster's
// draft together, judges a try at the verification gate, or hands the result
// over. Each step describes itself with a Meta, and the runner reaches it only
// through the interface of its Kind. This package imports no adapter.
package step

import (
	"context"
	"time"

	"github.com/carolsimone/continuo/agent-remediation/domain/prompt"
	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/domain/workflow"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/uow"
)

// Kind says which interface a step implements.
type Kind string

const (
	// KindContext is a ContextSource.
	KindContext Kind = "context"
	// KindPrompt is a Prompt.
	KindPrompt Kind = "prompt"
	// KindCheck is a Check, a CrossCheck or a LongCheck.
	KindCheck Kind = "check"
	// KindApprover picks who reviews a proposal.
	KindApprover Kind = "approver"
	// KindOutput is a PassOutput or a GiveUpOutput.
	KindOutput Kind = "output"
)

// Meta is how a step describes itself.
type Meta struct {
	Name        workflow.StepName
	Kind        Kind
	Description string
	// Needs lists the evidence keys the step reads.
	Needs []remediation.EvidenceKey
	// Provides lists the evidence keys the step writes.
	Provides []remediation.EvidenceKey
	// Stages lists the stages the step may be placed at.
	Stages []workflow.Stage
	// MayStandAsGateRun reports whether the step may stand in the flow's gate
	// as the check that runs the verification.
	MayStandAsGateRun bool
}

// ContextSource gathers evidence for one cluster.
type ContextSource interface {
	Meta() Meta
	Gather(ctx context.Context, in ContextInput) (ContextOutput, error)
}

// ContextInput is what a context step reads: the failure and the evidence
// gathered so far.
type ContextInput struct {
	Failure  remediation.Failure
	Evidence remediation.Evidence
}

// ContextOutput is what a context step returns: the evidence it gathered, or a
// decision to decline the cluster. A step declines by setting Declined; Decline
// is the optional reason text, and may be empty.
type ContextOutput struct {
	Evidence map[remediation.EvidenceKey]any
	Declined bool
	Decline  string
}

// Model is the handle the runner gives a prompt step for its model call.
type Model interface {
	Propose(ctx context.Context, req prompt.ProposeRequest) (ports.ProposeResult, error)
}

// Prompt builds the model's request for one cluster, calls the model through
// the Model it is given and reads the answer into a draft.
type Prompt interface {
	Meta() Meta
	Propose(ctx context.Context, in PromptInput, m Model) (PromptOutput, error)
}

// PromptInput is what a prompt step reads. Round is the pre-flight round, 0 for
// the first. Previous and Findings are set on a revision: the draft the last
// round produced and what the checks found in it.
type PromptInput struct {
	Failure  remediation.Failure
	Evidence remediation.Evidence
	Round    int
	Previous *remediation.Draft
	Findings []remediation.Finding
}

// PromptOutput is the draft a prompt step read from the model's answer, or a
// decision to decline the cluster. A step declines by setting Declined; Decline
// is the optional reason text, and may be empty.
type PromptOutput struct {
	Draft    remediation.Draft
	Declined bool
	Decline  string
}

// Check checks one cluster's draft before it is verified.
type Check interface {
	Meta() Meta
	Check(ctx context.Context, in CheckInput) (CheckOutput, error)
}

// CheckInput is what a check reads. Scratch is the draft applied to a checkout
// for this pass.
type CheckInput struct {
	Failure  remediation.Failure
	Evidence remediation.Evidence
	Draft    remediation.Draft
	Scratch  *Scratch
}

// CheckOutput is a check's result and any evidence it gathered.
type CheckOutput struct {
	Result   remediation.CheckResult
	Evidence map[remediation.EvidenceKey]any
}

// CrossCheck checks the drafts of every cluster of a try together.
type CrossCheck interface {
	Meta() Meta
	CheckAll(ctx context.Context, in CrossInput) (remediation.CheckResult, error)
}

// CrossInput is what a cross-cluster check reads.
type CrossInput struct {
	Trigger remediation.Trigger
	Drafts  []ClusterDraft
}

// ClusterDraft is one cluster with the draft it produced and the evidence it
// was made from.
type ClusterDraft struct {
	Cluster  remediation.Cluster
	Draft    remediation.Draft
	Evidence remediation.Evidence
}

// LongCheck is a check that starts work and answers later, such as the gate
// that submits verification runs and polls them. Submit starts the work and
// returns a verdict whose result is waiting and whose Verifications are the
// runs it started, so the runner can record them with the proposal. A Submit
// that refuses the try returns a verdict whose result is not waiting and holds
// the findings, with no run started. Poll reads the answer of a waiting result.
type LongCheck interface {
	Meta() Meta
	Submit(ctx context.Context, in GateInput) (GateVerdict, error)
	Poll(ctx context.Context, in GateInput, waiting remediation.CheckResult) (GateVerdict, error)
}

// GateInput is what a gate reads: the try's checked drafts and the proposal
// they belong to. SubmittedAt is when the gate's Submit committed, which the
// runner fills from the start of the gate step's run; it is zero before that.
// A poll counts how long runs have been unreadable from it, so a poll refuses
// an input without it.
type GateInput struct {
	Remediation remediation.Remediation
	Try         remediation.Try
	Trigger     remediation.Trigger
	Drafts      []ClusterDraft
	ProposalID  string
	SubmittedAt time.Time
}

// GateVerdict is the answer of a submit or a poll. Verifications are the runs
// that judged the drafts, as far as this call read them: a submit lists every
// run it started as queued, and a poll lists the runs whose status it read, so
// a run it could not read keeps the values already recorded. VerifyError is why
// the runs could not be judged, and Phases are the runs seen queued or running
// in this call, for the proposal row.
type GateVerdict struct {
	Result        remediation.CheckResult
	Verifications []proposal.Verification
	VerifyError   string
	Phases        []proposal.Verification
}

// PassOutput hands a verified proposal over, inside the unit of work that
// commits the pass.
type PassOutput interface {
	Meta() Meta
	Pass(ctx context.Context, u uow.UnitOfWork, in OutputInput, v remediation.Verified) error
}

// GiveUpOutput records that a remediation ended without a proposal, inside the
// unit of work that commits the end.
type GiveUpOutput interface {
	Meta() Meta
	GiveUp(ctx context.Context, u uow.UnitOfWork, in OutputInput) error
}

// OutputInput is what an output step reads.
type OutputInput struct {
	Remediation remediation.Remediation
	Try         remediation.Try
	Trigger     remediation.Trigger
	Snapshot    remediation.Snapshot
}

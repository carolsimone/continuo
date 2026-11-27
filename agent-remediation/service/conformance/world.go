package conformance

import (
	"github.com/google/uuid"

	"github.com/carolsimone/continuo/agent-remediation/domain/fix"
	"github.com/carolsimone/continuo/agent-remediation/domain/prompt"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/domain/repository"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/runner"
	"github.com/carolsimone/continuo/agent-remediation/service/uow"
	"github.com/carolsimone/continuo/pkg/outbox"
)

// Script is what one scenario makes the fakes of its world answer, beyond the
// fixture's fixed data. Each answer list is every call it answers, in call
// order, each answer given once: a call past the end of its list fails.
type Script struct {
	// Answers are the model's answers, in call order, for every cluster whose
	// target has no entry in MemberAnswers.
	Answers []ports.ProposeResult
	// MemberAnswers are the model's answers, in call order, for the clusters
	// whose target is the named node.
	MemberAnswers map[string][]ports.ProposeResult
	// FailedTries is how many tries of each round have their release
	// checks fail, with the fixture's gate errors, before they pass: a
	// check of try n of any round fails while n is at most FailedTries.
	FailedTries int
}

// NewWorld builds the world of one scenario: a fresh store, and fakes that
// answer from the fixture as the script says.
type NewWorld func(f Fixture, s Script) (World, error)

// World is the environment one scenario runs in: the runner's store and the
// ports its steps read through, each answering from one fixture, and a
// read-back of everything the run wrote and called.
type World interface {
	// NewUoW opens a unit of work on the world's store.
	NewUoW() uow.UnitOfWork
	// Repository is the remediation repository outside a unit of work, as
	// the worker and the gate poller use it to claim a remediation.
	Repository() repository.RemediationRepository
	// Collaborators are the ports the steps are built from, every one
	// answering from the fixture. The contract locator and inspector and the
	// release check timeout are left for the suite to set.
	Collaborators() runner.Collaborators
	// LLM is the scripted model behind the real response cache.
	LLM() ports.LLMProvider
	// Clock is the world's clock.
	Clock() ports.Clock
	// OnModelCall runs fn after every call that reaches the scripted model,
	// with the number of such calls so far. No unit of work is open then.
	OnModelCall(fn func(calls int))
	// FailNextCommit makes the next commit of any unit of work discard its
	// writes and fail, once.
	FailNextCommit()

	// Remediations is every remediation the store holds.
	Remediations() []remediation.Remediation
	// Tries is every try of the remediation, in try order.
	Tries(id uuid.UUID) []remediation.Try
	// Clusters is every cluster of every try of the remediation, in try and
	// cluster order.
	Clusters(id uuid.UUID) []remediation.Cluster
	// Steps is every journal row of the remediation.
	Steps(id uuid.UUID) []remediation.StepRun
	// FixRows is every fix row the store holds.
	FixRows() []FixRow
	// Outbox is every outbox entry the store holds.
	Outbox() []outbox.Entry
	// ModelCalls is every call that reached the scripted model, past the
	// response cache, in call order.
	ModelCalls() []ModelCall
	// Checks is every release check submitted, in submit order.
	Checks() []ports.ReleaseCheckRequest
}

// ModelCall is one call that reached the scripted model: the node its
// cluster targets, as the call's context names it, and its request.
type ModelCall struct {
	Target  string
	Request prompt.ProposeRequest
}

// FixRow is one fix row as the store holds it, with the error its
// release check reported.
type FixRow struct {
	Fix         fix.Fix
	VerifyError string
}

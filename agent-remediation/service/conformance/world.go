package conformance

import (
	"github.com/google/uuid"

	"github.com/carolsimone/continuo/agent-remediation/domain/prompt"
	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/domain/repository"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/runner"
	"github.com/carolsimone/continuo/agent-remediation/service/uow"
	"github.com/carolsimone/continuo/pkg/outbox"
)

// Script is what one scenario makes the fakes of its world answer, beyond the
// fixture's fixed data.
type Script struct {
	// Answers are the model's answers, in call order, for every cluster whose
	// target has no entry in MemberAnswers.
	Answers []ports.ProposeResult
	// MemberAnswers are the model's answers, in call order, for the clusters
	// whose target is the named node.
	MemberAnswers map[string][]ports.ProposeResult
	// FailedAttempts is how many of the release's attempts have their
	// verification runs fail, with the fixture's gate errors, before runs
	// pass: a run of attempt n fails while n is at most FailedAttempts.
	FailedAttempts int
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
	// verification timeout are left for the suite to set.
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
	// Attempts is every proposal row the store holds.
	Attempts() []Attempt
	// Outbox is every outbox entry the store holds.
	Outbox() []outbox.Entry
	// ModelRequests is every request that reached the scripted model, past
	// the response cache, in call order.
	ModelRequests() []prompt.ProposeRequest
	// Runs is every verification run submitted, in submit order.
	Runs() []ports.VerificationRequest
}

// Attempt is one proposal row as the store holds it, with the error its
// verification run reported.
type Attempt struct {
	Proposal    proposal.Proposal
	VerifyError string
}

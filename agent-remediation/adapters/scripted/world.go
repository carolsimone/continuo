// Package scripted is the world a conformance scenario runs in: every port a
// remediation step reads through, answering from one fixture, the scripted
// model behind the real response cache, a verification pipeline that passes or
// fails as the scenario's script says, and a fixed clock, over a memory store.
package scripted

import (
	"fmt"
	"log/slog"
	"maps"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/carolsimone/continuo/agent-remediation/adapters/memory"
	"github.com/carolsimone/continuo/agent-remediation/adapters/sanitizer"
	"github.com/carolsimone/continuo/agent-remediation/domain/prompt"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/domain/repository"
	"github.com/carolsimone/continuo/agent-remediation/service/conformance"
	"github.com/carolsimone/continuo/agent-remediation/service/llmcache"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/runner"
	"github.com/carolsimone/continuo/agent-remediation/service/uow"
	"github.com/carolsimone/continuo/pkg/outbox"
)

// modelID is the model the response cache keys every answer under.
const modelID = "conformance-model"

// startTime is the time the world's clock reads.
var startTime = time.Date(2026, 11, 13, 9, 0, 0, 0, time.UTC)

// World is one scenario's world over a memory store.
type World struct {
	store    *memory.Store
	collab   runner.Collaborators
	llm      *model
	cached   ports.LLMProvider
	pipeline *pipeline
	clock    fixedClock
}

var _ conformance.World = (*World)(nil)

// Worlds is the conformance world factory: each world runs over a store
// newStore makes.
func Worlds(newStore func() *memory.Store) conformance.NewWorld {
	return func(f conformance.Fixture, s conformance.Script) (conformance.World, error) {
		return NewWorld(newStore(), f, s)
	}
}

// NewWorld builds the world of fixture f over store, with the model and the
// verification pipeline answering as script s says. The repository at the
// trigger's commit holds f's repo files; paths that are not local to a
// checkout are refused, all named, in path order.
func NewWorld(store *memory.Store, f conformance.Fixture, s conformance.Script) (*World, error) {
	var outside []string
	for _, p := range slices.Sorted(maps.Keys(f.RepoFiles)) {
		if !filepath.IsLocal(filepath.FromSlash(p)) {
			outside = append(outside, strconv.Quote(p))
		}
	}
	if len(outside) > 0 {
		return nil, fmt.Errorf("%s: repo files %s are not paths inside the repository", f.File, strings.Join(outside, ", "))
	}
	logger := slog.New(slog.DiscardHandler)
	clock := fixedClock{now: startTime}
	repo := checkout{repo: f.Trigger.Repo, commit: f.Trigger.CommitSHA, files: f.RepoFiles}
	objects := newObjectStore(f.Objects)
	cat := catalog{c: f.Catalog}
	llm := newModel(s.Answers, s.MemberAnswers)
	pipe := newPipeline(s.FailedAttempts, f.GateErrors, clock)
	w := &World{
		store:    store,
		llm:      llm,
		cached:   llmcache.New(llm, newResponseCache(), modelID, logger),
		pipeline: pipe,
		clock:    clock,
	}
	w.collab = runner.Collaborators{
		Source:           repo,
		Evidence:         objects,
		Sanitizer:        sanitizer.Passthrough{},
		Artifacts:        objects,
		Logger:           logger,
		ServiceRepoPaths: f.ServiceRepoPaths,
		Locator:          cat,
		Upstream:         cat,
		Versions:         cat,
		Precedents:       cat,
		CandidateSource:  bundle{nodes: f.Bundle},
		Archive:          repo,
		Packager:         packager{p: f.Packager},
		Releases:         release{r: f.Release},
		PriorAttempts:    store.ProposalRepository(),
		Pipeline:         pipe,
		Clock:            clock,
	}
	return w, nil
}

// NewUoW opens a unit of work on the world's store.
func (w *World) NewUoW() uow.UnitOfWork { return memory.NewUnitOfWork(w.store) }

// Repository is the store's remediation repository outside a unit of work.
func (w *World) Repository() repository.RemediationRepository {
	return w.store.RemediationRepository()
}

// Collaborators are the ports the steps are built from. The contract locator
// and inspector and the verification timeout are left for the caller.
func (w *World) Collaborators() runner.Collaborators { return w.collab }

// LLM is the scripted model behind the real response cache.
func (w *World) LLM() ports.LLMProvider { return w.cached }

// Clock is the world's fixed clock.
func (w *World) Clock() ports.Clock { return w.clock }

// OnModelCall runs fn after every call that reaches the scripted model.
func (w *World) OnModelCall(fn func(calls int)) { w.llm.setOnCall(fn) }

// FailNextCommit makes the store's next commit fail, once.
func (w *World) FailNextCommit() { w.store.FailNextCommit() }

// Remediations is every remediation the store holds.
func (w *World) Remediations() []remediation.Remediation { return w.store.Remediations() }

// Tries is every try of the remediation.
func (w *World) Tries(id uuid.UUID) []remediation.Try { return w.store.Tries(id) }

// Clusters is every cluster of every try of the remediation.
func (w *World) Clusters(id uuid.UUID) []remediation.Cluster { return w.store.Clusters(id) }

// Steps is every journal row of the remediation.
func (w *World) Steps(id uuid.UUID) []remediation.StepRun { return w.store.Steps(id) }

// Attempts is every proposal row the store holds, in attempt order.
func (w *World) Attempts() []conformance.Attempt {
	stored := w.store.Proposals()
	out := make([]conformance.Attempt, 0, len(stored))
	for _, p := range stored {
		out = append(out, conformance.Attempt{Proposal: p.Proposal, VerifyError: p.VerifyError})
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Proposal.Attempt < out[j].Proposal.Attempt })
	return out
}

// Outbox is every outbox entry the store holds.
func (w *World) Outbox() []outbox.Entry { return w.store.OutboxEntries() }

// ModelRequests is every request that reached the scripted model.
func (w *World) ModelRequests() []prompt.ProposeRequest { return w.llm.requests() }

// Runs is every verification run submitted.
func (w *World) Runs() []ports.VerificationRequest { return w.pipeline.runs() }

// fixedClock always reads the same time.
type fixedClock struct{ now time.Time }

var _ ports.Clock = fixedClock{}

func (c fixedClock) Now() time.Time { return c.now }

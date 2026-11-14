// Package memory is the in-memory implementation of the unit of work and of
// every repository the runner, the proposal driver and the verification
// reconciler use. It keeps the semantics of the Postgres adapters that the code
// above them relies on: compare-and-set updates, the (release_id, attempt)
// uniqueness of proposals, message-processing dedup on both axes, foreign keys
// and cascades, and leases. The runner's tests and the repository contract test
// run against it.
package memory

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/google/uuid"

	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/domain/repository"
	"github.com/carolsimone/continuo/agent-remediation/service/uow"
	"github.com/carolsimone/continuo/pkg/messageprocessing"
	"github.com/carolsimone/continuo/pkg/outbox"
)

// errCommitFailed is what a commit armed by FailNextCommit returns.
var errCommitFailed = errors.New("memory store: commit failed")

// Store is the in-memory database. A unit of work holds the store's lock
// from Begin to Commit or Rollback and works on a copy, so its writes are
// invisible to others until Commit and a Rollback leaves no trace. A repository
// used outside a unit of work takes the lock for each call. Two units of work
// opened one after the other by one goroutine are fine; opening a second before
// the first ends blocks.
type Store struct {
	mu             sync.Mutex
	data           *data
	failNextCommit atomic.Bool
}

// NewStore returns an empty store.
func NewStore() *Store {
	return &Store{data: newData()}
}

// FailNextCommit makes the next Commit of any unit of work discard its writes
// and return an error, once.
func (s *Store) FailNextCommit() {
	s.failNextCommit.Store(true)
}

// RemediationRepository returns the remediation repository for use outside a
// unit of work, as the worker, the poller and the sweeper use it.
func (s *Store) RemediationRepository() repository.RemediationRepository {
	return &remediationRepository{gate: s.autocommit}
}

// ProposalRepository returns the proposal repository for use outside a unit of
// work, as the verification reconciler's lister uses it.
func (s *Store) ProposalRepository() repository.ProposalRepository {
	return &proposalRepository{gate: s.autocommit}
}

// autocommit runs fn on the committed data under the store's lock.
func (s *Store) autocommit(fn func(*data) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return fn(s.data)
}

// StoredProposal is a proposal row as stored: the proposal with the id the
// store gave it and the verification error the row recorded.
type StoredProposal struct {
	ID          string
	Proposal    proposal.Proposal
	VerifyError string
}

// read runs fn on the committed data under the store's lock.
func read[T any](s *Store, fn func(*data) T) T {
	s.mu.Lock()
	defer s.mu.Unlock()
	return fn(s.data)
}

// Proposals returns copies of the committed proposal rows, in insertion order.
func (s *Store) Proposals() []StoredProposal {
	return read(s, func(d *data) []StoredProposal {
		out := make([]StoredProposal, 0, len(d.proposals))
		for _, row := range d.proposals {
			out = append(out, StoredProposal{ID: row.id, Proposal: cloneProposal(row.p), VerifyError: row.verifyError})
		}
		return out
	})
}

// OutboxEntries returns copies of the committed outbox rows, in insertion order.
func (s *Store) OutboxEntries() []outbox.Entry {
	return read(s, func(d *data) []outbox.Entry {
		out := make([]outbox.Entry, 0, len(d.outbox))
		for _, e := range d.outbox {
			out = append(out, cloneOutboxEntry(*e))
		}
		return out
	})
}

// ProcessedMessages returns copies of the committed message-processing rows, in
// insertion order.
func (s *Store) ProcessedMessages() []messageprocessing.MessageProcessing {
	return read(s, func(d *data) []messageprocessing.MessageProcessing {
		out := make([]messageprocessing.MessageProcessing, 0, len(d.messages))
		for _, m := range d.messages {
			out = append(out, cloneMessage(*m))
		}
		return out
	})
}

// Remediations returns copies of the committed remediation rows, oldest first.
func (s *Store) Remediations() []remediation.Remediation {
	return read(s, func(d *data) []remediation.Remediation {
		out := make([]remediation.Remediation, 0, len(d.remediations))
		for _, r := range d.sortedRemediations() {
			out = append(out, cloneRemediation(*r))
		}
		return out
	})
}

// Tries returns copies of the committed tries of one remediation, by try.
func (s *Store) Tries(id uuid.UUID) []remediation.Try {
	return read(s, func(d *data) []remediation.Try {
		var out []remediation.Try
		for key, t := range d.tries {
			if key.remediationID == id {
				out = append(out, cloneTry(*t))
			}
		}
		sort.Slice(out, func(i, j int) bool { return out[i].Try < out[j].Try })
		return out
	})
}

// Clusters returns copies of the committed clusters of one remediation, by try
// then cluster.
func (s *Store) Clusters(id uuid.UUID) []remediation.Cluster {
	return read(s, func(d *data) []remediation.Cluster {
		var out []remediation.Cluster
		for key, c := range d.clusters {
			if key.remediationID == id {
				out = append(out, cloneCluster(*c))
			}
		}
		sort.Slice(out, func(i, j int) bool {
			if out[i].Try != out[j].Try {
				return out[i].Try < out[j].Try
			}
			return out[i].Cluster < out[j].Cluster
		})
		return out
	})
}

// Steps returns copies of the committed steps of one remediation across all its
// tries, in id order.
func (s *Store) Steps(id uuid.UUID) []remediation.StepRun {
	return read(s, func(d *data) []remediation.StepRun {
		var out []remediation.StepRun
		for _, st := range d.steps {
			if st.RemediationID == id {
				out = append(out, cloneStep(*st))
			}
		}
		return out
	})
}

// UnitOfWork is the in-memory uow.UnitOfWork.
type UnitOfWork struct {
	store *Store
	work  *data
}

var _ uow.UnitOfWork = (*UnitOfWork)(nil)

// NewUnitOfWork returns a unit of work over store.
func NewUnitOfWork(store *Store) uow.UnitOfWork {
	return &UnitOfWork{store: store}
}

// Begin takes the store's lock and starts working on a copy of its data.
func (u *UnitOfWork) Begin(context.Context) error {
	u.store.mu.Lock()
	u.work = u.store.data.clone()
	return nil
}

// Commit publishes the unit of work's copy as the store's data and releases the
// lock. After FailNextCommit it discards the copy and returns an error instead.
// Like the Postgres unit of work, it does nothing when no transaction is open.
func (u *UnitOfWork) Commit() error {
	if u.work == nil {
		return nil
	}
	defer u.end()
	if u.store.failNextCommit.CompareAndSwap(true, false) {
		return errCommitFailed
	}
	u.store.data = u.work
	return nil
}

// Rollback discards the unit of work's copy and releases the lock. It does
// nothing when no transaction is open.
func (u *UnitOfWork) Rollback() error {
	if u.work == nil {
		return nil
	}
	u.end()
	return nil
}

func (u *UnitOfWork) end() {
	u.work = nil
	u.store.mu.Unlock()
}

// gate runs fn on the unit of work's copy.
func (u *UnitOfWork) gate(fn func(*data) error) error {
	if u.work == nil {
		return fmt.Errorf("memory unit of work: no transaction is open")
	}
	return fn(u.work)
}

// ProposalRepo returns the proposal repository bound to the transaction.
func (u *UnitOfWork) ProposalRepo() repository.ProposalRepository {
	return &proposalRepository{gate: u.gate}
}

// RemediationRepo returns the remediation repository bound to the transaction.
func (u *UnitOfWork) RemediationRepo() repository.RemediationRepository {
	return &remediationRepository{gate: u.gate}
}

// OutboxRepo returns the outbox repository bound to the transaction.
func (u *UnitOfWork) OutboxRepo() outbox.Repository {
	return &outboxRepository{gate: u.gate}
}

// MessageProcessingRepo returns the message-processing repository bound to the
// transaction.
func (u *UnitOfWork) MessageProcessingRepo() messageprocessing.Repository {
	return &messageRepository{gate: u.gate}
}

// gate is how a repository reaches the data it works on.
type gate func(fn func(*data) error) error

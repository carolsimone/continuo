package neo4jinfra

import (
	"context"
	"errors"
	"fmt"

	"github.com/carolsimone/continuo/catalog/domain/casebase"
	"github.com/carolsimone/continuo/catalog/domain/repository"
)

// The repositories in this file are what a uow.Tx returns for a repository
// whose adapter opens its own Neo4j session and so cannot run on the unit of
// work's transaction. Every method fails with errRepositoryNotTransactional
// and touches nothing.

// errRepositoryNotTransactional is the error of every placeholder method.
var errRepositoryNotTransactional = errors.New("neo4j: this repository cannot run on the unit of work's transaction")

// caseBaseTxPlaceholder stands in for the case-base repository on a uow.Tx.
type caseBaseTxPlaceholder struct{}

var _ repository.CaseBaseRepository = caseBaseTxPlaceholder{}

func (caseBaseTxPlaceholder) RecordRejection(context.Context, casebase.Rejection) error {
	return fmt.Errorf("CaseBaseRepository.RecordRejection: %w", errRepositoryNotTransactional)
}

func (caseBaseTxPlaceholder) RecordProposal(context.Context, casebase.Proposal, casebase.PullRequest) error {
	return fmt.Errorf("CaseBaseRepository.RecordProposal: %w", errRepositoryNotTransactional)
}

func (caseBaseTxPlaceholder) RecordPullRequestOutcome(context.Context, casebase.PullRequestOutcome) error {
	return fmt.Errorf("CaseBaseRepository.RecordPullRequestOutcome: %w", errRepositoryNotTransactional)
}

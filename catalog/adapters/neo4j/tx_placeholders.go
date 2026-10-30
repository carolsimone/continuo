package neo4jinfra

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/carolsimone/continuo/catalog/domain/casebase"
	"github.com/carolsimone/continuo/catalog/domain/codeversion"
	"github.com/carolsimone/continuo/catalog/domain/repository"
	"github.com/carolsimone/continuo/catalog/domain/topology"
)

// The repositories in this file are what a uow.Tx returns for a repository
// whose adapter opens its own Neo4j session and so cannot run on the unit of
// work's transaction. Every method fails with errRepositoryNotTransactional
// and touches nothing.

// errRepositoryNotTransactional is the error of every placeholder method.
var errRepositoryNotTransactional = errors.New("neo4j: this repository cannot run on the unit of work's transaction")

// topologyTxPlaceholder stands in for the topology repository on a uow.Tx.
type topologyTxPlaceholder struct{}

var _ repository.ReleasePromotionRepository = topologyTxPlaceholder{}

func (topologyTxPlaceholder) PromoteRelease(
	context.Context, string, int64, []topology.ReleasePromotedTopologyNode, time.Time,
) (topology.PromotionOutcome, error) {
	return "", fmt.Errorf("ReleasePromotionRepository.PromoteRelease: %w", errRepositoryNotTransactional)
}

// codeVersionsTxPlaceholder stands in for the code-version repository on a
// uow.Tx.
type codeVersionsTxPlaceholder struct{}

var _ repository.CodeVersionRepository = codeVersionsTxPlaceholder{}

func (codeVersionsTxPlaceholder) WriteVersions(context.Context, codeversion.WriteInput) (codeversion.WriteResult, error) {
	return codeversion.WriteResult{}, fmt.Errorf("CodeVersionRepository.WriteVersions: %w", errRepositoryNotTransactional)
}

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

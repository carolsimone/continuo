package neo4jinfra

import (
	"context"
	"testing"

	"github.com/carolsimone/continuo/catalog/domain/casebase"
	"github.com/stretchr/testify/assert"
)

// A repository a uow.Tx hands out that cannot run on the open transaction
// fails every call with errRepositoryNotTransactional, never touching Neo4j.
func TestTx_PlaceholderRepositoriesRefuseEveryCall(t *testing.T) {
	ctx := context.Background()
	tx := &transaction{}

	assert.ErrorIs(t, tx.CaseBase().RecordRejection(ctx, casebase.Rejection{}), errRepositoryNotTransactional)
	assert.ErrorIs(t, tx.CaseBase().RecordProposal(ctx, casebase.Proposal{}, casebase.PullRequest{}), errRepositoryNotTransactional)
	assert.ErrorIs(t, tx.CaseBase().RecordPullRequestOutcome(ctx, casebase.PullRequestOutcome{}), errRepositoryNotTransactional)
}

package neo4jinfra_test

import (
	"context"
	"log/slog"
	"os"
	"testing"

	neo4jinfra "github.com/carolsimone/continuo/catalog/adapters/neo4j"
	"github.com/carolsimone/continuo/catalog/domain/casebase"
	"github.com/carolsimone/continuo/catalog/service/uow"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/require"
)

// The precedent query tests read the case base through
// PrecedentQueryRepository, which finds an edited node by its :Table label.
// Their fixtures write the case base with the case-base groups' repository,
// which writes one tenant's nodes and links a fix PR's edit to that tenant's
// :Node, so each rejection and edited node they seed carries
// precedentFixtureTenant, and an edited node carries both labels.

// precedentFixtureTenant is the tenant the precedent tests' case base is
// written under.
const precedentFixtureTenant = "precedent-query-test"

// caseBaseCleanup returns a closure that removes every node a precedent test
// could have touched: the nodes it seeds and writes carry the test's marker
// in test_marker or in an identity value (release_id, node_id, unique_id,
// signature, proposal_id), and the unit of work leaves the tenant's export
// lease node and processed-message markers.
func caseBaseCleanup(t *testing.T, client neo4jinfra.Neo4jClient, marker string) func() {
	t.Helper()
	ctx := context.Background()
	return func() {
		s := client.NewSession(ctx, neo4j.AccessModeWrite)
		defer s.Close(ctx)
		for _, q := range []string{`
			MATCH (n)
			WHERE n.test_marker = $m
			   OR n.release_id CONTAINS $m
			   OR n.node_id CONTAINS $m
			   OR n.unique_id CONTAINS $m
			   OR n.signature CONTAINS $m
			   OR n.proposal_id CONTAINS $m
			DETACH DELETE n`,
			`MATCH (n:Meta {tenant_id: $tenant_id}) DETACH DELETE n`,
			`MATCH (n:ProcessedMessage {tenant_id: $tenant_id}) DETACH DELETE n`,
		} {
			res, err := s.Run(ctx, q, map[string]any{"m": marker, "tenant_id": precedentFixtureTenant})
			require.NoError(t, err)
			_, err = res.Consume(ctx)
			require.NoError(t, err)
		}
	}
}

// seedEditTargetTable creates the node a fix PR's edit links [:EDITED] to;
// the case base never creates one itself.
func seedEditTargetTable(t *testing.T, client neo4jinfra.Neo4jClient, uniqueID, marker string) {
	t.Helper()
	ctx := context.Background()
	s := client.NewSession(ctx, neo4j.AccessModeWrite)
	defer s.Close(ctx)
	res, err := s.Run(ctx, `CREATE (:Node:Table {tenant_id: $tenant_id, unique_id: $uid, test_marker: $m})`,
		map[string]any{"tenant_id": precedentFixtureTenant, "uid": uniqueID, "m": marker})
	require.NoError(t, err)
	_, err = res.Consume(ctx)
	require.NoError(t, err)
}

// caseBaseWriter writes precedentFixtureTenant's case base, each call in one
// write of the unit of work under a fresh message of its group, as the
// case-base groups do.
type caseBaseWriter struct {
	uow uow.UnitOfWork
}

func newCaseBaseRepo(client neo4jinfra.Neo4jClient) *caseBaseWriter {
	return &caseBaseWriter{uow: neo4jinfra.NewUnitOfWork(client,
		slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})))}
}

func (w *caseBaseWriter) write(ctx context.Context, group string, fn func(ctx context.Context, tx uow.Tx) error) error {
	m := uow.Marker{TenantID: precedentFixtureTenant, Group: group, MessageID: uuid.NewString()}
	return w.uow.Write(ctx, m, true, fn)
}

func (w *caseBaseWriter) RecordRejection(ctx context.Context, rej casebase.Rejection) error {
	return w.write(ctx, streams.CatalogRemediationRequestedRejections, func(ctx context.Context, tx uow.Tx) error {
		return tx.CaseBase().RecordRejection(ctx, precedentFixtureTenant, rej)
	})
}

func (w *caseBaseWriter) RecordProposal(ctx context.Context, p casebase.Proposal, pr casebase.PullRequest) error {
	return w.write(ctx, streams.CatalogRemediationPrOpenedProposals, func(ctx context.Context, tx uow.Tx) error {
		return tx.CaseBase().RecordProposal(ctx, precedentFixtureTenant, p, pr)
	})
}

func (w *caseBaseWriter) RecordPullRequestOutcome(ctx context.Context, o casebase.PullRequestOutcome) error {
	return w.write(ctx, streams.CatalogRemediationPrClosedProvenance, func(ctx context.Context, tx uow.Tx) error {
		return tx.CaseBase().RecordPullRequestOutcome(ctx, precedentFixtureTenant, o)
	})
}

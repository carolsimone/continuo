//go:build integration

package postgres

import (
	"context"
	"log/slog"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/domain/event"
	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/proposals"
	"github.com/carolsimone/continuo/agent-remediation/service/uow"
	pkgconfig "github.com/carolsimone/continuo/pkg/config"
	pkgoutbox "github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
)

// The agent-remediation database keeps its outbox in agent_remediation_outbox:
// the table, its constraints, its indexes and the trigger that notifies its
// relay all carry the service's name, and the relay targets the same table
// through OutboxTable.
func TestOutboxTableIsAgentRemediationOutbox(t *testing.T) {
	db := newTestDB(t)

	assert.Equal(t, "agent_remediation_outbox", OutboxTable)

	var exists bool
	require.NoError(t, db.Get(&exists, `SELECT to_regclass('agent_remediation_outbox') IS NOT NULL`))
	assert.True(t, exists, "table agent_remediation_outbox exists")

	// Tables, views, materialized views and partitioned tables all count: a
	// view standing in for another outbox name must fail this test too.
	var outboxRelations []string
	require.NoError(t, db.Select(&outboxRelations, `
		SELECT relname FROM pg_class
		WHERE relkind IN ('r', 'v', 'm', 'p') AND relnamespace = 'public'::regnamespace AND relname LIKE '%\_outbox'
		ORDER BY relname`))
	assert.Equal(t, []string{"agent_remediation_outbox"}, outboxRelations, "the database holds no other outbox")

	var triggers []string
	require.NoError(t, db.Select(&triggers, `
		SELECT tgname FROM pg_trigger
		WHERE tgrelid = to_regclass('agent_remediation_outbox') AND NOT tgisinternal
		ORDER BY tgname`))
	assert.Equal(t, []string{"agent_remediation_outbox_notify"}, triggers, "the outbox carries its notify trigger and no other")

	var indexes []string
	require.NoError(t, db.Select(&indexes, `SELECT indexname FROM pg_indexes WHERE tablename = 'agent_remediation_outbox' ORDER BY indexname`))
	// Every constraint of the table, including the NOT NULL constraints
	// Postgres 18 stores in pg_constraint.
	var constraints []string
	require.NoError(t, db.Select(&constraints, `
		SELECT conname FROM pg_constraint WHERE conrelid = to_regclass('agent_remediation_outbox') ORDER BY conname`))

	for _, name := range slices.Concat(indexes, constraints, triggers) {
		assert.True(t, strings.HasPrefix(name, "agent_remediation_outbox") || strings.HasPrefix(name, "idx_agent_remediation_outbox"),
			"%s does not carry the table's name", name)
	}
	// The indexes the relay's queries rely on.
	for _, want := range []string{
		"idx_agent_remediation_outbox_claimable",
		"idx_agent_remediation_outbox_open_by_aggregate",
		"idx_agent_remediation_outbox_failed",
		"agent_remediation_outbox_pkey",
	} {
		assert.Contains(t, indexes, want)
	}
	assert.Contains(t, constraints, "agent_remediation_outbox_pkey")
	assert.Contains(t, constraints, "agent_remediation_outbox_status_check")
	assert.Contains(t, constraints, "agent_remediation_outbox_message_processing_id_fkey")
}

// Every row the service writes to its outbox carries the aggregate type
// event.AggregateType, so a release's events share one relay lane. The rows
// here come from the PR-lifecycle service on the real unit of work: the
// remediation.pr_opened:v1 and remediation.pr_closed:v1 entries it commits.
func TestNoOutboxRowKeepsTheOldAggregateType(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()
	repo := NewProposalRepository(db, nil)
	svc := proposals.New(proposals.Deps{
		Repo:   repo,
		NewUoW: func() uow.UnitOfWork { return NewUnitOfWork(db, slog.Default(), nil) },
		Clock:  ports.SystemClock{},
	})

	assert.Equal(t, "agent_remediation", event.AggregateType)

	id := seedProposal(t, repo, db, proposal.Proposal{
		Source:         "validation",
		ReleaseID:      "r-outbox-lane",
		NodeID:         "model.p.orders",
		ErrorSignature: "sig",
		Attempt:        1,
		Status:         proposal.StatusProposed,
		SourceResolved: true,
		Repo:           "acme/dbt-repo",
		CommitSHA:      "abc123",
		FilePath:       "models/orders.sql",
		CreatedAt:      time.Now().UTC(),
	})
	_, err := repo.BeginPR(ctx, id, "", "remediation/r-outbox-lane/attempt1", time.Now().UTC())
	require.NoError(t, err)
	require.NoError(t, svc.Record(ctx, proposals.RecordInput{ProposalID: id, PrURL: "https://gh/pr/1", PrNumber: 1, OpenedBy: "dev"}))
	require.NoError(t, svc.RecordOutcome(ctx, id, "", proposal.PROutcomeMerged, time.Now().UTC(), nil, nil))

	var rows int
	require.NoError(t, db.GetContext(ctx, &rows, `SELECT count(*) FROM agent_remediation_outbox`))
	require.Equal(t, 2, rows, "the service committed a pr_opened and a pr_closed entry")

	var strays int
	require.NoError(t, db.GetContext(ctx, &strays,
		`SELECT count(*) FROM agent_remediation_outbox WHERE aggregate_type <> $1`, event.AggregateType))
	assert.Zero(t, strays, "every outbox row carries the aggregate type %q", event.AggregateType)
}

// The outbox relay's waker listens on the channel named after OutboxTable, and
// a row committed through the unit of work wakes it.
func TestOutboxWakerWakesOnAgentRemediationOutbox(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)

	waker, err := pkgoutbox.NewPostgresWaker(ctx, pkgconfig.LoadPostgres(&pkgconfig.Validator{}).DSN(), OutboxTable, slog.Default())
	require.NoError(t, err)
	t.Cleanup(func() { _ = waker.Close() })

	u := NewUnitOfWork(db, slog.Default(), nil)
	require.NoError(t, u.Begin(ctx))
	defer func() { _ = u.Rollback() }()
	require.NoError(t, u.OutboxRepo().Create(ctx, &pkgoutbox.Entry{
		ID:            uuid.New(),
		AggregateType: event.AggregateType,
		AggregateID:   event.AggregateIDForRelease("r-waker"),
		EventType:     event.PROpenedEventType,
		Payload:       []byte(`{}`),
		StreamName:    streams.RemediationPrOpenedV1,
		Status:        "pending",
	}))
	require.NoError(t, u.Commit())

	select {
	case <-waker.Wake():
	case <-time.After(5 * time.Second):
		t.Fatal("the outbox waker was not woken by a row committed to agent_remediation_outbox")
	}
}

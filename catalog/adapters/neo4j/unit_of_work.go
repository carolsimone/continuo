package neo4jinfra

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/carolsimone/continuo/catalog/domain/repository"
	"github.com/carolsimone/continuo/catalog/service/uow"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// unitOfWork implements uow.UnitOfWork on Neo4j: a message's writes and its
// processed-message marker commit in one transaction, behind the tenant's
// export gate.
//
// The gate serialises a tenant's writes. A Write that loses a race for the
// same message waits on the gate until the winner commits, then finds the
// winner's marker and skips fn, so a message's effect lands once.
type unitOfWork struct {
	client Neo4jClient
	logger *slog.Logger
}

var _ uow.UnitOfWork = (*unitOfWork)(nil)

// NewUnitOfWork returns a unit of work that writes to client's graph. It holds
// no per-message state, so one instance serves every consumer.
func NewUnitOfWork(client Neo4jClient, logger *slog.Logger) uow.UnitOfWork {
	return &unitOfWork{client: client, logger: logger}
}

// Processed reads the marker of m in a read transaction. It takes no gate, so
// it answers while an export or a restore holds the tenant's lease.
func (u *unitOfWork) Processed(ctx context.Context, m uow.Marker) (bool, error) {
	if err := validateMarker(m); err != nil {
		return false, err
	}
	session := u.client.NewSession(ctx, neo4j.AccessModeRead)
	defer func() { _ = session.Close(ctx) }()
	recorded, err := neo4j.ExecuteRead(ctx, session, func(tx neo4j.ManagedTransaction) (bool, error) {
		run, err := NewTenantRunner(m.TenantID, tx)
		if err != nil {
			return false, err
		}
		return markerRecorded(ctx, run, m)
	})
	if err != nil {
		return false, fmt.Errorf("read processed-message marker: %w", err)
	}
	return recorded, nil
}

// Write runs fn in one write transaction for m's tenant, in this order: lock
// the export gate (ErrWritesPaused while a lease is held), re-check the
// marker (found: skip fn, return nil), run fn, CREATE the marker when final,
// unlock the gate, commit. Any error rolls the whole transaction back.
func (u *unitOfWork) Write(ctx context.Context, m uow.Marker, final bool, fn func(ctx context.Context, tx uow.Tx) error) error {
	if err := validateMarker(m); err != nil {
		return err
	}
	if fn == nil {
		return errors.New("neo4j: a write needs a function to run")
	}
	return inWriteTx(ctx, u.client, m.TenantID, func(run TenantRunner) (bool, error) {
		held, err := lockExportGate(ctx, run)
		if err != nil {
			return false, err
		}
		if held {
			u.logger.Debug("catalog write paused by the export lease",
				"tenant_id", m.TenantID, "group", m.Group, "message_id", m.MessageID)
			return false, fmt.Errorf("tenant %q: %w", m.TenantID, uow.ErrWritesPaused)
		}
		recorded, err := markerRecorded(ctx, run, m)
		if err != nil {
			return false, fmt.Errorf("re-check processed-message marker: %w", err)
		}
		if recorded {
			u.logger.Debug("message already processed; write skipped",
				"tenant_id", m.TenantID, "group", m.Group, "message_id", m.MessageID)
			return false, nil
		}
		if err := fn(ctx, &transaction{run: run, logger: u.logger}); err != nil {
			return false, err
		}
		if final {
			if err := recordMarker(ctx, run, m); err != nil {
				return false, fmt.Errorf("record processed-message marker: %w", err)
			}
		}
		if err := unlockExportGate(ctx, run); err != nil {
			return false, err
		}
		return true, nil
	})
}

// transaction is the uow.Tx of one Write: its open Neo4j transaction, bound to
// the marker's tenant.
type transaction struct {
	run    TenantRunner
	logger *slog.Logger
}

var _ uow.Tx = (*transaction)(nil)

// Topology returns the topology swap on this transaction.
func (t *transaction) Topology() repository.ReleasePromotionRepository {
	return &releasePromotionRepository{run: t.run, logger: t.logger}
}

// CodeVersions returns the code-version repository on this transaction.
func (t *transaction) CodeVersions() repository.CodeVersionRepository {
	return codeVersionsTxPlaceholder{}
}

// CaseBase returns the case-base repository on this transaction.
func (t *transaction) CaseBase() repository.CaseBaseRepository {
	return caseBaseTxPlaceholder{}
}

// inWriteTx runs body in one explicit write transaction whose statements are
// bound to tenantID. It commits when body returns true and no error, and rolls
// back otherwise. It does not retry: the consumer redelivers a message whose
// write failed.
func inWriteTx(ctx context.Context, client Neo4jClient, tenantID string, body func(run TenantRunner) (commit bool, err error)) error {
	session := client.NewSession(ctx, neo4j.AccessModeWrite)
	defer func() { _ = session.Close(ctx) }()

	tx, err := session.BeginTransaction(ctx)
	if err != nil {
		return fmt.Errorf("begin write transaction: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	run, err := NewTenantRunner(tenantID, tx)
	if err != nil {
		return err
	}
	commit, err := body(run)
	if err != nil || !commit {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit write transaction: %w", err)
	}
	return nil
}

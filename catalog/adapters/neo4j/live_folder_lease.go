package neo4jinfra

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/carolsimone/continuo/catalog/service/ports"
	"github.com/carolsimone/continuo/catalog/service/uow"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// liveFolderNodeKey is the key of the tenant's (:Meta) node that records its
// live contracts folder: the folder's watermark promotion_seq, the publishing
// settings' fingerprint settings_sha256, and the live-folder lease (holder,
// kind, until), kept as the export lease keeps its own on the export node.
//
// The node is not the export gate. A lease statement locks only this node, so
// a rewrite holding the lease pauses no graph write and stops no export, and
// an export holding the tenant's lease does not stop a rewrite from taking
// it. The writes of the watermark and the fingerprint change the graph, so
// they run behind the export gate: transaction B's through the unit of work,
// RecordSettings by taking the gate itself.
const liveFolderNodeKey = "contracts-live"

// liveFolderLeaseKind is the kind a live-folder lease records.
const liveFolderLeaseKind = "live-folder"

// recordLiveFolderCypher moves the tenant's live-folder watermark to the
// larger of its value and the promotion's seq and stores the settings
// fingerprint, creating the node on the first rewrite.
const recordLiveFolderCypher = `
MERGE (m:Meta {tenant_id: $tenant_id, key: $lease_key})
SET m.promotion_seq = CASE
      WHEN m.promotion_seq IS NULL OR m.promotion_seq < $promotion_seq THEN $promotion_seq
      ELSE m.promotion_seq END,
    m.settings_sha256 = $settings_sha256`

// recordLiveSettingsCypher stores the settings fingerprint and leaves the
// watermark as it is.
const recordLiveSettingsCypher = `
MERGE (m:Meta {tenant_id: $tenant_id, key: $lease_key})
SET m.settings_sha256 = $settings_sha256`

// readLiveFolderCypher reads the tenant's live-folder watermark and settings
// fingerprint. A tenant with no live-folder node returns no row.
const readLiveFolderCypher = `
MATCH (m:Meta {tenant_id: $tenant_id, key: $lease_key})
RETURN coalesce(m.promotion_seq, 0) AS promotion_seq, coalesce(m.settings_sha256, '') AS settings_sha256`

// RecordLiveFolder moves the tenant's live-folder watermark and stores the
// settings fingerprint on the open transaction. It refuses a tenantID other
// than the transaction's.
func (r *contractVersionRepository) RecordLiveFolder(ctx context.Context, tenantID string, promotionSeq int64, settingsSHA256 string) error {
	if err := r.run.requireTenant(tenantID); err != nil {
		return err
	}
	if err := execCypher(ctx, r.run, recordLiveFolderCypher, leaseNodeParams(liveFolderNodeKey, map[string]any{
		"promotion_seq": promotionSeq, "settings_sha256": settingsSHA256,
	})); err != nil {
		return fmt.Errorf("record the live contracts folder: %w", err)
	}
	return nil
}

// LiveFolderLease implements ports.LiveFolderLease and ports.LiveFolderState
// on each tenant's live-folder node.
type LiveFolderLease struct {
	client Neo4jClient
	logger *slog.Logger
}

var (
	_ ports.LiveFolderLease = (*LiveFolderLease)(nil)
	_ ports.LiveFolderState = (*LiveFolderLease)(nil)
)

// NewLiveFolderLease returns the live-folder lease and state kept in client's
// graph.
func NewLiveFolderLease(client Neo4jClient, logger *slog.Logger) *LiveFolderLease {
	return &LiveFolderLease{client: client, logger: logger}
}

// Acquire takes the tenant's live-folder lease for holder. A live lease,
// whoever holds it, is refused (acquired false); an expired one is taken
// over.
func (l *LiveFolderLease) Acquire(ctx context.Context, tenantID, holder string, ttl time.Duration) (bool, error) {
	ttlMs, err := leaseRequest(tenantID, holder, ttl)
	if err != nil {
		return false, err
	}
	acquired := false
	err = inWriteTx(ctx, l.client, tenantID, func(run TenantRunner) (bool, error) {
		gate, err := lockLeaseGate(ctx, run, liveFolderNodeKey)
		if err != nil {
			return false, fmt.Errorf("lock the live-folder lease: %w", err)
		}
		if gate.held {
			// Rolled back: the lock property never persists.
			return false, nil
		}
		if err := execCypher(ctx, run, leaseTakeCypher, leaseNodeParams(liveFolderNodeKey, map[string]any{
			"holder": holder, "kind": liveFolderLeaseKind, "ttl_ms": ttlMs,
		})); err != nil {
			return false, fmt.Errorf("take the live-folder lease: %w", err)
		}
		acquired = true
		return true, nil
	})
	if err != nil {
		return false, err
	}
	if acquired {
		l.logger.Debug("live-folder lease acquired", "tenant_id", tenantID, "holder", holder, "ttl", ttl.String())
	}
	return acquired, nil
}

// Renew extends holder's live lease to ttl from now. A lease another holder
// owns, an expired lease and a tenant with no lease are ports.ErrLeaseLost.
func (l *LiveFolderLease) Renew(ctx context.Context, tenantID, holder string, ttl time.Duration) error {
	ttlMs, err := leaseRequest(tenantID, holder, ttl)
	if err != nil {
		return err
	}
	return inWriteTx(ctx, l.client, tenantID, func(run TenantRunner) (bool, error) {
		owner, live, found, err := lockLeaseNode(ctx, run, liveFolderNodeKey)
		if err != nil {
			return false, err
		}
		if !found || !live || owner != holder {
			return false, fmt.Errorf("tenant %q, holder %q: live folder: %w", tenantID, holder, ports.ErrLeaseLost)
		}
		if err := execCypher(ctx, run, leaseExtendCypher, leaseNodeParams(liveFolderNodeKey, map[string]any{"ttl_ms": ttlMs})); err != nil {
			return false, fmt.Errorf("renew the live-folder lease: %w", err)
		}
		return true, nil
	})
}

// Release ends holder's lease, live or expired. A lease another holder owns,
// and a tenant with no lease, are left as they are.
func (l *LiveFolderLease) Release(ctx context.Context, tenantID, holder string) error {
	if err := leaseParties(tenantID, holder); err != nil {
		return err
	}
	return inWriteTx(ctx, l.client, tenantID, func(run TenantRunner) (bool, error) {
		owner, _, found, err := lockLeaseNode(ctx, run, liveFolderNodeKey)
		if err != nil {
			return false, err
		}
		if !found || owner != holder {
			return false, nil
		}
		if err := execCypher(ctx, run, leaseClearCypher, leaseNodeParams(liveFolderNodeKey, nil)); err != nil {
			return false, fmt.Errorf("release the live-folder lease: %w", err)
		}
		return true, nil
	})
}

// Read returns the tenant's live-folder watermark and settings fingerprint,
// 0 and "" when the tenant has no live-folder node.
func (l *LiveFolderLease) Read(ctx context.Context, tenantID string) (int64, string, error) {
	type state struct {
		seq      int64
		settings string
	}
	session := l.client.NewSession(ctx, neo4j.AccessModeRead)
	defer func() { _ = session.Close(ctx) }()
	got, err := neo4j.ExecuteRead(ctx, session, func(tx neo4j.ManagedTransaction) (state, error) {
		run, err := NewTenantRunner(tenantID, tx)
		if err != nil {
			return state{}, err
		}
		res, err := run.Run(ctx, readLiveFolderCypher, leaseNodeParams(liveFolderNodeKey, nil))
		if err != nil {
			return state{}, err
		}
		recs, err := res.Collect(ctx)
		if err != nil || len(recs) == 0 {
			return state{}, err
		}
		settings, _ := recordString(recs[0], "settings_sha256")
		return state{seq: toInt64(recordValue(recs[0], "promotion_seq")), settings: settings}, nil
	})
	if err != nil {
		return 0, "", fmt.Errorf("read the live contracts folder's state: %w", err)
	}
	return got.seq, got.settings, nil
}

// RecordSettings stores the settings fingerprint behind the tenant's export
// gate, as every graph write does: it returns uow.ErrWritesPaused, writing
// nothing, while an export or a restore holds the tenant's lease.
func (l *LiveFolderLease) RecordSettings(ctx context.Context, tenantID, settingsSHA256 string) error {
	return inWriteTx(ctx, l.client, tenantID, func(run TenantRunner) (bool, error) {
		gate, err := lockExportGate(ctx, run)
		if err != nil {
			return false, err
		}
		if gate.held {
			return false, fmt.Errorf("tenant %q: %w", tenantID, uow.ErrWritesPaused)
		}
		if err := execCypher(ctx, run, recordLiveSettingsCypher, leaseNodeParams(liveFolderNodeKey, map[string]any{
			"settings_sha256": settingsSHA256,
		})); err != nil {
			return false, fmt.Errorf("record the live folder's settings: %w", err)
		}
		if err := unlockExportGate(ctx, run); err != nil {
			return false, err
		}
		return true, nil
	})
}

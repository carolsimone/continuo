package repository

import (
	"context"
	"time"

	"github.com/carolsimone/continuo/state/domain/aggregate/run"
	"github.com/carolsimone/continuo/state/domain/topology"
	"github.com/google/uuid"
)

// LeaseWaiter is a pending task waiting for the write lease on its node: the
// tenant, the task, its run, the node, and when it began to wait.
type LeaseWaiter struct {
	TenantID     string
	TaskID       uuid.UUID
	RunID        uuid.UUID
	NodeKey      topology.NodeKey
	WaitingSince time.Time
}

// RepairedLease is a write lease deleted because the task that held it had
// ended without releasing it: the tenant, the holder, its run, the node, the
// status the holder ended in and when the lease was taken.
type RepairedLease struct {
	TenantID   string
	TaskID     uuid.UUID
	RunID      uuid.UUID
	NodeKey    topology.NodeKey
	TaskStatus run.TaskStatus
	AcquiredAt time.Time
}

// LeaseRepository answers, across every tenant, which waiting tasks a free
// node's write lease can be granted to, which leases outlived the task that
// held them (and deletes them), and how many tasks wait for a lease. Each
// tenant's node is its own: a waiter queues, and a lease is held, only among
// the rows of its own tenant, and every waiter and lease it returns names its
// tenant.
type LeaseRepository interface {
	// Grantable returns, for each node of each tenant that no task of the
	// tenant holds, its oldest waiter (the earliest lease_wait_since, then the
	// lowest task id) among the tenant's waiters whose run is running, up to
	// limit of them, the longest waiting first.
	Grantable(ctx context.Context, limit int) ([]LeaseWaiter, error)

	// RepairEnded deletes each lease, of any tenant, whose holder task has
	// ended (succeeded, failed, cancelled or skipped) and returns them.
	RepairEnded(ctx context.Context) ([]RepairedLease, error)

	// CountWaiters returns how many tasks of every tenant wait for a node's
	// write lease, whatever their run's status.
	CountWaiters(ctx context.Context) (int, error)
}

package model

import (
	"crypto/subtle"
	"errors"
	"time"

	pkgmodel "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/google/uuid"
)

// ErrStaleLease fences a caller whose lease no longer holds the task: the lease
// ID does not match, the token does not match, or the task carries no lease at
// all. A superseded worker reporting on work that was reassigned gets this.
var ErrStaleLease = errors.New("stale lease")

// Reservation records a Deployment's hold on one of the executor's shared
// execution slots. Jobs-mode and workers-mode work draw on the same pool, so a
// reserved-and-not-released row counts against MAX_CONCURRENT_EXECUTIONS
// whichever path created it.
type Reservation struct {
	ReservedAt *time.Time
	ReleasedAt *time.Time
}

// held reports whether the reservation is currently counted against capacity.
func (r Reservation) held() bool {
	return r.ReservedAt != nil && r.ReleasedAt == nil
}

// Lease is one worker's exclusive, expiring hold on a task.
//
// TokenSHA256 is the SHA-256 digest of the raw lease token; the raw token is
// returned to its worker exactly once at claim time and is never stored here or
// in the database. Every lease-bearing transition compares digests in constant
// time, so a rejected token leaks no information about the real one.
type Lease struct {
	ID          uuid.UUID
	TokenSHA256 string
	Owner       string
	PodName     string
	PodUID      string
	Attempt     int
	ExpiresAt   time.Time
	HeartbeatAt time.Time
	StartedAt   *time.Time
	FinishedAt  *time.Time
}

// authorizes reports whether leaseID and tokenSHA256 identify this lease.
func (l *Lease) authorizes(leaseID uuid.UUID, tokenSHA256 string) bool {
	if l == nil || l.ID != leaseID {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(l.TokenSHA256), []byte(tokenSHA256)) == 1
}

// ActiveLease identifies a live worker lease and the pod holding it, so a
// cancellation or expiry can terminate exactly that pod.
type ActiveLease struct {
	DeploymentID uuid.UUID
	LeaseID      uuid.UUID
	PodName      string
	PodUID       string
}

// PoolDemand is the backlog and in-flight load of one worker pool, used to size
// the pool's replicas.
type PoolDemand struct {
	PoolKey         string
	ServiceName     string
	ImageTag        string
	RuntimeManifest pkgmodel.RuntimeManifestRef
	Pending         int
	ActiveLeases    int
	OldestReadyAt   time.Time
}

// WorkerResult is a worker's terminal report for a claimed task. It is stored
// verbatim as the deployment's terminal_result for audit.
type WorkerResult struct {
	Succeeded              bool    `json:"succeeded"`
	Retryable              bool    `json:"retryable"`
	ErrorClass             string  `json:"error_class,omitempty"`
	ErrorMessage           string  `json:"error_message,omitempty"`
	ExecutionSeconds       float64 `json:"execution_seconds"`
	ReadyToDBTStartSeconds float64 `json:"ready_to_dbt_start_seconds,omitempty"`
	UploadSeconds          float64 `json:"upload_seconds,omitempty"`
	CacheStatus            string  `json:"cache_status,omitempty"`
	LogS3URI               string  `json:"log_s3_uri,omitempty"`
	RunResultsS3URI        string  `json:"run_results_s3_uri,omitempty"`
	UnsafeRuntime          bool    `json:"unsafe_runtime,omitempty"`
}

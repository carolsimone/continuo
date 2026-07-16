package http

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path"
	"strings"
	"time"

	"github.com/carolsimone/continuo/executor-controller/domain/model"
	"github.com/carolsimone/continuo/executor-controller/service/lease"
	"github.com/carolsimone/continuo/executor-controller/service/workerapi"
	"github.com/google/uuid"
)

// claimPollInterval is how often a waiting claim re-asks for work. A worker
// holds one connection for the whole wait rather than polling the executor.
const claimPollInterval = 500 * time.Millisecond

// runtimeDescriptorName is the descriptor that sits beside every runtime
// artifact, naming what the artifact is and what produced it.
const runtimeDescriptorName = "runtime-manifest.json"

// Result object names. A task's log and run results always land under the same
// lease-scoped prefix, so the executor can name them without asking the worker.
const (
	resultLogName        = "dbt.log"
	resultRunResultsName = "run_results.json"
)

// handleRuntime hands a worker signed reads of the artifact its pool executes
// against. A worker holds no object-store credential of its own.
func (s *Server) handleRuntime(w http.ResponseWriter, r *http.Request, pool *model.WorkerPool) {
	artifactURI := pool.RuntimeManifest.RuntimeManifestURI
	descriptorURI, err := descriptorURIFor(artifactURI)
	if err != nil {
		s.logger.Error("worker pool names no runtime artifact",
			"pool_key", pool.PoolKey, "error", err)
		s.writeError(w, r, http.StatusInternalServerError, "internal",
			"the pool's runtime artifact is not resolvable")
		return
	}

	descriptorURL, err := s.signer.PresignGet(r.Context(), descriptorURI, s.urlTTL)
	if err != nil {
		s.internal(w, r, "sign runtime descriptor", pool.PoolKey, err)
		return
	}
	artifactURL, err := s.signer.PresignGet(r.Context(), artifactURI, s.urlTTL)
	if err != nil {
		s.internal(w, r, "sign runtime artifact", pool.PoolKey, err)
		return
	}

	s.writeJSON(w, r, http.StatusOK, runtimeResponse{
		DescriptorURL: descriptorURL,
		ArtifactURL:   artifactURL,
	})
}

// descriptorURIFor names the descriptor beside an artifact, replacing the
// artifact's own basename.
func descriptorURIFor(artifactURI string) (string, error) {
	if !strings.HasPrefix(artifactURI, "s3://") {
		return "", fmt.Errorf("runtime artifact URI must be s3://bucket/key")
	}
	dir, base := path.Split(artifactURI)
	if base == "" || dir == "s3://" {
		return "", fmt.Errorf("runtime artifact URI must be s3://bucket/key")
	}
	return dir + runtimeDescriptorName, nil
}

// handleClaim gives the asking worker one task from its own pool, waiting up to
// the shorter of what the worker asked for and what the executor allows. A pool
// that cannot run anything is told so rather than handed work.
func (s *Server) handleClaim(w http.ResponseWriter, r *http.Request, pool *model.WorkerPool) {
	var req claimRequest
	if !s.decode(w, r, &req) {
		return
	}
	if !pool.Ready() {
		s.writeError(w, r, http.StatusConflict, "pool_not_ready",
			"the pool has not hydrated its runtime artifact")
		return
	}

	in := lease.ClaimInput{
		PoolKey: pool.PoolKey,
		Owner:   req.Owner,
		PodName: req.PodName,
		PodUID:  req.PodUID,
	}
	grant, err := s.claimUntil(r.Context(), in, s.claimDeadline(req.WaitSeconds))
	if err != nil {
		s.internal(w, r, "claim a task", pool.PoolKey, err)
		return
	}
	if grant == nil {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	s.writeJSON(w, r, http.StatusOK,
		newLeaseResponse(grant, grant.ExpiresAt.Format(time.RFC3339Nano)))
}

// claimDeadline decides how long this claim may wait for work.
//
// A wait of zero or less is a worker asking not to wait: it is answered by one
// look for work and whatever that finds. Anything longer is capped at the
// executor's own ceiling, so no request holds a connection longer than the
// executor allows — including a wait so large it does not fit a duration, which
// is treated as asking for more than the ceiling rather than wrapping into a
// deadline that has already passed.
func (s *Server) claimDeadline(waitSeconds int) time.Time {
	now := time.Now()
	if waitSeconds <= 0 {
		return now
	}
	wait := s.claimWait
	if asked := time.Duration(waitSeconds) * time.Second; asked > 0 && asked < wait {
		wait = asked
	}
	return now.Add(wait)
}

// claimUntil asks for work immediately and then keeps asking until the deadline
// or until the worker gives up. It returns no grant rather than an error when
// the pool simply has nothing due.
func (s *Server) claimUntil(
	ctx context.Context, in lease.ClaimInput, deadline time.Time,
) (*lease.Grant, error) {
	for {
		grant, err := s.leases.Claim(ctx, in)
		if err != nil || grant != nil {
			return grant, err
		}
		if !time.Now().Before(deadline) {
			return nil, nil
		}
		select {
		case <-ctx.Done():
			// The worker hung up or gave up; it is not owed an answer.
			return nil, nil
		case <-time.After(claimPollInterval):
		}
	}
}

// handleStart records that a lease holder's dbt process is running.
func (s *Server) handleStart(w http.ResponseWriter, r *http.Request, pool *model.WorkerPool) {
	leaseID, req, ok := s.leaseRequest(w, r)
	if !ok {
		return
	}
	depID, ok := s.deploymentID(w, r, req.DeploymentID)
	if !ok {
		return
	}
	err := s.leases.Start(r.Context(), lease.StartInput{
		PoolKey: pool.PoolKey, DeploymentID: depID, LeaseID: leaseID, Token: leaseToken(r),
	})
	s.writeLeaseOutcome(w, r, pool, "start a task", err)
}

// handleHeartbeat extends a lease its holder still wants.
func (s *Server) handleHeartbeat(w http.ResponseWriter, r *http.Request, pool *model.WorkerPool) {
	leaseID, req, ok := s.leaseRequest(w, r)
	if !ok {
		return
	}
	depID, ok := s.deploymentID(w, r, req.DeploymentID)
	if !ok {
		return
	}
	err := s.leases.Heartbeat(r.Context(), lease.HeartbeatInput{
		PoolKey: pool.PoolKey, DeploymentID: depID, LeaseID: leaseID, Token: leaseToken(r),
	})
	s.writeLeaseOutcome(w, r, pool, "heartbeat a task", err)
}

// handleResultURLs hands a lease holder the two uploads its task may make.
//
// The locations are derived from the task's own row, never from the request: a
// worker that could name its result keys could mint a capability to write into
// another task's prefix.
func (s *Server) handleResultURLs(w http.ResponseWriter, r *http.Request, pool *model.WorkerPool) {
	leaseID, req, ok := s.leaseRequest(w, r)
	if !ok {
		return
	}
	depID, ok := s.deploymentID(w, r, req.DeploymentID)
	if !ok {
		return
	}

	ref, err := s.leases.Task(r.Context(), lease.TaskInput{
		PoolKey: pool.PoolKey, DeploymentID: depID, LeaseID: leaseID, Token: leaseToken(r),
	})
	if err != nil {
		s.writeLeaseOutcome(w, r, pool, "read a task", err)
		return
	}

	log, err := s.signPut(r.Context(), s.resultURI(ref, leaseID, resultLogName), "text/plain")
	if err != nil {
		s.internal(w, r, "sign a result upload", pool.PoolKey, err)
		return
	}
	runResults, err := s.signPut(r.Context(), s.resultURI(ref, leaseID, resultRunResultsName), "application/json")
	if err != nil {
		s.internal(w, r, "sign a result upload", pool.PoolKey, err)
		return
	}

	s.writeJSON(w, r, http.StatusOK, resultURLsResponse{Log: log, RunResults: runResults})
}

// signPut mints one write capability and pairs it with the location it writes.
func (s *Server) signPut(ctx context.Context, uri, contentType string) (signedObject, error) {
	url, err := s.signer.PresignPut(ctx, uri, contentType, s.urlTTL)
	if err != nil {
		return signedObject{}, err
	}
	return signedObject{URL: url, S3URI: uri}, nil
}

// resultURI names one of a lease's result objects. Every identifier comes from
// the task's row and the lease in the path, so the same lease always resolves to
// the same two objects and no other lease can resolve to them.
func (s *Server) resultURI(ref lease.TaskRef, leaseID uuid.UUID, name string) string {
	return fmt.Sprintf("s3://%s/dbt-runs/%s/%s/%s/%s",
		s.bucket, ref.Command.ScheduleID, ref.Command.TaskID, leaseID, name)
}

// handleComplete applies a lease holder's terminal report.
//
// The report's artifact locations are checked against the ones this lease was
// issued: a task records only evidence the executor itself handed out.
func (s *Server) handleComplete(w http.ResponseWriter, r *http.Request, pool *model.WorkerPool) {
	leaseID, ok := s.leaseID(w, r)
	if !ok {
		return
	}
	var req completeRequest
	if !s.decode(w, r, &req) {
		return
	}
	depID, ok := s.deploymentID(w, r, req.DeploymentID)
	if !ok {
		return
	}

	ref, err := s.leases.Task(r.Context(), lease.TaskInput{
		PoolKey: pool.PoolKey, DeploymentID: depID, LeaseID: leaseID, Token: leaseToken(r),
	})
	if err != nil {
		s.writeLeaseOutcome(w, r, pool, "read a task", err)
		return
	}
	if err := s.checkIssuedURIs(ref, leaseID, req.Result); err != nil {
		s.writeError(w, r, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}

	err = s.leases.Complete(r.Context(), lease.CompleteInput{
		PoolKey: pool.PoolKey, DeploymentID: depID, LeaseID: leaseID,
		Token: leaseToken(r), Result: req.Result,
	})
	s.writeLeaseOutcome(w, r, pool, "complete a task", err)
}

// checkIssuedURIs accepts only the exact locations this lease was issued. A
// worker that uploaded nothing reports nothing, which is how a failure before
// upload is reported.
func (s *Server) checkIssuedURIs(ref lease.TaskRef, leaseID uuid.UUID, result model.WorkerResult) error {
	issued := map[string]string{
		"log_s3_uri":         s.resultURI(ref, leaseID, resultLogName),
		"run_results_s3_uri": s.resultURI(ref, leaseID, resultRunResultsName),
	}
	reported := map[string]string{
		"log_s3_uri":         result.LogS3URI,
		"run_results_s3_uri": result.RunResultsS3URI,
	}
	for field, got := range reported {
		if got != "" && got != issued[field] {
			return fmt.Errorf("%s is not a location issued for this lease", field)
		}
	}
	return nil
}

// handleInitialization records what a worker reports about hydrating its
// artifact. The pool it speaks for is the one it authenticated as.
func (s *Server) handleInitialization(w http.ResponseWriter, r *http.Request, pool *model.WorkerPool) {
	var req initializationRequest
	if !s.decode(w, r, &req) {
		return
	}
	err := s.pools.RecordInitialization(r.Context(), workerapi.InitializationReport{
		PoolKey:          pool.PoolKey,
		OK:               req.OK,
		ErrorCode:        req.ErrorCode,
		Message:          req.Message,
		HydrationSeconds: req.HydrationSeconds,
	})
	if err != nil {
		s.internal(w, r, "record a pool initialization report", pool.PoolKey, err)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// ------------------------------------------------------------------ plumbing

// leaseToken reads the lease token a worker presents. It travels in its own
// header so it is never confused with the pool credential, and it is passed
// straight to the lease service without being logged or stored.
func leaseToken(r *http.Request) string {
	return r.Header.Get(leaseTokenHeader)
}

// leaseRequest reads the lease from the path and the body every lease-scoped
// report shares.
func (s *Server) leaseRequest(w http.ResponseWriter, r *http.Request) (uuid.UUID, leaseRequest, bool) {
	leaseID, ok := s.leaseID(w, r)
	if !ok {
		return uuid.Nil, leaseRequest{}, false
	}
	var req leaseRequest
	if !s.decode(w, r, &req) {
		return uuid.Nil, leaseRequest{}, false
	}
	return leaseID, req, true
}

// leaseID reads the lease a request is about from its path.
func (s *Server) leaseID(w http.ResponseWriter, r *http.Request) (uuid.UUID, bool) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		s.writeError(w, r, http.StatusBadRequest, "invalid_request", "lease id must be a uuid")
		return uuid.Nil, false
	}
	return id, true
}

// deploymentID reads the row a report names.
func (s *Server) deploymentID(w http.ResponseWriter, r *http.Request, raw string) (uuid.UUID, bool) {
	id, err := uuid.Parse(raw)
	if err != nil {
		s.writeError(w, r, http.StatusBadRequest, "invalid_request", "deployment_id must be a uuid")
		return uuid.Nil, false
	}
	return id, true
}

// decode reads a JSON body, answering a malformed one with a stable code rather
// than a parser's prose. An empty body leaves every field at its default, which
// is how a worker asks for work without saying anything about itself.
//
// Fields the request carries that this API does not define are ignored. The
// executor never takes an identifier from a worker: it reads them from the
// task's own row, so a body naming a schedule or a pool changes nothing.
func (s *Server) decode(w http.ResponseWriter, r *http.Request, into any) bool {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxRequestBytes))
	if err != nil {
		s.writeError(w, r, http.StatusBadRequest, "invalid_request", "the request body could not be read")
		return false
	}
	if len(bytes.TrimSpace(body)) == 0 {
		return true
	}
	if err := json.Unmarshal(body, into); err != nil {
		s.writeError(w, r, http.StatusBadRequest, "invalid_request", "the request body is not valid JSON")
		return false
	}
	return true
}

// writeLeaseOutcome answers a lease-scoped report.
//
// Every rejection here is settled, not transient. A stale lease, a task that
// belongs to another pool, and a cancelled task are all final for the report
// that earned them: the worker is told to stop, never to try again. Answering
// any of them 5xx would make a superseded worker retry against the fence
// forever.
//
// A report naming a row that does not exist is answered as a stale lease, both
// because it is equally final and because a distinct answer would tell a caller
// which deployments exist.
func (s *Server) writeLeaseOutcome(
	w http.ResponseWriter, r *http.Request, pool *model.WorkerPool, action string, err error,
) {
	switch {
	case err == nil:
		w.WriteHeader(http.StatusOK)
	case errors.Is(err, model.ErrStaleLease), errors.Is(err, lease.ErrNoSuchDeployment):
		s.writeError(w, r, http.StatusConflict, "stale_lease", "lease token is no longer current")
	case errors.Is(err, lease.ErrPoolMismatch):
		s.writeError(w, r, http.StatusForbidden, "pool_mismatch", "the task belongs to another pool")
	case errors.Is(err, lease.ErrCancelled):
		s.writeError(w, r, http.StatusGone, "cancelled", "the task was cancelled")
	default:
		s.internal(w, r, action, pool.PoolKey, err)
	}
}

// internal reports a fault of the executor's own. The cause is logged for an
// operator and withheld from the caller, which can only retry.
func (s *Server) internal(w http.ResponseWriter, r *http.Request, action, poolKey string, err error) {
	s.logger.Error("worker request failed",
		"action", action, "pool_key", poolKey, "path", r.URL.Path, "error", err)
	s.writeError(w, r, http.StatusInternalServerError, "internal", "the request could not be completed")
}

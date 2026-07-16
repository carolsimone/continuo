package http_test

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"testing"
	"time"

	"github.com/carolsimone/continuo/executor-controller/domain/model"
	"github.com/carolsimone/continuo/executor-controller/service/lease"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	leasePath   = "/internal/v1/leases/44444444-4444-4444-4444-444444444444"
	depBody     = `{"deployment_id":"33333333-3333-3333-3333-333333333333"}`
	expectedLog = "s3://continuo/dbt-runs/22222222-2222-2222-2222-222222222222/" +
		"11111111-1111-1111-1111-111111111111/44444444-4444-4444-4444-444444444444/dbt.log"
	expectedRunResults = "s3://continuo/dbt-runs/22222222-2222-2222-2222-222222222222/" +
		"11111111-1111-1111-1111-111111111111/44444444-4444-4444-4444-444444444444/run_results.json"
)

func decodeJSON(t *testing.T, resp response, into any) {
	t.Helper()
	require.NoError(t, json.Unmarshal(resp.Body, into))
}

// ---------------------------------------------------------------- runtime

// TestRuntime_ReturnsBothArtifactCapabilities. A worker holds no object-store
// credential: the descriptor and the artifact reach it only as signed reads.
func TestRuntime_ReturnsBothArtifactCapabilities(t *testing.T) {
	r := newRig(t)

	resp := r.do(http.MethodGet, "/internal/v1/worker/runtime", "")
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var body struct {
		DescriptorURL string `json:"descriptor_url"`
		ArtifactURL   string `json:"artifact_url"`
	}
	decodeJSON(t, resp, &body)

	assert.NotEmpty(t, body.DescriptorURL)
	assert.NotEmpty(t, body.ArtifactURL)
	// The descriptor sits beside the artifact it describes.
	assert.Equal(t, []string{
		"s3://continuo/artifacts/finance/runtime-manifest.json",
		"s3://continuo/artifacts/finance/manifest.msgpack",
	}, r.signer.gets)
	// Reads only: a runtime URL never authorizes a write.
	assert.Empty(t, r.signer.puts)
}

// TestRuntime_RejectsAPoolWithNoArtifact stops a half-registered pool from
// signing a URL for an object that was never named.
func TestRuntime_RejectsAPoolWithNoArtifact(t *testing.T) {
	r := newRig(t)
	r.auth.pool.RuntimeManifest.RuntimeManifestURI = ""

	resp := r.do(http.MethodGet, "/internal/v1/worker/runtime", "")

	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
	assert.Empty(t, r.signer.gets)
}

func TestRuntime_SignerFailureIsReported(t *testing.T) {
	r := newRig(t)
	r.signer.err = errBoom

	resp := r.do(http.MethodGet, "/internal/v1/worker/runtime", "")

	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
	assert.Equal(t, "internal", decodeError(t, resp).Error.Code)
}

// ---------------------------------------------------------------- claim

func TestClaim_ReturnsTheGrantedLease(t *testing.T) {
	r := newRig(t)
	g := r.grant()

	resp := r.do(http.MethodPost, "/internal/v1/workers/claim",
		`{"wait_seconds":0,"owner":"worker-1","pod_name":"pod-a","pod_uid":"uid-a"}`)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var body struct {
		DeploymentID  string   `json:"deployment_id"`
		LeaseID       string   `json:"lease_id"`
		LeaseToken    string   `json:"lease_token"`
		Attempt       int      `json:"attempt"`
		ExpiresAt     string   `json:"expires_at"`
		ExecutionPath string   `json:"execution_path"`
		Argv          []string `json:"argv"`
		Task          struct {
			TaskID      string `json:"task_id"`
			ScheduleID  string `json:"schedule_id"`
			ServiceName string `json:"service_name"`
			SchemaName  string `json:"schema_name"`
			TableName   string `json:"table_name"`
			DBTUniqueID string `json:"dbt_unique_id"`
		} `json:"task"`
	}
	decodeJSON(t, resp, &body)

	assert.Equal(t, g.DeploymentID.String(), body.DeploymentID)
	assert.Equal(t, g.LeaseID.String(), body.LeaseID)
	assert.Equal(t, g.Token, body.LeaseToken)
	assert.Equal(t, 1, body.Attempt)
	assert.Equal(t, "native", body.ExecutionPath)
	assert.Equal(t, []string{"dbt", "run", "--select", "orders"}, body.Argv)
	assert.Equal(t, "model.finance.orders", body.Task.DBTUniqueID)
	assert.Equal(t, "orders", body.Task.TableName)
	// The lease deadline is machine-readable to the nanosecond.
	_, err := time.Parse(time.RFC3339Nano, body.ExpiresAt)
	assert.NoError(t, err)
}

// TestClaim_ScopesTheClaimToTheAuthenticatedPool is what stops a worker from
// claiming another team's work: the pool comes from the credential, never from
// the request.
func TestClaim_ScopesTheClaimToTheAuthenticatedPool(t *testing.T) {
	r := newRig(t)
	r.grant()

	r.do(http.MethodPost, "/internal/v1/workers/claim",
		`{"wait_seconds":0,"owner":"worker-1","pool_key":"`+otherPoolKey+`"}`)

	assert.Equal(t, testPoolKey, r.leases.lastClaim.PoolKey)
}

func TestClaim_EmptyDemandIsNoContent(t *testing.T) {
	r := newRig(t)

	resp := r.do(http.MethodPost, "/internal/v1/workers/claim", `{"wait_seconds":0}`)

	assert.Equal(t, http.StatusNoContent, resp.StatusCode)
}

// TestClaim_PollsUntilWorkAppears is the long poll: a worker asks once and is
// answered as soon as work exists, without hammering the executor.
func TestClaim_PollsUntilWorkAppears(t *testing.T) {
	r := newRig(t)
	r.grant()
	r.leases.grantAfter = time.Now().Add(600 * time.Millisecond)

	resp := r.do(http.MethodPost, "/internal/v1/workers/claim", `{"wait_seconds":5}`)

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Greater(t, r.leases.claims, 1)
}

// TestClaim_StopsAtTheConfiguredCeiling keeps a worker's asked-for wait from
// holding a connection longer than the executor allows.
func TestClaim_StopsAtTheConfiguredCeiling(t *testing.T) {
	r := newRig(t)

	start := time.Now()
	resp := r.do(http.MethodPost, "/internal/v1/workers/claim", `{"wait_seconds":600}`)

	assert.Equal(t, http.StatusNoContent, resp.StatusCode)
	// ClaimWait is 2s in the rig; the request cannot outlive it.
	assert.Less(t, time.Since(start), 5*time.Second)
}

// TestClaim_ANonPositiveWaitDoesNotWait pins the wait's boundary: a worker
// asking for no wait, or for a negative one, is answered by a single look for
// work rather than held for the executor's ceiling.
func TestClaim_ANonPositiveWaitDoesNotWait(t *testing.T) {
	for name, body := range map[string]string{
		"zero":     `{"wait_seconds":0}`,
		"negative": `{"wait_seconds":-1}`,
		"absent":   `{}`,
	} {
		t.Run(name, func(t *testing.T) {
			r := newRig(t)
			// Work only ever appears after the rig's 2s ceiling, so a request
			// that waits at all returns a grant.
			r.grant()
			r.leases.grantAfter = time.Now().Add(time.Hour)

			start := time.Now()
			resp := r.do(http.MethodPost, "/internal/v1/workers/claim", body)

			assert.Equal(t, http.StatusNoContent, resp.StatusCode)
			assert.Equal(t, 1, r.leases.claims, "a non-positive wait asks for work exactly once")
			assert.Less(t, time.Since(start), time.Second)
		})
	}
}

// TestClaim_AWaitTooLargeForADurationIsCappedNotWrapped keeps an absurd wait
// from arithmetic-overflowing into a deadline in the past, which would answer a
// worker that asked to wait as though it had asked not to.
func TestClaim_AWaitTooLargeForADurationIsCappedNotWrapped(t *testing.T) {
	r := newRig(t)
	r.grant()
	r.leases.grantAfter = time.Now().Add(600 * time.Millisecond)

	resp := r.do(http.MethodPost, "/internal/v1/workers/claim",
		fmt.Sprintf(`{"wait_seconds":%d}`, math.MaxInt64))

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Greater(t, r.leases.claims, 1, "the request waited rather than giving up at once")
}

// TestClaim_AnUnreadyPoolIsToldSoRatherThanHandedWork. A pool whose workers
// cannot hydrate their artifact would fail every task it claimed.
func TestClaim_AnUnreadyPoolIsToldSoRatherThanHandedWork(t *testing.T) {
	r := newRig(t)
	r.grant()
	r.auth.pool.InitializationError = "artifact_rejected: sha256 mismatch"

	resp := r.do(http.MethodPost, "/internal/v1/workers/claim", `{"wait_seconds":0}`)

	assert.Equal(t, http.StatusConflict, resp.StatusCode)
	assert.Equal(t, "pool_not_ready", decodeError(t, resp).Error.Code)
	assert.Zero(t, r.leases.claims)
}

// TestClaim_AFailedClaimIsTheExecutorsFault. A claim that errors is not "no
// work": the worker must not be told 204 and go back to sleep.
func TestClaim_AFailedClaimIsReportedNotSilentlyEmpty(t *testing.T) {
	r := newRig(t)
	r.leases.claimErr = errBoom

	resp := r.do(http.MethodPost, "/internal/v1/workers/claim", `{"wait_seconds":0}`)

	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
	assert.Equal(t, "internal", decodeError(t, resp).Error.Code)
}

func TestClaim_MalformedJSONIsRejected(t *testing.T) {
	r := newRig(t)

	resp := r.do(http.MethodPost, "/internal/v1/workers/claim", `{"wait_seconds":`)

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Equal(t, "invalid_request", decodeError(t, resp).Error.Code)
	assert.Zero(t, r.leases.claims)
}

// TestClaim_NeverEchoesTheLeaseTokenIntoALog. The raw token is handed to one
// worker once; a log line would make it recoverable by anyone reading logs.
func TestClaim_NeverEchoesTheLeaseTokenIntoALog(t *testing.T) {
	r := newRig(t)
	g := r.grant()

	resp := r.do(http.MethodPost, "/internal/v1/workers/claim", `{"wait_seconds":0}`)

	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.NotContains(t, r.logs.String(), g.Token)
}

// ---------------------------------------------------------------- start

func TestStart_AcknowledgesAndIsIdempotent(t *testing.T) {
	r := newRig(t)

	first := r.do(http.MethodPost, leasePath+"/start", depBody)
	second := r.do(http.MethodPost, leasePath+"/start", depBody)

	assert.Equal(t, http.StatusOK, first.StatusCode)
	assert.Equal(t, http.StatusOK, second.StatusCode)
	assert.Equal(t, 2, r.leases.starts)
}

// TestStart_TakesTheLeaseFromThePathAndThePoolFromTheCredential pins where each
// piece of a report's identity comes from.
func TestStart_TakesTheLeaseFromThePathAndThePoolFromTheCredential(t *testing.T) {
	r := newRig(t)

	resp := r.do(http.MethodPost, leasePath+"/start", depBody)

	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "44444444-4444-4444-4444-444444444444", r.leases.lastStart.LeaseID.String())
	assert.Equal(t, "33333333-3333-3333-3333-333333333333", r.leases.lastStart.DeploymentID.String())
	assert.Equal(t, testPoolKey, r.leases.lastStart.PoolKey)
}

// TestStart_ForwardsTheLeaseTokenToTheFence. The transport does not judge the
// token; it carries it to the aggregate, which is the only thing that can.
func TestStart_ForwardsTheLeaseTokenToTheFence(t *testing.T) {
	r := newRig(t)

	resp := r.do(http.MethodPost, leasePath+"/start", depBody)

	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, testLeaseToken, r.leases.lastStart.Token)
}

// TestLeaseToken_NeverReachesALogLine covers every lease-scoped endpoint,
// including the paths that reject the caller: a rejected token is still a
// secret, and a 409 must not be the thing that writes it down.
func TestLeaseToken_NeverReachesALogLine(t *testing.T) {
	r := newRig(t)
	r.leases.startErr = model.ErrStaleLease
	r.leases.beatErr = lease.ErrCancelled
	r.leases.doneErr = errBoom
	r.leases.taskErr = lease.ErrPoolMismatch

	for _, ep := range authenticatedPaths {
		r.do(ep.method, ep.path, ep.body)
	}

	assert.NotContains(t, r.logs.String(), testLeaseToken)
	assert.NotContains(t, r.logs.String(), "X-Continuo-Lease-Token")
}

func TestStart_MalformedLeaseIDIsRejected(t *testing.T) {
	r := newRig(t)

	resp := r.do(http.MethodPost, "/internal/v1/leases/not-a-uuid/start", depBody)

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Equal(t, "invalid_request", decodeError(t, resp).Error.Code)
	assert.Zero(t, r.leases.starts)
}

func TestStart_MissingDeploymentIDIsRejected(t *testing.T) {
	r := newRig(t)

	resp := r.do(http.MethodPost, leasePath+"/start", `{}`)

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Zero(t, r.leases.starts)
}

// ---------------------------------------------------------------- errors

// TestLeaseErrors_MapToTerminalStatuses is the mapping the whole worker loop
// depends on. A stale lease is settled, not transient: answering 5xx would make
// a superseded worker retry against the fence forever.
//
// A report about a row that does not exist is answered identically to a stale
// lease: it is equally final, and a distinct answer would let a caller learn
// which deployments exist.
func TestLeaseErrors_MapToTerminalStatuses(t *testing.T) {
	cases := []struct {
		name string
		err  error
		code int
		body string
	}{
		{"stale lease", model.ErrStaleLease, http.StatusConflict, "stale_lease"},
		{"no such deployment", lease.ErrNoSuchDeployment, http.StatusConflict, "stale_lease"},
		{"another pool", lease.ErrPoolMismatch, http.StatusForbidden, "pool_mismatch"},
		{"cancelled", lease.ErrCancelled, http.StatusGone, "cancelled"},
		{"unexpected", errBoom, http.StatusInternalServerError, "internal"},
	}
	for _, tc := range cases {
		t.Run("heartbeat/"+tc.name, func(t *testing.T) {
			r := newRig(t)
			r.leases.beatErr = tc.err

			resp := r.do(http.MethodPost, leasePath+"/heartbeat", depBody)

			assert.Equal(t, tc.code, resp.StatusCode)
			assert.Equal(t, tc.body, decodeError(t, resp).Error.Code)
		})
		t.Run("start/"+tc.name, func(t *testing.T) {
			r := newRig(t)
			r.leases.startErr = tc.err

			resp := r.do(http.MethodPost, leasePath+"/start", depBody)

			assert.Equal(t, tc.code, resp.StatusCode)
			assert.Equal(t, tc.body, decodeError(t, resp).Error.Code)
		})
		t.Run("complete/"+tc.name, func(t *testing.T) {
			r := newRig(t)
			r.leases.doneErr = tc.err

			resp := r.do(http.MethodPost, leasePath+"/complete",
				`{"deployment_id":"33333333-3333-3333-3333-333333333333","result":{"succeeded":true}}`)

			assert.Equal(t, tc.code, resp.StatusCode)
			assert.Equal(t, tc.body, decodeError(t, resp).Error.Code)
		})
	}
}

func TestHeartbeat_ExtendsTheLease(t *testing.T) {
	r := newRig(t)

	resp := r.do(http.MethodPost, leasePath+"/heartbeat", depBody)

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, 1, r.leases.beats)
	assert.Equal(t, testPoolKey, r.leases.lastBeat.PoolKey)
}

// ---------------------------------------------------------------- result URLs

// TestResultURLs_AreDerivedFromTheRowNotTheRequest. A worker that could name its
// own result keys could mint a capability to write into another schedule's
// prefix, so the identifiers come from the fenced row alone.
func TestResultURLs_AreDerivedFromTheRowNotTheRequest(t *testing.T) {
	r := newRig(t)

	resp := r.do(http.MethodPost, leasePath+"/result-urls",
		`{"deployment_id":"33333333-3333-3333-3333-333333333333",
		  "schedule_id":"99999999-9999-9999-9999-999999999999",
		  "task_id":"88888888-8888-8888-8888-888888888888"}`)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var body struct {
		Log struct {
			URL   string `json:"url"`
			S3URI string `json:"s3_uri"`
		} `json:"log"`
		RunResults struct {
			URL   string `json:"url"`
			S3URI string `json:"s3_uri"`
		} `json:"run_results"`
	}
	decodeJSON(t, resp, &body)

	assert.Equal(t, expectedLog, body.Log.S3URI)
	assert.Equal(t, expectedRunResults, body.RunResults.S3URI)
	assert.NotEmpty(t, body.Log.URL)
	assert.NotEmpty(t, body.RunResults.URL)
	// Writes only, and only to the two objects this lease owns.
	assert.Equal(t, []string{expectedLog, expectedRunResults}, r.signer.puts)
	assert.Empty(t, r.signer.gets)
	// The identifiers the caller supplied were ignored entirely.
	assert.NotContains(t, body.Log.S3URI, "99999999")
	assert.NotContains(t, body.Log.S3URI, "88888888")
}

func TestResultURLs_AreFencedByTheLease(t *testing.T) {
	r := newRig(t)
	r.leases.taskErr = model.ErrStaleLease

	resp := r.do(http.MethodPost, leasePath+"/result-urls", depBody)

	assert.Equal(t, http.StatusConflict, resp.StatusCode)
	assert.Equal(t, "stale_lease", decodeError(t, resp).Error.Code)
	assert.Empty(t, r.signer.puts)
}

func TestResultURLs_FromAnotherPoolIsForbidden(t *testing.T) {
	r := newRig(t)
	r.leases.taskErr = lease.ErrPoolMismatch

	resp := r.do(http.MethodPost, leasePath+"/result-urls", depBody)

	assert.Equal(t, http.StatusForbidden, resp.StatusCode)
	assert.Empty(t, r.signer.puts)
}

// TestResultURLs_NeverReachALogLine keeps a write capability out of the logs.
func TestResultURLs_NeverReachALogLine(t *testing.T) {
	r := newRig(t)

	resp := r.do(http.MethodPost, leasePath+"/result-urls", depBody)

	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.NotContains(t, r.logs.String(), "signed.example")
}

// ---------------------------------------------------------------- complete

func TestComplete_AcceptsTheURIsItIssued(t *testing.T) {
	r := newRig(t)

	resp := r.do(http.MethodPost, leasePath+"/complete",
		`{"deployment_id":"33333333-3333-3333-3333-333333333333",
		  "result":{"succeeded":true,"execution_seconds":12.5,
		            "log_s3_uri":"`+expectedLog+`",
		            "run_results_s3_uri":"`+expectedRunResults+`"}}`)

	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, 1, r.leases.completes)
	assert.True(t, r.leases.lastDone.Result.Succeeded)
	assert.Equal(t, 12.5, r.leases.lastDone.Result.ExecutionSeconds)
	assert.Equal(t, expectedLog, r.leases.lastDone.Result.LogS3URI)
}

// TestComplete_RejectsAURIItNeverIssued stops a worker recording someone else's
// object — or an arbitrary one — as this task's evidence.
func TestComplete_RejectsAURIItNeverIssued(t *testing.T) {
	forged := []string{
		"s3://continuo/dbt-runs/99999999/88888888/44444444-4444-4444-4444-444444444444/dbt.log",
		"s3://other-bucket/dbt-runs/x/y/z/dbt.log",
		"s3://continuo/../../etc/passwd",
	}
	for _, uri := range forged {
		t.Run(uri, func(t *testing.T) {
			r := newRig(t)

			resp := r.do(http.MethodPost, leasePath+"/complete",
				`{"deployment_id":"33333333-3333-3333-3333-333333333333",
				  "result":{"succeeded":true,"log_s3_uri":"`+uri+`"}}`)

			assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
			assert.Equal(t, "invalid_request", decodeError(t, resp).Error.Code)
			assert.Zero(t, r.leases.completes)
		})
	}
}

// TestComplete_AllowsAReportWithNoArtifacts. A worker that failed before it
// uploaded anything still has to report the failure.
func TestComplete_AllowsAReportWithNoArtifacts(t *testing.T) {
	r := newRig(t)

	resp := r.do(http.MethodPost, leasePath+"/complete",
		`{"deployment_id":"33333333-3333-3333-3333-333333333333",
		  "result":{"succeeded":false,"retryable":true,"error_class":"warehouse_timeout",
		            "error_message":"connection reset"}}`)

	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, 1, r.leases.completes)
	assert.False(t, r.leases.lastDone.Result.Succeeded)
	assert.True(t, r.leases.lastDone.Result.Retryable)
	assert.Equal(t, "warehouse_timeout", r.leases.lastDone.Result.ErrorClass)
}

// ---------------------------------------------------------------- initialization

func TestInitialization_RecordsAFailure(t *testing.T) {
	r := newRig(t)

	resp := r.do(http.MethodPost, "/internal/v1/workers/initialization",
		`{"ok":false,"error_code":"artifact_rejected","message":"sha256 mismatch"}`)

	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Len(t, r.pools.reports, 1)
	assert.Equal(t, testPoolKey, r.pools.reports[0].PoolKey)
	assert.False(t, r.pools.reports[0].OK)
	assert.Equal(t, "artifact_rejected", r.pools.reports[0].ErrorCode)
	assert.Equal(t, "sha256 mismatch", r.pools.reports[0].Message)
}

func TestInitialization_ClearsAnErrorAndRecordsHydration(t *testing.T) {
	r := newRig(t)

	resp := r.do(http.MethodPost, "/internal/v1/workers/initialization",
		`{"ok":true,"hydration_seconds":3.25}`)

	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Len(t, r.pools.reports, 1)
	assert.True(t, r.pools.reports[0].OK)
	assert.Equal(t, 3.25, r.pools.reports[0].HydrationSeconds)
}

// TestInitialization_ReportsAgainstTheAuthenticatedPoolOnly stops a worker
// marking another team's pool broken.
func TestInitialization_ReportsAgainstTheAuthenticatedPoolOnly(t *testing.T) {
	r := newRig(t)

	resp := r.do(http.MethodPost, "/internal/v1/workers/initialization",
		`{"ok":false,"error_code":"artifact_rejected","pool_key":"`+otherPoolKey+`"}`)

	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Len(t, r.pools.reports, 1)
	assert.Equal(t, testPoolKey, r.pools.reports[0].PoolKey)
}

func TestInitialization_MalformedJSONIsRejected(t *testing.T) {
	r := newRig(t)

	resp := r.do(http.MethodPost, "/internal/v1/workers/initialization", `{"ok":`)

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Empty(t, r.pools.reports)
}

// ---------------------------------------------------------------- server

// TestServer_KeepsTheHealthEndpoints. The worker API shares the executor's port
// with the probes Kubernetes already depends on.
func TestServer_KeepsTheHealthEndpoints(t *testing.T) {
	r := newRig(t)

	for path, want := range map[string]string{"/health": "OK", "/ready": "READY"} {
		resp := r.get(path)

		assert.Equal(t, http.StatusOK, resp.StatusCode)
		assert.Equal(t, want, string(resp.Body))
	}
}

// TestServer_HealthNeedsNoCredential keeps the probes working: they are not
// worker calls and hold no pool credential.
func TestServer_HealthNeedsNoCredential(t *testing.T) {
	r := newRig(t)

	resp := r.get("/health")

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Zero(t, r.auth.calls)
}

// TestServer_WrongMethodIsNotAcceptedOnAnEndpoint pins that each endpoint takes
// the one verb it defines.
func TestServer_WrongMethodIsNotAcceptedOnAnEndpoint(t *testing.T) {
	r := newRig(t)

	resp := r.do(http.MethodGet, "/internal/v1/workers/claim", "")

	assert.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode)
	assert.Zero(t, r.leases.claims)
}

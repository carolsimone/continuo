package http_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http/httptest"
	"strings"
	"sync"
	"time"

	nethttp "net/http"

	adapterhttp "github.com/carolsimone/continuo/executor-controller/adapters/http"
	"github.com/carolsimone/continuo/executor-controller/domain/command"
	"github.com/carolsimone/continuo/executor-controller/domain/model"
	"github.com/carolsimone/continuo/executor-controller/service/lease"
	"github.com/carolsimone/continuo/executor-controller/service/workerapi"
	pkgmodel "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/google/uuid"
)

const (
	testPoolKey    = "finance:sha-abc"
	testCredential = "pool-secret-credential"
	testBucket     = "continuo"
	otherPoolKey   = "marketing:sha-zzz"
	// #nosec G101 -- a fixture for tests that prove secrets are not logged.
	testLeaseToken = "raw-lease-token-never-logged"
)

func sha256Hex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

func testPool() *model.WorkerPool {
	return &model.WorkerPool{
		PoolKey:     testPoolKey,
		ServiceName: "finance",
		ImageTag:    "sha-abc",
		RuntimeManifest: pkgmodel.RuntimeManifestRef{
			RuntimeManifestURI:                "s3://continuo/artifacts/finance/manifest.msgpack",
			RuntimeManifestSHA256:             "a1b2",
			RuntimeManifestDBTVersion:         "1.12.0b1",
			RuntimeManifestParseContextSHA256: "c3d4",
		},
		CredentialSHA256: sha256Hex(testCredential),
	}
}

// fakeAuth authenticates exactly one pool with one credential, the way the real
// authenticator does: an unknown pool and a wrong credential answer alike.
type fakeAuth struct {
	pool  *model.WorkerPool
	err   error
	calls int
}

func newFakeAuth() *fakeAuth { return &fakeAuth{pool: testPool()} }

func (a *fakeAuth) Authenticate(_ context.Context, poolKey, credential string) (*model.WorkerPool, error) {
	a.calls++
	if a.err != nil {
		return nil, a.err
	}
	if poolKey != a.pool.PoolKey || !workerapi.VerifyCredential(credential, a.pool.CredentialSHA256) {
		return nil, workerapi.ErrUnauthenticated
	}
	return a.pool, nil
}

// fakeLeases records what the transport asked of the lease service and returns
// what a test tells it to.
type fakeLeases struct {
	mu sync.Mutex

	grant      *lease.Grant
	grantAfter time.Time
	claimErr   error
	claims     int
	lastClaim  lease.ClaimInput

	startErr  error
	starts    int
	lastStart lease.StartInput
	beatErr   error
	beats     int
	lastBeat  lease.HeartbeatInput
	doneErr   error
	completes int
	lastDone  lease.CompleteInput
	taskRef   lease.TaskRef
	taskErr   error
	tasks     int
}

func newFakeLeases() *fakeLeases {
	return &fakeLeases{taskRef: lease.TaskRef{PoolKey: testPoolKey, Command: testCommand()}}
}

func testCommand() command.DeployTask {
	return command.DeployTask{
		TaskID: "11111111-1111-1111-1111-111111111111", ScheduleID: "22222222-2222-2222-2222-222222222222",
		ServiceName: "finance", SchemaName: "public", TableName: "orders",
		NodeType: "dbt-model", ImageTag: "sha-abc", DBTUniqueID: "model.finance.orders",
	}
}

func (l *fakeLeases) Claim(_ context.Context, in lease.ClaimInput) (*lease.Grant, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.claims++
	l.lastClaim = in
	if l.claimErr != nil {
		return nil, l.claimErr
	}
	if !l.grantAfter.IsZero() && time.Now().Before(l.grantAfter) {
		return nil, nil
	}
	return l.grant, nil
}

func (l *fakeLeases) Start(_ context.Context, in lease.StartInput) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.starts++
	l.lastStart = in
	return l.startErr
}

func (l *fakeLeases) Heartbeat(_ context.Context, in lease.HeartbeatInput) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.beats++
	l.lastBeat = in
	return l.beatErr
}

func (l *fakeLeases) Complete(_ context.Context, in lease.CompleteInput) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.completes++
	l.lastDone = in
	return l.doneErr
}

func (l *fakeLeases) Task(_ context.Context, _ lease.TaskInput) (lease.TaskRef, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.tasks++
	if l.taskErr != nil {
		return lease.TaskRef{}, l.taskErr
	}
	return l.taskRef, nil
}

// fakePoolReports records initialization reports.
type fakePoolReports struct {
	reports []workerapi.InitializationReport
	err     error
}

func (p *fakePoolReports) RecordInitialization(_ context.Context, r workerapi.InitializationReport) error {
	if p.err != nil {
		return p.err
	}
	p.reports = append(p.reports, r)
	return nil
}

// fakeSigner returns a deterministic URL per operation and object, so a test can
// tell a read capability from a write one and see exactly which object was
// signed.
type fakeSigner struct {
	err  error
	gets []string
	puts []string
}

func (s *fakeSigner) PresignGet(_ context.Context, s3URI string, ttl time.Duration) (string, error) {
	if s.err != nil {
		return "", s.err
	}
	s.gets = append(s.gets, s3URI)
	return fmt.Sprintf("https://signed.example/get?o=%s&ttl=%s", s3URI, ttl), nil
}

func (s *fakeSigner) PresignPut(_ context.Context, s3URI, contentType string, ttl time.Duration) (string, error) {
	if s.err != nil {
		return "", s.err
	}
	s.puts = append(s.puts, s3URI)
	return fmt.Sprintf("https://signed.example/put?o=%s&ct=%s&ttl=%s", s3URI, contentType, ttl), nil
}

// rig is a running worker API backed by fakes.
type rig struct {
	server *httptest.Server
	auth   *fakeAuth
	leases *fakeLeases
	pools  *fakePoolReports
	signer *fakeSigner
	logs   *strings.Builder
}

// newRig builds the worker API over an in-memory stack. Its logger writes to a
// buffer, so a test can prove a secret never reaches a log line.
func newRig(t interface{ Cleanup(func()) }) *rig {
	r := &rig{
		auth:   newFakeAuth(),
		leases: newFakeLeases(),
		pools:  &fakePoolReports{},
		signer: &fakeSigner{},
		logs:   &strings.Builder{},
	}
	logger := slog.New(slog.NewTextHandler(r.logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	handler := adapterhttp.NewServer(0, adapterhttp.WorkerAPIConfig{
		Leases:    r.leases,
		Auth:      r.auth,
		Pools:     r.pools,
		Signer:    r.signer,
		Bucket:    testBucket,
		ClaimWait: 2 * time.Second,
		URLTTL:    15 * time.Minute,
	}, logger).Handler()
	r.server = httptest.NewServer(handler)
	t.Cleanup(r.server.Close)
	return r
}

// grant seeds a claimable lease and returns it.
func (r *rig) grant() *lease.Grant {
	g := &lease.Grant{
		DeploymentID: uuid.MustParse("33333333-3333-3333-3333-333333333333"),
		LeaseID:      uuid.MustParse("44444444-4444-4444-4444-444444444444"),
		Token:        testLeaseToken,
		Attempt:      1,
		ExpiresAt:    time.Unix(1000, 0).UTC(),

		ExecutionPath: model.ExecutionPathNative,
		Argv:          []string{"dbt", "run", "--select", "orders"},
		Command:       testCommand(),
	}
	r.leases.grant = g
	return g
}

// response is a completed exchange: the body is already drained and closed, so
// a test reads it as data rather than managing a stream.
type response struct {
	StatusCode int
	Body       []byte
}

// do issues an authenticated request unless the options override the
// credentials it presents.
func (r *rig) do(method, path, body string, opts ...func(*nethttp.Request)) response {
	var reader io.Reader
	if body != "" {
		reader = strings.NewReader(body)
	}
	req, err := nethttp.NewRequest(method, r.server.URL+path, reader)
	if err != nil {
		panic(err)
	}
	req.Header.Set("X-Continuo-Pool-Key", testPoolKey)
	req.Header.Set("Authorization", "Bearer "+testCredential)
	req.Header.Set("X-Continuo-Lease-Token", testLeaseToken)
	req.Header.Set("Content-Type", "application/json")
	for _, o := range opts {
		o(req)
	}
	return send(req)
}

// get issues a request presenting no credentials at all, the way a Kubernetes
// probe reaches the health endpoints.
func (r *rig) get(path string) response {
	req, err := nethttp.NewRequest(nethttp.MethodGet, r.server.URL+path, nil)
	if err != nil {
		panic(err)
	}
	return send(req)
}

func send(req *nethttp.Request) response {
	resp, err := nethttp.DefaultClient.Do(req)
	if err != nil {
		panic(err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}
	return response{StatusCode: resp.StatusCode, Body: body}
}

func noCredential(req *nethttp.Request) { req.Header.Del("Authorization") }

func wrongCredential(req *nethttp.Request) {
	req.Header.Set("Authorization", "Bearer not-the-credential")
}

func unknownPool(req *nethttp.Request) { req.Header.Set("X-Continuo-Pool-Key", "pool-nobody") }

var errBoom = errors.New("boom")

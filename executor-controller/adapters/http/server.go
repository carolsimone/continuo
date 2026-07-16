package http

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/carolsimone/continuo/executor-controller/domain/model"
	"github.com/carolsimone/continuo/executor-controller/service/lease"
	"github.com/carolsimone/continuo/executor-controller/service/ports"
	"github.com/carolsimone/continuo/executor-controller/service/workerapi"
)

// leaseTokenHeader carries the raw lease token a worker proves its hold with.
// It is a second credential, distinct from the pool credential that says which
// pool the caller is, and rides its own header so neither is mistaken for the
// other.
//
// #nosec G101 -- this is the name of a header, not a credential.
const leaseTokenHeader = "X-Continuo-Lease-Token"

// maxRequestBytes bounds a worker request body. Every body the API takes is a
// handful of identifiers and a small result, so anything larger is a fault.
const maxRequestBytes = 1 << 20

// readHeaderTimeout bounds how long a caller may take to send its headers.
const readHeaderTimeout = 5 * time.Second

// leaseService is the lease lifecycle this transport exposes.
type leaseService interface {
	Claim(ctx context.Context, in lease.ClaimInput) (*lease.Grant, error)
	Start(ctx context.Context, in lease.StartInput) error
	Heartbeat(ctx context.Context, in lease.HeartbeatInput) error
	Complete(ctx context.Context, in lease.CompleteInput) error
	Task(ctx context.Context, in lease.TaskInput) (lease.TaskRef, error)
}

// authenticator resolves a caller to the worker pool it serves.
type authenticator interface {
	Authenticate(ctx context.Context, poolKey, credential string) (*model.WorkerPool, error)
}

// poolReporter records what a worker reports about its own pool.
type poolReporter interface {
	RecordInitialization(ctx context.Context, report workerapi.InitializationReport) error
}

// WorkerAPIConfig wires the worker API to the application services behind it.
type WorkerAPIConfig struct {
	Leases leaseService
	Auth   authenticator
	Pools  poolReporter
	Signer ports.ObjectURLSigner
	// Bucket is the object store bucket a task's results are written to.
	Bucket string
	// ClaimWait caps how long a claim request may wait for work, however long
	// the asking worker said it was willing to wait.
	ClaimWait time.Duration
	// URLTTL is how long a signed URL stays usable. A capability is scoped in
	// time as well as to one object.
	URLTTL time.Duration
}

// Server serves the executor's HTTP surface: the probes Kubernetes reads and
// the internal API worker pods call, on one port.
type Server struct {
	server *http.Server
	mux    *http.ServeMux
	logger *slog.Logger

	leases    leaseService
	auth      authenticator
	pools     poolReporter
	signer    ports.ObjectURLSigner
	bucket    string
	claimWait time.Duration
	urlTTL    time.Duration
}

// NewServer builds the executor's HTTP server: health and readiness probes,
// open as they must be, plus the worker API, which serves only a caller holding
// a registered pool's credential.
func NewServer(port int, cfg WorkerAPIConfig, logger *slog.Logger) *Server {
	mux := http.NewServeMux()
	s := &Server{
		mux:       mux,
		logger:    logger,
		leases:    cfg.Leases,
		auth:      cfg.Auth,
		pools:     cfg.Pools,
		signer:    cfg.Signer,
		bucket:    cfg.Bucket,
		claimWait: cfg.ClaimWait,
		urlTTL:    cfg.URLTTL,
		server: &http.Server{
			Addr:              fmt.Sprintf(":%d", port),
			Handler:           mux,
			ReadHeaderTimeout: readHeaderTimeout,
		},
	}

	registerHealth(mux, logger)
	s.registerWorkerAPI(mux)
	return s
}

// registerWorkerAPI installs the endpoints worker pods call. Every one of them
// is behind authenticate: none serves a caller that has not proved which pool
// it is.
func (s *Server) registerWorkerAPI(mux *http.ServeMux) {
	mux.HandleFunc("GET /internal/v1/worker/runtime", s.authenticate(s.handleRuntime))
	mux.HandleFunc("POST /internal/v1/workers/claim", s.authenticate(s.handleClaim))
	mux.HandleFunc("POST /internal/v1/workers/initialization", s.authenticate(s.handleInitialization))
	mux.HandleFunc("POST /internal/v1/leases/{id}/start", s.authenticate(s.handleStart))
	mux.HandleFunc("POST /internal/v1/leases/{id}/heartbeat", s.authenticate(s.handleHeartbeat))
	mux.HandleFunc("POST /internal/v1/leases/{id}/result-urls", s.authenticate(s.handleResultURLs))
	mux.HandleFunc("POST /internal/v1/leases/{id}/complete", s.authenticate(s.handleComplete))
}

// Handler exposes the routed handler this server serves.
func (s *Server) Handler() http.Handler { return s.mux }

// Start serves until the server is shut down.
func (s *Server) Start() error {
	s.logger.Info("Starting HTTP server", "addr", s.server.Addr)
	if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("failed to start HTTP server: %w", err)
	}
	return nil
}

// Shutdown gracefully shuts down the HTTP server.
func (s *Server) Shutdown(ctx context.Context) error {
	s.logger.Info("Shutting down HTTP server")
	return s.server.Shutdown(ctx)
}

// writeJSON answers with a JSON body.
func (s *Server) writeJSON(w http.ResponseWriter, r *http.Request, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		// The status line is already sent, so the only thing left is to record
		// that the caller did not get a complete answer.
		s.logger.Warn("failed to write a response body", "path", r.URL.Path, "error", err)
	}
}

// writeError answers with the stable error envelope. Message is written for a
// worker to act on and never carries a credential, a lease token, or a signed
// URL: it is composed here, not echoed from the request.
func (s *Server) writeError(w http.ResponseWriter, r *http.Request, status int, code, message string) {
	s.writeJSON(w, r, status, errorResponse{Error: errorDetail{Code: code, Message: message}})
}

package neo4jinfra

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/config"
)

type Neo4jClient interface {
	NewSession(ctx context.Context, mode neo4j.AccessMode) neo4j.SessionWithContext
	Close(ctx context.Context) error
	VerifyConnectivity(ctx context.Context) error
}

type neo4jClient struct {
	driver neo4j.DriverWithContext
	logger *slog.Logger
}

// StartupPolicy bounds how long a service waits for Neo4j to accept connections
// at boot. The wait covers a Neo4j that is still starting, or whose Service name
// or endpoint has not propagated yet, which is the normal state when a Deployment
// rolls out next to a StatefulSet. A value the driver cannot honour (bad
// credentials, malformed URI) is not retried: it fails at once. TLS handshake
// failures are the exception: the driver reports them as connectivity errors, so
// they are retried until the budget is spent.
type StartupPolicy struct {
	// Timeout is the wall-clock budget for the whole wait, attempts included,
	// before giving up with the last error.
	Timeout time.Duration
	// InitialDelay is the pause after the first failed attempt; it doubles per
	// attempt up to MaxDelay.
	InitialDelay time.Duration
	MaxDelay     time.Duration
}

// DefaultStartupPolicy covers a cold bundled Neo4j (a JVM that takes a couple of
// minutes to open its Bolt port) without hiding a real outage for long.
func DefaultStartupPolicy() StartupPolicy {
	return StartupPolicy{
		Timeout:      5 * time.Minute,
		InitialDelay: time.Second,
		MaxDelay:     10 * time.Second,
	}
}

// NewNeo4jClient connects to Neo4j and fails if it is not reachable right now.
func NewNeo4jClient(uri, user, password string, logger *slog.Logger) (Neo4jClient, error) {
	client, err := newDriverClient(uri, user, password, logger)
	if err != nil {
		return nil, err
	}
	if err := client.VerifyConnectivity(context.Background()); err != nil {
		return nil, fmt.Errorf("failed to verify neo4j connectivity: %w", err)
	}
	logger.Info("Neo4j client created successfully", "uri", uri)
	return client, nil
}

// NewNeo4jClientWithRetry connects to Neo4j, retrying connectivity failures with
// capped exponential backoff for up to policy.Timeout. It returns early when ctx
// is cancelled or the failure is not a connectivity problem.
func NewNeo4jClientWithRetry(
	ctx context.Context, uri, user, password string, logger *slog.Logger, policy StartupPolicy,
) (Neo4jClient, error) {
	client, err := newDriverClient(uri, user, password, logger)
	if err != nil {
		return nil, err
	}
	if err := waitForConnectivity(ctx, client.VerifyConnectivity, policy, sleepCtx, logger); err != nil {
		_ = client.Close(context.Background())
		return nil, fmt.Errorf("failed to verify neo4j connectivity: %w", err)
	}
	logger.Info("Neo4j client created successfully", "uri", uri)
	return client, nil
}

func newDriverClient(uri, user, password string, logger *slog.Logger) (*neo4jClient, error) {
	driver, err := neo4j.NewDriverWithContext(
		uri,
		neo4j.BasicAuth(user, password, ""),
		func(cfg *config.Config) {
			cfg.MaxConnectionPoolSize = 50
			cfg.ConnectionAcquisitionTimeout = 60 * time.Second
			cfg.MaxConnectionLifetime = 1 * time.Hour
		},
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create neo4j driver: %w", err)
	}
	return &neo4jClient{driver: driver, logger: logger}, nil
}

// waitForConnectivity calls verify until it succeeds, the policy timeout elapses,
// ctx is cancelled, or verify returns an error that waiting cannot fix. The
// timeout bounds the whole wait, including a verify call that blocks, so the
// budget is wall-clock time. sleep is injected so the backoff is testable
// without real time.
func waitForConnectivity(
	ctx context.Context,
	verify func(context.Context) error,
	policy StartupPolicy,
	sleep func(context.Context, time.Duration) error,
	logger *slog.Logger,
) error {
	parent := ctx
	ctx, cancel := context.WithTimeout(ctx, policy.Timeout)
	defer cancel()

	var waited time.Duration
	delay := policy.InitialDelay
	for attempt := 1; ; attempt++ {
		err := verify(ctx)
		if err == nil {
			return nil
		}
		if !isStartupRetryable(err) {
			return err
		}
		if stopped := waitStopped(parent, ctx, err, waited, attempt); stopped != nil {
			return stopped
		}
		if waited+delay > policy.Timeout {
			return fmt.Errorf("neo4j still unreachable after %s (%d attempts): %w", waited, attempt, err)
		}
		logger.Warn("Neo4j not reachable yet, retrying",
			"attempt", attempt, "retry_in", delay.String(), "error", err)
		if serr := sleep(ctx, delay); serr != nil {
			if stopped := waitStopped(parent, ctx, err, waited, attempt); stopped != nil {
				return stopped
			}
			return fmt.Errorf("waiting for neo4j interrupted: %w (last error: %v)", serr, err)
		}
		waited += delay
		delay = min(delay*2, policy.MaxDelay)
	}
}

// waitStopped explains why the wait ended when its context is done: the caller
// cancelled it (shutdown) or the policy budget ran out. It returns nil while
// the wait is still live.
func waitStopped(parent, ctx context.Context, last error, waited time.Duration, attempt int) error {
	if ctx.Err() == nil {
		return nil
	}
	if parent.Err() != nil {
		return fmt.Errorf("waiting for neo4j interrupted: %w (last error: %v)", parent.Err(), last)
	}
	return fmt.Errorf("neo4j still unreachable after %s (%d attempts): %w", waited, attempt, last)
}

// isStartupRetryable reports whether err means "Neo4j is not up yet" (an
// unreachable host or a transient server state) rather than a value the client
// has wrong, such as bad credentials.
func isStartupRetryable(err error) bool {
	var connErr *neo4j.ConnectivityError
	return errors.As(err, &connErr) || neo4j.IsRetryable(err)
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func (c *neo4jClient) NewSession(ctx context.Context, mode neo4j.AccessMode) neo4j.SessionWithContext {
	return c.driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: mode})
}

func (c *neo4jClient) Close(ctx context.Context) error {
	c.logger.Info("Closing Neo4j connection")
	return c.driver.Close(ctx)
}

func (c *neo4jClient) VerifyConnectivity(ctx context.Context) error {
	return c.driver.VerifyConnectivity(ctx)
}

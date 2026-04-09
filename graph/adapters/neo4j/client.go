package neo4j

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// Neo4jClient is an interface for Neo4j database operations
type Neo4jClient interface {
	NewSession(ctx context.Context, mode neo4j.AccessMode) neo4j.SessionWithContext
	Close(ctx context.Context) error
	VerifyConnectivity(ctx context.Context) error
}

// neo4jClient implements the Neo4jClient interface
type neo4jClient struct {
	driver neo4j.DriverWithContext
	logger *slog.Logger
}

// NewNeo4jClient creates a new Neo4j client with connection pooling
func NewNeo4jClient(uri, user, password string, logger *slog.Logger) (Neo4jClient, error) {
	driver, err := neo4j.NewDriverWithContext(
		uri,
		neo4j.BasicAuth(user, password, ""),
		func(config *neo4j.Config) {
			config.MaxConnectionPoolSize = 50
			config.ConnectionAcquisitionTimeout = 60 * time.Second
			config.MaxConnectionLifetime = 1 * time.Hour
		},
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create neo4j driver: %w", err)
	}

	client := &neo4jClient{
		driver: driver,
		logger: logger,
	}

	// Verify connectivity on startup
	if err := client.VerifyConnectivity(context.Background()); err != nil {
		return nil, fmt.Errorf("failed to verify neo4j connectivity: %w", err)
	}

	logger.Info("Neo4j client created successfully", "uri", uri)
	return client, nil
}

// NewSession creates a new Neo4j session with the specified access mode
func (c *neo4jClient) NewSession(ctx context.Context, mode neo4j.AccessMode) neo4j.SessionWithContext {
	return c.driver.NewSession(ctx, neo4j.SessionConfig{
		AccessMode: mode,
	})
}

// Close closes the Neo4j driver connection
func (c *neo4jClient) Close(ctx context.Context) error {
	c.logger.Info("Closing Neo4j connection")
	return c.driver.Close(ctx)
}

// VerifyConnectivity verifies the connection to Neo4j
func (c *neo4jClient) VerifyConnectivity(ctx context.Context) error {
	return c.driver.VerifyConnectivity(ctx)
}

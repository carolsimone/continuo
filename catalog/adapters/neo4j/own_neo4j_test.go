//go:build integration

package neo4jinfra

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/testdeps"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// A test that issues Neo4j DDL (it drops and recreates constraints and
// indexes) runs against its own throwaway Neo4j, because that DDL would break
// any other suite working on the shared docker-compose Neo4j. The container
// starts on the first call of ownNeo4j and lives until the package's tests end.

const (
	ownNeo4jImage    = "neo4j:5.26.28-community"
	ownNeo4jPassword = "own-neo4j-pass"
)

var ownNeo4jContainer struct {
	once      sync.Once
	container testcontainers.Container
	uri       string
	err       error
}

// TestMain ends the package's tests by terminating the container ownNeo4j
// started, if any: Ryuk is disabled in this repository, so nothing else
// reaps it.
func TestMain(m *testing.M) {
	code := m.Run()
	if c := ownNeo4jContainer.container; c != nil {
		if err := c.Terminate(context.Background()); err != nil {
			os.Stderr.WriteString("terminate the test Neo4j container: " + err.Error() + "\n")
		}
	}
	os.Exit(code)
}

func startOwnNeo4j() {
	ctx := context.Background()
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        ownNeo4jImage,
			ExposedPorts: []string{"7687/tcp"},
			Env:          map[string]string{"NEO4J_AUTH": "neo4j/" + ownNeo4jPassword},
			WaitingFor:   wait.ForLog("Started.").WithStartupTimeout(2 * time.Minute),
		},
		Started: true,
	})
	if c != nil {
		ownNeo4jContainer.container = c
	}
	if err != nil {
		ownNeo4jContainer.err = err
		return
	}
	host, err := c.Host(ctx)
	if err != nil {
		ownNeo4jContainer.err = err
		return
	}
	if host == "localhost" {
		host = "127.0.0.1"
	}
	port, err := c.MappedPort(ctx, "7687")
	if err != nil {
		ownNeo4jContainer.err = err
		return
	}
	ownNeo4jContainer.uri = "bolt://" + host + ":" + port.Port()
}

// ownNeo4j returns a client to the package's throwaway Neo4j, emptied: every
// node, constraint and index but the token lookup indexes is gone. The test
// owns the graph, so it may reshape it freely.
func ownNeo4j(t *testing.T) Neo4jClient {
	t.Helper()
	ownNeo4jContainer.once.Do(startOwnNeo4j)
	if ownNeo4jContainer.err != nil {
		testdeps.Unavailable(t, "the test Neo4j container did not start: %v", ownNeo4jContainer.err)
	}
	ctx := context.Background()
	client, err := NewNeo4jClient(ownNeo4jContainer.uri, "neo4j", ownNeo4jPassword, quietLogger())
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close(ctx) })
	emptyGraph(t, client)
	return client
}

// emptyGraph deletes every node, then drops every constraint and every
// non-lookup index.
func emptyGraph(t *testing.T, client Neo4jClient) {
	t.Helper()
	ctx := context.Background()
	s := client.NewSession(ctx, neo4j.AccessModeWrite)
	defer func() { _ = s.Close(ctx) }()
	run := func(cypher string) []*neo4j.Record {
		res, err := s.Run(ctx, cypher, nil)
		require.NoError(t, err, cypher)
		recs, err := res.Collect(ctx)
		require.NoError(t, err, cypher)
		return recs
	}
	run(`MATCH (n) CALL { WITH n DETACH DELETE n } IN TRANSACTIONS`)
	for _, rec := range run(`SHOW CONSTRAINTS YIELD name RETURN name`) {
		name, _ := rec.Get("name")
		run("DROP CONSTRAINT `" + name.(string) + "` IF EXISTS")
	}
	for _, rec := range run(`SHOW INDEXES YIELD name, type WHERE type <> 'LOOKUP' RETURN name`) {
		name, _ := rec.Get("name")
		run("DROP INDEX `" + name.(string) + "` IF EXISTS")
	}
}

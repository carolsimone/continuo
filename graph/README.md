# Graph Service

A gRPC-based service for managing data lineage and dependency relationships in Neo4j.

## Architecture

- **Pattern**: DDD (Domain-Driven Design)
- **Server**: gRPC with async handlers
- **Database**: Neo4j graph database
- **Testing**: Integration tests with testcontainers

## Features

- **Node Management**: Create and update table nodes with metadata
- **Dependency Tracking**: Track upstream/downstream relationships
- **Freshness Monitoring**: Identify stale nodes based on last_update timestamp
- **DAG Traversal**: Query dependencies with configurable depth limits
- **Upstream Validation**: Check if all upstream dependencies are fresh before execution

## API Endpoints

### gRPC (Port 50052)

**Node Operations:**
- `CreateNode` - Create or update a table node with metadata
- `UpdateNodeTimestamp` - Update last_update timestamp for a node
- `GetStaleRootNodes` - Find root nodes (no upstream deps) that haven't been updated recently
- `GetDownstreamDependencies` - Get all downstream tables that depend on a node
- `CheckUpstreamFreshness` - Verify all upstream dependencies are fresh

### HTTP (Port 8081)

- `GET /health` - Health check endpoint

## Data Model

### Node Structure
```
(:Table {
  schedule_name: string,
  schema_name: string,
  table_name: string,
  service_name: string,
  node_type: string,   -- "dbt-model" | "dbt-seed" | "dbt-snapshot"
  owner: string,
  last_update: timestamp,
  created_at: timestamp
})
```

### Relationship
```
(source:Table)-[:DEPENDS_ON]->(target:Table)
```

## Getting Started

### Prerequisites

- Docker and Docker Compose
- Go 1.25.1+
- protoc (Protocol Buffers compiler)
- Neo4j 5.15+ running

### Running with Docker Compose

1. **Start Neo4j**:
```bash
docker-compose up neo4j -d
```

2. **Generate Proto Code** (if needed):
```bash
docker-compose exec graph bash -c "cd /app/graph && ./generate_proto.sh"
```

3. **Start the Service**:
```bash
go run main.go
```

The service will be available at:
- gRPC: `localhost:50052`
- Health: `http://localhost:8081/health`

### Testing with grpcurl

**Create a node:**
```bash
grpcurl -plaintext -d '{
  "schedule_name": "daily",
  "schema_name": "public",
  "table_name": "users",
  "service_name": "analytics",
  "node_type": "dbt-model",
  "owner": "data-team",
  "depends_on": []
}' localhost:50052 graph.v1.GraphService/CreateNode
```

**Get stale root nodes:**
```bash
grpcurl -plaintext -d '{
  "schedule_name": "daily",
  "hours_threshold": 24
}' localhost:50052 graph.v1.GraphService/GetStaleRootNodes
```

**Check upstream freshness:**
```bash
grpcurl -plaintext -d '{
  "schema_name": "public",
  "table_name": "orders",
  "hours_threshold": 2
}' localhost:50052 graph.v1.GraphService/CheckUpstreamFreshness
```

## Running Tests

The graph service has comprehensive integration tests using Neo4j testcontainers.

### All Tests

```bash
docker exec graph bash -c "cd /app/graph && make test"
```

**Test Results**: ✅ 6/6 tests passing

### Individual Test Suites

**Unit tests only** (fast):
```bash
make test-unit
```

**Integration tests** (with testcontainers):
```bash
make test-integration
```

### Test Coverage

- **Node creation**: Simple nodes, nodes with dependencies, validation
- **Timestamp updates**: Update existing nodes, error handling for missing nodes
- **Stale detection**: Find stale root nodes with custom thresholds
- **Dependency traversal**: Downstream dependencies with max depth
- **Freshness checks**: Upstream validation before execution

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `GRPC_PORT` | `50052` | gRPC server port |
| `HTTP_PORT` | `8081` | HTTP health check port |
| `NEO4J_URI` | `bolt://localhost:7687` | Neo4j connection URI |
| `NEO4J_USER` | `neo4j` | Neo4j username |
| `NEO4J_PASSWORD` | `password` | Neo4j password |

## Development

### Project Structure

```
graph/
├── main.go                         # Application entry point
├── proto/graph/v1/graph.proto     # gRPC service definition
├── config/config.go                # Configuration
├── internal/
│   ├── lifecycle/lifecycle.go     # Graceful shutdown
│   └── grpc/
│       ├── server.go              # gRPC server wrapper
│       └── handlers/
│           └── graph_handler.go   # Graph endpoints
├── domain/model/model.go          # Domain models
├── adapters/
│   ├── neo4j/
│   │   └── repository.go          # Neo4j operations
│   └── http/
│       ├── server.go              # Health server
│       └── handlers.go            # Health handler
└── test/
    └── integration_test.go        # Integration tests
```

### Adding New Endpoints

1. Update `proto/graph/v1/graph.proto`
2. Regenerate proto code: `./generate_proto.sh`
3. Add handler method in `internal/grpc/handlers/`
4. Wire up in `internal/grpc/server.go`
5. Add tests in `test/`

## Use Cases

### 1. DAG Initialization
When a schedule starts, find all root nodes (tables with no dependencies) to begin execution:
```
GetStaleRootNodes(schedule_name, hours_threshold)
→ Returns list of tables ready to execute
```

### 2. Dependency-Based Execution
Before executing a table, verify all upstream tables have fresh data:
```
CheckUpstreamFreshness(schema, table, hours_threshold)
→ Returns is_fresh: true/false + list of stale dependencies
```

### 3. Impact Analysis
Find all downstream tables affected by a change:
```
GetDownstreamDependencies(schedule, schema, table, max_depth)
→ Returns list of impacted tables
```

### 4. Lineage Tracking
Update timestamp after successful execution to track data freshness:
```
UpdateNodeTimestamp(schema, table)
→ Marks table as fresh with current timestamp
```

## Troubleshooting

### Proto generation fails

Make sure you have protoc installed:
```bash
brew install protobuf
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
```

### Neo4j connection errors

Check that Neo4j is running and accessible:
```bash
docker-compose ps neo4j
docker-compose logs neo4j
```

Test connection with cypher-shell:
```bash
docker exec -it continuo-neo4j-1 cypher-shell -u neo4j -p atlas_password
```

### gRPC connection refused

Ensure the service is running:
```bash
ps aux | grep graph
lsof -i :50052
```

## License

Internal use only.

# Executor-Controller Service

A Go microservice that consumes K8s deployment events from Redis, deploys Kubernetes Jobs, and updates task statuses using the reliable outbox pattern.

## Overview

The executor-controller service:
1. Consumes events from Redis streams `query.model:v1` (startup-controller) and `task.retry:v1` (retry path)
2. Deduplicates events using the `processed_events` table (exactly-once processing)
3. Writes deployment intents to a PostgreSQL outbox table (transactional)
4. Background processor deploys K8s Jobs (idempotent)
5. Updates task_tracker to "running" via state-service gRPC
6. Publishes JobDeployed events to `executor.deployed:v1` stream

## Architecture

```
Redis (query.model:v1 / task.retry:v1)
        ↓
   Consumer → dedup check (processed_events)
        ↓
   DeployHandler → PostgreSQL (deployment_outbox)
        ↓
OutboxProcessor (background)
    ├── K8s Job Creation (idempotent)
    ├── Task Status Update (gRPC)
    └── Event Publishing (Redis executor.deployed:v1)
```

See [DATA_FLOW.md](./DATA_FLOW.md) for detailed data flow diagrams.

### Key Features
- **Outbox Pattern**: Ensures reliable deployment with exactly-once semantics
- **Deduplication**: `processed_events` table prevents double-processing of retried messages
- **Idempotent Deployments**: Prevents duplicate K8s Jobs
- **Retry Logic**: Automatic retries with configurable max attempts
- **Crash Recovery**: Pending deployments resumed on restart; un-ACKed messages reclaimed
- **Transaction Safety**: `FOR UPDATE SKIP LOCKED` prevents race conditions

## Build & Run

### Prerequisites
- Go 1.25.1+
- PostgreSQL (for deployment_outbox table)
- Redis (for event streaming)
- Kubernetes cluster (for job deployment)
- Docker (for tests with testcontainers)

### Using Makefile

```bash
# Build the service
make build

# Run locally
make run

# Run unit tests (no Docker required)
make test-unit

# Run all tests (requires Docker)
make test

# Format code
make fmt

# Run linter
make vet

# Clean build artifacts
make clean

# Download dependencies
make deps
```

### Available Make Targets

| Target | Description | Requirements |
|--------|-------------|--------------|
| `build` | Compile binary to `bin/executor-controller` | Go |
| `run` | Run service locally | All deps |
| `test-unit` | Run unit tests with fakes | None |
| `test` | Run all tests | Docker |
| `test-integration` | Run integration tests | Docker |
| `test-coverage` | Generate coverage report | Docker |
| `test-race` | Run tests with race detector | Docker |
| `fmt` | Format code | Go |
| `vet` | Run go vet | Go |
| `lint` | Run golangci-lint | golangci-lint |
| `clean` | Remove build artifacts | None |
| `deps` | Download and tidy dependencies | Go |

## Configuration

Environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `REDIS_HOST` | localhost | Redis host |
| `REDIS_PORT` | 6379 | Redis port |
| `REDIS_CONSUMER_STREAM` | query.model:v1 | Primary input stream |
| `REDIS_CONSUMER_RETRY_STREAM` | task.retry:v1 | Retry input stream |
| `REDIS_PRODUCER_STREAM` | executor.deployed:v1 | Output stream |
| `REDIS_CONSUMER_GROUP` | executor_controller_consumers | Consumer group |
| `POSTGRES_HOST` | localhost | PostgreSQL host |
| `POSTGRES_PORT` | 5432 | PostgreSQL port |
| `POSTGRES_DB` | continuo_executor | Database name |
| `POSTGRES_USER` | runner | Database user |
| `POSTGRES_PASSWORD` | runner | Database password |
| `STATE_SERVICE_GRPC_ADDR` | localhost:50051 | State service address |
| `K8S_NAMESPACE` | default | K8s namespace |
| `HTTP_PORT` | 8084 | Health check port |

## Testing

See [test/README.md](test/README.md) for comprehensive testing documentation.

### Quick Start

```bash
# Fast unit tests (no Docker)
make test-unit

# All tests (requires Docker)
make test

# With coverage
make test-coverage
```

### Test Structure
- **Fakes**: Production-like test implementations (preferred over mocks)
- **Unit Tests**: Handler and processor logic
- **Repository Tests**: Real PostgreSQL via testcontainers
- **E2E Tests**: Complete workflows

## Database Schema

The service uses two tables:

```sql
CREATE TABLE deployment_outbox (
    id UUID PRIMARY KEY,
    task_id UUID NOT NULL,
    schedule_id UUID NOT NULL,
    schedule_name VARCHAR(255) NOT NULL,
    service_name VARCHAR(255) NOT NULL,
    schema_name VARCHAR(255) NOT NULL,
    table_name VARCHAR(255) NOT NULL,
    job_name VARCHAR(63) NOT NULL,
    node_type TEXT NOT NULL DEFAULT 'dbt-model',
    status VARCHAR(20) DEFAULT 'pending',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    processed_at TIMESTAMPTZ,
    retry_count INT DEFAULT 0,
    max_retries INT DEFAULT 3,
    error_message TEXT
);
```

`processed_events` tracks which upstream outbox entries have already been handled, preventing double-processing when messages are retried:

```sql
CREATE TABLE processed_events (
    outbox_entry_id UUID PRIMARY KEY
);
```

## Deployment

### Docker Compose

The service is included in `docker-compose.yml`:

```bash
# Start all services
docker-compose up -d

# View logs
docker-compose logs -f executor-controller

# Run tests inside container
docker-compose exec executor-controller make test-unit
```

### Kubernetes

Deploy using the in-cluster K8s config. The service creates Jobs in the configured namespace.

## Monitoring

### Health Checks
- `/health` - Service health status
- `/ready` - Readiness probe

Example:
```bash
curl http://localhost:8084/health
# OK

curl http://localhost:8084/ready
# READY
```

### Logs
Structured JSON logs with slog:
```json
{
  "time": "2024-01-23T15:00:00Z",
  "level": "INFO",
  "msg": "Processing deployment outbox entry",
  "entry_id": "123e4567-e89b-12d3-a456-426614174000",
  "task_id": "789e4567-e89b-12d3-a456-426614174000",
  "job_name": "dbt-public-users"
}
```

## Error Handling

| Error Type | Behavior |
|------------|----------|
| Redis consume failure | Message unACKed, redelivered |
| Outbox write failure | Transaction rollback, no ACK |
| K8s Job creation failure | Retry with backoff |
| Job already exists | Idempotent success |
| gRPC state update failure | Retry (K8s job exists) |
| Redis publish failure | Retry |
| Max retries exceeded | Mark as failed, log error |

## Development

### Project Structure

```
executor-controller/
├── main.go                 # Service entry point
├── Makefile                # Build and test recipes
├── config/                 # Configuration management
├── domain/                 # Domain models, commands, events
├── adapters/               # External integrations
│   ├── grpc/              # State service client
│   ├── http/              # Health endpoints
│   ├── k8s/               # Kubernetes client
│   ├── postgres/          # Database client & repository
│   └── redis/             # Redis consumer & producer
├── service/               # Business logic
│   ├── handlers/          # Command handlers & processors
│   ├── messagebus/        # CQRS message bus
│   └── uow/               # Unit of Work
├── internal/              # Internal utilities
│   └── lifecycle/         # Graceful shutdown
└── test/                  # Test suite
    ├── fakes/             # Test doubles (preferred)
    └── README.md          # Testing documentation
```

### Code Style

- Follow standard Go conventions
- Use `make fmt` before committing
- Run `make vet` to catch issues
- Prefer fakes over mocks in tests
- Use structured logging with slog

## License

Internal project - see organization license.

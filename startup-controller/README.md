# Startup Controller Service

The startup-controller service is responsible for initializing DAG execution by identifying root nodes and preparing them for execution.

## Purpose

This service:
1. Consumes `scheduler.started:v1` events from Redis
2. Queries Neo4j for root nodes (tables with no upstream dependencies)
3. Creates or updates task_tracker entries via state_service gRPC
4. Publishes nodes to `query.model:v1` stream for execution
5. Uses outbox pattern for reliable event publishing

## Architecture

- **CQRS/DDD Architecture**: Commands, events, and handlers
- **Outbox Pattern**: Ensures exactly-once delivery of events to Redis
- **Idempotent**: Safely handles service restarts and retries
- **Crash Recovery**: Automatically resets in-progress initializations on startup

See [DATA_FLOW.md](./DATA_FLOW.md) for detailed data flow diagrams.

## Dependencies

- **Neo4j**: Graph database for querying root nodes
- **PostgreSQL**: Outbox table for reliable event publishing
- **Redis**: Event streaming (consumes scheduler.started, produces query.model events)
- **State Service**: gRPC client for task_tracker operations

## Configuration

Environment variables:

```bash
# Redis
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_CONSUMER_STREAM=scheduler.started:v1
REDIS_CONSUMER_GROUP=startup_controller_consumers
REDIS_PRODUCER_STREAM=query.model:v1

# Neo4j
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=atlas_password

# PostgreSQL
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=continuo_startup
POSTGRES_USER=runner
POSTGRES_PASSWORD=runner

# gRPC
STATE_SERVICE_GRPC_ADDR=localhost:50051

# HTTP
HTTP_PORT=8083
```

## Building

```bash
make build
```

## Running

```bash
make run
```

## Testing

The startup-controller has comprehensive E2E tests validating the complete event-driven workflow.

### Running Tests

**From host machine** (requires docker-compose services running):
```bash
POSTGRES_HOST=localhost NEO4J_HOST=localhost REDIS_HOST=localhost STATE_HOST=localhost make test
```

**From within container** (recommended):
```bash
docker exec startup-controller bash -c "cd /app/startup-controller && make test"
```

**Prerequisites**:
- State service gRPC must be running on port 50051
- PostgreSQL, Neo4j, Redis must be accessible
- Redis databases 1-2 are automatically flushed before tests

**Test Results**: ✅ 2/2 E2E tests passing

### Test Coverage

- **Stream consumption**: Full scheduler.started → query.model flow
- **Outbox pattern**: Transactional event writing and publishing
- **Crash recovery**: Automatic replay of pending outbox entries
- **job_name integration**: Validates job_name is computed and included in events
- **Initialization tracking**: Verifies initialization_status state machine

## Docker

```bash
docker build -f Dockerfile.dev -t startup-controller:dev .
docker run -p 8083:8083 startup-controller:dev
```

## Health Check

- `/health` - Basic health check
- `/ready` - Readiness probe

## Event Flow

1. **Input**: `scheduler.started:v1` event
   ```json
   {
     "runner_id": "uuid",
     "schedule_name": "daily_pipeline"
   }
   ```

2. **Processing**:
   - Query Neo4j for root nodes (tables with no upstream dependencies)
   - For each root node:
     - Create/update task_tracker entry via state service gRPC
     - **job_name automatically computed** (K8s DNS-1123 compliant)
     - Write event to outbox table (transactional)
   - Mark initialization_status as complete
   - OutboxProcessor publishes events to Redis

3. **Output**: `query.model:v1` events (one per root node)
   ```json
   {
     "schedule_id": "uuid",
     "schedule_name": "daily_pipeline",
     "service_name": "analytics",
     "schema": "public",
     "table_name": "users",
     "task_id": "uuid",
     "job_name": "analytics-public-users",
     "node_type": "dbt-model"
   }
   ```

   > **Note**: The `task_id` is retrieved from the task_tracker table in the state service. The `job_name` field is automatically computed following Kubernetes naming conventions (max 63 chars, lowercase alphanumeric + hyphens). The `node_type` field is read from the Neo4j node property and validated against the known types (`dbt-model`, `dbt-seed`, `dbt-snapshot`); nodes with an invalid or missing type are skipped.

## Implementation Details

### Outbox Pattern

The service uses the transactional outbox pattern to ensure reliable event publishing:

1. Write events to PostgreSQL outbox table (atomic with business logic)
2. Background OutboxProcessor polls for pending entries
3. Publish to Redis and mark as processed
4. Retry failed entries with exponential backoff

### Idempotency

The service is idempotent through `initialization_status` tracking in the state service:

- `pending`: Not yet started
- `in_progress`: Currently processing
- `completed`: Finished successfully

On startup, the service calls `ResetInProgressInitializations()` which resets any `in_progress` status back to `pending`. This handles the crash-during-processing scenario. If a scheduler run's status is already `completed`, the handler returns early without re-processing.

### Error Handling

- Failed outbox entries are retried up to 3 times
- Permanently failed entries are marked as 'failed' with error message
- Redis consumer does not ACK failed messages (automatic retry)

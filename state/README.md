# State Service

A gRPC-based state tracking service for scheduler executions and tasks.

## Architecture

- **Pattern**: DDD (Domain-Driven Design)
- **Server**: gRPC with async handlers
- **Database**: PostgreSQL with sqlx
- **Message Queue**: Redis Streams (producer + consumer)
- **Testing**: E2E tests with testcontainers

## Features

- Full CRUD operations for schedulers and tasks
- **Kubernetes-compliant job naming**: Automatic generation of DNS-1123 compliant job names
- **YAML-driven schedule configuration**: Cron schedules loaded from `config/schedules.yaml`
- **Schedule catalog reconciliation**: Consumes `schedules.loaded:v1` events from manifest-controller to keep the active schedule catalog in sync
- Automatic cron-based schedule activation with duplicate-run prevention
- Initialization status tracking for crash recovery
- Cancellation support with audit trails
- Filtering and pagination
- Structured logging with slog
- Graceful shutdown
- Health check endpoint
- **Task rerun functionality**: `TriggerRerun` gRPC method and Redis stream for rerunning failed tasks

## API Endpoints

### gRPC (Port 50051)

**Scheduler Operations:**
- `CreateScheduler` - Create a new scheduler
- `GetScheduler` - Get scheduler by ID
- `UpdateScheduler` - Update scheduler status/timestamps
- `CancelScheduler` - Cancel a scheduler with audit
- `ListSchedulers` - List schedulers with filters
- `ActivateSchedule` - Manually trigger schedule activation
- `ListAllSchedules` - List all schedules from the catalog
- `TriggerSchedule` - Manually trigger a named schedule

**Task Operations:**
- `CreateTask` - Create a new task (requires client-generated UUID)
- `GetTask` - Get task by ID
- `UpdateTask` - Update task status/retry count
- `DeleteTask` - Delete a task
- `ListTasks` - List tasks by schedule with filters

### HTTP (Port 8082)

- `GET /health` - Health check endpoint

## Getting Started

### Prerequisites

- Docker and Docker Compose
- Go 1.25.1+
- protoc (Protocol Buffers compiler)
- grpcurl (for testing gRPC endpoints)

### Running with Docker Compose

1. **Generate Proto Code** (inside Docker container):
```bash
docker-compose exec state bash
cd /app/state
./generate_proto.sh
```

2. **Install Dependencies**:
```bash
docker-compose exec state bash
cd /app/state
go mod download
```

3. **Run Migrations**:
Migrations run automatically via Flyway service in docker-compose.

4. **Start the Service**:
```bash
docker-compose up state
```

The service will be available at:
- gRPC: `localhost:50051`
- Health: `http://localhost:8082/health`

### Testing with grpcurl

**List available services:**
```bash
grpcurl -plaintext localhost:50051 list
```

**Create a scheduler:**
```bash
grpcurl -plaintext -d '{
  "schedule_name": "test-schedule",
  "status": 1
}' localhost:50051 state.v1.StateService/CreateScheduler
```

**Get a scheduler:**
```bash
grpcurl -plaintext -d '{
  "schedule_id": "YOUR-UUID-HERE"
}' localhost:50051 state.v1.StateService/GetScheduler
```

**Cancel a scheduler:**
```bash
grpcurl -plaintext -d '{
  "schedule_id": "YOUR-UUID-HERE",
  "cancelled_by": "admin",
  "cancellation_reason": "Test cancellation"
}' localhost:50051 state.v1.StateService/CancelScheduler
```

**Create a task:**
```bash
grpcurl -plaintext -d '{
  "task_id": "YOUR-TASK-UUID",
  "schedule_id": "YOUR-SCHEDULE-UUID",
  "service_name": "dbt-service",
  "schema_name": "public",
  "table_name": "users",
  "max_retries": 3,
  "status": 1
}' localhost:50051 state.v1.StateService/CreateTask
```

> **Note**: The `job_name` field is automatically computed from `service_name`, `schema_name`, and `table_name` following Kubernetes DNS-1123 naming conventions (max 63 chars, lowercase alphanumeric + hyphens).

**List tasks for a schedule:**
```bash
grpcurl -plaintext -d '{
  "schedule_id": "YOUR-SCHEDULE-UUID",
  "page_size": 10
}' localhost:50051 state.v1.StateService/ListTasks
```

## Running Tests

The state service has comprehensive test coverage across unit, integration, and E2E tests.

### All Tests (Recommended)

```bash
docker exec state bash -c "cd /app/state && make test"
```

This runs:
- **Unit tests**: job_name computation, validation logic
- **Repository tests**: Database operations with testcontainers
- **E2E tests**: Full scheduler activation flow with Redis streams

**Test Results**: ✅ 22/22 tests passing (15 unit + 6 repository + 1 E2E)

### Individual Test Suites

**Unit tests only** (fast, no external dependencies):
```bash
docker exec state bash -c "cd /app/state && make test-unit"
```

**Integration tests** (with testcontainers):
```bash
docker exec state bash -c "cd /app/state && make test-integration"
```

### Test Coverage

- **job_name computation**: 15 test cases covering K8s DNS-1123 validation
- **Repository operations**: CRUD, duplicate keys, nullable fields
- **Scheduler activation**: Cron triggering, duplicate run prevention, Redis publishing

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `GRPC_PORT` | `50051` | gRPC server port |
| `HEALTH_PORT` | `8082` | HTTP health check port |
| `POSTGRES_HOST` | `localhost` | PostgreSQL host |
| `POSTGRES_PORT` | `5432` | PostgreSQL port |
| `POSTGRES_DB` | `runner` | PostgreSQL database |
| `POSTGRES_USER` | `runner` | PostgreSQL user |
| `POSTGRES_PASSWORD` | `runner` | PostgreSQL password |
| `DB_SSLMODE` | `disable` | PostgreSQL SSL mode |
| `DB_POOL_SIZE` | `10` | Connection pool size |
| `DB_MAX_OVERFLOW` | `20` | Max overflow connections |
| `REDIS_ADDR` | `redis:6379` | Redis server address |
| `REDIS_STREAM_SCHEDULER_STARTED` | `scheduler.started:v1` | Stream name for outbound activation events |
| `REDIS_STREAM_SCHEDULES_LOADED` | `schedules.loaded:v1` | Stream name consumed for catalog updates |
| `REDIS_STREAM_RERUN_COMMANDS` | `command.rerun:v1` | Stream name for outbound rerun commands |
| `REDIS_GROUP_SCHEDULE_CATALOG` | `state-schedule-catalog` | Consumer group for catalog events |

## Redis Streams

### scheduler.started:v1 Stream

**Stream:** `scheduler.started:v1`

**Message Format (Redis Stream Entry):**
```json
{
  "runner_id": "58947c36-b234-43a4-bf8f-2dd2755c9140",
  "schedule_name": "daily"
}
```

### Rerun Commands Stream

**Stream:** `command.rerun:v1`

**Message Format (Redis Stream Entry):**
```json
{
  "schedule_id": "uuid",
  "service_name": "string",
  "schema_name": "string",
  "table_name": "string"
}
```

**Downstream Consumers:**
- **startup-controller**: Consumes rerun commands to re-initialize failed tasks
- **executor-service**: May consume rerun commands to directly retry task execution

**Usage:**
The rerun stream enables selective retry of failed tasks without requiring a full schedule restart. When a task fails, callers invoke the `TriggerRerun` gRPC method on `state` (exposed to end-users via the BFF route `POST /api/schedulers/{id}/rerun` on ui-service), which publishes a rerun command to this stream. Downstream services consume these commands and retry the specified tasks.

## Development

### Project Structure

```
state/
├── main.go                          # Application entry point
├── proto/state/v1/state.proto      # gRPC service definition
├── config/config.go                 # Configuration
├── internal/
│   ├── lifecycle/lifecycle.go      # Graceful shutdown
│   ├── scheduler/
│   │   ├── scheduler.go            # Cron scheduler (YAML-driven)
│   │   └── activator.go            # Schedule activation logic
│   └── grpc/
│       ├── server.go               # gRPC server wrapper
│       └── handlers/
│           ├── scheduler_handler.go # Scheduler endpoints
│           └── task_handler.go      # Task endpoints
├── domain/model/model.go           # Domain models
├── adapters/
│   ├── postgres/
│   │   ├── scheduler_repository.go     # Scheduler repo
│   │   ├── task_repository.go          # Task repo
│   │   └── schedule_catalog_repository.go # Schedule catalog repo
│   ├── redis/
│   │   ├── producer.go                 # Publishes scheduler.started:v1
│   │   ├── schedule_catalog_consumer.go # Consumes schedules.loaded:v1
│   │   └── schedule_catalog_handler.go  # Payload processing + idempotency
│   └── http/
│       ├── server.go               # Health server
│       ├── handlers.go             # Health handler
│       └── rerun_handler.go        # Rerun endpoint handler
├── database/database.go            # DB connection
└── test/
    ├── e2e_grpc_test.go           # E2E tests
    └── repository_test.go          # Repository tests
```

### Adding New Endpoints

1. Update `proto/state/v1/state.proto`
2. Regenerate proto code: `./generate_proto.sh`
3. Add handler method
4. Wire up in `internal/grpc/server.go`
5. Add tests

## Troubleshooting

### Proto generation fails

Make sure you're running inside the Docker container with protoc installed:
```bash
docker-compose exec state bash
apt-get update && apt-get install -y protobuf-compiler
./generate_proto.sh
```

### Database connection errors

Check that PostgreSQL is running and migrations have completed:
```bash
docker-compose ps postgres
docker-compose logs flyway
```

### gRPC connection refused

Ensure the service is running and port is exposed:
```bash
docker-compose ps state
docker-compose logs state
```

## Domain Models

### ScheduleCatalog
Tracks the set of known schedule names, reconciled from `schedules.loaded:v1` events published by manifest-controller.

**Fields:**
- `schedule_name` (string) - Primary key; name of the schedule (e.g., "daily", "hourly")
- `deleted_at` (timestamp, nullable) - Set when a schedule is soft-deleted during reconciliation

**Reconciliation logic (on each event):**
1. Upsert all schedule names present in the event payload
2. Soft-delete any existing schedule not included in the new list
3. Record the processed `event_id` in `processed_events` for deduplication

### SchedulerTracker
Tracks the lifecycle of a schedule execution.

**Fields:**
- `schedule_id` (UUID) - Primary key
- `schedule_name` (string) - Name of the schedule (e.g., "daily", "hourly")
- `status` (enum) - Current execution status
- `initialization_status` (enum) - Initialization progress (pending/in_progress/completed/failed)
- `created_at`, `started_at`, `completed_at` (timestamps)
- `cancelled_at`, `cancelled_by`, `cancellation_reason` (audit fields)

**Initialization Status:**
- `pending`: Not yet started
- `in_progress`: Currently processing
- `completed`: Finished successfully
- `failed`: Encountered error

### TaskTracker
Tracks individual table/node execution within a schedule.

**Fields:**
- `task_id` (UUID) - Primary key
- `schedule_id` (UUID) - Foreign key to scheduler_tracker
- `service_name`, `schema_name`, `table_name` (string) - Table identifier
- **`job_name`** (string, max 63 chars) - **Auto-computed Kubernetes job name**
- `status` (enum) - Current task status
- `retry_count`, `max_retries` (int) - Retry configuration
- `cancelled_at`, `cancelled_by` (audit fields)

**job_name Computation:**
- Format: `{service}-{schema}-{table}` (sanitized)
- Rules: Lowercase, alphanumeric + hyphens only, max 63 chars
- Example: `analytics-public-users` → Kubernetes-compliant job name

## Status Enums

**SchedulerStatus:**
- `SCHEDULER_STATUS_PENDING` (1)
- `SCHEDULER_STATUS_RUNNING` (2)
- `SCHEDULER_STATUS_SUCCEEDED` (3)
- `SCHEDULER_STATUS_FAILED` (4)
- `SCHEDULER_STATUS_CANCELLED` (5)

**TaskStatus:**
- `TASK_STATUS_PENDING` (1)
- `TASK_STATUS_RUNNING` (2)
- `TASK_STATUS_SUCCEEDED` (3)
- `TASK_STATUS_FAILED` (4)
- `TASK_STATUS_CANCELLED` (5)

## License

Internal use only.

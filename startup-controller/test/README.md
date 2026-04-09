# End-to-End Tests for Startup Controller

This directory contains comprehensive E2E tests for the startup-controller service.

## Prerequisites

Before running the tests, you need the following services running:

1. **PostgreSQL** (port 5432) with `continuo_test` database
2. **Neo4j** (port 7687)
3. **Redis** (port 6379)
4. **State Service** (gRPC on port 50051)

## Test Database Setup

Create the test database:

```bash
psql -U postgres -c "CREATE DATABASE continuo_test;"
```

Run migrations on the test database:

```bash
# Apply V1, V2, and V3 migrations
flyway migrate -url=jdbc:postgresql://localhost:5432/continuo_test
```

Or manually:

```sql
-- Connect to continuo_test
\c continuo_test

-- Run the migration scripts
\i db/migration/V1__setup.sql
\i db/migration/V2__fix_task_tracker.sql
\i db/migration/V3__add_outbox_and_init_status.sql
```

## Running Tests

### Run all E2E tests:

```bash
cd startup-controller
go test -v ./test/...
```

### Run specific test:

```bash
# Test 1: Consumer flow
go test -v ./test -run TestE2E_StartupController_ConsumeFromStream

# Test 2: Outbox pattern crash recovery
go test -v ./test -run TestE2E_OutboxPattern_CrashRecovery
```

### Skip E2E tests (run only unit tests):

```bash
go test -v -short ./...
```

## Test Scenarios

### Test 1: Startup Controller Consume From Stream

**Purpose**: Verify the main flow of consuming scheduler.started events and publishing to query.model stream.

**Steps**:
1. Setup Neo4j with test data (3 root nodes, 2 downstream nodes with dependencies)
2. Publish `scheduler.started:v1` event with `schedule_name="hourly"`
3. Process the event through the handler
4. Verify outbox entries are created for all 3 root nodes
5. Run outbox processor to publish to Redis
6. Verify all 3 events appear in `query.model:v1` stream
7. Verify all outbox entries are marked as `processed`

**Assertions**:
- ✅ Exactly 3 root nodes are identified (no downstream nodes)
- ✅ Exactly 3 outbox entries are created
- ✅ Exactly 3 events are published to Redis
- ✅ All outbox entries are marked as processed
- ✅ Event payloads contain correct schedule_id, schedule_name, and table_name

### Test 2: Outbox Pattern Crash Recovery

**Purpose**: Verify the outbox pattern handles service crashes and ensures exactly-once delivery.

**Steps**:
1. Setup Neo4j with test data (3 root nodes)
2. Process the command to populate outbox with 3 entries
3. **Simulate partial processing**: Manually process only 1-2 entries
4. Verify partial state (some processed, some pending)
5. **Simulate service restart**: Run outbox processor again
6. Verify ALL entries are now processed
7. Verify ALL 3 events are in Redis (no duplicates)

**Assertions**:
- ✅ After crash: Some entries are processed, some are pending
- ✅ After recovery: All entries are processed
- ✅ No duplicate events in Redis
- ✅ Exactly 3 events in output stream

## Test Data Structure

### Neo4j Graph (Hourly Schedule)

```
Root Nodes (no upstream):
- analytics.public.users
- analytics.public.orders
- analytics.public.products

Downstream Nodes (with dependencies):
- analytics.public.user_stats → DEPENDS_ON → users
- analytics.public.order_stats → DEPENDS_ON → orders
```

### Expected Outbox Entries

```json
[
  {
    "schedule_id": "<uuid>",
    "schedule_name": "hourly",
    "service_name": "analytics",
    "schema": "public",
    "table_name": "users"
  },
  {
    "schedule_id": "<uuid>",
    "schedule_name": "hourly",
    "service_name": "analytics",
    "schema": "public",
    "table_name": "orders"
  },
  {
    "schedule_id": "<uuid>",
    "schedule_name": "hourly",
    "service_name": "analytics",
    "schema": "public",
    "table_name": "products"
  }
]
```

## Troubleshooting

### Tests fail with "connection refused"

Make sure all prerequisite services are running:

```bash
# Check PostgreSQL
psql -U postgres -c "SELECT 1"

# Check Neo4j
curl http://localhost:7474

# Check Redis
redis-cli ping

# Check State Service
grpcurl -plaintext localhost:50051 list
```

### Tests fail with "database not found"

Create the test database:

```bash
psql -U postgres -c "CREATE DATABASE continuo_test;"
```

### Tests fail with "table does not exist"

Run the migrations on the test database (see Test Database Setup above).

### Tests fail with cleanup warnings

This is normal - cleanup warnings appear when test data doesn't exist from previous runs. The tests will still pass.

## CI/CD Integration

To run these tests in CI/CD, use docker-compose to spin up the required services:

```yaml
version: '3.8'
services:
  postgres:
    image: postgres:15
    environment:
      POSTGRES_DB: continuo_test
      POSTGRES_PASSWORD: password
    ports:
      - "5432:5432"

  neo4j:
    image: neo4j:5
    environment:
      NEO4J_AUTH: neo4j/password
    ports:
      - "7687:7687"

  redis:
    image: redis:7
    ports:
      - "6379:6379"

  state:
    build: ./state
    ports:
      - "50051:50051"
    depends_on:
      - postgres
```

Then run:

```bash
docker-compose up -d
sleep 10  # Wait for services to be ready
go test -v ./test/...
docker-compose down
```

## Test Coverage

Current E2E test coverage:

- ✅ Redis stream consumption
- ✅ Neo4j root node query
- ✅ State service gRPC integration
- ✅ Outbox table writes
- ✅ Outbox processor publishing
- ✅ Crash recovery and retry
- ✅ Idempotency (no duplicates)
- ✅ Exactly-once delivery guarantee

## Future Test Enhancements

- [ ] Test with large number of nodes (performance test)
- [ ] Test with failed gRPC calls (error handling)
- [ ] Test with Redis unavailable (retry logic)
- [ ] Test with Neo4j unavailable (error handling)
- [ ] Test concurrent schedule initializations
- [ ] Test scheduler already completed (idempotency)

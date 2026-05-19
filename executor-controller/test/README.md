# Executor-Controller Test Suite

This directory contains a comprehensive test suite for the executor-controller service, following best practices by preferring **fakes over mocks** wherever possible.

## Test Structure

### Fakes (`test/fakes/`)
Production-like implementations of external dependencies that maintain state in memory:

- **`k8s_client.go`** - Fake Kubernetes client with in-memory job storage
- **`state_client.go`** - Fake state service gRPC client that records all updates
- **`redis_producer.go`** - Fake Redis producer that records published messages

**Why Fakes?** Fakes test real behavior and interactions, making tests more maintainable and resistant to implementation changes.

### Unit Tests

#### `outbox_processor_test.go`
Tests the OutboxProcessor background worker:
- ✓ Successful batch processing (K8s + state + Redis)
- ✓ Idempotent K8s job creation
- ✓ Retry logic on K8s failures
- ✓ Retry logic on state service failures
- ✓ Max retries exceeded handling
- ✓ Partial failure scenarios

### Repository Tests (`outbox_repository_test.go`)
Tests database operations using **real PostgreSQL** via testcontainers:
- ✓ Create outbox entries
- ✓ Get pending batch with ordering
- ✓ Batch limits
- ✓ FOR UPDATE SKIP LOCKED verification
- ✓ Mark processed/failed
- ✓ Increment retry count
- ✓ Max retries filtering

**Why Real DB?** Repository tests use testcontainers to spin up real PostgreSQL instances, ensuring SQL queries work correctly against actual database constraints and indexes.

## Running Tests

### Prerequisites
- Docker (for testcontainers)
- Go 1.25.1+

### Run All Tests (Requires Docker)
```bash
cd executor-controller
make test
# OR
go test ./test/... -v
```

### Run Unit Tests with Fakes (No Docker Required)
```bash
# Fast unit tests using fakes - no external dependencies
make test-unit
# OR
go test -v ./test/fakes_test.go
```

### Run Specific Test Suite
```bash
# Fake tests only (no testcontainers, no Docker)
go test ./test/fakes_test.go -v

# Repository/E2E tests (requires Docker)
go test ./test/ -v -run "TestOutboxRepository|TestE2E"

# Repository tests (requires Docker)
go test ./test/ -v -run "TestOutboxRepository"

# E2E tests
go test ./test/ -v -run "TestE2E"
```

### Run Without Long-Running Tests
```bash
go test ./test/... -v -short
```

### Run with Race Detection
```bash
go test ./test/... -v -race
```

## Test Coverage

Run coverage analysis:
```bash
go test ./test/... -coverprofile=coverage.out
go tool cover -html=coverage.out
```

## Key Testing Patterns

### 1. Fake Over Mock
```go
// ✓ Good: Fake with real behavior
fakeK8s := fakes.NewFakeK8sClient()
fakeK8s.CreateQueryJob(ctx, params)
assert.Contains(t, fakeK8s.GetCreatedJobs(), "default/job-name")

// ✗ Avoid: Mock with expectations
mockK8s.EXPECT().CreateJob(gomock.Any(), gomock.Any()).Times(1)
```

### 2. Real Database for Repository Tests
```go
// ✓ Good: Real PostgreSQL via testcontainers
db, cleanup := setupPostgres(t)
defer cleanup()

repo := postgres.NewOutboxRepository(db, logger)
// Test against real SQL queries and constraints
```

### 3. Table-Driven Tests
Consider adding table-driven tests for complex scenarios:
```go
tests := []struct {
    name string
    // ... test cases
}{
    {name: "success case", ...},
    {name: "failure case", ...},
}
for _, tt := range tests {
    t.Run(tt.name, func(t *testing.T) {
        // ... test logic
    })
}
```

## Debugging Tests

### Enable Verbose Logging
```go
logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
    Level: slog.LevelDebug, // Change to Debug
}))
```

### Inspect Fake State
```go
// Check what jobs were created
jobs := fakeK8s.GetCreatedJobs()
t.Logf("Created jobs: %+v", jobs)

// Check task updates
updates := fakeState.GetTaskUpdates()
t.Logf("Task updates: %+v", updates)

// Check published messages
msgs := fakeProducer.GetPublishedMessages()
t.Logf("Published messages: %+v", msgs)
```

### Testcontainers Troubleshooting
If testcontainers fail:
1. Ensure Docker is running
2. Check Docker socket permissions
3. On macOS: Use `127.0.0.1` instead of `localhost`
4. Increase container startup timeout if needed

## Continuous Integration

Tests are designed to run in CI environments:
- Testcontainers work with Docker-in-Docker
- No external dependencies required
- Isolated test databases per test
- Graceful cleanup on test failure

## Future Enhancements

Potential areas for additional testing:
- [ ] Concurrent outbox processing with multiple workers
- [ ] Chaos testing (random failures)
- [ ] Performance/load testing
- [ ] Metric collection validation
- [ ] Log output verification

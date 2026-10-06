.PHONY: build run test test-unit test-integration test-ci fmt vet lint clean deps

# Build the service
build:
	go build -o bin/execution-controller .

# Run the service locally
run:
	go run main.go

# Run all tests against an externally migrated, empty executor test database.
# See test/README.md for POSTGRES_* configuration and isolation requirements.
test:
	@echo "Running all tests..."
	@echo "NOTE: Database tests require an empty, externally migrated executor database"
	go test -v ./... -timeout 5m

# Run unit tests only with fakes (no Docker required)
test-unit:
	@echo "Running unit tests with fakes (no Docker required)..."
	go test -v ./test/fakes_test.go -timeout 2m

# Run integration tests against the externally migrated executor database.
test-integration:
	@echo "Running integration tests..."
	REQUIRE_TEST_DEPS=1 go test -tags integration -v ./... -timeout 5m

# Format code
fmt:
	go fmt ./...

# Run go vet
vet:
	go vet ./...

# Run linter
lint:
	golangci-lint run

# Clean build artifacts
clean:
	rm -rf bin/

# Download dependencies
deps:
	go mod download
	go mod tidy

# Run full test suite with coverage (used in CI)
test-ci:
	go test -v ./... -timeout 5m -coverprofile=/tmp/coverage.out -covermode=atomic -coverpkg=$(shell go list -f '{{if ne .Name "main"}}{{.ImportPath}}{{end}}' ./... | grep . | grep -v '/config' | grep -v '/test/fakes' | paste -sd,)
	@echo "=== Coverage: execution-controller ==="
	@go tool cover -func=/tmp/coverage.out
	@echo "====================================="

# Run tests with race detector
test-race:
	@echo "Running tests with race detector..."
	go test -v -race ./test/... -timeout 5m

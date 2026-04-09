.PHONY: build run test test-ci fmt vet lint clean

# Build the service
build:
	go build -o bin/startup-controller .

# Run the service locally
run:
	go run main.go

# Run all tests (with Redis cleanup)
test:
	@echo "Cleaning Redis test databases..."
	@docker exec continuo-redis-1 redis-cli -n 1 FLUSHDB > /dev/null 2>&1 || true
	@docker exec continuo-redis-1 redis-cli -n 2 FLUSHDB > /dev/null 2>&1 || true
	@echo "Running tests..."
	go test -v ./...

# Run full test suite with coverage (used in CI)
test-ci:
	@docker exec continuo-redis-1 redis-cli -n 1 FLUSHDB > /dev/null 2>&1 || true
	@docker exec continuo-redis-1 redis-cli -n 2 FLUSHDB > /dev/null 2>&1 || true
	go test -v ./... -timeout 5m -coverprofile=/tmp/coverage.out -covermode=atomic -coverpkg=$(shell go list -f '{{if ne .Name "main"}}{{.ImportPath}}{{end}}' ./... | grep . | grep -v '/config' | paste -sd,)
	@echo "=== Coverage: startup-controller ==="
	@go tool cover -func=/tmp/coverage.out
	@echo "==================================="

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

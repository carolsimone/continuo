.PHONY: test test-unit test-integration test-ci build fmt vet lint clean proto

# Generate proto code
proto:
	./generate_proto.sh

# Run all tests
test:
	go test -v ./... -timeout 5m

# Run unit tests only (exclude integration tests)
test-unit:
	go test -v -short ./...

# Run integration tests with testcontainers
test-integration:
	go test -v ./test/... -timeout 5m

# Run full test suite with coverage (used in CI)
test-ci:
	go test -v ./... -timeout 5m -coverprofile=/tmp/coverage.out -covermode=atomic -coverpkg=$(shell go list -f '{{if ne .Name "main"}}{{.ImportPath}}{{end}}' ./... | grep . | grep -v '/api/' | grep -v '/config' | paste -sd,)
	@echo "=== Coverage: graph ==="
	@go tool cover -func=/tmp/coverage.out
	@echo "======================"

# Build the service
build:
	go build -o bin/graph .

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
	rm -rf api/

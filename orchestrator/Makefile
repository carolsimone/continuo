.PHONY: test test-unit test-integration test-ci build fmt vet lint clean

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
	go test -v ./... -timeout 5m -coverprofile=/tmp/coverage.out -covermode=atomic -coverpkg=$(shell go list -f '{{if ne .Name "main"}}{{.ImportPath}}{{end}}' ./... | grep . | grep -v '/proto/' | grep -v '/config' | paste -sd,)
	@echo "=== Coverage: orchestrator ==="
	@go tool cover -func=/tmp/coverage.out
	@echo "======================"

build:
	go build -o bin/orchestrator .

fmt:
	go fmt ./...

vet:
	go vet ./...

clean:
	rm -rf bin/

.PHONY: test test-ci build fmt vet lint clean

# Run all tests
test:
	go test -v ./... -timeout 5m

# Run full test suite with coverage (used in CI)
test-ci:
	go test -v ./... -timeout 5m -coverprofile=/tmp/coverage.out -covermode=atomic -coverpkg=$(shell go list -f '{{if ne .Name "main"}}{{.ImportPath}}{{end}}' ./... | grep . | grep -v '/config' | grep -v '/test/fakes' | paste -sd,)
	@echo "=== Coverage: k8s-controller ==="
	@go tool cover -func=/tmp/coverage.out
	@echo "================================"

# Build the service
build:
	go build -o bin/k8s-controller .

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

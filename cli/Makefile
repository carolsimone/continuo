.PHONY: build test test-unit test-integration install fmt vet clean

# Build the static binary.
build:
	CGO_ENABLED=0 go build -o bin/continuo ./cmd/continuo

# All tests except integration (matches sibling-service convention).
test: test-unit

test-unit:
	go test -v -short ./...

test-integration:
	go test -v -tags=integration ./... -timeout 2m

# Install to $GOBIN (falls back to ~/go/bin) so humans can run `continuo`.
install:
	go install ./cmd/continuo

fmt:
	go fmt ./...

vet:
	go vet ./...

clean:
	rm -rf bin/

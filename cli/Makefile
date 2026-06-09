.PHONY: build test test-unit test-integration install fmt vet clean

# cli is deliberately outside the parent go.work so service Docker builds
# don't need to COPY cli/ into their contexts. GOWORK=off keeps these
# targets working from inside cli/ regardless of the parent workspace.
export GOWORK=off

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

# Regenerate gRPC stubs from the vendored public .proto contracts under proto/.
# Never generates from a service's source tree — the CLI is a pure client.
# Requires protoc + protoc-gen-go + protoc-gen-go-grpc on PATH.
.PHONY: proto
proto:
	protoc \
	  --go_out=. --go_opt=paths=source_relative \
	  --go-grpc_out=. --go-grpc_opt=paths=source_relative \
	  proto/state/v1/state.proto \
	  proto/orchestrator/v1/orchestrator.proto

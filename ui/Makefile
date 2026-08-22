.PHONY: build dev install test lint fmt clean sync-proto

# Build Docker image
build:
	docker build -t continuo-ui:dev .

# Run in development mode (Vite + Express, requires npm install first)
dev:
	npm run dev

# Install dependencies
install:
	npm ci

# Run tests
test:
	npm test

# Run linter
lint:
	npm run lint

# Format code
fmt:
	npm run fmt

# Clean build artifacts
clean:
	rm -rf dist/ dist-server/ node_modules/

# Sync ui/proto/state.proto with the canonical state proto.
# @grpc/proto-loader reads this file at startup; drift here silently drops
# fields on the wire (see tests/server/proto-wire.test.ts for the guard).
sync-proto:
	cp ../state/proto/state/v1/state.proto proto/state.proto
	diff ../state/proto/state/v1/state.proto proto/state.proto

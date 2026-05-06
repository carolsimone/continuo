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
#
# -p 1 serializes packages: integration tests across packages share a Neo4j
# singleton (:TopologyRoot {id:'singleton'}) and the topology_state Postgres
# row. With the default GOMAXPROCS-wide parallelism, run_repository_test.go's
# TestSnapshotGraph_StampsGeneration cleanup runs REMOVE root.topology_generation
# while ingest_topology_integration_test.go is mid-flight, leaving the read
# in TestIngestTopology_RetiresNodesMissingFromLatestManifestSnapshot with
# nil — caught only when test runtimes overlap, hence the historical flake.
test-ci:
	go test -v -p 1 ./... -timeout 5m -coverprofile=/tmp/coverage.out -covermode=atomic -coverpkg=$(shell go list -f '{{if ne .Name "main"}}{{.ImportPath}}{{end}}' ./... | grep . | grep -v '/proto/' | grep -v '/config' | paste -sd,)
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

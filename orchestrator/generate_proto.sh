#!/bin/bash
set -e
cd "$(dirname "$0")"
echo "Generating proto code..."
mkdir -p api/orchestrator/v1
protoc \
  --go_out=. \
  --go_opt=paths=source_relative \
  --go-grpc_out=. \
  --go-grpc_opt=paths=source_relative \
  proto/orchestrator/v1/orchestrator.proto
mv proto/orchestrator/v1/*.pb.go api/orchestrator/v1/
echo "Proto code generated successfully!"
echo "Generated files:"
echo "  - api/orchestrator/v1/orchestrator.pb.go"
echo "  - api/orchestrator/v1/orchestrator_grpc.pb.go"

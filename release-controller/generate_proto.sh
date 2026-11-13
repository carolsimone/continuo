#!/bin/bash
set -e
cd "$(dirname "$0")"
echo "Generating proto code..."
mkdir -p api/release/v1
protoc \
  --go_out=. \
  --go_opt=paths=source_relative \
  --go-grpc_out=. \
  --go-grpc_opt=paths=source_relative \
  proto/release/v1/release.proto
mv proto/release/v1/*.pb.go api/release/v1/
echo "Proto code generated successfully!"
echo "Generated files:"
echo "  - api/release/v1/release.pb.go"
echo "  - api/release/v1/release_grpc.pb.go"

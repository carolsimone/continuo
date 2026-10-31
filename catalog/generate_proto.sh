#!/bin/bash
set -e
cd "$(dirname "$0")"
echo "Generating proto code..."
mkdir -p api/catalog/v1
protoc \
  --go_out=. \
  --go_opt=paths=source_relative \
  --go-grpc_out=. \
  --go-grpc_opt=paths=source_relative \
  proto/catalog/v1/catalog.proto
mv proto/catalog/v1/*.pb.go api/catalog/v1/
echo "Proto code generated successfully!"
echo "Generated files:"
echo "  - api/catalog/v1/catalog.pb.go"
echo "  - api/catalog/v1/catalog_grpc.pb.go"

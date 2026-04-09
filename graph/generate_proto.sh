#!/bin/bash
# Generate proto code for graph service
# Run this inside the Docker container with protoc installed

set -e

cd "$(dirname "$0")"

echo "Generating proto code..."

# Create api directory if it doesn't exist
mkdir -p api/graph/v1

protoc \
  --go_out=. \
  --go_opt=paths=source_relative \
  --go-grpc_out=. \
  --go-grpc_opt=paths=source_relative \
  proto/graph/v1/graph.proto

# Move generated files to api directory
mv proto/graph/v1/*.pb.go api/graph/v1/

echo "Proto code generated successfully!"
echo "Generated files:"
echo "  - api/graph/v1/graph.pb.go"
echo "  - api/graph/v1/graph_grpc.pb.go"

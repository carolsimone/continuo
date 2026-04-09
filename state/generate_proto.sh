#!/bin/bash
# Generate proto code for state service
# Run this inside the Docker container with protoc installed

set -e

cd "$(dirname "$0")"

echo "Generating proto code..."

protoc \
  --go_out=. \
  --go_opt=paths=source_relative \
  --go-grpc_out=. \
  --go-grpc_opt=paths=source_relative \
  proto/state/v1/state.proto

echo "Proto code generated successfully!"
echo "Generated files:"
echo "  - proto/state/v1/state.pb.go"
echo "  - proto/state/v1/state_grpc.pb.go"

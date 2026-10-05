#!/bin/bash
set -e
cd "$(dirname "$0")"
echo "Generating proto code..."
mkdir -p api/deadletter/v1
protoc \
  --go_out=. \
  --go_opt=paths=source_relative \
  --go-grpc_out=. \
  --go-grpc_opt=paths=source_relative \
  proto/deadletter/v1/deadletter.proto
mv proto/deadletter/v1/*.pb.go api/deadletter/v1/
echo "Proto code generated successfully!"
echo "Generated files:"
echo "  - api/deadletter/v1/deadletter.pb.go"
echo "  - api/deadletter/v1/deadletter_grpc.pb.go"

#!/bin/bash
set -e
cd "$(dirname "$0")"
echo "Generating proto code..."
mkdir -p api/remediation/v1
protoc \
  --go_out=. \
  --go_opt=paths=source_relative \
  --go-grpc_out=. \
  --go-grpc_opt=paths=source_relative \
  proto/remediation/v1/remediation.proto
mv proto/remediation/v1/*.pb.go api/remediation/v1/
echo "Proto code generated successfully!"
echo "Generated files:"
echo "  - api/remediation/v1/remediation.pb.go"
echo "  - api/remediation/v1/remediation_grpc.pb.go"

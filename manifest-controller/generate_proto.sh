#!/usr/bin/env bash
set -e

PROTO_SRC="../graph/proto"
OUT_DIR="."

uv run --extra dev python -m grpc_tools.protoc \
  -I"${PROTO_SRC}" \
  --python_out="${OUT_DIR}/proto" \
  --grpc_python_out="${OUT_DIR}/proto" \
  graph/v1/graph.proto

uv run --extra dev python -c "from pathlib import Path; p = Path('proto/graph/v1/graph_pb2_grpc.py'); text = p.read_text(); text = text.replace('from graph.v1 import graph_pb2 as graph_dot_v1_dot_graph__pb2', 'from proto.graph.v1 import graph_pb2 as graph_dot_v1_dot_graph__pb2'); text = text.replace('but the generated code in graph/v1/graph_pb2_grpc.py depends on', 'but the generated code in proto/graph/v1/graph_pb2_grpc.py depends on'); p.write_text(text)"

echo "Proto files generated in proto/graph/v1/"

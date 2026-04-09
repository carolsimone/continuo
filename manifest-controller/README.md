# manifest-controller

A Python service that consumes `update.graph:v1` Redis Stream events, parses dbt `manifest.json` files, resolves upstream table dependencies via sqlglot, maps each resource's `resource_type` to a `NodeType` (`dbt-model`, `dbt-seed`, `dbt-snapshot`), loads every node into the graph gRPC service, and publishes a `schedules.loaded:v1` event so the state service can reconcile its schedule catalog.

## Architecture

Follows the DDD/hexagonal pattern used across the monorepo:

```
manifest-controller/
├── config/          # Environment-based configuration
├── domain/          # Domain models (ManifestNode, NodeRegistry, UpstreamDep)
├── service/         # Business logic (parser, resolver, manifest handler)
├── adapters/
│   ├── redis/
│   │   ├── consumer.py    # Redis Stream consumer (update.graph:v1)
│   │   └── publisher.py   # Redis Stream producer (schedules.loaded:v1)
│   ├── grpc/        # Graph service gRPC client
│   └── filesystem/  # CSV-based node registry repository
├── proto/           # Generated protobuf stubs (from graph/proto)
└── tests/           # pytest test suite
```

**Flow:** Redis event → parse manifest.json → persist registry CSV → resolve upstream deps via sqlglot → map `resource_type` → `NodeType` → `GraphService.CreateNode` for each node (with `node_type`) → publish `schedules.loaded:v1` with discovered schedule names → ACK input message.

## Tech Stack

- Python 3.12, uv
- sqlglot — SQL parsing for upstream dependency resolution
- redis-py — Redis Streams consumer with at-least-once delivery
- grpcio + protobuf — gRPC client for the graph service

## dbt-core Constraints

All SQL models must reference tables with fully qualified `schema.table` names (e.g. `public.orders`). Unqualified references (e.g. bare `orders`) cause the manifest-controller to raise `UnqualifiedTableReferenceError` during upstream dependency resolution, and the node will be rejected.

## Running Tests

Inside the container:

```bash
docker exec continuo-manifest-controller-1 uv run pytest -v
```

## Configuration

| Env var | Default | Description |
|---|---|---|
| `REDIS_URL` | `redis://redis:6379` | Redis connection URL |
| `REDIS_STREAM` | `update.graph:v1` | Stream name to consume |
| `REDIS_GROUP` | `manifest-controller` | Consumer group name |
| `SCHEDULES_LOADED_STREAM` | `schedules.loaded:v1` | Stream name for outbound schedule catalog events |
| `GRAPH_GRPC_ADDR` | `graph:50052` | Graph service gRPC address |
| `REGISTRY_PATH` | `/data/registry.csv` | Path to the node ownership registry CSV |
| `MANIFESTS_BASE` | `/manifests` | Base directory where manifest files are mounted |
| `S3_ENDPOINT_URL` | `http://localstack:4566` | S3-compatible endpoint (localstack in dev) |
| `S3_BUCKET` | `continuo` | S3 bucket containing manifest files |
| `S3_ENV` | `local` | Environment label used to prefix S3 object keys |

## Proto Generation

The gRPC stubs are compiled from `graph/proto/graph/v1/graph.proto`. To regenerate:

```bash
cd manifest-controller
./generate_proto.sh
```

Note: the generated `proto/graph/v1/graph_pb2_grpc.py` import path is patched to use `proto.graph.v1` instead of the default `graph.v1`.

## Starting the Service

The container runs `tail -f /dev/null` by default (dev mode). To start the consumer:

```bash
docker exec continuo-manifest-controller-1 uv run python main.py
```

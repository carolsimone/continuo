import os

REDIS_URL       = os.getenv("REDIS_URL", "redis://redis:6379")
REDIS_STREAM    = os.getenv("REDIS_STREAM", "update.graph:v1")
REDIS_GROUP     = os.getenv("REDIS_GROUP", "manifest-controller")
GRAPH_GRPC_ADDR = os.getenv("GRAPH_GRPC_ADDR", "graph:50052")
REGISTRY_PATH   = os.getenv("REGISTRY_PATH", "/data/registry.csv")
MANIFESTS_BASE  = os.getenv("MANIFESTS_BASE", "/manifests")

S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL", "http://localstack:4566")
S3_BUCKET       = os.getenv("S3_BUCKET", "continuo")
S3_ENV          = os.getenv("S3_ENV", "local")

SCHEDULES_LOADED_STREAM = os.getenv("SCHEDULES_LOADED_STREAM", "schedules.loaded:v1")

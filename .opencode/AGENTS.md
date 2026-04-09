# Structure
This is a monorepo with multiple services. 

At the moment:
* `state`
* `graph`
* `startup-controller`
* `executor-controller`
* `dependency-controller`
* `k8s-controller`
* `manifest-controller` — Python 3.12/uv service (not Go); consumes `update.graph:v1` Redis Stream events, batch-loads all dbt manifest.json files from `/manifests` (mounted from `dbt/services/`), resolves cross-service upstream deps via sqlglot, and loads nodes into the graph gRPC service. Run tests with `docker exec manifest-controller uv run pytest -v`. Start the process manually (container runs `tail -f /dev/null` by default): `docker exec -d manifest-controller bash -c "cd /app && PYTHONPATH=/app/proto uv run python main.py > /tmp/mc.log 2>&1"`.
These are the services that are actually part of the project.

Fundamentally, in terms of architecture we use event-driven design and CQRS, always keeping things aligned with
DDD philosophy. I believe CQRS is only applicable when the service has various consumer and producer components;
otherwise simply DDD is enough, like in the `state` service.

Services should be Go based service, find `Dockerfile.dev` in the service folders if you need to know how to build them.
The exception is `manifest-controller`, which is Python 3.12 and uses uv for dependency management.
All the other services should, more or less, use a similar stack but with different dependencies.
Remove any dependency that is not needed.

## How to run it
All must run against the docker compose infrastructure. I use macOS M4 and colima to be precise.
All should be added to the `docker-compose.yml` file at the root of the project.

**Basic:**
* Build the base image first: `DOCKER_BUILDKIT=1 docker build -t continuo-base:latest -f Dockerfile.base .`
* Run `bash scripts/setup.sh`: this creates a `kind` cluster, builds all service images via `docker compose build`,
  and dumps the kubeconfig into the service directories that need it for Kubernetes API calls.
* You need to access the service/image using docker. Once you are in the container, you can run all the
  `tests`. Tests must pass successfully.
* Abide by the Go coding standards.
* `pkg` folder should contain all the logic shared between services.

**Running the e2e test:**

First-time setup (if the environment is not yet running):
```bash
DOCKER_BUILDKIT=1 docker build -t continuo-base:latest -f Dockerfile.base .
bash scripts/setup.sh
```

On subsequent runs (environment already up), rebuild changed images and restart services:
```bash
DOCKER_BUILDKIT=1 docker-compose build
docker-compose up -d
```

Start the Go services inside their containers (required before running tests):
```bash
bash e2e/start-services.sh
```

Then run the e2e test:
```bash
docker exec -e UI_HTTP_BASE=http://ui:8090 startup-controller go test -v -timeout 10m /app/e2e/...
```

To watch a specific service's logs while the test runs (in a separate terminal):
```bash
docker logs -f <service-name>   # e.g. docker logs -f dependency-controller
```

Note: services run with `tail -f /dev/null` by default (hot-reload dev mode); `start-services.sh` launches `go run main.go` inside each container.

**Troubleshooting — neo4j fails to start ("Neo4j is already running"):** This happens when colima is stopped while neo4j is running, leaving a stale `store_lock` in the `neo4j_data` volume or a stale PID state in the container layer. Fix:
```bash
docker run --rm -v continuo_neo4j_data:/data alpine rm -f /data/databases/store_lock
docker-compose rm -f neo4j
docker-compose up -d --force-recreate neo4j
```

### Running E2E Tests
Please refer to: `e2e/README.md`.

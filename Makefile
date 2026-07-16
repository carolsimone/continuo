# Use docker compose plugin if available (CI), fall back to standalone binary (macOS/Colima)
DOCKER_COMPOSE := $(shell docker compose version > /dev/null 2>&1 && echo "docker compose" || echo "docker-compose")

# Build base image (run once or when base changes)
.PHONY: build-base
build-base:
	DOCKER_BUILDKIT=1 docker build -t continuo-base:latest -f Dockerfile.base .

# Build all dev images (depends on base)
.PHONY: build-dev
build-dev: build-base
	DOCKER_BUILDKIT=1 $(DOCKER_COMPOSE) build

# Build single service for rapid iteration
.PHONY: build-service
build-service:
	@test -n "$(SERVICE)" || (echo "Usage: make build-service SERVICE=state" && exit 1)
	DOCKER_BUILDKIT=1 $(DOCKER_COMPOSE) build $(SERVICE)

# Rebuild base from scratch (rarely needed)
.PHONY: rebuild-base
rebuild-base:
	DOCKER_BUILDKIT=1 docker build --no-cache -t continuo-base:latest -f Dockerfile.base .

# Existing build target updated
.PHONY: build
build: build-dev

# Build all production images
.PHONY: build-prod
build-prod: build-base
	DOCKER_BUILDKIT=1 docker build -t continuo-state:prod -f state/Dockerfile.prod .
	DOCKER_BUILDKIT=1 docker build -t continuo-executor-controller:prod -f executor-controller/Dockerfile.prod .
	DOCKER_BUILDKIT=1 docker build -t continuo-k8s-controller:prod -f k8s-controller/Dockerfile.prod .
	DOCKER_BUILDKIT=1 docker build -t continuo-orchestrator:prod -f orchestrator/Dockerfile.prod .
	DOCKER_BUILDKIT=1 docker build -t continuo-release-controller:prod -f release-controller/Dockerfile.prod .
	DOCKER_BUILDKIT=1 docker build -t continuo-agent-runner:prod -f agent-runner/Dockerfile.prod .
	DOCKER_BUILDKIT=1 docker build -t continuo-remediation:prod -f remediation/Dockerfile.prod .
	DOCKER_BUILDKIT=1 docker build -t continuo-remediation-agent:prod -f remediation-agent/Dockerfile.prod .

# Build single production service
.PHONY: build-prod-service
build-prod-service:
	@test -n "$(SERVICE)" || (echo "Usage: make build-prod-service SERVICE=state" && exit 1)
	DOCKER_BUILDKIT=1 docker build -t continuo-$(SERVICE):prod -f $(SERVICE)/Dockerfile.prod .

up:
	$(DOCKER_COMPOSE) up

down:
	$(DOCKER_COMPOSE) down --remove-orphans --volumes

typecheck:
	uv run mypy .

lint:
	uv run ruff check . --fix

format:
	uv run ruff format .

.PHONY: lint-go
lint-go:  ## Run golangci-lint over Go modules (all, or MODULE=<name>)
	@bash scripts/lint-go.sh $(MODULE)

.PHONY: e2e-setup
e2e-setup:  ## Provision K8s test environment for E2E testing
	@echo "Setting up K8s controllers..."
	@bash tests/e2e/provision-k8s-test-env.sh

.PHONY: e2e-test
e2e-test: e2e-setup  ## Run E2E tests (assumes docker-compose up and e2e-start-services already done)
	@echo "Running E2E tests..."
	@docker exec -e UI_HTTP_BASE=http://ui:8090 orchestrator go test -v -count=1 -timeout 25m /app/tests/e2e/...
	@$(MAKE) e2e-cleanup

.PHONY: e2e-cleanup
e2e-cleanup:  ## Cleanup K8s controllers
	@echo "Cleaning up K8s controllers..."
	@bash tests/e2e/cleanup-k8s-controllers.sh

.PHONY: e2e-start-services
e2e-start-services:  ## Start Go services inside docker-compose containers and wait for health
	@echo "Starting services..."
	@bash tests/e2e/start-services.sh

# Remove dangling images, stopped containers, unused networks, and build cache.
# Safe to run between test runs — does NOT delete named volumes or the kind cluster.
.PHONY: prune
prune:
	docker container prune -f
	docker image prune -f
	docker network prune -f
	docker builder prune -f

# Full reset: tears down compose (including volumes), deletes the kind cluster,
# and runs a full Docker system prune. Use when disk is critically low or you
# want a completely clean slate before re-running setup.sh.
.PHONY: nuke
nuke:
	$(DOCKER_COMPOSE) down --remove-orphans --volumes || true
	kind delete cluster --name continuo || true
	docker system prune -af --volumes

.PHONY: e2e-full
e2e-full:  ## Complete E2E test from a running docker-compose env (up -d + start-services + test + cleanup)
	@echo "Starting docker-compose services..."
	@$(DOCKER_COMPOSE) up -d
	@echo "Waiting for neo4j and redis to become healthy..."
	@$(DOCKER_COMPOSE) up -d --wait --no-recreate neo4j redis
	@echo "Waiting for flyway migrations to complete..."
	@for svc in flyway-state flyway-executor flyway-orchestrator flyway-k8s flyway-release flyway-agent-runner flyway-remediation flyway-remediation-agent; do \
		cid=$$($(DOCKER_COMPOSE) ps -q $$svc 2>/dev/null); \
		if [ -n "$$cid" ]; then docker wait $$cid 2>/dev/null || true; fi; \
	done
	@echo "Flyway migrations done."
	@$(MAKE) e2e-start-services
	@echo "Building dbt-base image (required for e2e-setup)..."
	@DOCKER_BUILDKIT=1 docker build -t dbt-base:latest dbt/base/
	@echo "Building validation-runner image (required for e2e-setup)..."
	@DOCKER_BUILDKIT=1 docker build -t validation-runner:latest validation-runner/
	@$(MAKE) e2e-setup
	@docker exec -e UI_HTTP_BASE=http://ui:8090 orchestrator go test -v -count=1 -timeout 25m /app/tests/e2e/...
	@$(MAKE) e2e-cleanup

# ── CI contract: SINGLE entrypoints used identically by local dev and CI jobs.
GO_SERVICES := state orchestrator executor-controller k8s-controller \
               release-controller remediation remediation-agent agent-runner
FLYWAY_JOBS := flyway-state flyway-executor flyway-orchestrator flyway-k8s flyway-release \
               flyway-agent-runner flyway-remediation flyway-remediation-agent

# Data dependencies for Go tests: Postgres+Neo4j+Redis up and migrated. No service
# images are built. Tests reach these via POSTGRES_HOST=localhost.
.PHONY: test-deps-up
test-deps-up:
	$(DOCKER_COMPOSE) up -d postgres neo4j redis
	@for f in $(FLYWAY_JOBS); do $(DOCKER_COMPOSE) up $$f; done

.PHONY: test-deps-down
test-deps-down:
	$(DOCKER_COMPOSE) rm -fsv postgres neo4j redis $(FLYWAY_JOBS) 2>/dev/null || true

# Connection parameters for the local test Postgres brought up by test-deps-up.
# They mirror docker-compose.yml. Declared once here so every consumer below —
# the per-service env vars and the DSN derived from them — cannot drift apart.
PG_HOST := localhost
PG_PORT := 5432
PG_USER := continuo_svc
PG_PASSWORD := continuo
PG_SSLMODE := disable

# Connection parameters for the local test Neo4j brought up by test-deps-up.
# They mirror NEO4J_AUTH in docker-compose.yml. Every Neo4j-backed test skips
# itself when it cannot connect, so each of these being wrong buys a green run
# that executed nothing:
#   - NEO4J_URI defaults to the compose hostname (bolt://neo4j:7687) in-test,
#     which does not resolve from the host, so it must name the published port.
#   - NEO4J_USER/NEO4J_PASSWORD must match compose. Wrong credentials do more
#     than skip their own package: repeated failed handshakes trip Neo4j's
#     authentication rate limiter, which then locks out the packages testing
#     alongside them and skips those too.
NEO4J_HOST := localhost
NEO4J_BOLT_PORT := 7687
NEO4J_URI := bolt://$(NEO4J_HOST):$(NEO4J_BOLT_PORT)
NEO4J_USER := neo4j
NEO4J_PASSWORD := atlas_password

# Run go tests for one service (SERVICE=x) or all. Brings deps up first; points
# DB-backed tests at the local Postgres/Neo4j with the FULL per-service env.
# pkgconfig.LoadPostgres requires POSTGRES_DB/USER/PASSWORD and defaults
# DB_SSLMODE; each service uses its own database continuo_<svc>.
# release-controller's DB-backed tests connect via a DSN instead of the discrete
# vars, so RELEASE_TEST_PG_DSN is derived from the same parameters; without it
# those tests skip themselves and report a green run having executed nothing.
# Includes -tags integration (closes the coverage gap). On macOS add
# TESTCONTAINERS_RYUK_DISABLED=true; on CI Linux runners RYUK works, so omit it.
.PHONY: test-go
test-go: test-deps-up
	@rc=0; for s in $(or $(SERVICE),$(GO_SERVICES)); do \
	  case $$s in \
	    state) db=continuo_state;; orchestrator) db=continuo_orchestrator;; \
	    executor-controller) db=continuo_executor;; k8s-controller) db=continuo_k8s;; \
	    release-controller) db=continuo_release;; remediation) db=continuo_remediation;; \
	    remediation-agent) db=continuo_remediation_agent;; agent-runner) db=continuo_agent;; \
	    *) echo "unknown service $$s" >&2; exit 2;; \
	  esac; \
	  echo "== go test $$s (db=$$db) =="; \
	  (cd $$s && POSTGRES_HOST=$(PG_HOST) POSTGRES_PORT=$(PG_PORT) POSTGRES_DB=$$db \
	     POSTGRES_USER=$(PG_USER) POSTGRES_PASSWORD=$(PG_PASSWORD) DB_SSLMODE=$(PG_SSLMODE) \
	     NEO4J_HOST=$(NEO4J_HOST) NEO4J_URI=$(NEO4J_URI) \
	     NEO4J_USER=$(NEO4J_USER) NEO4J_PASSWORD=$(NEO4J_PASSWORD) \
	     RELEASE_TEST_PG_DSN="postgres://$(PG_USER):$(PG_PASSWORD)@$(PG_HOST):$(PG_PORT)/$$db?sslmode=$(PG_SSLMODE)" \
	     go test -tags integration -count=1 ./... -timeout 20m) || rc=1; \
	done; exit $$rc

.PHONY: test-ui
test-ui:
	cd ui-service && npm ci && npm test

.PHONY: test-manifest
test-manifest:
	DOCKER_BUILDKIT=1 $(DOCKER_COMPOSE) build manifest-controller
	$(DOCKER_COMPOSE) up -d manifest-controller
	docker exec manifest-controller uv run pytest -v

.PHONY: guards
guards:
	bash scripts/check-ci-alignment.sh
	cd pkg && go test ./...
	diff state/proto/state/v1/state.proto ui-service/proto/state.proto

.PHONY: stack-up
stack-up:
	bash scripts/setup.sh

.PHONY: e2e
e2e:
	bash tests/e2e/start-services.sh
	$(DOCKER_COMPOSE) up -d ui
	bash -c 'source scripts/lib/common.sh && wait_for_http_host http://localhost:8090/api/schedulers'
	bash tests/e2e/deploy-k8s-controllers.sh
	docker exec -e UI_HTTP_BASE=http://ui:8090 orchestrator go test -v -count=1 -timeout 25m /app/tests/e2e/...
	bash tests/e2e/cleanup-k8s-controllers.sh

# Reproduce the whole pipeline locally (the "will CI pass?" gate).
.PHONY: ci
ci: guards test-go test-ui test-manifest stack-up e2e
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

.PHONY: e2e-setup
e2e-setup:  ## Provision K8s test environment for E2E testing
	@echo "Setting up K8s controllers..."
	@bash tests/e2e/provision-k8s-test-env.sh

.PHONY: e2e-test
e2e-test: e2e-setup  ## Run E2E tests (assumes docker-compose up and e2e-start-services already done)
	@echo "Running E2E tests..."
	@docker exec -e UI_HTTP_BASE=http://ui:8090 startup-controller go test -v -count=1 -timeout 10m /app/tests/e2e/...
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
	@for svc in flyway-state flyway-startup flyway-executor flyway-orchestrator flyway-k8s; do \
		cid=$$($(DOCKER_COMPOSE) ps -q $$svc 2>/dev/null); \
		if [ -n "$$cid" ]; then docker wait $$cid 2>/dev/null || true; fi; \
	done
	@echo "Flyway migrations done."
	@$(MAKE) e2e-start-services
	@echo "Building dbt-base image (required for e2e-setup)..."
	@DOCKER_BUILDKIT=1 docker build -t dbt-base:latest dbt/base/
	@$(MAKE) e2e-setup
	@docker exec -e UI_HTTP_BASE=http://ui:8090 startup-controller go test -v -count=1 -timeout 10m /app/tests/e2e/...
	@$(MAKE) e2e-cleanup
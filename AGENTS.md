# Structure
This is a monorepo with multiple microservices.

## Go services (8)
* `state` — owns run lifecycle state (pending → running → finalized) and schedule records; the authoritative write-path for task and run transitions.
* `orchestrator` — owns Neo4j topology and run projections, Postgres outbox/dedup. Consumes `node.updated:v1`, `scheduler.started:v1`, `release.promoted:v1`, `trigger.rerun:v1`, `trigger.rebase:v1`, `trigger.single_node_run:v1`, `trigger.promoted_seeds:v1`, `run.finalized:v1`, `schedule.cancelled:v1`, `remediation.requested:v2`, `remediation.pr_opened:v1`, `remediation.pr_closed:v1`. Produces `query.model:v1`, `schedules.loaded:v1`. Serves gRPC `OrchestratorQuery` for UI reads.
* `executor-controller` — schedules dbt task execution; emits K8s Job specs and publishes execution events downstream.
* `k8s-controller` — watches Kubernetes Jobs and surfaces their terminal status back into the run lifecycle.
* `release-controller` — manages blue/green candidate-release lifecycle; tracks the `current_prod` pointer and drives promotion/rejection.
* `remediation` — failure classifier; triages a rejected release's failing nodes one by one, records every decision, and emits ONE `remediation.requested:v2` heal trigger per (release, remediation round) carrying every fixable failure.
* `agent-remediation` — LLM fix-proposer; works a whole rejected release at a time. Receives one batched heal trigger, groups the failing set (same error signature + a shared changed ancestor become one cluster fixed at that ancestor), calls the LLM once per cluster, and reads source from GitHub (read-only) for compile/seed_build/duplicate_table failures and primarily from the release's code bundle in S3 for validation failures (falling back to GitHub only on a permanent bundle miss); also reads narrow graph context (source location, upstream diffs, current version, failure precedent) from orchestrator. Every fix is verified by a real verification run per edited service before it is offered, and one attempt yields one proposal and one fix PR for human approval.
* `agent-chat` — chat and agent gRPC backend; hosts the conversational LLM interface used by the UI.

## Python service (1)
* `topology-controller` — Python 3.12/uv service (not Go); consumes `release.requested:v1` Redis Stream events, batch-loads the release's dbt manifest.json files, resolves cross-service upstream deps via sqlglot, and publishes the resolved candidate topology to `manifest.loaded.candidate:v1` for release-controller (which promotes it into the orchestrator's Neo4j topology via `release.promoted:v1`). Run tests with `docker exec topology-controller uv run pytest -v`. Start the process manually (container runs `tail -f /dev/null` by default): `docker exec -d topology-controller bash -c "cd /app && PYTHONPATH=/app/proto uv run python main.py > /tmp/mc.log 2>&1"`.

## Node service (1)
* `ui` — HTTP API and web UI; serves the operator dashboard and proxies gRPC reads from `orchestrator` and other backend services.

## Supporting pieces (not long-running services)
* `pkg/` — shared Go library; stream constants, domain models, and utilities consumed by all Go services.
* `cli/` — the `continuo` CLI; a separate Go module (outside `go.work`) that talks to services exclusively via their public gRPC interfaces.
* `tests/e2e/` — end-to-end test harness; spins up the full stack and exercises cross-service flows.
* `migrations/` (`db/`) — Flyway SQL migrations for all service databases.
* `dbt/base`, `dbt/services/*` — dbt base image and per-service dbt project images used by executor-controller K8s Jobs.

# CLI (`cli/`)
The `continuo` CLI is a standalone client of the system, intended primarily for the LLM chat to call. It is a separate Go module (`github.com/carolsimone/continuo/cli`), deliberately kept outside the parent `go.work`, and emits machine-readable JSON to stdout (human text to stderr).

Hard constraint: the CLI may consume services **only through their public gRPC interfaces**. It must never import or reuse any backend internal implementation — no `state/*`, `orchestrator/*`, `pkg/*`, or any domain/service/adapter package. Its only dependencies are cobra, grpc, protobuf, and the generated stubs of the public `.proto` contracts vendored under `cli/proto/`. New behavior composes public RPCs client-side; it does not reach into service internals, and even at gen-time the stubs are regenerated from the vendored `.proto` copy, never from a service's source tree.

LLM-friendliness is a requirement, not a nicety. The CLI is consumed primarily by an LLM, so it must be self-describing:
- Every command MUST populate `Short`, a full `Long` (purpose phrased as user intent → arguments → stdout JSON shape → the `CLIError` codes / exit codes it can return), at least one `Example`, and a clear `Usage` on every flag. Machine-readable extras go in cobra `Annotations`: `output_schema` (success-payload field names + types) and `exit_codes`.
- `continuo describe` emits a machine-readable catalog of every command, **derived** from the cobra tree (never a hand-maintained manifest). Adding a command means it appears in `describe` automatically; a test enforces that every command carries a non-empty `Long` and an `Example`, and that each `output_schema` matches the JSON the command actually emits.

# Architecture
This repository follows an event-driven, DDD-oriented architecture using Clean Architecture boundaries.
Domain code must stay independent of infrastructure concerns. Databases, Redis, gRPC, Kubernetes, S3, framework code, and serialization details belong in adapters. Application and domain code should depend on ports/interfaces, not concrete infrastructure implementations.
Use CQRS only when it provides a clear benefit: separate write and read models, projections, asynchronous event consumers/producers, or different consistency/read requirements. Simpler services should use a straightforward DDD/application-service structure instead of unnecessary CQRS.
Handlers should be thin and delegate to application/use-case services. Use repositories and other ports to express persistence and external dependencies. Keep adapter implementations behind those ports.
Apply SOLID, clean code, repository, service-layer, and design-pattern practices pragmatically. Prefer clear, testable, explicit code over abstraction for its own sake.
Shared cross-service logic belongs in `pkg`; service-specific logic belongs inside the owning service.

## Port ownership
A port is owned by the innermost layer whose vocabulary it speaks, and adapters only *implement* ports — they never declare them. Place ports as follows, and have the `adapters/*` package implement them (with a `var _ Port = (*impl)(nil)` assertion):
- **Domain repository ports** — collection-like abstractions over an aggregate (e.g. `RunRepository`, `CancelledSchedulesRepository`) live in `<service>/domain/repository`.
- **Technical / application ports** — collaborators that are not domain concepts (e.g. `LogUploader`, `OutboxPublisher`, `Clock`) live in `<service>/service/ports`. The `UnitOfWork` interface stays in `<service>/service/uow`.
- **Concrete implementations** — including every `*UnitOfWork` — live in `<service>/adapters/*`.

Rules:
- The dependency arrow always runs adapter → port, never application → adapter. Code under `<service>/service/handlers` must import no `adapters/*` package; reach every collaborator through a port owned by the application/domain layer. This is enforced by the AST guard `TestServiceHandlersDoNotImportAdapters` in `pkg/streams/handler_imports_test.go` — add new services to its `handlerDirs`.
- An interface declared in an adapter package and consumed *only by other adapters* (e.g. a gRPC/Neo4j client seam) is adapter-internal and may stay there; the rule targets application→adapter inversion, not adapter-to-adapter wiring.

# Stream and consumer-group names
Every Redis stream and consumer group is declared in `pkg/streams/contract.yaml`. A Go generator emits `pkg/streams/streams.gen.go` (`streams.QueryModelV1`, `streams.RetryTaskV1`, `streams.ExecutorRetry`, etc.) and `topology-controller/streams_contract.py` for Python.

The same file's `vocabularies:` block declares the closed value sets several services agree on (`parse_failure_kind`, `reject_reason`). The same generator run emits those into the **shared domain packages**, not the transport ones: `pkg/domain/model/vocabulary.gen.go` (`model.ParseFailureKind`, `model.RejectReason`, each with `IsValid()`/`Healable()`) and `topology-controller/domain/contract_vocabulary.py`. Domain, application and adapter code names a contract value from there.

`pkg/streams` and `streams_contract` hold stream and consumer-group names only, and **no `*/domain` package imports either** — enforced by `TestDomainPackagesDoNotImportStreams` in `pkg/streams/domain_imports_test.go` (Go) and `topology-controller/tests/test_domain_imports_no_streams_contract.py` (Python).

Rules:
- Never inline a versioned stream name (`"query.model:v1"`) or a service-prefixed consumer-group name in Go, Python, or tests. Always reference the constant from `pkg/streams` (Go) or `streams_contract` (Python).
- This applies to production code, integration tests, unit-test fixtures, and adapter bindings (`message_processing.stream_name` values must come from the constant, not a local `const fooStreamName = "foo:v1"`).
- The AST wiring detector in `pkg/streams/wiring_test.go` rejects hardcoded versioned-stream literals in service `main.go` files; new occurrences in handlers, bindings, or tests should be removed for the same reason.
- Adding a new stream, group, or vocabulary value means editing `pkg/streams/contract.yaml`, regenerating (`go generate ./pkg/streams/...`), and committing the regenerated files. CI's `go generate && git diff --exit-code` check enforces freshness.
- The AST guard `TestLifecycleGoNeverWrapsAServerStart` in `pkg/streams/lifecycle_wiring_test.go` discovers every service's `main.go` by glob and fails if a blocking-server method (`Start`/`Serve`/`ListenAndServe`/`ListenAndServeTLS`/`Run`) is called, inside a `lifecycleManager.Go(...)` tracked goroutine, on a receiver that is also stopped by a `RegisterShutdownHandler(...)` closure in the same file — that combination deadlocks the drain step of graceful shutdown, since the server can only return after the shutdown handler that follows the drain runs.

# Install-level configuration must reach the code
A property of the operator's environment — warehouse engine, region, auth mode, timezone, an external component's version — is declared once in `deploy/continuo/values.yaml` and must travel from there to every process that acts on it. Two halves of the same bug, both of which have shipped here:

- **A hardcoded literal.** An environment-dependent value baked into code (`dialect="postgres"`, a region, a port, a schema name) works for whoever wrote it and is wrong for every other install.
- **A declared value that stops at the chart.** A key that only feeds a Helm template — picking an image, naming a Secret — makes the system *look* configurable while every service still runs the author's default. This is the harder half to see: grepping the key name finds `values.yaml`, and you conclude it is wired.

Rules:
- A values key that describes the user's environment must reach the services that act on it — normally as a key on the shared ConfigMap in `templates/configmap.yaml`, mirrored into `docker-compose.yml` and any other deployment path. If it is genuinely install-time-only (image selection, Secret naming), that is fine, but say so in the values comment so the next reader need not re-derive it.
- Services **fail closed** at startup on a value they cannot honour, rather than degrading to a default mid-operation. `_helpers.tpl` already fails an install on an unsupported `validation.engine`; the service must refuse to boot rather than emit another engine's output.
- Where the same set of allowed values is enumerated in two places, pin them to each other with a test (`test_supported_engines_match_the_chart` in `topology-controller/tests/test_config.py`), so the chart cannot start accepting a value the code cannot honour.
- When you remove a hardcoded literal, sweep every site of that concept rather than only the one that was flagged, and separate the **read** sites from the **write/generate** sites. The generate site is usually the real defect: its output leaves the process as an artifact another system executes, so a wrong value there surfaces far from its cause. `topology-controller/tests/test_dialect_guard.py` is the forcing function for the SQL-dialect case; add the equivalent guard when you fix a new instance of this class.

# Architecture documentation
The architecture pack under `docs/arch/` is part of the working agreement for this repository.
Before completing any task that changes service behavior, interfaces, storage ownership, Redis flows, gRPC interactions, Kubernetes behavior, or S3 usage, the LLM must review and update the relevant files in `docs/arch/`.
Do not consider a task complete until the architecture documentation has been reconciled with the implemented code changes.

**Arch docs describe the system's current working only — present tense, self-contained.** They are held to the same standard as code comments (see the IMPORTANT rules below): a new reader with no prior knowledge must understand what the system does today from the text alone. So:
- No references to past PRs, past edits, migrations-as-history, or a change ("previously", "used to", "was renamed from", "after the cutover", "we removed"). State what is true now. Describing a value that is *currently* absent is fine when it reads as current state ("the projection stores only X"), not as a change narrative.
- No future/roadmap framing ("will be added later", "planned", "TODO"). Habitual/runtime "will" ("the reconciler emits once a release answers") is fine.
- No cross-references that only resolve relative to an edit or another section's numbering — e.g. citing "Step 3a" outside the numbered flow that defines it. Name the mechanism (the function, the field) instead.
- Deprecated/removed features are simply absent, not documented as history.

Services should be Go based service, find `Dockerfile.dev` in the service folders if you need to know how to build them.
The exception is `topology-controller`, which is Python 3.12 and uses uv for dependency management.
All the other services should, more or less, use a similar stack but with different dependencies.
Remove any dependency that is not needed.

# Helm chart versioning (`deploy/continuo/`)
`release.yml` locks the chart's `version` and `appVersion` together — one `vX.Y.Z` git tag stamps both identically and retags every service image to match, so there is no chart-vs-app compatibility matrix to track. The only thing chart versioning has to protect is: **does an unmodified existing user's `values.yaml`/overrides still produce a working install after `helm upgrade` to the new version?**

Whenever a PR changes `deploy/continuo/values.yaml`, `values.schema.json`, `Chart.yaml`, or `templates/**`:
- **Update `values.schema.json` to match.** `additionalProperties: false` is set throughout on purpose — adding, renaming, or removing a values key without updating the schema fails `helm lint` (`scripts/install-test/lint.sh`), which is the intended forcing function.
- **Add an entry under `## [Unreleased]`** in `deploy/continuo/CHANGELOG.md` (Added/Changed/Removed/Breaking, [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) format). CI (`scripts/install-test/changelog-check.sh`, wired into `install-test.yml`) fails a PR that touches the chart's user-facing surface without this.
- **Decide the semver bump** the change implies, scoped to the values contract rather than internal service APIs:
  - PATCH — template/default fixes, no values schema change.
  - MINOR — new values added with safe defaults; existing overrides keep working unmodified.
  - MAJOR/Breaking — anything that breaks an unmodified existing values file on upgrade: a renamed/removed key, a previously-optional value becoming required, an immutable field changing (Service selector, PVC size, StatefulSet volumeClaimTemplates), or dropping support for an older `kubeVersion`.
- **If a value becomes newly required**, add an `{{ if .Release.IsUpgrade }}` warning to `templates/NOTES.txt` telling operators what to set — the changelog is easy to miss, but NOTES.txt is what actually prints at `helm upgrade` time.
- Validate with `bash scripts/install-test/lint.sh` (helm lint + template + kube-linter across all four values topologies) before pushing.

# How to run it
Everything needs to run against docker images within my host machine. I use MacOS M4 and colima to be precise.
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
Please read: `tests/e2e/README.md`.

# Deployment
Continuo deploys as a Helm chart; see `deploy/README.md` for install modes and values.
Provisioning the infrastructure a production install runs against, and obtaining cluster
credentials for it, are managed outside this repository.

# IMPORTANT
* NEVER create a branch directly (no `git checkout -b`, `git branch`, `git switch -c`, or any equivalent). Branch ONLY by creating a git worktree (via the using-git-worktrees skill or `git worktree add`), which branches off `origin/main`. Creating branches off the local checkout drags in un-pushed local commits and messes up my workflow.
* As the very last step before finishing a branch and pushing the last commit it, LLM must:
  * only merge to main from a PR.
  * run e2e tests (read tests/e2e/README.md for how to do that).
  * update `docs/arch/*` documentations.
  * whenever you find edge cases on the logic and you solve the problem, let's build a proper test to avoid this issue will resurface again.
* Comments in the code must be reflecting what the code does, not referring to PRs, deprecated features, or other irrelevant information. A new user reading the code must understand what the code does without having prior knowledge.

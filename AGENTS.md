# Structure
This is a monorepo with multiple microservices.

Service at the moment are:
* `state`
* `orchestrator` — merged replacement for the former `graph` and `dependency-controller` services. Owns Neo4j topology and run projections, Postgres outbox/dedup. Consumes `node.updated:v1`, `manifest.loaded:v1`, `initialize.run:v1`. Produces `query.model:v1`, `schedules.loaded:v1`, `run.initialized:v1`, `rerun.ready:v1`. Serves gRPC `OrchestratorQuery` for UI reads.
* `executor-controller`
* `k8s-controller`
* `manifest-controller` — Python 3.12/uv service (not Go); consumes `update.graph:v1` Redis Stream events, batch-loads all dbt manifest.json files from `/manifests` (mounted from `dbt/services/`), resolves cross-service upstream deps via sqlglot, and publishes topology to `manifest.loaded:v1` for the orchestrator. Run tests with `docker exec manifest-controller uv run pytest -v`. Start the process manually (container runs `tail -f /dev/null` by default): `docker exec -d manifest-controller bash -c "cd /app && PYTHONPATH=/app/proto uv run python main.py > /tmp/mc.log 2>&1"`.

# Architecture
This repository follows an event-driven, DDD-oriented architecture using Clean Architecture boundaries.
Domain code must stay independent of infrastructure concerns. Databases, Redis, gRPC, Kubernetes, S3, framework code, and serialization details belong in adapters. Application and domain code should depend on ports/interfaces, not concrete infrastructure implementations.
Use CQRS only when it provides a clear benefit: separate write and read models, projections, asynchronous event consumers/producers, or different consistency/read requirements. Simpler services should use a straightforward DDD/application-service structure instead of unnecessary CQRS.
Handlers should be thin and delegate to application/use-case services. Use repositories and other ports to express persistence and external dependencies. Keep adapter implementations behind those ports.
Apply SOLID, clean code, repository, service-layer, and design-pattern practices pragmatically. Prefer clear, testable, explicit code over abstraction for its own sake.
Shared cross-service logic belongs in `pkg`; service-specific logic belongs inside the owning service.

# graphify
This project has a graphify knowledge graph at graphify-out/.

Rules:
- Before answering architecture or codebase questions, read graphify-out/GRAPH_REPORT.md for god nodes and community structure
- If graphify-out/wiki/index.md exists, navigate it instead of reading raw files
- After modifying code files in this session, run `python3 -c "from graphify.watch import _rebuild_code; from pathlib import Path; _rebuild_code(Path('.'))"` to keep the graph current

# Architecture documentation
The architecture pack under `docs/arch/` is part of the working agreement for this repository.
Before completing any task that changes service behavior, interfaces, storage ownership, Redis flows, gRPC interactions, Kubernetes behavior, or S3 usage, the LLM must review and update the relevant files in `docs/arch/`.
Do not consider a task complete until the architecture documentation has been reconciled with the implemented code changes.

Services should be Go based service, find `Dockerfile.dev` in the service folders if you need to know how to build them.
The exception is `manifest-controller`, which is Python 3.12 and uses uv for dependency management.
All the other services should, more or less, use a similar stack but with different dependencies.
Remove any dependency that is not needed.

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

# Deployment in Server (or debugging server issues)
* You can access the server by running: `ssh continuo-server`. Now you are in the server ubuntu machine.

* LLM can run kubectl/helm locally against the remote cluster, in this way:
  1. Fetch the k3s kubeconfig: 
     `scp continuo-server:/etc/rancher/k3s/k3s.yaml ~/.kube/hetzner-continuo.yaml`
  2. Patch the server URL (k3s sets it to 127.0.0.1 by default):
    `sed -i '' "s|server: https://127.0.0.1:6443|server: https://<HETZNER_HOST>:6443|" ~/.kube/hetzner-continuo.yaml`
  3. Test locally:
   `KUBECONFIG=~/.kube/hetzner-continuo.yaml kubectl get nodes`

# IMPORTANT
* As the very last step before finishing a branch and pushing the last commit it, LLM must:
  * only merge to main from a PR.
  * run e2e tests (read tests/e2e/README.md for how to do that).
  * update `docs/arch/*` documentations.
  * whenever you find edge cases on the logic and you solve the problem, let's build a proper test to avoid this issue will resurface again.
* Comments in the code must be reflecting what the code does, not referring to PRs, deprecated features, or other irrelevant information. A new user reading the code must understand what the code does without having prior knowledge.

<!-- rtk-instructions v2 -->
# RTK (Rust Token Killer) - Token-Optimized Commands

## Golden Rule

**Always prefix commands with `rtk`**. If RTK has a dedicated filter, it uses it. If not, it passes through unchanged. This means RTK is always safe to use.

**Important**: Even in command chains with `&&`, use `rtk`:
```bash
# ❌ Wrong
git add . && git commit -m "msg" && git push

# ✅ Correct
rtk git add . && rtk git commit -m "msg" && rtk git push
```

## RTK Commands by Workflow

### Build & Compile (80-90% savings)
```bash
rtk cargo build         # Cargo build output
rtk cargo check         # Cargo check output
rtk cargo clippy        # Clippy warnings grouped by file (80%)
rtk tsc                 # TypeScript errors grouped by file/code (83%)
rtk lint                # ESLint/Biome violations grouped (84%)
rtk prettier --check    # Files needing format only (70%)
rtk next build          # Next.js build with route metrics (87%)
```

### Test (90-99% savings)
```bash
rtk cargo test          # Cargo test failures only (90%)
rtk vitest run          # Vitest failures only (99.5%)
rtk playwright test     # Playwright failures only (94%)
rtk test <cmd>          # Generic test wrapper - failures only
```

### Git (59-80% savings)
```bash
rtk git status          # Compact status
rtk git log             # Compact log (works with all git flags)
rtk git diff            # Compact diff (80%)
rtk git show            # Compact show (80%)
rtk git add             # Ultra-compact confirmations (59%)
rtk git commit          # Ultra-compact confirmations (59%)
rtk git push            # Ultra-compact confirmations
rtk git pull            # Ultra-compact confirmations
rtk git branch          # Compact branch list
rtk git fetch           # Compact fetch
rtk git stash           # Compact stash
rtk git worktree        # Compact worktree
```

Note: Git passthrough works for ALL subcommands, even those not explicitly listed.

### GitHub (26-87% savings)
```bash
rtk gh pr view <num>    # Compact PR view (87%)
rtk gh pr checks        # Compact PR checks (79%)
rtk gh run list         # Compact workflow runs (82%)
rtk gh issue list       # Compact issue list (80%)
rtk gh api              # Compact API responses (26%)
```

### JavaScript/TypeScript Tooling (70-90% savings)
```bash
rtk pnpm list           # Compact dependency tree (70%)
rtk pnpm outdated       # Compact outdated packages (80%)
rtk pnpm install        # Compact install output (90%)
rtk npm run <script>    # Compact npm script output
rtk npx <cmd>           # Compact npx command output
rtk prisma              # Prisma without ASCII art (88%)
```

### Files & Search (60-75% savings)
```bash
rtk ls <path>           # Tree format, compact (65%)
rtk read <file>         # Code reading with filtering (60%)
rtk grep <pattern>      # Search grouped by file (75%)
rtk find <pattern>      # Find grouped by directory (70%)
```

### Analysis & Debug (70-90% savings)
```bash
rtk err <cmd>           # Filter errors only from any command
rtk log <file>          # Deduplicated logs with counts
rtk json <file>         # JSON structure without values
rtk deps                # Dependency overview
rtk env                 # Environment variables compact
rtk summary <cmd>       # Smart summary of command output
rtk diff                # Ultra-compact diffs
```

### Infrastructure (85% savings)
```bash
rtk docker ps           # Compact container list
rtk docker images       # Compact image list
rtk docker logs <c>     # Deduplicated logs
rtk kubectl get         # Compact resource list
rtk kubectl logs        # Deduplicated pod logs
```

### Network (65-70% savings)
```bash
rtk curl <url>          # Compact HTTP responses (70%)
rtk wget <url>          # Compact download output (65%)
```

### Meta Commands
```bash
rtk gain                # View token savings statistics
rtk gain --history      # View command history with savings
rtk discover            # Analyze Claude Code sessions for missed RTK usage
rtk proxy <cmd>         # Run command without filtering (for debugging)
rtk init                # Add RTK instructions to CLAUDE.md
rtk init --global       # Add RTK to ~/.claude/CLAUDE.md
```

## Token Savings Overview

| Category | Commands | Typical Savings |
|----------|----------|-----------------|
| Tests | vitest, playwright, cargo test | 90-99% |
| Build | next, tsc, lint, prettier | 70-87% |
| Git | status, log, diff, add, commit | 59-80% |
| GitHub | gh pr, gh run, gh issue | 26-87% |
| Package Managers | pnpm, npm, npx | 70-90% |
| Files | ls, read, grep, find | 60-75% |
| Infrastructure | docker, kubectl | 85% |
| Network | curl, wget | 65-70% |

Overall average: **60-90% token reduction** on common development operations.
<!-- /rtk-instructions -->
# Contributing to Continuo

Thanks for your interest. Continuo is maintained by one person, so please read this
before opening a large pull request — it will save us both time.

## Before you start

For anything beyond a bug fix or a docs correction, **open an issue first** and describe
what you want to change. Continuo has strong architectural conventions (below), and a
pull request that cuts across them is painful to land no matter how good the code is.

## Licensing and sign-off

Continuo is licensed under the Apache License 2.0. Contributions are accepted under the
same license.

We use the [Developer Certificate of Origin](DCO) — a short statement that you wrote the
code, or otherwise have the right to submit it. You agree to it by adding a sign-off line
to each commit:

```
Signed-off-by: Your Name <your.email@example.com>
```

`git commit -s` adds this for you. To sign off a branch you already wrote:

```bash
git rebase --signoff origin/main
git push --force-with-lease
```

A CI check enforces this on every pull request. There is no separate agreement to sign.

We do **not** use per-file license headers. The root `LICENSE` covers the whole
repository; please do not add headers to new files.

## Development setup

Prerequisites: Docker (or [colima](https://github.com/abiosoft/colima)), `kind`,
`kubectl`, Helm 3.14+, Go 1.25+, Node 20, and [uv](https://docs.astral.sh/uv/) for the
Python services.

```bash
# Build the shared base image, then bring up the whole stack
DOCKER_BUILDKIT=1 docker build -t continuo-base:latest -f Dockerfile.base .
bash scripts/setup.sh
```

`scripts/setup.sh` creates a `kind` cluster, builds every service image, and writes the
kubeconfig into the service directories that need Kubernetes API access. It also runs
`scripts/ensure-dev-env.sh`, which generates a throwaway `.env` — **no real credentials
are needed for local development**, and none should ever be committed.

Run a service's tests inside its container, for example:

```bash
docker exec manifest-controller uv run pytest -v
```

For the end-to-end suite, see [tests/e2e/README.md](tests/e2e/README.md).

## Architecture conventions

These are enforced by tests, not just by review. [AGENTS.md](AGENTS.md) has the full set;
these are the ones that most often trip people up:

- **Clean Architecture boundaries.** Domain code must not know about Postgres, Redis,
  gRPC, Kubernetes, or S3 — those belong in `adapters/`. Application code depends on
  ports, never on a concrete adapter. `TestServiceHandlersDoNotImportAdapters` in
  `pkg/streams/handler_imports_test.go` fails the build otherwise.
- **Port ownership.** Domain repository ports live in `<service>/domain/repository`;
  technical ports live in `<service>/service/ports`. Adapters *implement* ports and never
  declare them.
- **Stream names are generated, never inlined.** Every Redis stream and consumer group is
  declared in `pkg/streams/contract.yaml`. Add yours there, run
  `go generate ./pkg/streams/...`, and commit the regenerated files. Writing
  `"query.model:v1"` as a literal anywhere — including in tests — fails CI.
- **Shared logic goes in `pkg/`.** Service-specific logic stays in the owning service.
- **Python services use the standard `logging` module**, never `print`, except for
  machine-parsed stdout protocols.
- **Architecture docs are part of the change.** If your pull request alters service
  behavior, interfaces, storage ownership, Redis flows, gRPC surfaces, Kubernetes
  behavior, or S3 usage, update the relevant file under `docs/arch/` in the same pull
  request.

## Before you open a pull request

```bash
scripts/lint-go.sh --ci     # go vet, staticcheck, gosec
make security-scan          # govulncheck, gitleaks, Trivy
```

For `ui-service`: run the **full** `npm test` (tests live in both `src/client/` and
`tests/`, so a scoped run hides failures) and `npm run build`. There is no ESLint config
in this project, so `npm run lint` is expected to fail — ignore it.

If you change the Helm chart under `deploy/continuo/`, you must also update
`deploy/continuo/values.schema.json`, add an entry to `deploy/continuo/CHANGELOG.md`
under `## [Unreleased]`, and pass `bash scripts/install-test/lint.sh`. CI enforces all
three.

## Reporting security issues

Please don't open a public issue — see [SECURITY.md](SECURITY.md).

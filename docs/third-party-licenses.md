# Third-party licenses

Continuo is licensed under the Apache License 2.0. It depends on third-party software
across three ecosystems, and it runs — but does not redistribute — a set of upstream
container images.

**Summary: every dependency Continuo links into a shipped binary or bundle is under a
permissive license.** There is no GPL, LGPL, AGPL, SSPL, CDDL, EPL, or MPL code in any
build artifact.

Generated 2026-07-29. Regenerate with the commands in each section below.

## Go

108 unique third-party modules across the ten Go modules (`state`, `orchestrator`,
`executor-controller`, `k8s-controller`, `release-controller`, `remediation`,
`remediation-agent`, `agent-chat`, `pkg`, `cli`).

| License | Modules |
| --- | --- |
| Apache-2.0 | 60 |
| BSD-3-Clause | 26 |
| MIT | 19 |
| BSD-2-Clause | 2 |
| ISC | 1 |

No copyleft dependencies.

```bash
go install github.com/google/go-licenses@latest
for m in state orchestrator executor-controller k8s-controller release-controller \
         remediation remediation-agent agent-chat pkg cli; do
  (cd "$m" && go-licenses csv ./... 2>/dev/null)
done
```

## npm (`ui`)

277 production packages (`--production`, so build and test tooling is excluded — that
code is not shipped).

| License | Packages |
| --- | --- |
| MIT | 217 |
| Apache-2.0 | 30 |
| ISC | 16 |
| BSD-3-Clause | 12 |
| 0BSD | 1 |
| UNLICENSED | 1 |

No copyleft dependencies. The `0BSD` package is `tslib`, which is permissive. The
`UNLICENSED` entry is `continuo-ui` itself — this repository's own package, covered by
the root `LICENSE`, not a third-party dependency.

```bash
cd ui && npm ci
npm exec --yes -- license-checker@25.0.1 --production --summary
```

## Python

`manifest-controller` is the only Python 3.12 service in this repository, managed
with uv. The python-node runtime and validation stack (the
`continuo-python-runtime-<engine>` images the Helm chart pulls) is built and
released from the separate `continuo-python-runtime` repository, so its Python
dependencies — including `continuo-engine-contract` 0.7.0, the PyPI-published
engine-adapter contract, published from that same repository on the same `v*` tag
that publishes the images — are covered by that repository's own third-party
licenses, not this document.

| Package | License | Used by |
| --- | --- | --- |
| `sqlglot` | MIT | manifest-controller |
| `redis` | MIT | manifest-controller |
| `grpcio` | Apache-2.0 | manifest-controller |
| `protobuf` | BSD-3-Clause | manifest-controller |
| `boto3` | Apache-2.0 | manifest-controller |

```bash
pip install pip-licenses
pip-licenses --format=markdown --packages sqlglot redis grpcio protobuf boto3
```

## Container images Continuo runs but does not redistribute

The Helm chart's optional quickstart mode starts upstream datastore images. Continuo
pulls and runs these as separate processes; it does not link against, embed, modify, or
redistribute their code. Their licenses therefore do not extend to Continuo or to your
use of it.

| Component | Image | License |
| --- | --- | --- |
| PostgreSQL | `postgres:18.3` | PostgreSQL License (permissive) |
| Redis | `redis:8.6.4` | AGPLv3 / RSALv2 / SSPLv1 (tri-licensed) |
| Neo4j | `neo4j:5.26.28-community` | GPLv3 |
| MinIO | `minio/minio` | AGPLv3 |
| Dex | `dexidp/dex:v2.41.1` | Apache-2.0 |

Three of these are copyleft. If your organisation's policy prohibits running them, use
the external datastore mode (`external*` / `existingSecret` values) and bring your own —
that is the supported production configuration in any case. See
[deploy/README.md](../deploy/README.md).

## Notes on how this was scoped

- The npm figures are production-only. Development dependencies are not shipped in the
  `ui` image, so their licenses do not affect distribution.
- The Go figures exclude this repository's own modules. Every Go module directory carries
  a copy of the root `LICENSE`, so a consumer running
  `go get github.com/carolsimone/continuo/pkg` receives the license with the module.
- dbt itself (`dbt-core`, `dbt-postgres`) is Apache-2.0 and is installed into the dbt job
  images, not into any Continuo service image.

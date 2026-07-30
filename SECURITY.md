# Security Policy

## Reporting a vulnerability

**Please do not open a public issue for security vulnerabilities.**

Report privately through GitHub's
[private vulnerability reporting](https://github.com/carolsimone/continuo/security/advisories/new). 
If you cannot use that form, email carolini.simone@gmail.com instead. Include enough detail to
reproduce the problem: affected component, version or commit, and the steps or
input that trigger it. A proof of concept helps but is not required.

What to expect:

| Stage | Target |
| --- | --- |
| Acknowledgement of your report | 3 working days |
| Initial assessment and severity | 10 working days |
| Fix or documented mitigation | depends on severity, communicated in the assessment |

You will be credited in the release notes for the fix unless you prefer to stay
anonymous. Please give us a chance to ship a fix before disclosing publicly.

## Supported versions

Continuo is pre-1.0. Only the latest release receives security fixes; there are
no backports to earlier tags.

| Version | Supported |
| --- | --- |
| Latest release | Yes |
| Everything earlier | No |

## Scope

In scope — anything that lets someone cross a trust boundary the system is
supposed to enforce:

- Authentication or authorisation bypass in `ui-service`, including the operator
  role checks that gate run triggering, release promotion, and PR creation.
- Any path that lets an unapproved change reach production, since the release
  pipeline's core promise is that promotion is human-gated.
- Injection into generated dbt commands or SQL.
- Escaping the Kubernetes Job sandbox that executes dbt models.
- Leaking credentials through logs, events, API responses, or run artifacts.

Out of scope:

- Findings that require an already-compromised operator account or cluster
  admin access.
- Vulnerabilities in dependencies with no reachable call path from this code.
  `scripts/security-scan.sh vuln` is the arbiter of reachability.
- Denial of service through resource exhaustion on a self-hosted deployment you
  control.
- Missing hardening headers or similar findings with no demonstrated impact.

## How we check our own code

These run in CI on every pull request and weekly on a schedule
(`.github/workflows/security.yml`). Each is also runnable locally through the
same script CI uses:

```bash
make security-scan            # everything
make security-scan SCAN=vuln  # one scanner
```

| Check | Tool | Blocks a merge |
| --- | --- | --- |
| Go vulnerabilities, reachability-filtered | `govulncheck` | Yes |
| Committed secrets | `gitleaks` | Yes |
| Dependency CVEs | `trivy filesystem` | No — advisory |
| Dockerfile and Kubernetes misconfiguration | `trivy config` | No — advisory |
| Dependency freshness | Dependabot, weekly grouped PRs | n/a |

The two advisory scans report HIGH and CRITICAL findings without failing the
build. Base-image and transitive CVEs are frequently unfixable upstream, so
gating on them would block every merge behind an ignore-list edit rather than
producing a fix. Dependabot is what actually resolves them.

## Known advisories we have assessed as not reachable

`govulncheck` filters Go findings by reachability automatically. npm has no
equivalent, so where we ship a dependency with an open advisory we record the
analysis here rather than leaving you to wonder.

**GHSA-qwww-vcr4-c8h2 — `react-router` (high).** *RSC Mode CSRF Bypass Allows
Action Execution Before 400 Response*, affecting `>=7.12.0 <8.3.0`. `ui-service`
ships `react-router-dom@7.18.2`, which is in range, so `npm audit` reports it.

It is not reachable here. The advisory is specific to React Server Components
mode. `ui-service` uses plain declarative client-side routing — `BrowserRouter`
in `src/client/App.tsx`, with no `createBrowserRouter`, no `RouterProvider`, and
no react-router usage anywhere under `src/server/`. There is no code path that
reaches the vulnerable behaviour.

We are moving to `react-router` v8 regardless, tracked in
[#337](https://github.com/carolsimone/continuo/issues/337). It is not a quick
version bump: `react-router-dom` was discontinued at 7.18.2 and its API moved
into the `react-router` package, so closing this means a deliberate migration
rather than a patch. If you find a path that *does* reach it, please report it
using the process above — we would rather be wrong about this in private.

## Credentials in local development

The docker compose stack talks to local stubs, not real services, and needs no
real credentials. The one value that must be generated rather than defaulted is
`GITHUB_APP_PRIVATE_KEY`: `ui-service` builds its GitHub App client with
octokit's `createAppAuth`, whose constructor rejects a key that is not a valid
PEM. `scripts/ensure-dev-env.sh` generates a throwaway RSA key into a
git-ignored `.env` on first run, so no key material lives in the repository.

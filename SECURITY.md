# Security Policy

## Reporting a vulnerability

**Please do not open a public issue for security vulnerabilities.**

Report them privately to **carolini.simone@gmail.com**. Include enough detail to
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

## Credentials in local development

The docker compose stack talks to local stubs, not real services, and needs no
real credentials. The one value that must be generated rather than defaulted is
`GITHUB_APP_PRIVATE_KEY`: `ui-service` builds its GitHub App client with
octokit's `createAppAuth`, whose constructor rejects a key that is not a valid
PEM. `scripts/ensure-dev-env.sh` generates a throwaway RSA key into a
git-ignored `.env` on first run, so no key material lives in the repository.

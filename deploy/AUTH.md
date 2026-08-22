# ui authentication — operator setup

`ui` is the only HTTP edge of the system. It authenticates every user as
an OpenID Connect (OIDC) relying party and enforces `viewer` / `operator` roles.
There is **no unauthenticated mode**: if `AUTH_MODE=oidc` and the OIDC settings
are missing, the pod fails to boot on purpose (it never serves an open UI).

This document explains how to configure authentication for a deployment, with a
detailed walkthrough of the **Google loopback** setup — the fastest way to run
real authentication with no domain name, suitable for validating the deployment
or for a single operator.

## Auth modes

`AUTH_MODE` (set in the `ui` `env` block of `deploy/continuo/values.yaml`) has
exactly two values:

| Mode | Behavior | When to use |
| --- | --- | --- |
| `oidc` | Real login against an identity provider (IdP). Requires issuer URL, client ID, client secret, and public URL. | Any deployment reachable by more than yourself. |
| `dev` | Injects a fixed `operator` identity for **every** request — no IdP, no login. Logs a loud boot warning. | Local development only, or a deployment that is **not** publicly reachable (see warning below). |

> **What happens if you deploy `oidc` with empty settings:** the container throws
> `AUTH_OIDC_ISSUER_URL is required when AUTH_MODE=oidc` at boot and exits, so the
> pod enters `CrashLoopBackOff`. The `/healthz` readiness probe never passes, the
> ingress has no healthy backend, and the URL returns 502/503. This is the
> intended fail-closed behavior — fix the configuration, don't work around it.

> **`AUTH_MODE=dev` warning:** dev mode makes anyone who can reach the URL a full
> operator (they can trigger, rerun, and cancel runs). It is only safe when the
> service is **not** publicly exposed — i.e. `ingress.enabled: false` and you
> reach it through an SSH tunnel / `kubectl port-forward`. Never point a public
> DNS name at a dev-mode deployment.

---

## Recommended for testing: Google as the IdP over a loopback redirect

Google is a standards-compliant OIDC provider, and its OAuth clients allow
**loopback redirect URIs** (`http://localhost:<port>`). That means you can run
genuine `AUTH_MODE=oidc` against Google with **no domain and no self-hosted IdP**:
you reach `ui` through a `kubectl port-forward` to a local port, and
Google redirects back to that same loopback address after login.

Role assignment uses an **email allowlist** (Google does not emit a `groups`
claim by default). Google asserts `email_verified: true` for verified accounts,
which `ui` requires before honoring the allowlist, so this works cleanly.

### Step 1 — Create the Google OAuth client

1. Open the [Google Cloud Console](https://console.cloud.google.com/) and select
   or create a project.
2. **APIs & Services → OAuth consent screen**:
   - User type: **External**.
   - Fill in the app name and your support email.
   - Under **Test users**, add the Google account(s) you will log in with. While
     the consent screen is in "Testing" status, only these accounts can sign in.
   - The default `openid`, `email`, `profile` scopes are all `ui` needs;
     you do not need to add any.
3. **APIs & Services → Credentials → Create Credentials → OAuth client ID**:
   - Application type: **Web application**.
   - **Authorized redirect URIs** → add exactly:
     ```
     http://localhost:18090/auth/callback
     ```
     This must match `AUTH_PUBLIC_URL` + `/auth/callback` character-for-character
     (scheme, host, port, path). `http://localhost` is permitted by Google for web
     clients — this is the loopback exception that makes the no-domain flow work.
     For a **Web application** client Google matches the port exactly, so register
     the specific port you will forward to (you can add more than one redirect URI
     to the same client).

   > **Choosing the local port.** `18090` here is the **local** port you will
   > forward to; it is your choice, not fixed. Pick any free port — `8090` itself
   > is frequently already taken on a developer machine (a local colima/SSH mux
   > commonly holds it), which is why this guide uses `18090`. Whatever you pick
   > must be identical in all three places: this redirect URI, `AUTH_PUBLIC_URL`,
   > and the local side of the `port-forward`. The container always listens on
   > `8090` internally, so the *remote* side of the `port-forward` stays `8090`.
4. Save, and copy the **Client ID** and **Client secret**.

### Step 2 — Configure continuo

All per-deployment identity settings live under top-level `auth` and are read
from the **secret values file** that the deploy passes with `-f`, which is kept
on the deployment host rather than in this repository. Nothing here goes in
the committed `deploy/continuo/values.yaml` — the chart's `auth.*` defaults are
empty (a deploy fails closed until they are set), and the on-box file deep-merges
real values over them. So you only edit the on-box secret file; add an `auth`
block next to the existing `postgres`/`redis`/`neo4j`/`s3` secrets:

```yaml
auth:
  issuerUrl: https://accounts.google.com
  clientId: <your-client-id>.apps.googleusercontent.com
  publicUrl: http://localhost:18090   # no trailing slash; http so the session
                                       # cookie's Secure flag is off and flows
                                       # over loopback http. 18090 is the local
                                       # forward port (Step 1) — match it in all
                                       # three places.
  operatorEmails: you@example.com      # your Google account → operator
  oidcClientSecret: <your-client-secret>
```

That on-box `auth` block is the **only** change the loopback test needs —
no edits to the committed chart at all. `AUTH_MODE` (`oidc`), `REDIS_URL` (the
in-cluster Redis where sessions live), and the rest are already wired.

You reach the UI by port-forward, so the public ingress is irrelevant here; you
can leave it enabled (its route just goes unused). If you want to suppress the
stale ingress and its failing certificate order, set the chart-level
`ingress.enabled` to `false` in `deploy/continuo/values.yaml` — but that is a
commit to `main`, so it is optional and not part of the test.

### Step 3 — Deploy

The `auth.*` sentinel expansion must be present in the chart the cluster
deploys. If your deploy renders the chart from a checkout of `main`, this chart
change has to be on `main` first, after which a normal deploy picks up the
`auth` values from your secret values file.

Trigger the deploy the way you normally do, or apply it by hand with both value
files — the committed chart plus your secrets:

```bash
helm upgrade --install <release> deploy/continuo \
  -f <your-secret-values>.yaml \
  --set global.imageTag=<current-sha> \
  -n <namespace> --wait
```

Confirm the pod is healthy (not crashlooping):

```bash
kubectl -n continuo get pods -l app=ui
kubectl -n continuo logs -l app=ui | tail
#   -> "Continuo UI running on http://localhost:8090 (auth mode: oidc)"
```

### Step 4 — Reach it and log in

Port-forward the service to `localhost:18090`. The **local** port (left of the colon)
must match the redirect URI and `AUTH_PUBLIC_URL`; the **remote** port (right of
the colon) is always `8090`, the port the container listens on:

```bash
kubectl -n continuo port-forward svc/ui 18090:8090
```

Then open `http://localhost:18090` in your browser:

1. You land on the sign-in page.
2. Click **Sign in** → you are redirected to Google (accept the "unverified app"
   notice — it is your own app in testing mode).
3. Google redirects to `http://localhost:18090/auth/callback`; `ui`
   validates the token, sees your verified email on the operator allowlist, and
   creates a session.
4. You are now signed in as `operator`. `GET /auth/me` shows your email and role.

To confirm the authorization rules: an account that authenticates with Google but
is **not** on any allowlist gets "your account has no continuo role" and is
denied; that is the expected default-deny behavior.

---

## Moving to a real deployment (domain + public ingress)

When you have a domain and want the UI reachable from any browser:

1. In the on-box secret file, set the real public URL:
   ```yaml
   auth:
     publicUrl: https://continuo.yourdomain.com   # https in production
   ```
   and point DNS at the cluster with ingress enabled
   (`ingress.enabled: true`, `ingress.host: continuo.yourdomain.com` —
   chart-level values in `deploy/continuo`).
2. Add `https://continuo.yourdomain.com/auth/callback` to the Google OAuth
   client's authorized redirect URIs (you can keep the loopback URI alongside it).
3. Redeploy. The session cookie is now marked `Secure` automatically because
   `AUTH_PUBLIC_URL` is https.

No domain but want browser access? A free wildcard-DNS hostname
(`<cluster-ip>.sslip.io`, DuckDNS, …) pointed at the cluster gives you a real
HTTPS host for Let's Encrypt without buying a domain. Self-hosting **Dex** in the
cluster is the alternative when you want GitHub/LDAP connectors or group-based
roles instead of an email allowlist.

## Role assignment reference

Resolution order at login (first match wins, strongest role wins on ties):

1. **Groups** — `AUTH_GROUPS_CLAIM` (default `groups`) mapped through
   `AUTH_ROLE_MAPPING` (e.g. `data-platform=operator,data-eng=viewer`). Group
   membership comes from the IdP directory and is trusted directly. Google does
   not emit this claim by default, so for Google you use the email lists below.
2. **Email allowlists** — `AUTH_OPERATOR_EMAILS` / `AUTH_VIEWER_EMAILS`
   (comma-separated, case-insensitive). **Only honored when the ID token asserts
   a verified email** (`email_verified: true`); an unverified or absent claim is
   ignored, to prevent escalation via unverified aliases.
3. **Default** — `AUTH_DEFAULT_ROLE` (default `none` = denied).

## Troubleshooting

| Symptom | Cause / fix |
| --- | --- |
| Pod `CrashLoopBackOff`, log `AUTH_OIDC_* is required` | An OIDC value is still empty. Fill issuer URL, client ID, public URL, and the client secret. |
| Google error `redirect_uri_mismatch` | The Google client's redirect URI must exactly equal `AUTH_PUBLIC_URL` + `/auth/callback` — scheme, host, port, and path all included. |
| Logged in, but "your account has no continuo role" | Your email is not on `AUTH_OPERATOR_EMAILS`/`AUTH_VIEWER_EMAILS` (case-insensitive), or the IdP did not assert `email_verified: true`. |
| Signed in but immediately bounced back to sign-in | Cookie scheme mismatch: for loopback use an `http://localhost:<port>` `AUTH_PUBLIC_URL`, so the `Secure` flag is off and the cookie flows. An https `AUTH_PUBLIC_URL` requires you to actually be on https. |
| Boot log `OIDC discovery failed after N attempts` | The pod cannot reach `https://accounts.google.com`. Check cluster egress / DNS. |
| Browser can't reach `http://localhost:<port>` | The `port-forward` is not running, or the local port you forwarded does not match the redirect URI and `AUTH_PUBLIC_URL` (all three must agree). |
| `kubectl port-forward` fails with "address already in use" | The local port is taken. Pick another free one and update the redirect URI and `AUTH_PUBLIC_URL` to match; the remote side stays `:8090`. |

# ui-service authentication — operator setup

`ui-service` is the only HTTP edge of the system. It authenticates every user as
an OpenID Connect (OIDC) relying party and enforces `viewer` / `operator` roles.
There is **no unauthenticated mode**: if `AUTH_MODE=oidc` and the OIDC settings
are missing, the pod fails to boot on purpose (it never serves an open UI).

This document explains how to configure authentication for a deployment, with a
detailed walkthrough of the **Google loopback** setup — the fastest way to run
real authentication with no domain name, suitable for validating the deployment
or for a single operator.

## Auth modes

`AUTH_MODE` (set in the `ui-service` `env` block of `deploy/app/values.yaml`) has
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
you reach `ui-service` through a `kubectl port-forward` to `localhost:8090`, and
Google redirects back to that same loopback address after login.

Role assignment uses an **email allowlist** (Google does not emit a `groups`
claim by default). Google asserts `email_verified: true` for verified accounts,
which `ui-service` requires before honoring the allowlist, so this works cleanly.

### Step 1 — Create the Google OAuth client

1. Open the [Google Cloud Console](https://console.cloud.google.com/) and select
   or create a project.
2. **APIs & Services → OAuth consent screen**:
   - User type: **External**.
   - Fill in the app name and your support email.
   - Under **Test users**, add the Google account(s) you will log in with. While
     the consent screen is in "Testing" status, only these accounts can sign in.
   - The default `openid`, `email`, `profile` scopes are all `ui-service` needs;
     you do not need to add any.
3. **APIs & Services → Credentials → Create Credentials → OAuth client ID**:
   - Application type: **Web application**.
   - **Authorized redirect URIs** → add exactly:
     ```
     http://localhost:8090/auth/callback
     ```
     This must match `AUTH_PUBLIC_URL` + `/auth/callback` character-for-character
     (scheme, host, port, path). `http://localhost` is permitted by Google for web
     clients — this is the loopback exception that makes the no-domain flow work.
4. Save, and copy the **Client ID** and **Client secret**.

### Step 2 — Configure continuo

Edit the `ui-service` `env` block in `deploy/app/values.yaml`:

```yaml
    ingress:
      enabled: false            # reached via port-forward, not a public host
    env:
      AUTH_MODE: oidc
      AUTH_OIDC_ISSUER_URL: https://accounts.google.com
      AUTH_OIDC_CLIENT_ID: <your-client-id>.apps.googleusercontent.com
      AUTH_PUBLIC_URL: http://localhost:8090   # no trailing slash; http so the
                                               # session cookie's Secure flag is
                                               # off and flows over loopback http
      AUTH_OPERATOR_EMAILS: you@example.com    # your Google account → operator
      # AUTH_OIDC_ISSUER_URL/CLIENT_ID/PUBLIC_URL above replace the empty defaults
```

Put the **client secret** in your secret values file (the real, git-ignored copy
of `values.secret.yaml.example`), not in `values.yaml`:

```yaml
global:
  auth:
    oidcClientSecret: <your-client-secret>
```

The chart wires `oidcClientSecret` into the credentials Secret and injects it as
`AUTH_OIDC_CLIENT_SECRET`. `REDIS_URL` is already wired to the in-cluster Redis,
which is where sessions live — no extra setup.

### Step 3 — Deploy

Apply the chart the same way you normally do, passing your secret values file:

```bash
helm upgrade --install continuo-app deploy/app -f values.secret.yaml
```

Confirm the pod is healthy (not crashlooping):

```bash
kubectl -n <namespace> get pods -l app=ui-service
kubectl -n <namespace> logs -l app=ui-service | tail
#   -> "Continuo UI running on http://localhost:8090 (auth mode: oidc)"
```

### Step 4 — Reach it and log in

Port-forward the service to `localhost:8090` (over your SSH tunnel to the cluster,
using your Hetzner kubeconfig). The local port **must be 8090** to match the
redirect URI and `AUTH_PUBLIC_URL`:

```bash
kubectl -n <namespace> port-forward svc/ui-service 8090:8090
```

Then open `http://localhost:8090` in your browser:

1. You land on the sign-in page.
2. Click **Sign in** → you are redirected to Google (accept the "unverified app"
   notice — it is your own app in testing mode).
3. Google redirects to `http://localhost:8090/auth/callback`; `ui-service`
   validates the token, sees your verified email on the operator allowlist, and
   creates a session.
4. You are now signed in as `operator`. `GET /auth/me` shows your email and role.

To confirm the authorization rules: an account that authenticates with Google but
is **not** on any allowlist gets "your account has no continuo role" and is
denied; that is the expected default-deny behavior.

---

## Moving to a real deployment (domain + public ingress)

When you have a domain and want the UI reachable from any browser:

1. Point DNS at the cluster and re-enable the ingress:
   ```yaml
   ingress:
     enabled: true
     host: continuo.yourdomain.com
   env:
     AUTH_PUBLIC_URL: https://continuo.yourdomain.com   # https in production
   ```
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
| Signed in but immediately bounced back to sign-in | Cookie scheme mismatch: for loopback use `AUTH_PUBLIC_URL: http://localhost:8090` (http), so the `Secure` flag is off and the cookie flows. https `AUTH_PUBLIC_URL` requires you to actually be on https. |
| Boot log `OIDC discovery failed after N attempts` | The pod cannot reach `https://accounts.google.com`. Check cluster egress / DNS. |
| Browser can't reach `http://localhost:8090` | The `port-forward` is not running, or you forwarded a different local port than 8090 (then the redirect URI and `AUTH_PUBLIC_URL` must change to match). |

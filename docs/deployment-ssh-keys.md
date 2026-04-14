# Deployment SSH Keys

Two separate SSH key pairs are used in the deployment setup. They serve **opposite directions** and must not be confused.

## Overview

| Key | Stored on | Used by | Direction | Purpose |
|-----|-----------|---------|-----------|---------|
| `hetzner-deploy` | Hetzner server (`~/.ssh/id_ed25519`) | Server → GitHub | Server pulls from GitHub | `git pull` during deploy |
| `github-actions-deploy` | GitHub Actions secret (`HETZNER_SSH_KEY`) | GitHub Actions → Server | GitHub Actions SSHes into server | Runs `helm upgrade` on the server |

---

## Key 1: `hetzner-deploy`

**What it is:** An SSH key pair generated on the Hetzner server.

**Where it lives:**
- Private key: `/root/.ssh/id_ed25519` on the Hetzner server
- Public key: registered as a **GitHub Deploy Key** (read-only) at `Settings → Deploy keys`

**What it does:** Allows the Hetzner server to authenticate to GitHub and run `git pull --ff-only origin main` during the deploy workflow, without a password.

**How it was set up:**
```bash
# On the Hetzner server
ssh-keygen -t ed25519 -C "hetzner-deploy" -f ~/.ssh/id_ed25519 -N ""
# Public key was then added to GitHub → Settings → Deploy keys (read-only)
```

---

## Key 2: `github-actions-deploy`

**What it is:** An SSH key pair generated locally, used by GitHub Actions to SSH into the Hetzner server.

**Where it lives:**
- Private key: stored as the `HETZNER_SSH_KEY` GitHub Actions secret
- Public key: appended to `/root/.ssh/authorized_keys` on the Hetzner server

**What it does:** Allows the GitHub Actions `deploy` job (using `appleboy/ssh-action`) to open an SSH session on the Hetzner server and execute the `helm upgrade` command.

**How it was set up:**
```bash
# Locally
ssh-keygen -t ed25519 -C "github-actions-deploy" -f /tmp/hetzner_deploy_key -N ""
# Public key was appended to /root/.ssh/authorized_keys on the server
# Private key was added to GitHub → Settings → Secrets → Actions → HETZNER_SSH_KEY
```

---

## Why two keys?

The deployment flow has **two separate authenticated connections**:

```
GitHub Actions runner
  │
  │  (uses github-actions-deploy private key → HETZNER_SSH_KEY secret)
  ▼
Hetzner server
  │  git pull --ff-only origin main
  │  (uses hetzner-deploy private key → ~/.ssh/id_ed25519)
  ▼
GitHub (repo)
```

These cannot be the same key because:
- `hetzner-deploy`'s private key must stay on the server — it is never shared.
- `github-actions-deploy`'s private key lives in GitHub — the server never needs it.

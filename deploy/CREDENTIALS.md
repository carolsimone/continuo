# Credentials

Every continuo service signs in to Postgres and to Redis as its own login, and
reads each credential from an environment variable or from a file. This guide
lists every login continuo uses, says how to create each one, and shows how to
hand the passwords to the pods.

- **With the bundled datastores** (the quickstart, `postgresql.enabled` and
  `redis.enabled`), the chart does all of it: it generates a password for
  every login into the Secret `<release>-logins` and keeps it across upgrades.
  The migrate Job creates the Postgres logins, and the bundled Redis starts
  with every Redis login. Nothing in this guide is needed.
- **With your own Postgres and Redis**, your platform team creates each login
  and a Kubernetes Secret holding its password, and names that Secret in the
  values. continuo then does the rest on every deploy: it grants each Postgres
  login its rights, creates every Redis stream, and checks that every login
  exists and holds no more than its own rights.

continuo never needs a Postgres superuser, `CREATEROLE`, or a Redis login that
can grant rights.

## Every login

| Datastore | Login | What it can do | Used by |
|---|---|---|---|
| Postgres | the migration login (`externalDatabase.username`) | Owns every continuo database and every table in it; runs the Flyway migrations and grants each runtime login its rights | the migrate Job |
| Postgres | `continuo_run_controller` | Connect to `continuo_run_controller`; read and write its tables and sequences | run-controller |
| Postgres | `continuo_execution_controller` | Connect to `continuo_execution`; read and write its tables and sequences | execution-controller |
| Postgres | `continuo_release_controller` | Connect to `continuo_release`; read and write its tables and sequences | release-controller and its commands |
| Postgres | `continuo_remediation` | Connect to `continuo_remediation`; read and write its tables and sequences | remediation |
| Postgres | `continuo_agent_remediation` | Connect to `continuo_agent_remediation`; read and write its tables and sequences | agent-remediation |
| Postgres | `continuo_agent_chat` | Connect to `continuo_agent_chat`; read and write its tables and sequences | agent-chat |
| Postgres | `continuo_dead_letter_controller` | Connect to `continuo_dead_letter`; read and write its tables and sequences | dead-letter-controller |
| Postgres | `continuo_warehouse` | Owns the warehouse database (`validation.warehouseDatabase`) and every object in it | dbt and validation Job pods, bundled quickstart only (with your own Postgres the warehouse login is yours) |
| Redis | `continuo_run_controller` | Read its streams, write the streams it produces | run-controller |
| Redis | `continuo_catalog` | Read its streams, write the dead-letter stream | catalog |
| Redis | `continuo_execution_controller` | Read its streams, write the streams it produces | execution-controller |
| Redis | `continuo_release_controller` | Read its streams, write the streams it produces | release-controller |
| Redis | `continuo_remediation` | Read its streams, write the streams it produces | remediation |
| Redis | `continuo_agent_remediation` | Read its stream, write the streams it produces, read and write its LLM cache keys (`continuo:llmcache:*`) | agent-remediation |
| Redis | `continuo_agent_chat` | Read and write its rate-limit keys (`continuo:agent-chat:ratelimit:*`) | agent-chat |
| Redis | `continuo_dead_letter_controller` | Read both dead-letter streams, write every stream it redrives to, trim streams, read `INFO memory` | dead-letter-controller |
| Redis | `continuo_topology_controller` | Read its stream, write the streams it produces | topology-controller |
| Redis | `continuo_ui` | Read and write the ui sessions (`continuo:uisession:*`) | ui |
| Redis | `continuo_redis_migrate` | Create every stream and consumer group under `continuo:` | the `streams-init` Job |

Every name is a value: `credentials.postgres.<service>.username` and
`credentials.redis.logins.<name>.username`. The names above are the defaults,
and the published Redis rules use them.

## Postgres

### Create the logins

Create each runtime login with `LOGIN` and a password and nothing else. Grant it
nothing: the migrate Job grants each login its rights on every deploy, as the
migration login, which owns the tables.

```sql
CREATE ROLE continuo_run_controller LOGIN PASSWORD '...';
CREATE ROLE continuo_execution_controller LOGIN PASSWORD '...';
CREATE ROLE continuo_release_controller LOGIN PASSWORD '...';
CREATE ROLE continuo_remediation LOGIN PASSWORD '...';
CREATE ROLE continuo_agent_remediation LOGIN PASSWORD '...';
CREATE ROLE continuo_agent_chat LOGIN PASSWORD '...';
CREATE ROLE continuo_dead_letter_controller LOGIN PASSWORD '...';
```

With Terraform's `postgresql_role`, set `login = true` and leave every other
attribute at its default. Do not manage `postgresql_grant` or
`postgresql_default_privileges` on a continuo database: the migrate Job owns
those grants, and two owners of the same grants undo each other.

Then create one Secret per login, with the password under the key `password`,
and name it in the values:

```yaml
credentials:
  postgres:
    run-controller: {existingSecret: continuo-pg-run-controller}
    execution-controller: {existingSecret: continuo-pg-execution-controller}
    release-controller: {existingSecret: continuo-pg-release-controller}
    remediation: {existingSecret: continuo-pg-remediation}
    agent-remediation: {existingSecret: continuo-pg-agent-remediation}
    agent-chat: {existingSecret: continuo-pg-agent-chat}
    dead-letter-controller: {existingSecret: continuo-pg-dead-letter-controller}
```

`key` names another key in the Secret. A password the pod receives as a file of
your own (see [Hand the passwords to the pods](#hand-the-passwords-to-the-pods))
needs no Secret: give `POSTGRES_PASSWORD` in `credentials.files.<service>`.

### The migration login owns every continuo database

The migration login is `externalDatabase.username`. It creates the databases
when `databaseInit.enabled` is true (it then needs `CREATEDB`), and it owns
every database it creates. If your DBA creates the databases
(`databaseInit.enabled: false`), make the migration login their owner:

```sql
ALTER DATABASE continuo_run_controller OWNER TO continuo;
ALTER DATABASE continuo_execution OWNER TO continuo;
ALTER DATABASE continuo_release OWNER TO continuo;
ALTER DATABASE continuo_remediation OWNER TO continuo;
ALTER DATABASE continuo_agent_remediation OWNER TO continuo;
ALTER DATABASE continuo_agent_chat OWNER TO continuo;
ALTER DATABASE continuo_dead_letter OWNER TO continuo;
```

(`continuo` is the default `externalDatabase.username`.)

### What the migrate Job does on every deploy

As the migration login, for each database (shown for run-controller):

```sql
REVOKE CONNECT ON DATABASE continuo_run_controller FROM PUBLIC;
GRANT  CONNECT ON DATABASE continuo_run_controller TO continuo_run_controller;
-- Flyway migrate, then, in continuo_run_controller:
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
GRANT USAGE ON SCHEMA public TO continuo_run_controller;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO continuo_run_controller;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO continuo_run_controller;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO continuo_run_controller;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT USAGE, SELECT ON SEQUENCES TO continuo_run_controller;
```

It then reads the catalog back and fails the deploy unless every runtime login:

- can connect to its own database and to no other continuo database (nor, in
  the bundled quickstart, to the warehouse);
- cannot create objects in its database's `public` schema;
- can select, insert, update and delete on every table of its database;
- is not a superuser and has no `CREATEROLE`, `CREATEDB`, `REPLICATION` or
  `BYPASSRLS`;
- is not a member of the migration login, and owns no object.

### The warehouse

dbt Job pods and the validation runner sign in to the warehouse with the
Secret `validation.warehouseSecret`, attached to each Job pod with `envFrom`.
With your own Postgres the warehouse, its login and that Secret are yours: set
`validation.createWarehouseSecret: false` and `validation.warehouseSecret` to
your Secret. continuo never builds that Secret from one of its own logins, and
does not create the warehouse database.

**Objects continuo's login created in your warehouse.** On an install where the
migration login ran dbt, the tables and views in the warehouse belong to it,
and your warehouse login cannot replace them. Before you upgrade, hand each one
to your warehouse login, object by object, as a superuser or as their owner,
inside the warehouse database. List them:

```sql
SELECT format('ALTER SCHEMA %I OWNER TO continuo_warehouse;', nspname)
FROM pg_namespace
WHERE nspowner = 'continuo'::regrole
UNION ALL
SELECT format('ALTER %s %s OWNER TO continuo_warehouse;',
              CASE relkind WHEN 'v' THEN 'VIEW' WHEN 'm' THEN 'MATERIALIZED VIEW' ELSE 'TABLE' END,
              oid::regclass)
FROM pg_class
WHERE relowner = 'continuo'::regrole AND relkind IN ('r', 'p', 'v', 'm')
UNION ALL
SELECT format('ALTER ROUTINE %s OWNER TO continuo_warehouse;', oid::regprocedure)
FROM pg_proc
WHERE proowner = 'continuo'::regrole;
```

Then run the statements it prints (a table's sequences follow it). Do not use
`REASSIGN OWNED BY continuo`: it also hands over every continuo database the
migration login owns.

In the bundled quickstart the migrate Job does this itself: it creates
`continuo_warehouse`, makes it the owner of the warehouse database, and hands
it every schema, table, view, sequence and routine there that it does not own.

### Managed Postgres

- **A hardened `postgres` database.** A service's `wait-for-migrations` init
  container (bundled Postgres only) and the migrate Job's checks connect to
  the `postgres` maintenance database. Where `CONNECT` on it is revoked from
  `PUBLIC`, grant it to each runtime login.

- **Cloud SQL:** a user created through the Cloud SQL API or console gets
  `CREATEDB` and `CREATEROLE` and joins `cloudsqlsuperuser`, so the migrate
  Job's check refuses it. Create each runtime login in SQL instead,
  with `CREATE ROLE ... LOGIN PASSWORD`, signed in as an admin user.
- **A cluster upgraded from Postgres 14 or earlier** (`pg_upgrade`) keeps the
  `public` schema owned by the bootstrap superuser, so the migration login
  cannot revoke `CREATE` on it, and the check reports
  `has CREATE on schema public`. As a superuser, run
  `ALTER SCHEMA public OWNER TO continuo;` in each continuo database once.

## Redis

### The key prefix

Every key continuo uses starts with `continuo:`: its streams
(`continuo:task.dispatch:v1`), the ui sessions (`continuo:uisession:<id>`),
agent-chat's rate-limit keys and agent-remediation's LLM cache. On a Redis
shared with other applications, `~continuo:*` fences continuo away from their
keys, and the per-login rules below fence each service away from the others'.
During this release continuo also drains the stream names it used before the
prefix (`task.dispatch:v1`), so the published rules name those too.

### Create the logins

Each login's rule is generated from the stream contract and published with the
chart, in two forms:

- `deploy/redis-acl-rules.acl`: one `user <login> on <rule>` line per login,
  without a password;
- `deploy/continuo/files/redis-acl-rules.json`: the same, one entry per login,
  for your tooling:

  ```json
  {"key_prefix": "continuo:",
   "logins": [{"service": "run-controller", "username": "continuo_run_controller", "rule": "~continuo:release.promoted:v2 ... -@all +ping ..."}]}
  ```

Create each login with its rule and a password. With `redis-cli`, as a login
that may run `ACL SETUSER`:

```bash
redis-cli ACL SETUSER continuo_run_controller reset on '>the-password' <the rule's tokens>
```

On ElastiCache, read the JSON into `aws_elasticache_user`:

```hcl
locals {
  continuo_rules = jsondecode(file("${path.module}/redis-acl-rules.json")).logins
}

resource "random_password" "continuo" {
  for_each = { for l in local.continuo_rules : l.username => l }
  length   = 32
  special  = false
}

resource "aws_elasticache_user" "continuo" {
  for_each      = { for l in local.continuo_rules : l.username => l }
  user_id       = replace(each.key, "_", "-")
  user_name     = each.key
  engine        = "redis"
  access_string = "on ${each.value.rule}"
  authentication_mode {
    type      = "password"
    passwords = [random_password.continuo[each.key].result]
  }
}
```

Then create one Secret per login, with the password under the key `password`,
and name it in the values:

```yaml
credentials:
  redis:
    mode: perService
    logins:
      streams-init: {existingSecret: continuo-redis-streams-init}
      run-controller: {existingSecret: continuo-redis-run-controller}
      catalog: {existingSecret: continuo-redis-catalog}
      agent-chat: {existingSecret: continuo-redis-agent-chat}
      execution-controller: {existingSecret: continuo-redis-execution-controller}
      ui: {existingSecret: continuo-redis-ui}
      topology-controller: {existingSecret: continuo-redis-topology-controller}
      release-controller: {existingSecret: continuo-redis-release-controller}
      remediation: {existingSecret: continuo-redis-remediation}
      agent-remediation: {existingSecret: continuo-redis-agent-remediation}
      dead-letter-controller: {existingSecret: continuo-redis-dead-letter-controller}
```

A service's password given as a file of your own needs no Secret: give
`REDIS_PASSWORD` in `credentials.files.<service>`.

**Rules change with releases.** A release that changes a rule says so in the
chart's CHANGELOG with the words "Redis rule". Apply the new rules before, or
with, the upgrade that carries them.

**A Redis with no ACL users** (some managed offerings have a single password)
sets `credentials.redis.mode: shared`. Every service then signs in with the one
password in `externalRedis`, and any service's credential reaches every key.

### The streams step

The `streams-init` Job creates every stream and consumer group of the release
before the services roll, from ID `0` where one is missing, signed in as
`continuo_redis_migrate`. Running it twice changes nothing; no service creates
a stream or a group itself. To create them yourself, set
`redisInit.createStreams: false` and run `/streams-init` from the
dead-letter-controller image with a login that may, before the upgrade; the
Job then creates nothing but still checks every login.

### What continuo checks

- **Before any service rolls**, `streams-init` signs in as every login whose
  password is in a Secret and stops the deploy when one is missing or its
  password is wrong.
- **When a service starts**, it asks Redis about a key no rule covers
  (`continuo-permission-probe`) and refuses to start unless Redis answers
  `NOPERM`: a login that reaches keys outside its own fails here. A login whose
  password reaches its pod only as a file of your own is checked here too.
- **A rule that lacks a command or stream the release needs** shows as `NOPERM`
  when the service first uses it, which is why the CHANGELOG names every rule
  change.

## Hand the passwords to the pods

`credentials.delivery` (and a service's `credentialDelivery`) chooses how each
credential reaches a service's pod:

| Mode | The chart | Fits |
|---|---|---|
| `env` (default) | sets the variable (`POSTGRES_PASSWORD`) from the Secret's key | External Secrets Operator, Vault Secrets Operator, a Secret created by Terraform |
| `file` | mounts the same Secret keys read-only under `/var/run/secrets/continuo` and sets `<VAR>_FILE`; no credential is in the environment | the same, for a company that keeps credentials out of the environment |
| your own files (`credentials.files`) | attaches no Secret for that credential and sets `<VAR>_FILE` to the path you give | Vault Agent injector, Secrets Store CSI driver |

Every service reads `<VAR>` or `<VAR>_FILE`, never both; one trailing newline is
trimmed. The Postgres and Redis passwords are read from the file again for
every new connection, so a rotated file needs no restart. Postgres keeps open
sessions across a password change.

**External Secrets Operator:** an `ExternalSecret` per login that writes the
Secret the values name:

```yaml
apiVersion: external-secrets.io/v1
kind: ExternalSecret
metadata:
  name: continuo-pg-run-controller
spec:
  secretStoreRef: {name: company-store, kind: ClusterSecretStore}
  target: {name: continuo-pg-run-controller}
  data:
    - secretKey: password
      remoteRef: {key: continuo/postgres/continuo_run_controller}
```

**Vault Agent injector:** the agent writes the file in the pod, and
`credentials.files` names it:

```yaml
credentials:
  files:
    run-controller:
      POSTGRES_PASSWORD: /vault/secrets/postgres
services:
  - name: run-controller
    # ... the rest of the chart's entry
    podAnnotations:
      vault.hashicorp.com/agent-inject: "true"
      vault.hashicorp.com/agent-init-first: "true"
      vault.hashicorp.com/role: continuo-run-controller
      vault.hashicorp.com/agent-inject-secret-postgres: secret/data/continuo/run-controller
      vault.hashicorp.com/agent-inject-template-postgres: |
        {{ with secret "secret/data/continuo/run-controller" }}{{ .Data.data.password }}{{ end }}
```

Every service runs under a ServiceAccount of its own, named after it, which a
Vault Kubernetes role binds to.

**Secrets Store CSI driver:** mount the volume in the service's entry, and
name the file:

```yaml
credentials:
  files:
    execution-controller:
      POSTGRES_PASSWORD: /mnt/secrets-store/postgres-password
services:
  - name: execution-controller
    # ... the rest of the chart's entry
    volumes:
      - name: secrets-store
        csi:
          driver: secrets-store.csi.k8s.io
          readOnly: true
          volumeAttributes: {secretProviderClass: continuo-execution-controller}
    volumeMounts:
      - name: secrets-store
        mountPath: /mnt/secrets-store
        readOnly: true
```

Job pods cannot read a file execution-controller was given: they read the S3
key from the Secret `execution.jobS3Secret`, and the warehouse from
`validation.warehouseSecret`.

## How an upgrade names a missing login

Each problem stops the upgrade before any service rolls, so the running version
stays up:

| Problem | Where | Message |
|---|---|---|
| A login's Secret is not named in the values | `helm upgrade` (render) | `credentials.postgres.run-controller.existingSecret is required: create the login continuo_run_controller and a Secret holding its password (deploy/CREDENTIALS.md)`, or the same for `credentials.redis.logins` |
| A Postgres login does not exist | the migrate Job | `login continuo_release_controller does not exist: create it as described in deploy/CREDENTIALS.md` |
| A Postgres login has more rights than its own | the migrate Job | `login continuo_agent_chat: has CREATEDB` |
| A Redis login does not exist, or its password is wrong | the `streams-init` Job | `redis login continuo_ui: ...; create it as described in deploy/CREDENTIALS.md` |
| A Redis login reaches keys outside its own | the service, at start | `redis login continuo_ui can read keys outside its own: apply the rules in deploy/redis-acl-rules.acl` |

`helm upgrade` prints every login to create, in its notes, when the release
runs against your own Postgres or Redis.

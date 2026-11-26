# Database Migrations

## Structure

Each component that keeps Postgres owns one database and one directory here:

| Directory | Database | Component |
|-----------|----------|---------|
| `run_controller/` | `continuo_run_controller` | run-controller |
| `execution/` | `continuo_execution` | execution-controller |
| `release/` | `continuo_release` | release-controller |
| `agent_chat/` | `continuo_agent_chat` | agent-chat |
| `remediation/` | `continuo_remediation` | remediation |
| `agent_remediation/` | `continuo_agent_remediation` | agent-remediation |
| `dead_letter/` | `continuo_dead_letter` | dead-letter-controller |

Each directory starts with one `V1__` file that builds the whole database: every table as one block (its columns and constraints, then its indexes, triggers and comments), and the rows the components need from the start. A change to a table or column edits that block, or adds the next migration.

`catalog` keeps no Postgres: its graph and its processed-message markers live in Neo4j, so it has no migration directory, no `continuo_*` database and no Flyway job. `TestProvisioning_ListsExactlyTheComponentsThatKeepPostgres` (`pkg/deploycheck`) holds every provisioning site (`db/migrate-all.sh`, `db/Dockerfile.migrate`, `db/init-databases.sh`, the chart's db-init loop, the compose Flyway jobs and the directories here) to the same list of components.

## Running Migrations

One image, built from `db/Dockerfile.migrate`, holds every component's migrations and runs them in turn through `db/migrate-all.sh`, signed in as the migration login, which owns every continuo database. For each database it then lets only that component's login connect (`CONTINUO_PG_LOGINS`), grants it read and write on its tables and sequences (`db/logins/grant.sql`), and reads the catalog back to refuse a login with any other right (`db/logins/check.sql`). It stops before any change when a login does not exist.

```bash
DOCKER_BUILDKIT=1 docker build -t continuo-migrations:test -f db/Dockerfile.migrate db/
bash db/test/migrate-logins_test.sh   # runs the image against throwaway Postgres containers
```

docker-compose runs the same image as the one-shot `migrations` service; `db/init-databases.sh` creates the databases and the logins first, each with the password `local-<login>`:

```bash
docker compose up -d postgres
docker compose up --exit-code-from migrations migrations
```

Or start the whole stack:

```bash
docker compose up -d
```

## Naming Convention

Migrations follow the Flyway convention, `V{version}__{description}.sql`, with versions counted from 1 within each directory.

## Creating New Migrations

1. Find the directory of the component that owns the table.
2. Add `V{next version}__{description}.sql` there.
3. Run `make test-deps-up`, which applies every migration to the docker-compose Postgres, then the component's integration tests.

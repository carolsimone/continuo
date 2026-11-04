# Database Migrations

## Structure

Migrations are organized by service ownership:

```
db/migration/
├── run_controller/ Run-controller (runs, tasks, task_attempts, run_controller_outbox)
├── execution/     Execution-controller (message_processing, execution_outbox, cancelled_runs, deployments, validation_aggregates)
├── dependency/    Dependency-controller (outbox, message_processing, published_messages)
├── dead_letter/   Dead-letter-controller (dead_letters, dead_letter_outbox)
```

## Database Mapping

| Service | Database | Tables |
|---------|----------|--------|
| Run-controller | continuo_run_controller | runs, tasks, task_attempts, run_controller_outbox |
| Execution-controller | continuo_execution | message_processing, execution_outbox, cancelled_runs, deployments, validation_aggregates |
| Dependency-controller | continuo_dependency | outbox, message_processing, published_messages |
| Dead-letter-controller | continuo_dead_letter | dead_letters, dead_letter_outbox |

`catalog` keeps no Postgres: its graph and its processed-message markers live in Neo4j, so it has no migration directory, no `continuo_*` database and no Flyway job. `TestProvisioning_ListsExactlyTheServicesThatKeepPostgres` (`pkg/deploycheck`) holds every provisioning site (`db/migrate-all.sh`, `db/Dockerfile.migrate`, `db/init-databases.sh`, the chart's db-init loop, the compose Flyway jobs and the directories here) to the same list of services.

## Running Migrations

One image, built from `db/Dockerfile.migrate`, holds every service's migrations and runs them in turn through `db/migrate-all.sh`, signed in as the migration login, which owns every continuo database. For each database it then lets only that service's login connect (`CONTINUO_PG_LOGINS`), grants it read and write on its tables and sequences (`db/logins/grant.sql`), and reads the catalog back to refuse a login with any other right (`db/logins/check.sql`). It stops before any change when a login does not exist.

```bash
DOCKER_BUILDKIT=1 docker build -t continuo-migrations:test -f db/Dockerfile.migrate db/
bash db/test/migrate-logins_test.sh   # runs the image against throwaway Postgres containers
```

docker-compose runs the same image as the one-shot `migrations` service; `db/init-databases.sh` creates the databases and the logins first, each with the password `local-<login>`:

```bash
docker compose up -d postgres
docker compose up --exit-code-from migrations migrations
```

Or run all services:

```bash
docker compose up -d
```

## Naming Convention

All migrations follow Flyway standard:

```
V{version}__{description}.sql
```

Examples:
- V1__init_scheduler_tracker.sql
- V2__add_index_on_status.sql

Version numbers start at 1 within each service directory.

## Creating New Migrations

1. Identify which service owns the table
2. Create migration in appropriate directory (db/migration/{service}/)
3. Use next available version number
4. Follow naming convention
5. Test migration locally
6. Commit with descriptive message

## Migration History

- **2026-02-13:** Initial consolidation from scattered migrations
  - Centralized all migrations to db/migration/
  - Established logical database separation
  - Standardized on Flyway naming convention

See `docs/plans/2026-02-13-database-migration-consolidation-design.md` for complete design details.

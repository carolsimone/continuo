# Database Migrations

## Structure

Migrations are organized by service ownership:

```
db/migration/
├── state/         State service (scheduler_tracker, task_tracker, task_execution)
├── executor/      Executor-controller (deployment_outbox, processed_events)
├── dependency/    Dependency-controller (outbox, message_processing, published_messages)
└── k8s/           K8s-controller (k8s_status_outbox)
```

## Database Mapping

| Service | Database | Tables |
|---------|----------|--------|
| State, Startup-controller | continuo_state | scheduler_tracker, task_tracker, task_execution |
| Executor-controller | continuo_executor | deployment_outbox, processed_events |
| Dependency-controller | continuo_dependency | outbox, message_processing, published_messages |
| K8s-controller | continuo_k8s | k8s_status_outbox |

## Running Migrations

Migrations run automatically via Flyway on `docker-compose up`:

```bash
docker-compose up -d postgres          # Start Postgres
docker-compose up flyway-state         # Run state migrations
docker-compose up flyway-executor      # Run executor migrations
docker-compose up flyway-dependency    # Run dependency migrations
docker-compose up flyway-k8s           # Run k8s migrations
```

Or run all services:

```bash
docker-compose up -d
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

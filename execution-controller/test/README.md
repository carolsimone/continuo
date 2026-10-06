# Executor database tests

Database tests connect through the standard `POSTGRES_HOST`, `POSTGRES_PORT`,
`POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, and `DB_SSLMODE` settings.
They do not create databases, tables, or replay migrations. Provision a dedicated
empty database externally and apply `db/migration/execution` with Flyway before
running the suite. A missing table fails with an instruction to run migrations.

For a provisioned Postgres container named `execution-test-postgres` on the
`execution-test` Docker network, apply migrations from the repository root:

```sh
docker run --rm --network execution-test \
  -v "$PWD/db/migration/execution:/flyway/sql" flyway/flyway migrate \
  -url=jdbc:postgresql://execution-test-postgres:5432/continuo_execution \
  -user=continuo_svc -password=continuo -connectRetries=30
```

From `execution-controller`, with that database exposed on port 5432:

```sh
env POSTGRES_HOST=127.0.0.1 POSTGRES_PORT=5432 POSTGRES_DB=continuo_execution \
  POSTGRES_USER=continuo_svc POSTGRES_PASSWORD=continuo DB_SSLMODE=disable \
  REQUIRE_TEST_DEPS=1 GOFLAGS=-p=1 go test -tags integration -count=1 -v ./...
```

All executor database suites acquire the same session advisory lock, including
when Go runs packages concurrently. Each test requires empty executor tables,
owns every row inserted while it holds the lock, and truncates those rows at
cleanup. Setup refuses a nonempty database without deleting existing data.
Use a database with no running controller, relay, or external writer; the lock
coordinates test suites only. Database tests skip when connection settings are
absent unless `REQUIRE_TEST_DEPS=1` is set, in which case they fail.

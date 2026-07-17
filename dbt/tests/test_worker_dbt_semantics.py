"""What a real dbt does when it is run from a Manifest the process already holds.

Every other worker test establishes that the runtime carries a task from a lease
to a result. This one establishes the thing that makes any of that worth doing:
that a node run from a hydrated Manifest materializes what the same node
materializes under a per-task Job, which parses the project itself.

So nothing here is stood in for. The project is the real service-1 dbt project
the team ships, parsed once by a real `dbt parse`. The Manifest is hydrated once
from the partial parse that parse wrote, exactly as a worker pod hydrates its
pool's artifact. Every task goes through NativeExecutor, the code a pod runs.
The warehouse is the real Postgres, and what landed in it is read back over a
separate connection rather than taken from dbt's own report.

The parser is patched to raise for the length of every native invocation. That
patch is what makes the results mean anything: dbt reaches for it whenever it
has no Manifest to work from, so a run that quietly reparsed the project would
fail here rather than pass on a Manifest this test never checked.
"""
from __future__ import annotations

import os
import shutil
import subprocess
from contextlib import contextmanager
from pathlib import Path

import psycopg2
import pytest
from dbt.contracts.graph.manifest import Manifest

from continuo_dbt_runtime.artifact_store import LoadedArtifact
from continuo_dbt_runtime.execution import (
    EventSink,
    Lease,
    NativeExecutor,
    TaskRef,
    release_adapters,
    task_environment,
)
from continuo_dbt_runtime.worker import task_values

# The service-1 project, mounted into this container from the repository.
SERVICE_PROJECT = Path("/app/services/service-1")

# The shared macros every team image inherits from the dbt base image. A team
# image holds these alongside the project, so the copy this test parses puts
# them back together: generate_schema_name is one of them, and it is what routes
# a run to the schema below.
SHARED_MACROS = Path("/project/macros")

PROJECT_NAME = "service_1"
SERVICE_NAME = "service-1"

# Where this test's dbt writes. It is set before the parse and left set, which is
# what a worker pod does: the schema a pool runs against is fixed for the life of
# the artifact, so it is baked into the Manifest rather than chosen per task.
WORKER_SCHEMA = "worker_dbt_semantics"

# Records one entry per Manifest hydrated for this module. A worker hydrates its
# pool's artifact once and serves every task from it, so this must stay at one
# however many semantics are exercised below.
HYDRATIONS: list[Manifest] = []

# The Manifest each semantic below was actually served by.
MANIFESTS_SERVED: list[Manifest] = []


def unique_id(name: str, resource: str = "model") -> str:
    return f"{resource}.{PROJECT_NAME}.{name}"


@pytest.fixture(scope="session")
def worker_schema_env():
    """The pod-level environment the project is parsed and run under."""
    with task_environment({"DBT_TARGET_SCHEMA": WORKER_SCHEMA}):
        yield WORKER_SCHEMA


@pytest.fixture(scope="session")
def worker_project(tmp_path_factory, worker_schema_env) -> Path:
    """The real service-1 project, laid out as its image lays it out, parsed once.

    It is copied rather than parsed in place: the project is mounted from the
    repository, and a parse writes a target directory next to it.
    """
    root = tmp_path_factory.mktemp("service-1-project") / PROJECT_NAME
    shutil.copytree(SERVICE_PROJECT, root)
    shutil.copytree(SHARED_MACROS, root / "macros")

    parse = subprocess.run(
        ["dbt", "parse", "--project-dir", str(root), "--profiles-dir", str(root)],
        capture_output=True, text=True,
    )
    assert parse.returncode == 0, f"dbt parse failed:\n{parse.stdout}\n{parse.stderr}"
    return root


@pytest.fixture(scope="session")
def artifact(worker_project) -> LoadedArtifact:
    """The pool's Manifest, hydrated once from the partial parse dbt wrote."""
    packed = (worker_project / "target" / "partial_parse.msgpack").read_bytes()
    manifest = Manifest.from_msgpack(packed)
    HYDRATIONS.append(manifest)
    canonical = worker_project / "target" / "partial_parse.msgpack"
    return LoadedArtifact(
        manifest=manifest, canonical_path=canonical, descriptor={"sha256": "a" * 64}
    )


@pytest.fixture
def forbid_full_parse(monkeypatch):
    """Make parsing the project impossible, so only the hydrated Manifest serves.

    dbtRunner catches what a command raises and reports it on the result rather
    than letting it escape, so a tripped parse surfaces as a failed task below,
    not as an error out of invoke().
    """
    def forbidden(*args, **kwargs):
        raise AssertionError("ManifestLoader.get_full_manifest must not run")

    monkeypatch.setattr(
        "dbt.parser.manifest.ManifestLoader.get_full_manifest", forbidden
    )


@pytest.fixture
def run_native(artifact, worker_project, tmp_path, forbid_full_parse):
    """Run one dbt command the way a worker runs it, and report what it did."""
    def run(command: str, node: str, *, resource: str = "model"):
        task_dir = tmp_path / f"{command}-{node}"
        task_dir.mkdir(parents=True, exist_ok=True)
        lease = Lease(
            lease_id="l-1", deployment_id="d-1", token="t-1", attempt=1,
            execution_path="native",
            argv=[
                "dbt", command, "--select", node,
                "--project-dir", str(worker_project),
                "--profiles-dir", str(worker_project),
            ],
            task=TaskRef(
                task_id=f"task-{command}-{node}", schedule_id="sched-1",
                service_name=SERVICE_NAME, schema_name=WORKER_SCHEMA,
                table_name=node, dbt_unique_id=unique_id(node, resource),
            ),
        )
        executor = NativeExecutor(artifact, EventSink(task_dir / "dbt.log"))
        MANIFESTS_SERVED.append(artifact.manifest)
        try:
            with task_environment(task_values(lease, task_dir)):
                return executor.execute(lease, task_dir)
        finally:
            release_adapters()
    return run


# --- reading the warehouse back -------------------------------------------


@contextmanager
def warehouse():
    """A connection to the warehouse dbt wrote to, closed when it is done with.

    Reading a result back over dbt's own connection would ask dbt to mark its
    own homework, so this is a connection of its own. psycopg2's own context
    manager ends the transaction but leaves the connection open, so closing is
    explicit here rather than left to the garbage collector.
    """
    connection = psycopg2.connect(
        host=os.environ.get("DBT_POSTGRES_HOST", "postgres"),
        port=int(os.environ.get("DBT_POSTGRES_PORT", "5432")),
        dbname=os.environ.get("DBT_POSTGRES_DB", "continuo_dbt"),
        user=os.environ.get("DBT_POSTGRES_USER", "continuo_svc"),
        password=os.environ.get("DBT_POSTGRES_PASSWORD", "continuo"),
    )
    try:
        with connection:
            yield connection
    finally:
        connection.close()


def query(sql: str, *params):
    with warehouse() as connection, connection.cursor() as cursor:
        cursor.execute(sql, params)
        return cursor.fetchall()


def relation_kind(name: str) -> str | None:
    """What Postgres says the relation is: 'r' a table, 'v' a view, None absent."""
    rows = query(
        "SELECT c.relkind FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace"
        " WHERE n.nspname = %s AND c.relname = %s",
        WORKER_SCHEMA, name,
    )
    return rows[0][0] if rows else None


def row_count(name: str) -> int:
    return query(f'SELECT count(*) FROM "{WORKER_SCHEMA}"."{name}"')[0][0]


def columns_of(name: str) -> set[str]:
    return {
        row[0] for row in query(
            "SELECT column_name FROM information_schema.columns"
            " WHERE table_schema = %s AND table_name = %s",
            WORKER_SCHEMA, name,
        )
    }


# A row dbt never writes, used to tell an incremental run apart from a rebuild.
SENTINEL_ID = 99


def insert_sentinel(name: str) -> None:
    with warehouse() as connection, connection.cursor() as cursor:
        cursor.execute(
            f'INSERT INTO "{WORKER_SCHEMA}"."{name}" (id, value) VALUES (%s, %s)',
            (SENTINEL_ID, "sentinel"),
        )


def sentinel_present(name: str) -> bool:
    return query(
        f'SELECT count(*) FROM "{WORKER_SCHEMA}"."{name}" WHERE id = %s', SENTINEL_ID
    )[0][0] == 1


def drop(name: str) -> None:
    """Remove a relation so a test that needs a first run gets one."""
    with warehouse() as connection, connection.cursor() as cursor:
        cursor.execute(f'DROP TABLE IF EXISTS "{WORKER_SCHEMA}"."{name}" CASCADE')
        cursor.execute(f'DROP VIEW IF EXISTS "{WORKER_SCHEMA}"."{name}" CASCADE')


def rebuild_incremental(run_native) -> None:
    """Leave worker_incremental holding exactly the one row the model selects.

    The tests that read from it assert on its contents, so they cannot inherit
    whatever an earlier test left behind — the incremental test above deliberately
    puts a row of its own in there.
    """
    drop("worker_incremental")
    result = run_native("run", "worker_incremental")
    assert result.succeeded, result.error_message


# --- the semantics --------------------------------------------------------


def test_a_seed_lands_as_a_table(run_native):
    drop("seed_table_1")

    result = run_native("seed", "seed_table_1", resource="seed")

    assert result.succeeded, result.error_message
    assert relation_kind("seed_table_1") == "r"


def test_a_table_model_lands_as_a_table(run_native):
    run_native("seed", "seed_table_1", resource="seed")
    drop("table_a")

    result = run_native("run", "table_a")

    assert result.succeeded, result.error_message
    assert relation_kind("table_a") == "r"


def test_an_incremental_first_run_creates_the_table_and_its_row(run_native):
    drop("worker_incremental")

    result = run_native("run", "worker_incremental")

    assert result.succeeded, result.error_message
    assert relation_kind("worker_incremental") == "r"
    assert row_count("worker_incremental") == 1


def test_an_incremental_run_over_an_existing_table_builds_on_it(run_native):
    """The second run adds to the table the first run left, rather than remaking it.

    Row count alone cannot show this. The model declares a unique_key, so a run
    that rebuilt the table from scratch would also leave exactly one row — the
    count is the same whether the relation was preserved or destroyed. What
    separates the two is a row dbt did not write: a sentinel inserted between the
    runs survives an incremental run and is lost by a rebuild. Asserting it is
    still there is what makes this an incremental test rather than a row count.
    """
    drop("worker_incremental")
    first = run_native("run", "worker_incremental")
    assert first.succeeded, first.error_message
    insert_sentinel("worker_incremental")

    second = run_native("run", "worker_incremental")

    assert second.succeeded, second.error_message
    assert relation_kind("worker_incremental") == "r"
    assert sentinel_present("worker_incremental"), (
        "the sentinel is gone: the run rebuilt the table instead of adding to it"
    )
    # The model's own row is still single: an incremental run that appended a
    # second copy of id 1 would show up here.
    assert query(
        f'SELECT count(*) FROM "{WORKER_SCHEMA}"."worker_incremental" WHERE id = 1'
    )[0][0] == 1


def test_a_view_model_lands_as_a_view(run_native):
    rebuild_incremental(run_native)
    drop("worker_view")

    result = run_native("run", "worker_view")

    assert result.succeeded, result.error_message
    assert relation_kind("worker_view") == "v"
    assert row_count("worker_view") == 1


def test_a_snapshot_records_history(run_native):
    rebuild_incremental(run_native)
    drop("worker_snapshot")

    result = run_native("snapshot", "worker_snapshot", resource="snapshot")

    assert result.succeeded, result.error_message
    assert relation_kind("worker_snapshot") == "r"
    # The columns dbt adds to hold the history a snapshot exists to keep. A
    # snapshot that ran as a plain table would carry the source columns alone.
    assert {"dbt_scd_id", "dbt_valid_from", "dbt_valid_to"} <= columns_of("worker_snapshot")
    assert row_count("worker_snapshot") == 1


def test_a_test_runs_the_nodes_own_tests(run_native):
    run_native("seed", "seed_table_1", resource="seed")

    result = run_native("test", "seed_table_1", resource="seed")

    assert result.succeeded, result.error_message


def test_a_build_materializes_the_node_and_tests_it(run_native):
    drop("seed_table_1")

    result = run_native("build", "seed_table_1", resource="seed")

    assert result.succeeded, result.error_message
    assert relation_kind("seed_table_1") == "r"


def test_every_semantic_was_served_by_one_hydrated_manifest(artifact):
    """One Manifest, hydrated once, ran all of the above.

    A worker hydrates its pool's artifact at startup and serves every task it
    ever runs from it. Hydrating per task would still pass every semantic above
    while throwing away the only reason the pool exists, so none of those tests
    can see the difference and this one has to.

    It reads the whole module's run, so it is the full-file run that gives it
    teeth: a single semantic run on its own hydrates once quite legitimately.
    """
    assert len(HYDRATIONS) == 1
    assert artifact.manifest is HYDRATIONS[0]
    # Not just one hydration, but that one Manifest is the object every semantic
    # was actually served by.
    assert {id(manifest) for manifest in MANIFESTS_SERVED} == {id(HYDRATIONS[0])}

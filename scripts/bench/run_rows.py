"""SQL that reads one run's rows for the per-hand-off breakdown, as one JSON array.

  run_rows.py deployments RUN_ID   execution-controller's production deployments of the run
  run_rows.py tasks RUN_ID         state's tasks of the run

Each prints one statement for `psql -At`. The run id must be a UUID: it is the
only value placed into the statement.
"""
from __future__ import annotations

import logging
import sys
import uuid

log = logging.getLogger(__name__)

QUERIES = {
    # One row per deployment: the table it runs, when it was queued, when its
    # Job was created and when its result was recorded.
    "deployments": (
        "SELECT coalesce(json_agg(json_build_object("
        "'table', job_params->>'table_name', 'queued', created_at, 'created', deployed_at, 'recorded', outcome_at)"
        " ORDER BY created_at), '[]') FROM deployments"
        " WHERE schedule_id = '{run_id}' AND mode = 'production'"),
    # One row per task: its table and when state first dispatched it.
    "tasks": (
        "SELECT coalesce(json_agg(json_build_object('table', table_name, 'dispatched', dispatched_at)), '[]')"
        " FROM tasks WHERE run_id = '{run_id}'"),
}


def query(kind: str, run_id: str) -> str:
    """The statement for KIND; KeyError for an unknown kind, ValueError for a run id that is not a UUID."""
    return QUERIES[kind].format(run_id=uuid.UUID(run_id))


def main(argv: list) -> int:
    logging.basicConfig(stream=sys.stderr, level=logging.INFO, format="%(levelname)s %(message)s")
    if len(argv) != 2:
        log.error("usage: run_rows.py deployments|tasks RUN_ID")
        return 2
    try:
        print(query(argv[0], argv[1]))
    except KeyError:
        log.error("unknown kind %r (want deployments or tasks)", argv[0])
        return 2
    except ValueError:
        log.error("run id %r is not a UUID", argv[1])
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))

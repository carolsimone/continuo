"""python-api e2e node for the upsert write path: makes no network call and
returns the rows of the phase that the Secret's UPSERT_PHASE names.

Phase 1 returns ids 1 to 3. Phase 2 returns id 2 with a new label, id 3
unchanged and the new id 4. Phase 3 returns no row, with the declared column
types. Any other value raises, so a run whose Secret is missing the key or
holds another value fails instead of writing the wrong rows.

The script logs through the logging module: the runtime's result block owns
stdout.
"""
import logging
import os

import pyarrow as pa

logger = logging.getLogger("py_upsert")

_PHASES = {
    "1": [(1, "a"), (2, "b"), (3, "c")],
    "2": [(2, "B"), (3, "c"), (4, "d")],
    "3": [],
}


def run(ctx):
    phase = os.environ.get("UPSERT_PHASE", "")
    if phase not in _PHASES:
        raise RuntimeError(f"UPSERT_PHASE must be one of {sorted(_PHASES)}, got {phase!r}")
    rows = _PHASES[phase]
    logger.info("upsert phase %s returns %d rows", phase, len(rows))
    return pa.table(
        {
            "id": pa.array([r[0] for r in rows], type=pa.int32()),
            "label": pa.array([r[1] for r in rows], type=pa.string()),
        }
    )

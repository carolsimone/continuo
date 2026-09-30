"""python-api e2e node: makes no network call; reports whether the Secret's
PROBE_API_KEY reached the pod as an env var."""
import os

import pyarrow as pa


def run(ctx):
    present = 1 if os.environ.get("PROBE_API_KEY") else 0
    return pa.table(
        {
            "id": pa.array([1], type=pa.int32()),
            "key_present": pa.array([present], type=pa.int32()),
        }
    )

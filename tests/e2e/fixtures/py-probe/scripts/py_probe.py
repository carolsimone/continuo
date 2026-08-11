"""Conforming e2e node: returns exactly the declared columns and types.

pyarrow is a dependency of the runtime base image, so this fixture installs
nothing of its own.
"""
import pyarrow as pa


def run(ctx):
    ctx.read("tables")
    return pa.table(
        {
            "id": pa.array([1, 2, 3], type=pa.int32()),
            "label": pa.array(["a", "b", "c"], type=pa.string()),
        }
    )

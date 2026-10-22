"""Timeout-fixture node: returns exactly the declared column.

The release that carries this node is validated and promoted but never run, so
the script only has to be a conforming node body.
"""
import pyarrow as pa


def run(ctx):
    ctx.read("tables")
    return pa.table({"id": pa.array([1], type=pa.int32())})

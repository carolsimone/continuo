"""Non-conforming e2e node: returns a string where INTEGER is declared.

The harness's strict Arrow cast refuses this and raises ConformError, which is
the deterministic error class the run path's structured result block must carry
to the control plane.
"""
import pyarrow as pa


def run(ctx):
    ctx.read("tables")
    return pa.table({"id": pa.array(["not-an-integer"], type=pa.string())})

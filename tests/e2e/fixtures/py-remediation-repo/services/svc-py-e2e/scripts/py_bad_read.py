"""Produces the single declared column of e2e_schema.py_bad_read.

Blue/green validation never executes this script: it bind-checks the
contract's declared reads and then builds the empty typed table from
output_columns. The file still has to exist and stay readable, because the
packaging tool folds its sha256 into the node's content hash — which is why a
fix to this node is always made in the contract yaml beside it, never here.
"""
import pyarrow as pa


def run(ctx):
    ctx.read("missing")
    return pa.table({"id": pa.array([1], type=pa.int32())})

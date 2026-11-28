"""Produces the single declared column of e2e_schema.py_bad_read.

The release check never executes this script: it bind-checks the read
queries of the node's ODCS contract (contracts/<table>.odcs.yaml) and then
builds the empty typed table from the columns the contract's schema declares.
The file still has to exist and stay readable, because the packaging tool folds
its sha256 into the node's content hash — which is why a fix to this node is
always made in its ODCS contract file, never here.
"""
import pyarrow as pa


def run(ctx):
    ctx.read("missing")
    return pa.table({"id": pa.array([1], type=pa.int32())})

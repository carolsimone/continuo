#!/usr/bin/env python3
"""A stand-in for a team's dbt wrapper: a real child process, not a mock.

A wrapper is a program the worker starts and does not otherwise understand, so
the only honest way to test the wrapper path is to start a real one and ask it
what it received. This script records its own invocation — argv, working
directory, and environment — and then behaves however the test told it to.

It is driven entirely through its environment, because inheriting the parent's
environment is the mechanism under test: a test that can steer this script
through an inherited variable, while the pool credential does not arrive, has
proved the worker scrubs what it means to scrub and nothing else.

  FAKE_WRAPPER_RECORD   File to write the recorded invocation JSON to.
  FAKE_WRAPPER_CODES    Comma-separated dbt event codes to emit on stdout, in
                        order, as dbt's own JSON log format.
  FAKE_WRAPPER_STDERR   A line to write to stderr.
  FAKE_WRAPPER_HANG     When set, sleep forever after emitting, so a test can
                        prove the worker terminates the process group.
  FAKE_WRAPPER_EXIT     The exit status to return. Defaults to 0.
"""
import json
import os
import sys
import time

# Mirrors the envelope dbt writes under DBT_LOG_FORMAT=json: every event carries
# an info object naming the event's code.
def emit(code: str) -> None:
    print(
        json.dumps({"info": {"code": code, "level": "info", "msg": f"event {code}"}}),
        flush=True,
    )


def main() -> int:
    record = os.environ.get("FAKE_WRAPPER_RECORD")
    if record:
        with open(record, "w", encoding="utf-8") as handle:
            json.dump(
                {"argv": sys.argv, "cwd": os.getcwd(), "env": dict(os.environ)},
                handle,
            )

    for code in filter(None, os.environ.get("FAKE_WRAPPER_CODES", "").split(",")):
        emit(code)

    stderr_line = os.environ.get("FAKE_WRAPPER_STDERR")
    if stderr_line:
        print(stderr_line, file=sys.stderr, flush=True)

    if os.environ.get("FAKE_WRAPPER_HANG"):
        while True:
            time.sleep(0.01)

    return int(os.environ.get("FAKE_WRAPPER_EXIT", "0"))


if __name__ == "__main__":
    sys.exit(main())

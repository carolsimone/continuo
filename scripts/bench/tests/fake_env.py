"""Fake continuo, kubectl and docker executables for running the benchmark's shell scripts in tests.

The fakes keep their state in FAKE_DIR:
  state, current, n  the schedule's latest run (continuo)
  calls              one line per `schedule cancel` (continuo) and per `docker network ...` call
  exec-calls.jsonl   argv and stdin of every `kubectl exec` and every announce-topology `docker exec`; exec-output is
                     what they print, announce-output (when present) what an announce-topology call prints instead,
                     and FAKE_ANNOUNCE_RC the announce-topology exit code
  jobs.json          what `kubectl get jobs` prints
  events             one line per `schedule cancel` ("cancel <reason>") and per `kubectl apply` or `kubectl
                     delete` ("kubectl <arguments>"), in call order; `delete jobs` fails once
                     FAKE_DELETE_JOBS_FAIL_AFTER calls of it have succeeded, and every `kubectl delete` takes
                     FAKE_DELETE_SLEEP seconds. Like the real CLI, a failing call prints an error envelope on
                     stdout and exits 5: `schedule status` (one line in status-calls per call, failed or not)
                     fails when FAKE_STATUS_FAIL is set or the file
                     FAKE_STATUS_FAIL_WHEN_EXISTS names exists, and `schedule cancel` when FAKE_CANCEL_FAIL is
                     set; with FAKE_CANCEL_NOOP a cancel is accepted and the run stays live
  deployments.json, tasks.json  what psql prints in a `docker exec` for a query of deployments or tasks
                     (`[]` when absent); docker-calls.jsonl records the argv of every such psql call, and
                     `docker exec <container> printenv VAR` prints fake_<var>
  logs/<svc>.log     the service logs that `docker exec <svc> ... /tmp/<svc>.log` reads
  on-trigger/<svc>.log  lines a trigger appends to logs/<svc>.log, standing for what services log during the run
Runs never finish on their own unless FAKE_RUN_RESULT names the status a trigger ends with. Run ids are
run-<n>, or UUIDs when FAKE_UUID_RUNS is set.
"""
import json
import os
import time
from pathlib import Path

FAKE_CONTINUO = r'''#!/usr/bin/env python3
import json, os, sys
from pathlib import Path
d = Path(os.environ["FAKE_DIR"])
def read(name, default=""):
    p = d / name
    return p.read_text().strip() if p.exists() else default
def unavailable():
    print(json.dumps({"error": {"code": "unavailable", "message": "connection refused", "retryable": True}}))
    sys.exit(5)
cmd = sys.argv[1:3]
if cmd == ["schedule", "trigger"]:
    if read("state") == "running":
        print(json.dumps({"error": {"code": "conflict"}}))
        sys.exit(4)
    n = int(read("n", "0")) + 1
    (d / "n").write_text(str(n))
    run_id = f"00000000-0000-4000-8000-{n:012d}" if os.environ.get("FAKE_UUID_RUNS") else f"run-{n}"
    (d / "current").write_text(run_id)
    (d / "state").write_text(os.environ.get("FAKE_RUN_RESULT") or "running")
    for extra in sorted((d / "on-trigger").glob("*.log")):
        with open(d / "logs" / extra.name, "a") as log:
            log.write(extra.read_text())
    print(json.dumps({"run_id": run_id, "schedule_name": sys.argv[3], "triggered_at": "2026-10-03T09:00:00Z"}))
elif cmd == ["schedule", "status"]:
    with open(d / "status-calls", "a") as calls:
        calls.write("status\n")
    marker = os.environ.get("FAKE_STATUS_FAIL_WHEN_EXISTS")
    if os.environ.get("FAKE_STATUS_FAIL") or (marker and os.path.exists(marker)):
        unavailable()
    state = read("state")
    current = read("current")
    run = {"run_id": current, "schedule_name": sys.argv[3], "status": state} if current else None
    print(json.dumps({"run": run, "tasks": []}))
elif cmd == ["schedule", "cancel"]:
    if os.environ.get("FAKE_CANCEL_FAIL"):
        unavailable()
    with open(d / "events", "a") as events:
        events.write(f"cancel {sys.argv[4]}\n")
    if not os.environ.get("FAKE_CANCEL_NOOP"):
        (d / "state").write_text("cancelled")
    with open(d / "calls", "a") as calls:
        calls.write(f"cancel {sys.argv[4]}\n")
    print(json.dumps({"run_id": read("current"), "cancelled_at": "2026-10-03T09:00:05Z"}))
else:
    sys.exit(f"unexpected continuo call: {sys.argv[1:]}")
'''

FAKE_KUBECTL = r'''#!/usr/bin/env python3
import json, os, sys, time
args = " ".join(sys.argv[1:])
if " exec " in f" {args} " and os.environ.get("FAKE_EXEC_FAIL"):
    sys.exit("Unable to connect to the server: net/http: TLS handshake timeout")
if " exec " in f" {args} ":
    fake = os.environ["FAKE_DIR"]
    announce = "announce-topology" in sys.argv[1:]
    with open(os.path.join(fake, "exec-calls.jsonl"), "a") as calls:
        calls.write(json.dumps({"argv": sys.argv[1:], "stdin": sys.stdin.read()}) + "\n")
    output = os.path.join(fake, "announce-output" if announce else "exec-output")
    if announce and not os.path.exists(output):
        output = os.path.join(fake, "exec-output")
    if os.path.exists(output):
        sys.stdout.write(open(output).read())
    sys.exit(int(os.environ.get("FAKE_ANNOUNCE_RC", "0")) if announce else 0)
if "get deploy" in args:
    env = [{"name": name, "value": "fake-" + name.lower()}
           for name in ("POSTGRES_USER", "POSTGRES_DB", "POSTGRES_PASSWORD", "REDIS_PASSWORD")]
    print(json.dumps({"spec": {"template": {"spec": {"containers": [{"name": "svc", "env": env}]}}}}))
    sys.exit(0)
if " apply " in f" {args} " or " delete " in f" {args} ":
    fake = os.environ["FAKE_DIR"]
    with open(os.path.join(fake, "events"), "a") as events:
        events.write("kubectl " + args + "\n")
    if " delete " in f" {args} " and os.environ.get("FAKE_DELETE_SLEEP"):
        time.sleep(float(os.environ["FAKE_DELETE_SLEEP"]))
    if "delete jobs" in args and os.environ.get("FAKE_DELETE_JOBS_FAIL_AFTER") is not None:
        count = os.path.join(fake, "delete-jobs-count")
        done = int(open(count).read()) if os.path.exists(count) else 0
        open(count, "w").write(str(done + 1))
        if done >= int(os.environ["FAKE_DELETE_JOBS_FAIL_AFTER"]):
            sys.exit("Error from server (InternalError): an error on the server has prevented the request")
    sys.exit(0)
if "get jobs" in args:
    sys.stdout.write(open(os.path.join(os.environ["FAKE_DIR"], "jobs.json")).read())
    sys.exit(0)
sys.exit(f"unexpected kubectl call: {args}")
'''

FAKE_DOCKER = r'''#!/usr/bin/env python3
import json, os, subprocess, sys
fake = os.environ["FAKE_DIR"]
logs = os.path.join(fake, "logs") + "/"
args = sys.argv[1:]
if args[0] == "inspect" and "--format" in args:
    print("proj")
elif args[0] == "inspect":
    print(json.dumps([{"NetworkSettings": {"Networks": {"proj_default": {"Aliases": ["proj-postgres-1", "postgres"]}}}}]))
elif args[0] == "network":
    with open(os.path.join(fake, "calls"), "a") as calls:
        calls.write("docker " + " ".join(args) + "\n")
elif args[0] == "ps":
    print("cid")
elif args[0] == "stats":
    if os.environ.get("FAKE_DOCKER_STATS_FAIL"):
        sys.exit("Error response from daemon: fake stats failure")
    print("state\t1.00%\t10MiB / 1GiB")
elif args[0] == "exec":
    rest = [a for a in args[1:] if a not in ("-i", "-e", "REDISCLI_AUTH")]
    if any(part.endswith("announce-topology") for part in rest):
        with open(os.path.join(fake, "exec-calls.jsonl"), "a") as calls:
            calls.write(json.dumps({"argv": args, "stdin": sys.stdin.read()}) + "\n")
        for name in ("announce-output", "exec-output"):
            output = os.path.join(fake, name)
            if os.path.exists(output):
                sys.stdout.write(open(output).read())
                break
        sys.exit(int(os.environ.get("FAKE_ANNOUNCE_RC", "0")))
    if rest[1:2] == ["printenv"]:
        print("fake_" + rest[2].lower())
        sys.exit(0)
    if "psql" in rest:
        with open(os.path.join(fake, "docker-calls.jsonl"), "a") as calls:
            calls.write(json.dumps({"argv": args}) + "\n")
        sql = rest[rest.index("-c") + 1]
        for name, marker in (("deployments.json", "FROM deployments"), ("tasks.json", "FROM tasks")):
            path = os.path.join(fake, name)
            if marker in sql and os.path.exists(path):
                sys.stdout.write(open(path).read())
                sys.exit(0)
        print("[]")
        sys.exit(0)
    command = [part.replace("/tmp/", logs) for part in rest[1:]]
    if "redis-cli" in command:
        sys.stdin.read()
        if "SCAN" in command:
            print("0")
    else:
        sys.exit(subprocess.call(command))
else:
    sys.exit(f"unexpected docker call: {args}")
'''


def job(run_id: str, table: str = "n00_0000") -> dict:
    return {
        "metadata": {"name": f"bench-bench-{table.replace('_', '-')}-{run_id}", "creationTimestamp": "2026-10-03T09:00:01Z",
                     "labels": {"run-id": run_id}},
        "spec": {"template": {"spec": {"containers": [
            {"name": "dbt-job", "env": [{"name": "TABLE_NAME", "value": table}]}]}}},
        "status": {"succeeded": 1, "completionTime": "2026-10-03T09:00:02Z"},
    }


def make(tmp_path: Path, run_ids: list) -> dict:
    """Installs the fakes and returns the environment that puts them first on PATH."""
    fakebin = tmp_path / "bin"
    fakebin.mkdir()
    state = tmp_path / "fake"
    (state / "logs").mkdir(parents=True)
    (state / "on-trigger").mkdir()
    for name, body in (("continuo", FAKE_CONTINUO), ("kubectl", FAKE_KUBECTL), ("docker", FAKE_DOCKER)):
        path = fakebin / name
        path.write_text(body)
        path.chmod(0o755)
    (state / "jobs.json").write_text(json.dumps({"items": [job(run_id) for run_id in run_ids]}))
    return dict(os.environ, PATH=f"{fakebin}:{os.environ['PATH']}", FAKE_DIR=str(state),
                CONTINUO_CLI=str(fakebin / "continuo"), BENCH_TARGET="compose", REDIS_PASSWORD="x")


def write_payload(tmp_path: Path) -> Path:
    payload = tmp_path / "payload.json"
    payload.write_text(json.dumps({"topology": [
        {"unique_id": "bench.n00_0000", "table_name": "n00_0000", "upstream_unique_ids": []}]}))
    return payload


def live_processes_mentioning(text: str) -> list:
    found = []
    for proc in Path("/proc").iterdir():
        if not proc.name.isdigit():
            continue
        try:
            cmdline = (proc / "cmdline").read_bytes().replace(b"\0", b" ").decode()
        except OSError:
            continue
        if text in cmdline:
            found.append(cmdline)
    return found


def wait_until_gone(text: str, seconds: float = 5.0) -> list:
    deadline = time.time() + seconds
    while live_processes_mentioning(text) and time.time() < deadline:
        time.sleep(0.2)
    return live_processes_mentioning(text)

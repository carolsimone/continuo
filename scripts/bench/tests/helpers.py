from datetime import datetime, timedelta, timezone

T0 = datetime(2026, 10, 3, 9, 0, 0, tzinfo=timezone.utc)


def at(seconds):
    return T0 + timedelta(seconds=seconds)


def iso(seconds):
    return at(seconds).strftime("%Y-%m-%dT%H:%M:%SZ")


def job(run_id, table, created, finished=None, failed=False, name=None):
    status = {}
    if failed:
        status["failed"] = 1
        if finished is not None:
            status["conditions"] = [{"type": "Failed", "status": "True", "lastTransitionTime": iso(finished)}]
    elif finished is not None:
        status["succeeded"] = 1
        status["completionTime"] = iso(finished)
    return {
        "metadata": {"name": name or f"bench-bench-{table}", "creationTimestamp": iso(created),
                     "labels": {"schedule-id": run_id}},
        "spec": {"template": {"spec": {"containers": [
            {"name": "dbt-job", "env": [{"name": "TABLE_NAME", "value": table}]}]}}},
        "status": status,
    }

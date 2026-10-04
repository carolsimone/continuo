import base64

import pytest

import k8s_json


def deploy(env=None, env_from=None):
    return {"spec": {"template": {"spec": {"containers": [
        {"name": "svc", "env": env or [], "envFrom": env_from or []}]}}}}


def test_literal_value():
    d = deploy(env=[{"name": "POSTGRES_DB", "value": "continuo_release"}])
    assert k8s_json.env_sources(d, "POSTGRES_DB") == [("value", "continuo_release")]


def test_secret_key_ref():
    d = deploy(env=[{"name": "POSTGRES_PASSWORD",
                     "valueFrom": {"secretKeyRef": {"name": "creds", "key": "postgres-password"}}}])
    assert k8s_json.env_sources(d, "POSTGRES_PASSWORD") == [("secret", "creds", "postgres-password")]


def test_config_map_key_ref():
    d = deploy(env=[{"name": "POSTGRES_USER", "valueFrom": {"configMapKeyRef": {"name": "cm", "key": "user"}}}])
    assert k8s_json.env_sources(d, "POSTGRES_USER") == [("configmap", "cm", "user")]


def test_env_from_sources_are_candidates_when_not_set_explicitly():
    d = deploy(env_from=[{"configMapRef": {"name": "cm1"}}, {"secretRef": {"name": "s1"}}])
    assert k8s_json.env_sources(d, "POSTGRES_USER") == [("envfrom-configmap", "cm1"), ("envfrom-secret", "s1")]


def test_data_value_decodes_secrets_and_rejects_missing_keys():
    secret = {"data": {"pw": base64.b64encode(b"s3cret").decode()}}
    assert k8s_json.data_value(secret, "pw", encoded=True) == "s3cret"
    assert k8s_json.data_value({"data": {"u": "continuo"}}, "u", encoded=False) == "continuo"
    with pytest.raises(KeyError):
        k8s_json.data_value(secret, "missing", encoded=True)


def test_slim_jobs_keeps_what_the_metrics_read_and_drops_every_other_env():
    import metrics
    from helpers import at, job
    full = job("r1", "n00_0000", 0, 5)
    full["spec"]["template"]["spec"]["initContainers"] = [
        {"name": "hydrate-parse-cache", "env": [{"name": "AWS_SECRET_ACCESS_KEY", "value": "s3cret"}]}]
    full["spec"]["template"]["spec"]["containers"][0]["env"].append({"name": "AWS_SECRET_ACCESS_KEY", "value": "s3cret"})
    slim = k8s_json.slim_jobs({"items": [full]})
    assert "s3cret" not in str(slim)
    assert metrics.attempts_for_run(slim, "r1") == metrics.attempts_for_run({"items": [full]}, "r1")
    assert at(5) == metrics.attempts_for_run(slim, "r1")[0].finished

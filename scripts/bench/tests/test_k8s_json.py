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


def test_legacy_jobs_are_finished_dbt_jobs_without_ttl():
    def job(name, app="dbt-job", ttl=None, status=None):
        spec = {} if ttl is None else {"ttlSecondsAfterFinished": ttl}
        return {"metadata": {"name": name, "labels": {"app": app}}, "spec": spec, "status": status or {}}
    doc = {"items": [
        job("old-ok", status={"succeeded": 1}),
        job("old-failed", status={"failed": 1}),
        job("new", ttl=86400, status={"succeeded": 1}),
        job("running", status={"active": 1}),
        job("reaper", app="stream-reaper", status={"succeeded": 1}),
    ]}
    assert k8s_json.legacy_jobs(doc) == ["old-failed", "old-ok"]


def test_data_value_decodes_secrets_and_rejects_missing_keys():
    secret = {"data": {"pw": base64.b64encode(b"s3cret").decode()}}
    assert k8s_json.data_value(secret, "pw", encoded=True) == "s3cret"
    assert k8s_json.data_value({"data": {"u": "continuo"}}, "u", encoded=False) == "continuo"
    with pytest.raises(KeyError):
        k8s_json.data_value(secret, "missing", encoded=True)

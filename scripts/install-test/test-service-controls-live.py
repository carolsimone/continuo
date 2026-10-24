#!/usr/bin/env python3
"""Optional Helm/Kubernetes E2E for the service templates in an isolated namespace.

Requires helm, kubectl, a current test cluster, and an image serving a TCP echo
on 50060 with Python available for the chart's process probe. Pass its image
reference as the only argument. No full Continuo dependency stack is started.
"""
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import uuid

ROOT = Path(__file__).resolve().parents[2]
NAMESPACE = "chart-controls-" + uuid.uuid4().hex[:8]


def run(*args):
    result = subprocess.run(args, check=True, text=True, capture_output=True)
    return result.stdout


def main():
    if len(sys.argv) != 2:
        raise SystemExit("usage: test-service-controls-live.py <tcp-echo-image>")
    with tempfile.TemporaryDirectory(prefix="service-controls-") as directory:
        chart = Path(directory)
        source = ROOT / "deploy/continuo"
        for name in ["Chart.yaml", "values.yaml", "values.schema.json"]:
            shutil.copy(source / name, chart / name)
        shutil.copytree(source / "files", chart / "files")
        (chart / "templates").mkdir()
        # Keep the production service templates and their shared ConfigMap.
        # Datastore provisioning and controllers are outside this regression.
        for name in ["_helpers.tpl", "deployment.yaml", "service.yaml", "rbac.yaml", "configmap.yaml"]:
            shutil.copy(source / "templates" / name, chart / "templates" / name)
        run("kubectl", "create", "namespace", NAMESPACE)
        try:
            service = {"name": "control-worker", "enabled": True, "replicas": 0,
                       "image": sys.argv[1], "grpcPort": 50060,
                       "rbac": {"enabled": True, "rules": []}}

            def upgrade():
                run("helm", "upgrade", "--install", "service-controls", str(chart),
                    "--namespace", NAMESPACE, "--set-json", "services=" + json.dumps([service]))

            upgrade()
            deployment = json.loads(run("kubectl", "get", "deployment/control-worker",
                                        "-n", NAMESPACE, "-o", "json"))
            assert deployment["spec"]["replicas"] == 0
            pods = json.loads(run("kubectl", "get", "pods", "-n", NAMESPACE, "-o", "json"))
            assert not pods["items"]
            print("replicas=0: Kubernetes stores zero replicas and creates no Pods", flush=True)

            service["replicas"] = 1
            upgrade()
            run("kubectl", "rollout", "status", "deployment/control-worker", "-n", NAMESPACE, "--timeout=90s")
            endpoint = json.loads(run("kubectl", "get", "service/control-worker",
                                     "-n", NAMESPACE, "-o", "json"))
            assert [(p["name"], p["port"]) for p in endpoint["spec"]["ports"]] == [("grpc", 50060)]
            code = ("import socket; s=socket.create_connection(('control-worker',50060),5); "
                    "s.sendall(b'chart-service-ok'); assert s.recv(1024)==b'chart-service-ok'; s.close()")
            run("kubectl", "exec", "deployment/control-worker", "-n", NAMESPACE, "--", "python", "-c", code)
            print("grpcPort only: Service routes TCP traffic to the ready workload", flush=True)

            service["enabled"] = False
            upgrade()
            for kind in ["deployment", "service", "serviceaccount", "role", "rolebinding"]:
                result = run("kubectl", "get", kind, "control-worker", "-n", NAMESPACE,
                             "--ignore-not-found", "-o", "name")
                assert not result.strip(), (kind, result)
            print("enabled=false: Helm upgrade removes Deployment, Service and RBAC", flush=True)
        finally:
            run("kubectl", "delete", "namespace", NAMESPACE, "--wait=false")


if __name__ == "__main__":
    main()

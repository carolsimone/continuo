#!/usr/bin/env python3
"""Exercise the public services values against real Helm renders."""
import json
from pathlib import Path
import subprocess
import unittest

import yaml

ROOT = Path(__file__).resolve().parents[2]


def render(service):
    result = subprocess.run(
        ["helm", "template", "service-controls", str(ROOT / "deploy/continuo"),
         "--kube-version", "1.29.0", "--set-json", "services=" + json.dumps([service])],
        check=True, capture_output=True, text=True,
    )
    return [doc for doc in yaml.safe_load_all(result.stdout) if doc]


def owned_resources(documents):
    return {doc["kind"]: doc for doc in documents
            if doc.get("metadata", {}).get("name") == "control-worker"}


class ServiceControlsTest(unittest.TestCase):
    def test_disabled_service_has_no_workload_service_or_rbac(self):
        resources = owned_resources(render({
            "name": "control-worker", "enabled": False, "httpPort": 8080,
            "grpcPort": 50060, "rbac": {"enabled": True, "rules": []},
        }))
        self.assertEqual(resources, {})

    def test_replica_count_preserves_zero_and_defaults_only_when_omitted(self):
        for replicas, expected in [(None, 1), (0, 0), (3, 3)]:
            with self.subTest(replicas=replicas):
                service = {"name": "control-worker", "enabled": True}
                if replicas is not None:
                    service["replicas"] = replicas
                resources = owned_resources(render(service))
                self.assertEqual(resources["Deployment"]["spec"]["replicas"], expected)

    def test_service_exposes_each_configured_application_port(self):
        for ports, expected in [
            ({"httpPort": 8080}, {"http": 8080}),
            ({"grpcPort": 50060}, {"grpc": 50060}),
            ({"httpPort": 8080, "grpcPort": 50060}, {"http": 8080, "grpc": 50060}),
            ({}, {}),
        ]:
            with self.subTest(ports=ports):
                resources = owned_resources(render({
                    "name": "control-worker", "enabled": True, **ports,
                }))
                self.assertIn("Deployment", resources)
                if not expected:
                    self.assertNotIn("Service", resources)
                    continue
                self.assertIn("Service", resources)
                actual = resources["Service"]["spec"]["ports"]
                self.assertEqual({p["name"]: p["port"] for p in actual}, expected)
                self.assertTrue(all(p["targetPort"] == p["port"] for p in actual))

    def test_probes_use_a_configured_port_without_running_python(self):
        # The Go service images carry no Python, so an entry with an
        # application port is probed through that port by the kubelet.
        for ports, handler, port in [
            ({"httpPort": 8080}, "httpGet", 8080),
            ({"httpPort": 8080, "grpcPort": 50060}, "httpGet", 8080),
            ({"grpcPort": 50060}, "tcpSocket", 50060),
        ]:
            with self.subTest(ports=ports):
                deployment = owned_resources(render({
                    "name": "control-worker", "enabled": True, **ports,
                }))["Deployment"]
                container = next(c for c in deployment["spec"]["template"]["spec"]["containers"]
                                 if c["name"] == "control-worker")
                for probe in ["readinessProbe", "livenessProbe"]:
                    self.assertNotIn("exec", container[probe])
                    self.assertEqual(container[probe][handler]["port"], port)

    def test_enabled_service_retains_requested_rbac(self):
        resources = owned_resources(render({
            "name": "control-worker", "enabled": True,
            "rbac": {"enabled": True, "rules": [
                {"apiGroups": [""], "resources": ["pods"], "verbs": ["get"]},
            ]},
        }))
        self.assertTrue({"Deployment", "ServiceAccount", "Role", "RoleBinding"}
                        .issubset(resources))


if __name__ == "__main__":
    unittest.main()

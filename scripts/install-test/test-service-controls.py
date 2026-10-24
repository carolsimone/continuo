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
